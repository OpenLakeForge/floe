# Decision — Rewrite Floe's core in Python, scoped to the OpenLakeForge stack

## Status

**Proposed.** Accepted or rejected by the go/no-go spike in #594 (sub-issues #596–#600).
Nothing in `crates/` or `orchestrators/` changes until this record is Accepted.

## Context

Floe is now part of OpenLakeForge (OLF). Its priority is maturity on the OLF stack:
**Iceberg through the Polaris REST catalog, run by Dagster, heavy full and incremental loads, and
merge into Iceberg.** The Rust core blocks that on three fronts.

1. **Arrow version lockstep.** Each `arrow` major is a distinct Rust type, so `deltalake`,
   `iceberg`, `duckdb`, `orc-rust`, `df-interchange` and Polars must all upgrade together. The
   whole v0.8 dependency track (#523, #538, #565–#569) and the blocked DuckLake work (#387) exist
   only because of this, and it recurs with every arrow-rs major.
2. **No Iceberg merge.** iceberg-rust has no MERGE, so the Iceberg sink supports only `overwrite`
   and `append` (`crates/floe-core/src/io/write/iceberg.rs:178-180`; see
   `merge-delegated-to-sink.md`). DuckDB ≥ 1.5.3 supports `MERGE INTO` on Iceberg, but its Rust
   crate needs Arrow 58: the same wall again.
3. **Memory on heavy loads.** Accepted rows for an entity are concatenated into one DataFrame
   before the write (`crates/floe-core/src/run/entity/accepted_write.rs:75`), and unique checks
   on append load the target's historical keys into a `HashSet`
   (`crates/floe-core/src/io/unique_seed/mod.rs:19`). Peak memory grows with the entity and with
   the table's history.

Most Iceberg bugs closed so far were integration plumbing around iceberg-rust: FileIO factories,
credential chains and catalog metadata (#310, #348, #349, #352, #426, #433). PyIceberg covers
these out of the box.

Contributors are data engineers. They can read, debug and patch a Python library; few can do
that with async Rust.

## Decision

Rewrite Floe's core as a Python library, scoped to the OLF stack, **if and only if** the spike
gates below pass.

- **Polars (Python)** reads and validates. It is the same Rust engine, so the column operations
  run at the same speed.
- **PyIceberg** owns the Polaris catalog, schema evolution and plain commits.
- **DuckDB**, attached to Polaris, does anything that touches the target table at scale: MERGE,
  and unique checks against history. It spills to disk.
- Python's Arrow libraries exchange data through the Arrow C Data Interface, which doesn't depend
  on library versions. The lockstep upgrade problem goes away.

### Scope

| Keep | Drop / freeze |
|---|---|
| Iceberg via Polaris REST | Delta + Unity, DuckDB/MotherDuck sinks, Glue |
| Parquet rejected / quarantine output | CSV rejected output (later, on demand) |
| CSV, Parquet, JSON readers | XML, fixed-width, XLSX, ORC, Avro (port on demand) |
| S3 + local storage | ADLS, GCS |
| YAML contract, run report, manifest: byte-compatible | Homebrew / Scoop / native packages (#278) |
| Dagster integration | Airflow (frozen, not ported) |

### Target architecture

| Mode | Mechanism | Memory |
|---|---|---|
| Full load (`overwrite`) | One PyIceberg transaction: `delete(AlwaysTrue())`, then one `append` per chunk, then one catalog commit | Bounded by chunk size |
| Incremental `append` | Same, without the delete. The snapshot carries `floe.run_id` and a hash of the claimed files; a retry that finds its run id skips the write and only promotes state. | Bounded |
| `merge_scd1` / `merge_scd2` | Chunks staged as Parquet, then one DuckDB `MERGE INTO` (merge-on-read, positional deletes). SCD2 sends each changed row twice: once to close the current version, once to insert the new one. | Spills to disk |
| `unique` vs target history | DuckDB anti-join between staged keys and the table's key columns | Spills to disk |

Re-running an SCD1 merge with the same batch leaves the table unchanged, and an SCD2 batch with
no changes adds no versions. Only `append` needs the run-id guard.

CAS incremental state (`incremental-state-cas.md`) is ported as is. It is a port, not a risk.

## Go / no-go gates

The volumes are placeholders, to be replaced with real OLF numbers before the spikes report. The
thresholds live in #594.

| Gate | Spike | Proves |
|---|---|---|
| G1 | #597 | Large entity → Iceberg under a fixed memory limit, one snapshot |
| G2 | #597 | `kill -9` at any point, then rerun → exact row count, no duplicates |
| G3 | #598 | SCD2 merge at scale, one snapshot, same semantics as `docs/write_modes.md` |
| G4 | #599 | Validation within 1.3× of Rust wall time and peak RSS, unique + PII enabled |
| G5 | #599 | Identical run report and rejected rows on existing fixtures |
| G6 | #600 | Exact unique check against large history with bounded memory |

#596 provides the local Polaris + S3 harness the other spikes run on.

## Trade-offs

- **Lost:** a single static binary, a small Docker image, and compiler-enforced correctness.
  The mitigations are a strict type checker and pydantic config models.
- **Library installs can conflict:** in shared Airflow or Dagster environments, Floe would compete
  over `pyarrow` and Polars versions. Run Floe in its own environment or image, as the
  orchestrators already do via subprocess and Kubernetes Jobs.
- **DuckDB Iceberg writes are recent.** Partition transforms, schema evolution during MERGE and
  Polaris authentication must be proven in #598, not assumed.
- **Merge-on-read leaves positional delete files.** Compaction and snapshot expiry are table
  maintenance. They are an OLF responsibility, not Floe's, but they must have an owner before
  merge ships.

## Consequences

**If Accepted:**
- A port milestone is opened. The Python package is built module by module within the
  `AGENTS.md` size budget, with the Rust core as the reference until parity, then the Rust core
  is deleted (no long-term dual maintenance).
- This record supersedes the Iceberg part of `merge-delegated-to-sink.md` and the Rust-specific
  parts of `polars-and-arrow.md`.
- The Arrow / Polars upgrade track (#523, #538, #565–#569, #387) and non-OLF sink requests
  (#249, #250, #261) are closed.
- #545–#549, #556 and #560 carry over as design requirements for the Python core.

**If Rejected:** the failing gate and its numbers are recorded here, and the v0.8 Rust milestone
resumes.
