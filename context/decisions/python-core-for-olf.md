# Decision — Rewrite Floe's core in Python, scoped to the OpenLakeForge stack

## Status

**Accepted** on 2026-10-23, after the go/no-go spike in #594 (sub-issues #596–#600, #609). Every
gate passed; see [Outcome](#outcome).

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
| Incremental `append` | Same, without the delete. The snapshot carries `floe.run_id` and `floe.input_hash`, a fingerprint of the claimed files; a retry that finds its input fingerprint skips the write and only promotes state. | Bounded |
| `merge_scd1` / `merge_scd2` | Chunks staged as Parquet, then one DuckDB `MERGE INTO` (merge-on-read, positional deletes). SCD2 sends each changed row twice: once to close the current version, once to insert the new one. | Spills to disk |
| `unique` vs target history | DuckDB anti-join between staged keys and the table's key columns | Spills to disk |

Re-running an SCD1 merge with the same batch leaves the table unchanged, and an SCD2 batch with
no changes adds no versions. Only `append` needs the retry guard.

CAS incremental state (`incremental-state-cas.md`) is ported as is. It is a port, not a risk.

## Go / no-go gates

The volumes are placeholders, to be replaced with real OLF numbers before the spikes report. The
thresholds live in #594. The "one atomic catalog commit" and "byte metrics excluded" wording reflects the two rulings recorded under [Outcome](#outcome).

| Gate | Spike | Proves |
|---|---|---|
| G1 | #597 | Large entity → Iceberg under a fixed memory limit, one atomic catalog commit |
| G2 | #597 | `kill -9` at any point, then rerun → exact row count, no duplicates |
| G3 | #598 | SCD2 merge at scale, one atomic catalog commit, same semantics as `docs/write_modes.md` |
| G4 | #599 | Validation within 1.3× of Rust wall time and peak RSS, unique + PII enabled |
| G5 | #599 | Identical run report and rejected rows on existing fixtures (writer byte metrics excluded) |
| G6 | #600 | Exact unique check against large history with bounded memory |

#596 provides the local Polaris + S3 harness the other spikes run on.

## Outcome

Measured on a 4 vCPU / 16 GB sandbox against the #596 harness (Polaris 1.8.0, SeaweedFS 4.48),
with DuckDB 1.5.6, PyIceberg 0.12.0, pyarrow 25.0.1 and Polars 2.0.0. Rulings on #594:
- a run must land as **one atomic catalog commit**; several snapshots inside it are fine;
- writer-dependent byte metrics are excluded from report parity.

| Gate | Result | Evidence |
|---|---|---|
| G1 bounded-memory load | Pass: 17.8 GB CSV (152M rows) under a 4 GB limit (also 1.5 GB), ~0.9 GB working memory, one commit | #597, PR #606 |
| G2 crash / retry | Pass: 24/24 `kill -9` + rerun cases exact. The retry guard is load-bearing (without it, 10M duplicates). The spike reran with the same run id; the port keys the guard on the input fingerprint so a new run id is caught too (see "Retry guard") | #597, PR #606 |
| G3 merge at scale | Pass: 10M-row SCD2 into 500M rows. Unpartitioned 72–79 s, `day()` 105 s, `bucket(16)` 16–19 min | #598, #609, PRs #607, #610 |
| G4 validation performance | Pass: Python 0.30–0.44× Rust wall time at 1M–20M rows; memory at parity on the same engine | #599, PR #605 |
| G5 report parity | Pass: rows, rejected output and every report field identical (byte metrics excluded) | #599, PR #605 |
| G6 unique vs history | Pass: exact on 57/57 runs against 100M and 500M histories; memory bounded by the batch | #600, PR #608 |

OLF sizes its platform at **about 1 TB per layer**. The gates validated merge targets up to about
5 GB of Parquet (500M narrow rows). Loads, validation and unique checks are bounded by the batch,
not the table. **Merge time grows with the target's size,** because every MERGE scans the whole
target. Phase 2 benchmarks the largest OLF table before merge ships.

### Supported limits and design notes for the port

- **Memory.** Run DuckDB with `memory_limit=4GB`, `temp_directory` and
  `max_temp_directory_size` set. Size pods at `memory_limit` + 2 GiB: DuckDB's RSS ran
  0.2–1.9 GiB over the limit, and the overshoot grows as delete files accumulate. Spill has a
  cliff below 2 GB for merges.
- **Write path.** Loads use one PyIceberg producer feeding every chunk, which gives one commit
  and one snapshot (#597's `single` path). It relies on PyIceberg internals (the same calls
  `Transaction.append` makes). Public-API fallbacks, both measured in #597 as one commit:
  - unpartitioned tables: one `append(RecordBatchReader)` (PyIceberg 0.12 rejects streaming
    writes to partitioned tables);
  - partitioned tables: stage Parquet files that each hold a single partition value, then
    `add_files` in one transaction.
- **Merges.**
  - Unpartitioned and `day()` targets: one plain DuckDB `MERGE INTO`.
  - `bucket()` targets: one MERGE per bucket inside one DuckDB transaction (one commit). It is
    slow because each MERGE scans the whole target. The fix is to prune each MERGE to its bucket,
    in Floe first, then upstream in duckdb-iceberg.
  - DuckDB writes that span many `day()` partitions must be split by date range: one INSERT
    across 365 days ran out of memory or wrote 256k tiny files.
  - Time travel can reach a merge's intermediate snapshots. Floe's reports reference the final
    snapshot.
- **Retry guard (detailed design in #545).** A rerun after a crash gets a new run id
  (`crates/floe-core/src/run/context.rs:69-73`), so the guard can't match on the run id. #545
  must meet these requirements:
  - **Fingerprint.** Each append commit carries `floe.input_hash`: a fingerprint of the entity's
    stable identity plus its sorted claimed file URIs, with size and modification time or ETag.
    Including the entity scopes it, because two entities may write the same table. A run that
    finds its fingerprint in the table's snapshots skips the write and only promotes state.
    `floe.run_id` is kept for traceability only.
  - **Scope.** It covers claimed-but-unpromoted files only. The URI-based incremental contract
    (`docs/incremental.md`) is unchanged: promoted URIs are still skipped, even if their content
    changes.
  - **Retention.** The evidence must outlive any retry. Snapshot expiry, including
    `floe maintain`, must not drop a snapshot whose fingerprint isn't promoted yet; otherwise the
    fingerprint has to be kept elsewhere until promotion.
  - **Rejected output.** It must be idempotent too: deterministic paths derived from the
    fingerprint, overwritten on retry, so a kill before or after the commit neither duplicates
    nor loses rejected rows.
  - **Deliberate reload.** Reloading identical inputs after a state reset bypasses the guard
    explicitly.
- **Casts.** Polars 2.0 removed string → date/datetime casts. A conformance table of
  date/datetime inputs, recorded from the Rust 0.7 binary, comes before the cast module is
  ported.
- **Dependencies.** `pyiceberg-core` is required for PyIceberg writes to `day()` or `bucket()`
  tables. DuckDB stays pinned at 1.5.6, the version the Iceberg gates ran on, until 2.0 passes
  the same checks.

### Table maintenance becomes an optional Floe feature

Merge-on-read adds position delete files on every merge: about 7 per merge on unpartitioned
targets, 16 on `bucket(16)`, up to 365 on `day()`. Failed writes also leave orphan files. Floe
therefore offers optional maintenance:
- **What to do** comes from a per-table policy: compaction, snapshot expiry, orphan-file removal,
  sort order.
- It runs as a **separate `floe maintain` command,** exposed as a dagster-floe asset, never inside
  `floe run`.
- **Dagster decides when:** typically daily, after ingestion, run sequentially, because Polaris
  handles concurrent commits to the same table poorly.

What the Python Iceberg stack can do natively (compaction, z-order) is not yet verified. A
feasibility spike comes before the feature.

## Migration plan

### Where the code lives

**This repo, in a new pure-Python package at the root.** Not a new repo, and not inside
`crates/floe-python`.

- **Not a new repo.** During the port the Rust binary is the reference, and parity is checked by
  running both in the same CI. A new repo would also lose the issues, milestones, history and
  `orchestrators/dagster-floe`, which switches to the new library in lockstep. The clean tree a new
  repo offers comes anyway, when `crates/` is deleted at cutover.
- **Not inside `crates/floe-python`.** It is a maturin/PyO3 build, so building it needs the Rust
  toolchain, and it would hold two implementations of `floe.run` in one `floe` namespace during
  the port.
- **What carries over from `floe-python`:**
  - the distribution name `floe-python`. `floe` and `floe-core` on PyPI belong to other projects.
  - the import name `floe`.
  - the public API defined in `crates/floe-python/python/floe/_floe.pyi`: `run`, `validate`,
    `load_config`, `inspect_entity_state`, `reset_entity_state`, the observer hooks,
    `RunOutcome` and the typed exceptions.

  The new package implements that surface, so existing Python users see no API change.

```
floe/
├── pyproject.toml   ← pure Python, dist "floe-python", console script `floe`
├── src/floe/        ← the new core
├── tests/           ← pytest
├── conformance/     ← golden cases: config + inputs + expected report / accepted / rejected
├── spikes/python/   ← throwaway spike code (#596–#600)
├── crates/          ← frozen Rust reference; deleted at cutover
└── orchestrators/   ← dagster-floe moves to `import floe`; airflow-floe frozen
```

### Conformance suite

The Rust integration tests build their inputs inline in code and commit no fixture files, so they
cannot be reused as they are. Instead:

- `conformance/` holds one case per kept feature: config, input files, expected run report,
  accepted rows and rejected rows.
- The expected outputs are recorded once from the Rust 0.7 binary.
- CI runs the Python core against every case. Parity means zero diff, ignoring timings, run ids
  and writer byte metrics (`total_bytes_written`, `avg_file_size_mb`, per the G5 ruling).
  Generated identifiers (Iceberg `snapshot_id`, UUID-based data file names in `part_files`) are
  compared by shape, not by value.
- Once `crates/` is deleted, the recorded cases remain the regression suite.

### Phases

| Phase | Content | Exit |
|---|---|---|
| 0. Spike | #594 | Go / no-go |
| 1. Foundation | Python versions of `AGENTS.md` and `context/` (ruff, pyright strict, pytest; size budget extended to `src/floe/`), Python CI workflow, root `pyproject.toml`, conformance recorder and first cases | CI green on an empty package |
| 2. Port | config (pydantic, same error messages) → readers and checks → report → CAS state → Iceberg overwrite / append → merge → manifest and CLI (same commands and flags) → `floe` API per `_floe.pyi` → PII, lineage | Every conformance case passes |
| 3. Dagster | dagster-floe runs Floe in-process or through Dagster Pipes; job per domain (#580) | OLF pipelines run on the Python core |
| 4. Cutover | One PR deletes `crates/`, the Rust CI and the cargo / Homebrew / Scoop release jobs. Release becomes `uv build`, PyPI trusted publishing and a Python Docker image. | `floe-python` 0.8.0 on PyPI |

Phases 1–3 follow the usual `AGENTS.md` workflow: one sub-issue per PR, within the size budget.

### Rust freeze and versioning

- From Acceptance on, `crates/` takes security fixes only. Any other Rust change would have to be
  made twice.
- The last Rust release is tagged (0.7.x). A maintenance branch is cut from that tag only if a
  security fix is ever needed.
- The first Python release is **0.8.0**. Staying pre-1.0 allows the non-OLF sinks to be dropped,
  with the breaking changes listed in `CHANGELOG.md`. **1.0** follows once OLF has run it in
  production.

## Trade-offs

- **Lost:** a single static binary, a small Docker image, and compiler-enforced correctness.
  The mitigations are a strict type checker and pydantic config models.
- **Library installs can conflict:** in shared Airflow or Dagster environments, Floe would compete
  over `pyarrow` and Polars versions. Run Floe in its own environment or image, as the
  orchestrators already do via subprocess and Kubernetes Jobs.
- **DuckDB Iceberg writes are recent.** Partition transforms, schema evolution before a MERGE
  and Polaris authentication were proven in #598 and #609. Partitioned writes have the limits
  listed under [Outcome](#outcome).
- **Merge-on-read leaves positional delete files.** Floe takes on optional table maintenance
  (see [Outcome](#outcome)), scheduled by Dagster.

## Consequences

- Milestones are opened for phases 1–4 of the migration plan above, starting with phase 1.
- This record supersedes the Iceberg part of `merge-delegated-to-sink.md` and the Rust-specific
  parts of `polars-and-arrow.md`.
- The Arrow / Polars upgrade track (#523, #538, #565–#569, #387) and non-OLF sink requests
  (#249, #250, #261) are closed.
- #545–#549, #556 and #560 carry over as design requirements for the Python core.
- The spike PRs (#602, #605–#608, #610) are closed unmerged. Their code stays reachable through
  `refs/pull/<n>/head`.
