# Decision — Entity commit, retry and idempotency guarantees

## Status

Accepted (design #545, concurrent writers #549; maintainer decisions of 2026-10-26). Implemented by sub-issues #914–#924. This ADR is sub-issue #925.

## Context

An entity run touches several places in a fixed order: rejected output, input archive, Iceberg commit, state promotion (D18), then the report. Only the Iceberg commit is atomic. A crash between any two steps leaves the others out of sync, and the retry then loses or duplicates rows.

Observed on `main` at 18707ff:

- `iceberg/write.py` stamps `floe.run_id` and `floe.input_hash` on the snapshot, but nothing reads them. The retry guard does not exist yet.
- `run/entity.py` archives the input (`_archive`) before the sink commit. Any exception then calls `inc.release()`.
- Merges (`iceberg/merge.py`) go through DuckDB, so their snapshots carry no `floe.*` properties. Merge idempotence comes from the `IS DISTINCT FROM` guard, not from a marker.
- `write_table` opens `table.transaction()` on a `Table` loaded before the long data write, with no conflict handling.

## Decision

**One table is atomic, the run is end-to-end; there is no cross-table transaction.** An Iceberg commit makes one table's change atomic. Across the rejected output, the archive, the state and the report, Floe guarantees that a retry converges to the right result, not that the steps commit together. Floe does not coordinate commits across tables and will not (AGENTS.md §7).

The mechanisms below make the end-to-end run safe to retry.

### 1. Retry outcome per mode and crash point

P1 = before the Iceberg commit · P2 = after the commit, before promote · P3 = after promote, before the report.

| Mode | P1 | P2 | P3 |
|---|---|---|---|
| overwrite | Retry rewrites; same result. Orphan files only. | Retry overwrites with the same inputs: same content, one extra snapshot. OK. | State promoted, nothing pending. Report lost (D18, accepted). |
| append | Retry writes. OK. | Duplicates without the guard. With the guard: skip the write, promote. | OK |
| merge_scd1 | Retry merges. OK. | No-op (change guard). OK, unless a newer batch landed first (S7). | OK |
| merge_scd2 (#654) | Retry merges. | No-op if nothing changed since; same S7 caveat, and worse (history). | OK |

| Incremental mode | Effect on a retry |
|---|---|
| `file` | Claims persist until the TTL (1 h). Retry inside the hour: files are skipped as `active_claim` and nothing runs. After the TTL: claims are pruned and the files are re-read. |
| `archive` / `none` + `sink.archive` | The input is moved before the commit (S1, fixed by #914). The archive is the marker; there is no state. |
| `none` | No marker by contract: every run reprocesses all inputs. Only an exact same-set retry can be recognised. |

### 2. The retry guard

- **Where.** Right after the claim, before reading any file. After validation it is too late: the unique seed finds the rows the dead run already committed and rejects every row as a duplicate (S3).
- **File mode.** A stale claim carries the dead run's `run_id`, `size` and `mtime`. Group expired claims by `run_id` and recompute `input_hash(entity, files)` from them. This reproduces the dead run's hash exactly even when the retry's file set is larger (S4), so no state-schema change is needed. Walk the table's snapshot ancestors from the loaded metadata (no manifest I/O). Stop at snapshots older than the claim's `acquired_at`. Match `summary[floe.input_hash]`. On a match, promote those files as processed, report them as skipped with `skip_reason: already_committed`, and do not read or write them. Otherwise drop the stale claims and process normally.
- **`archive` / `none`.** There is no claim record. Hash the pending set and walk all ancestors. This only matches an identical set (limit below).
- **Cost.** O(snapshots), in memory, and only when stale claims exist (file mode) or once per append entity (other modes). It does not depend on table size or row count. A fixed "last N snapshots" window would be unsafe once snapshots churn.
- **Only `append` needs it.** Overwrite and SCD1 are idempotent, and DuckDB merges do not stamp the hash.
- **Reload after `reset_entity_state`** is unchanged: the state is gone, so no stale claim exists.

### 3. Conflict policy (two runs on one table)

Two layers, no new locks:

1. **Claims** (the existing CAS state, `incremental-state-cas.md`) keep two runs off the same files while a claim is live.
2. **The Iceberg commit is a CAS on the table's main-branch snapshot id.** The catalog answers 409 to the stale one of two concurrent commits. That is atomicity, not key uniqueness: two writers that validated against snapshot S can each pass `unique`.

On a conflict (append/overwrite): refresh the table and re-run the guard, so an overlapping Dagster retry loses and skips. Then check unique keys only against the files added since S (an incremental scan, like `_added_data_files` in `merge.py`; cost grows with concurrent writes, not history). On a hit, fail closed: commit nothing, and the next run revalidates from scratch. Otherwise recommit reusing the written data files, with a bounded jittered retry (5 attempts, as in `state._backoff`). For merges, retry the whole MERGE; counts and changed rows are recomputed per attempt, so SCD1 on a key both writers inserted becomes an update. Different tables never interact. Two entities writing one table are covered by the same commit CAS, because the hash is scoped by `entity.id`.

Unverified assumption: PyIceberg and DuckDB-Iceberg surface the 409 as a catchable error and do not retry on their own. The contention test in #920 must establish this before the policy is relied on.

### 4. Scenarios

| # | Scenario | Result today | Fix |
|---|---|---|---|
| S1 | `archive` mode: input moved, then the commit fails or the process dies | Rows lost | Archive after the sink commit, before promote (#914) |
| S2 | Commit succeeded, then an exception in `build_report` → `inc.release()` | Duplicates on rerun (append) | Track `committed`; promote instead of release (#915) |
| S3 | Retry validates before any guard; the unique seed sees its own committed rows | All rows rejected, bogus rejected files, nothing written | Guard before read (#916) |
| S4 | P2 crash, TTL expires, new file B arrives; the retry's set is A∪B so its hash differs | A duplicated | Recompute the dead run's hash from stale claims (#917) |
| S5 | File `abort` mid-entity: earlier files are written, then `finish` releases | Fixed file → earlier files duplicated | Promote the files written, release only the rest (#918) |
| S6 | Rejected output on retry (append naming) | Rejected rows duplicated | Deterministic part name from the input hash, overwritten on retry (#919) |
| S7 | Merge modes: run A crashes, B (newer files) runs while A's claim is live, A replays | Older values overwrite newer; SCD2 history corrupted | Merge-mode entity barrier (#923) |
| S8 | Run over 1 h with a failed heartbeat, or no-claim modes: two runs append the same files | Duplicates | Commit conflict → guard re-check (#920) |
| S9 | Two writers validated against the same snapshot, overlapping keys | Duplicate PK | Delta unique check on conflict (#921), MERGE retry (#922) |
| S10 | Large load: transaction built on a `Table` loaded before the write, another commit meanwhile | Run fails after writing all data files | Refresh and recommit reusing the files (#920) |

#924 adds the advisory `floe/table` Dagster concurrency tag so the orchestrator keeps runs on one table apart.

### Maintainer decisions (2026-10-26)

1. **S5 (abort).** Rust 0.7 writes the earlier files' accepted rows after an abort (`validate_split.rs`: `break`, then `accepted_buffer.finish(...)` still runs). Keep that, and promote the files that were written; release only the rest.
2. **Report.** A new `skip_reason: already_committed` (a Python-only delta, added to the run report contract).
3. **`archive`/`none` superset retries.** Accept the limit and document it: the hash covers the whole batch, so retry before new files land, or use `file` mode.
4. **Retention floor.** 7 days and at least 10 snapshots. If the evidence is older than that and stale claims remain, the run fails closed and asks for `floe state reset`. The floor is enforced in `floe maintain` expiry, which is blocked on #618.
5. **Stable caller run id** for an immediate Dagster retry: deferred. A new input becomes its own `feat:` later.
6. **S7 barrier.** Merge modes only, not `append`.

## Rationale

- Idempotence is cheapest where the evidence already lives: the snapshot summary. No new state and no lock server.
- Guarding before the read is the only point where a retry cannot reject its own committed rows.
- Failing closed on stale evidence or a unique hit loses no data; the cost is a manual `floe state reset` or a rerun.

## Trade-offs

- `archive`/`none` retries are only exact for an identical input set (decision 3).
- The 1 h claim TTL stays blunt: an immediate retry of a dead run is a no-op, and still reports success, until the claim expires (decision 5).
- No cross-table atomicity: a run that writes several tables can end with some committed and some not. Each table converges on retry; the set does not commit as one.

## Consequences

- Sub-issues, in order: #914, #915 → #916 → #917, #918, #919 → #920 → #921, #922 → #923, #924, #925.
- The run report gains `skip_reason: already_committed`.
- Snapshot expiry must honour the retention floor before the guard is relied on.
