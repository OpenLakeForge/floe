# Decision — Ingesting a corrected file that a partner re-sends

## Status

Accepted (design for #1257, approved by the maintainer on the decision comment of #1257, refs checked against `main`
at `6526c789`). Docs only. Related: #264 (replay), #1203 (Parquet append retry limitation), #545/#549
(`entity-commit-guarantees.md`), `delta-sink.md`. Code: #1365 (`floe state forget`), blocked by #1354. The 1.x policy
is #1367.

## Context

Today a changed file is skipped. `skipped_reports` compares the recorded `(size, mtime)` with the current values
(`src/floe/run/incremental.py:116`); on a mismatch it skips with `already_ingested_changed` and a warning
(`:120-124`), and emits `file_finished` with that `skip_reason` (`:135-138`). The URI is the identity
(`src/floe/state.py:192-193`). Size and mtime come from S3 `size`/`last_modified` or local `st_size`/`st_mtime_ns`
(`src/floe/storage/inputs.py:277,302`); there is no ETag or content hash, so a re-PUT always changes the mtime.
`input_hash` is the sha256 of `entity.id` plus the sorted `[uri, size, mtime]` list (`inputs.py:329-332`).

The only way out is `floe state reset`, which empties the whole state (`src/floe/_cli.py:210-235`,
`src/floe/state.py:337-353`).

Guards that matter for a re-ingest:

- Iceberg: `Incremental.skip_committed` (`incremental.py:178`, called at `run/entity.py:381`). It only looks at the dead
  runs' expired claims and searches snapshots since the claim's `acquired_at`.
- Delta: `txn` appId `floe:<input_hash>` (`src/floe/delta/guard.py`).
- Parquet append with a unique key: accepted parts are named from the batch `input_hash`, and rejected parts are keyed
  per file (`run/file.py:411-413`). #1203 is still open.
- Merge ordering: the S7 barrier blocks every input while another run holds a live claim. Within a batch, a merge
  reads the newest file first (`run/entity.py:398-400`), and inputs are sorted by URI.

## Decision

| Question | Decision |
|---|---|
| Scope for 1.0 | Ship only `floe state forget -c <cfg> --entity X --file <uri> [--file …] --yes`, which re-queues the named files for the next run. No config field, no state-schema change, no report change. |
| Opt-in policy `on_changed_file` | Deferred to 1.x. It is an additive optional field with default `skip`, so it does not block the #1248 freeze. Its semantics are fixed below. |
| `merge_scd1` / `merge_scd2` | Allowed. The re-sent file's rows become current, even over values that newer files wrote since. To replay in order, forget the corrected file and the newer ones in one command. One batch processes them in URI order, as today. |
| `append` | `forget` allowed, the automatic policy refused. Floe does not delete the old rows, so the next run duplicates them, or rejects them as duplicates when the entity has a unique key. Only an operator who removed the old rows can make append correct, so only the manual command allows it, and its output says so. |
| `overwrite` | Allowed, no special case. In `file` mode the output is the batch the run lists (`docs/concepts/incremental.md:176-180`). `--full-refresh` stays the tool to rebuild from every input. |
| Concurrency | Safe with CAS alone (AGENTS.md §7). `forget` is one conditional state write. It refuses a URI under a live claim, and every concurrent `promote`/`release`/`renew` reloads on a version mismatch, so none brings back the removed entry. |
| Retry guards | Unchanged, not worsened. A corrected file has a new size and mtime, so it gets a new `input_hash`. The Iceberg guard is time-bounded and cannot match the original snapshot. The Delta guard has no time bound on a match: a pre-existing gap (R1), fixed by #1354. |
| Report / events | No change in 1.0. A forgotten file is just a pending file, with the normal per-file report and `file_finished` event. `forget` prints what it removed (URI, recorded size, mtime and processed_at) as the audit record. |

### 1.0: `floe state forget`

```
floe state forget -c config.yml --entity orders --file s3://raw/incoming/orders/orders_001.csv [--file …] --yes
```

- **State:** a new `state.forget(client, uri, entity, files) -> dict[str, FileState]`, modelled on `reset` and
  `claim_inputs`. It loads, checks, deletes `files[u]`, and writes with `write_conditional` inside the `CAS_RETRIES`
  loop with `_backoff`. It leaves `claims`, `watermark` and `pruned_before` alone. All or nothing: if any URI fails a
  check, nothing is written.
- **Refusals** (named `FloeConfigError`s, exit 1):
  - the URI has a live claim ("claimed by run R until T");
  - the URI is not in `files`: the message hints at `floe state inspect`, and if the URI has an expired claim it says
    the next run retries it anyway;
  - with `source.partitioning`, the URI's partition date is before `window(...)`: the next run would not list it, so
    the message says to use `--full-refresh`;
  - the entity is not in `incremental_mode: file`.
- **Matching:** exact match against state keys, in the form `floe state inspect` prints. No normalization, because
  guessing a URI could forget the wrong file.
- **`--yes`:** required, as for `reset`.
- **Output:** for each URI, the recorded size, mtime and processed_at. Then one line for the write mode:
  - append: "old rows stay: duplicated, or rejected by the unique key, unless you removed them";
  - merges: "re-sent rows become current over newer files' values".
- **Discoverability:** the `already_ingested_changed` warning ends with "run `floe state forget --entity X --file <uri>`
  to re-ingest it". The code and `skip_reason` are unchanged.

### Why the guards and #1203 are not worsened

- **Corrected file (new size or mtime):** the run claims it fresh, with no expired claim, so `skip_committed` does not
  check it. That is correct: it is new data. If that run dies after committing, its retry rebuilds the hash from the
  new size and mtime and skips. The original snapshot carries a different hash and is older than `since`.
- **Unchanged file forgotten (same size and mtime):**
  - Iceberg: `find_committed` only searches snapshots since the dead run's claim, so it never matches the original
    ingestion. Safe.
  - Parquet append with a unique key: if the original batch was exactly `{F}`, the parts get the same names, are
    overwritten in place, and are excluded from the seed. The result is replaced rows, not duplicates.
  - Delta: R1, fixed by #1354.
- **#1203:** a forgotten file is one more pending file. A dead batch retried together with it is the existing "larger
  pending set" case: the same symptom (bogus rejects, never duplicate accepted rows) and no new path.
- **Concurrency:** a run that started before `forget` holds a stale `claimed.state`. Its first `_mutate` fails the
  version check and reloads (`src/floe/state.py:249-269`). `promote` moves only that run's own claims (`:274-298`), so
  the removed entry stays removed. Merges still wait behind the barrier.

### 1.x (not 1.0): `incremental.on_changed_file: skip | fail | reprocess`

Recorded now so the 1.0 design does not reopen it.

- `skip`: the default, today's behaviour.
- `fail`: the file is reported `failed` with a named error, so the run exits 1.
- `reprocess`: `claim_inputs` treats a changed `files` entry as pending. `floe validate` refuses `reprocess` with
  `write_mode: append`. Merges and overwrite follow the contracts above.
- **Report:** one optional per-file object, `reprocessed: {size, mtime, processed_at}`, holding the previous record
  (additive, so the 1.0 report needs no placeholder). `forget` does not set it.
- This is a config YAML change. It needs maintainer sign-off on its own issue (AGENTS.md §4), and the
  `entity.schema.yaml` resync ships as a separate PR. Filed as #1367, a sub-issue of #264, with no milestone.

### Shared with #264

Both features re-queue a source on purpose. `forget` is the "re-read the immutable source" step that #264 needs for
masked rows (#1255). A future `floe replay` can call `state.forget` for a file-level replay instead of adding a
second mechanism.

## Open risks

- **R1. The Delta guard has no time bound on a match** (pre-existing). `committed()` returns True whenever
  `txn floe:<hash>` exists, and `since` bounds only the unknown answer. After a `reset` or `forget` of an unchanged
  single-file batch and a crash before commit, the retry matches the original `txn` and drops the reload. Already
  filed as #1354: stamp the `txn` version with the claim's `acquired_at` in ms, and count a match only when
  `transaction_version >= since_ms`. #1365 is blocked by #1354.
- **R2. Merges overwrite newer values.** Forgetting an old file without the newer ones regresses the keys the newer
  files updated (SCD2 gains a current version with older values). This is documented and stated in the command
  output. Floe cannot detect it, because there is no ordering column, and adding one is out of scope.
- **R3. Rejected parts of an unchanged forgotten file are overwritten.** The per-file key gives the same part names.
  If the new run writes fewer parts, stale higher-index parts survive (the same ceiling as #1203). This is
  acceptable, because the rejects reflect the file's latest validation, but it is in tension with #1254. Corrected
  files get new names.
- **R4. Append without removing the old rows** gives duplicates, or rejects by the unique key. This is deliberate and
  stated in the output.
- **R5. `--file` must be the exact state key.** Mistyped or relative paths are refused, not guessed. This costs
  usability, not correctness.

## Issues

- #1364 this record, docs only. Part of #1257.
- #1365 `feat(state): floe state forget re-queues named files`. Blocked by #1354. Its PR closes the 1.0 part of #1257.
- #1366 `docs: drop the never-emitted incremental_file_changed event name`, a separate docs issue.
- #1354 `fix(delta)`: the guard matches a txn from before the dead run's claim. Lands before #1365.
- 1.x: #1367 `feat(incremental): on_changed_file policy for re-sent files`, a sub-issue of #264.
