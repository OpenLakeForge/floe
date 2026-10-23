# S2b: DuckDB MERGE into partitioned Iceberg targets at scale (#609)

Follow-up to #598 (`../s2-merge/`, PR #607). #598's 10M-row SCD2 MERGE ran out of memory on a
`bucket(16,id)` 100M-row target. This spike reproduces that failure, then measures the approaches
listed in #609 in order, stopping at the first one that meets the exit condition.

Throwaway spike code (#594 rules): never merged. It reuses #598's `common.py`, `merge.py` (the
SCD2 `MERGE INTO`, unchanged) and `scale.py` (schema, row generator, seed) by import. Nothing under
`../s2-merge/` is modified.

## Verdict: exit (a), at full scale with no scale-down

A 10M-row SCD2 batch into a **500M-row `bucket(16,id)`** target passes under
`memory_limit=4GB` as **one MERGE per bucket inside one DuckDB transaction**. It lands as **one
catalog commit** (one new `metadata.json`, 32 snapshots) with a correct SCD2 contract.

Costs, measured:
- **Wall time: 985 s, then 1,117 s on a second merge into the same table.** #598's
  unpartitioned merge at the same scale takes 72–79 s, so this is about 13–15× slower. Every per-bucket MERGE scans the whole target: DuckDB doesn't prune the
  MERGE join by bucket, so time grows with buckets × target size (about 60 s per bucket at 500M,
  6 s at 100M).
- **Peak memory: VmHWM 5,107 MiB, then 5,980 MiB at `memory_limit=4GB`**, no spill. That's
  about +1.1 to +1.9 GiB over the limit, and it grows as delete files accumulate, like #598's
  unpartitioned 4.3 → 6.1 GB over 5 merges. Size pods at `memory_limit` + 2 GiB.
- **32 snapshots per commit** (16 × `overwrite` + `append`). Readers of the current table see the
  merge atomically. Time travel can reach the 31 intermediate snapshots (the #594 ruling accepts
  this).

The same check on a **500M-row `day(event_date)`** target passes both ways. The plain single
MERGE passes (104.5 s, VmHWM 4,253 MiB). One MERGE per group of days passes too (1,233 s, VmHWM
5,645 MiB). So the plain MERGE's failure is specific to `bucket(16,id)`, the key-hash layout.
It is not about partitioned MERGE in general, and not about file size (see the diagnostic run).

## Results (DuckDB 1.5.6, `memory_limit=4GB`, 4 threads, 10M-row SCD2 batch: 5M changed, 3M unchanged, 2M new keys)

Peak memory is the child's `VmHWM` / peak `RssAnon`, polled every 100 ms from `/proc` (not
`ru_maxrss`). Spill is the peak size of `temp_directory`. Commits are new `*.metadata.json`
objects in the table's metadata prefix. Contract means: closed = 5M, inserted = 7M, current rows
= before + 2M, no key with 2 current rows. Parity means PyIceberg and DuckDB return identical rows
on 300k rows of touched keys (changed, unchanged and new).

| # | Target | Approach | Result | Wall s | VmHWM MiB | RssAnon MiB | Spill MiB | Snapshots | Commits | Contract | Parity |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | bucket(16,id) 100M | plain (#598), **test that must fail first** | **OOM**: `could not allocate block of size 76.5 MiB (3.7 GiB/3.7 GiB used)` | 63.5 | 4,478 | 4,369 | 8,655 | 0 | 0 | – | – |
| 2 | bucket(16,id) 100M | (a) one MERGE per bucket, 1 txn | pass | 107.6 | 2,397 | 2,298 | 0 | 32 | **1** | pass | pass |
| 3 | bucket(16,id) 500M | plain | **OOM**: `could not allocate block of size 76.5 MiB (3.6 GiB/3.7 GiB used)` | 86.1 | 4,326 | 4,229 | 99 | 0 | 0 | – | – |
| 4 | **bucket(16,id) 500M** | **(a) one MERGE per bucket, 1 txn** | **pass** | **984.7** | **5,107** | 5,007 | 0 | 32 | **1** | pass | pass |
| 5 | bucket(16,id) 500M | (a) again (2nd merge on the same table) | pass | 1,116.5 | 5,980 | 5,897 | 0 | 32 | **1** | pass | pass |
| 6 | day(event_date) 500M | plain | pass | 104.5 | 4,253 | 4,158 | 0 | 2 | **1** | pass (checked after the run) | not run |
| 7 | day(event_date) 500M | (a) one MERGE per group of ~23 days (16 groups), 1 txn | pass | 1,233.3 | 5,645 | 5,556 | 0 | 32 | **1** | pass | pass |
| 8 | bucket(16,id) 100M, small files (0.78M rows/file) | plain (diagnostic) | **OOM**: `failed to offload data block of size 256.0 KiB (7.4 GiB/7.4 GiB used)` at the 8GB spill cap | 30.7 | 4,177 | 4,095 | 7,629 | 0 | 0 | – | – |

Per-statement times, file counts and the full records are in `results/runs.jsonl`; logs are in
`logs/`.

Full read-back, PyIceberg streaming the whole table with delete files honoured, against DuckDB
`count(*)` (`results/full-count.jsonl`):

| Table, after | Total rows | Current | Closed | Identical |
|---|---|---|---|---|
| bucket(16,id) 500M, after run 4 | 507,000,000 | 502,000,000 | 5,000,000 | yes |
| day(event_date) 500M, after runs 6 and 7 | 514,000,000 | 504,000,000 | 10,000,000 | yes |

### Approaches tried (the #609 ladder)

| Step | Approach | Tried | Outcome |
|---|---|---|---|
| 1 | One MERGE per partition in one transaction | yes, runs 2, 4, 5, 7 | **Meets exit (a)**: passes at 500M bucket and day, 1 catalog commit, contract correct |
| 2 | Smaller batches per MERGE | no | Not run: step 1 met the exit condition (#609: stop climbing). Step 1 is a smaller batch per MERGE (about 625k rows per bucket statement) |
| 3 | DuckDB settings (`preserve_insertion_order`, `threads`, `partitioned_write_*`) | no | Not run, for the same reason. Lead for whoever picks this up: the iceberg extension writes partitioned tables through DuckDB's `PARTITION_BY` copy (`src/execution/operator/iceberg_insert.cpp`, `partition_output = true`). Its memory is governed by `partitioned_write_flush_threshold` (524,288 rows) and `partitioned_write_max_open_files` (100) |
| 4 | DuckDB 2.0.x | no | Not run, for the same reason. `duckdb==2.0.0` is on PyPI (2026-10-21) |

### Golden fixture through approach (a)

`check_golden_split.py` runs #598's `check_golden.run_case` unchanged, with the merge swapped for
"split the batch by the target's partition value, one MERGE per partition, one transaction".
SCD1 and SCD2 × unpartitioned / identity(region) / day(event_date) / bucket(4,id): **all pass**.
That covers DuckDB and PyIceberg reading the golden rows, partition consistency of every data
file, SCD2 metrics (closed 3, inserted 4, unchanged 2), rerun with no change, and **one catalog
commit per run** (1 to 6 MERGE statements per run). Snapshots per run: 2 to 7, reported for
information only (`results/golden-split.txt`).

Correctness doesn't depend on the batch and target partitions lining up. Each per-partition MERGE
joins against the whole target (there is no partition predicate), so a row whose partition value
changes is still matched. The golden SCD1 identity case moves key 2 from `eu` to `us` and passes.

## Other findings

- **Partitioned INSERTs run out of memory too, not only MERGE.** Seeding the day table with #598's
  `scale.py seed` (50M-row INSERTs, each touching all 365 days) failed at 4GB with the same
  `could not allocate block of size 76.5 MiB (3.7 GiB/3.7 GiB used)`. At 10GB it passed, but
  wrote **256,350 data files (about 1,950 rows each)**, because DuckDB keeps at most
  `partitioned_write_max_open_files=100` files open and the INSERT spans 365 partitions. With
  `partitioned_write_max_open_files=1000` it ran out of memory even at 10GB (`9.2 GiB/9.3 GiB
  used`). The day table used here was seeded with the same rows, one INSERT per 30 days
  (`--days-per-insert 30`), at 10GB: 365 files per 500M rows, 545 s. For the port: a
  day-partitioned **full load** that spans many days needs the same split as the merge.
- **A failed write leaves orphan files.** Nothing is committed, but the files stay in storage: 31
  objects (80 MiB) after the failed plain MERGE on the 500M bucket table (`orphans.py`), and 11,312
  objects after the failed day INSERT. That adds to the maintenance question on #594 (orphan
  cleanup).
- **Delete files per run.** About 16 position delete files per run on bucket(16), and 365 for the
  day layout (one per partition touched). See `results/runs.jsonl`.
- `pyiceberg-core` 0.10.1 is used only to compute Iceberg bucket values for the split. It agrees
  with PyIceberg's `BucketTransform` (`bucket[16]` of 0, 1, 34, -1 = 12, 4, 3, 8, spec example
  34 → 3). DuckDB's iceberg extension has its own `iceberg_bucket()` on newer builds; it wasn't
  used.

## Supported limit and approach to use (for the port)

| Layout | Plain single MERGE at 4GB | Approach to use |
|---|---|---|
| unpartitioned | passes at 10M into 500M (#598) | plain |
| bucket(16, key) | fails at 10M into 100M and 500M. 1M into 100M passes (#598). The largest passing size between 1M and 10M was not measured | one MERGE per bucket in one transaction: passes at 10M into 500M (985–1,117 s, VmHWM 5.0–5.8 GiB) |
| day(event_date) | passes at 10M into 500M (105 s) | plain. Per-day-group also passes, about 12× slower |

## Environment

- 4 vCPU Intel Xeon @ 2.10 GHz, 16 GB RAM, no swap, no cgroup memory limit. About 30 GB free disk
  at the start, shared by SeaweedFS, staging and spill (spill capped with
  `max_temp_directory_size`, 12GB or 8GB). Linux 6.18.44, Docker 29.9.1.
- Python 3.11.17. **duckdb 1.5.6** (extensions: iceberg `890b78a9c`, httpfs `4bc690d`, avro
  `f9d5902`), pyiceberg 0.12.0, **pyiceberg-core 0.10.1**, pyarrow 25.0.1, boto3 1.43.117 (to
  count metadata files and delete dropped tables' files, since Polaris purge is disabled). Polars
  not used.
- Polaris 1.8.0 and SeaweedFS 4.48, pulled by the digests pinned in the harness README.

## Reproduce

```bash
cd spikes/python/harness && docker compose up -d --wait && python3 bootstrap.py
cd ../s2b-partitioned-merge && uv venv .venv && uv pip install --python .venv/bin/python -r requirements.txt
P=.venv/bin/python
# 1. test that must fail first (#598's failure)
$P spike.py seed --table b16_100m --rows 100000000 --partition bucket
$P spike.py run --table b16_100m --target-rows 100000000 --batch-rows 10000000 --approach plain --memory-limit 4GB
# 2. approach (a): one MERGE per bucket, one transaction
$P spike.py run --table b16_100m --target-rows 100000000 --batch-rows 10000000 --approach split --slices 16 --memory-limit 4GB --verify
$P spike.py seed --table b16_500m --rows 500000000 --partition bucket
$P spike.py run --table b16_500m --target-rows 500000000 --batch-rows 10000000 --approach plain --memory-limit 4GB
$P spike.py run --table b16_500m --target-rows 500000000 --batch-rows 10000000 --approach split --slices 16 --memory-limit 4GB --verify
$P spike.py run --table b16_500m --batch-rows 10000000 --approach split --slices 16 --memory-limit 4GB --max-spill 8GB --verify
$P full_count.py b16_500m && $P orphans.py b16_500m
# 3. day(event_date) at the same scale
$P spike.py seed --table day_500m --rows 500000000 --partition day --days-per-insert 30 --memory-limit 10GB
$P spike.py run --table day_500m --target-rows 500000000 --batch-rows 10000000 --approach plain --memory-limit 4GB --max-spill 8GB
$P posthoc_contract.py day_500m 500000000
$P spike.py run --table day_500m --batch-rows 10000000 --approach split --slices 16 --memory-limit 4GB --max-spill 8GB --verify
$P full_count.py day_500m
# 4. diagnostic: bucketed target with small files
$P spike.py seed --table b16s_100m --rows 100000000 --partition bucket --chunk 12500000
$P spike.py run --table b16s_100m --target-rows 100000000 --batch-rows 10000000 --approach plain --memory-limit 4GB --max-spill 8GB --verify
# golden fixture through approach (a)
$P check_golden_split.py
$P drop.py <table>   # drop a table and delete its files
```

`spike.py run` stages a batch at fresh key offsets per table (`results/keys-<table>.json`,
advanced only after a committed run), so runs on the same table stay checkable.
`--python <venv>/bin/python` runs the timed child under another DuckDB version (not used: step 4
was not reached).
