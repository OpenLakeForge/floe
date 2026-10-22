# Spike S2 (#598): DuckDB `MERGE INTO` Iceberg via Polaris for SCD1 / SCD2

Throwaway spike code for gate G3 of #594. Not for production. Runs on the #596 harness
(`../harness/`: Polaris 1.8.0 + SeaweedFS 4.48).

## Verdict

**G3: FAIL on the literal #594 threshold**, at the full placeholder scale (10M-row SCD2 batch
into a 500M-row target):

| G3 criterion | Result |
|---|---|
| Volume completes | **Yes.** 5 consecutive merges, 72–79 s wall each, `memory_limit=4GB`, 4 vCPU |
| Within SLA | SLA is still TBD in #594 |
| Results match the SCD2 contract | **Yes.** Every merge: closed = 5,000,000, inserted = 7,000,000, 0 keys with 2 current rows; PyIceberg and DuckDB agree |
| One snapshot | **No.** DuckDB 1.5.6 writes every MERGE as **2 snapshots** (`overwrite`, then `append`), in **1 catalog commit** |

What the snapshot failure means: both snapshots are added by a single metadata transition
(one new `metadata.json`). The intermediate snapshot never becomes current and is not in
`snapshot-log`, so readers of the current table see the merge atomically. It is still in the
table history, though, and time travel to it shows a half-applied merge (updates/closes done,
inserts missing). Whether "one atomic commit, two snapshots" satisfies G3 is a ruling for #594;
this spike doesn't make it.

Other findings that matter for the decision:

- **RSS exceeds `memory_limit`, and grows with each merge.** Under `memory_limit=4GB`, peak RSS
  went 4.4 → 4.8 → 5.3 → 5.6 → 6.1 GB over 5 merges (6.6 GB on the no-op rerun), as position
  delete files accumulate. A pod limit has to be sized well above `memory_limit`.
- **Spill has a cliff.** On the 500M target with a 10M batch: 2 GB → 101 MB spilled, passes;
  1.5 GB and 1 GB → spill grows to the 15 GB cap and the merge fails with
  `Out of Memory Error: failed to offload data block`. The first 1 GB attempt, without a cap,
  filled the disk (>17 GB spilled, `No space left on device`). Nothing is committed when a merge
  fails.
- **Partitioned target at scale fails.** On a `bucket(16, id)` 100M-row target, the same
  10M-row SCD2 batch fails with `Out of Memory` at `memory_limit=4GB` (11 GB spilled) and at
  8 GB (8.9 GB spilled). The plan is identical to the unpartitioned case, so the extra memory is
  in the partitioned write inside `MERGE_INTO`. One confound wasn't separated: the bucketed seed
  files are larger (~3.1M rows each, against ~0.86M unpartitioned). A 1M-row batch on the same
  table passes (1.7 GB RSS). Partitioned merges work at golden size with identity, day and
  bucket transforms (below).
- **PyIceberg 0.12 can't append to day- or bucket-partitioned tables** without `pyiceberg-core`,
  which isn't an approved spike dependency (`NotInstalledError: pyiceberg_core needs to be
  installed`). Targets are seeded with DuckDB `INSERT` instead. PyIceberg reads those tables fine.

## Results

Environment: 4 vCPU Intel Xeon @ 2.10 GHz, 16 GB RAM, no swap, no cgroup memory limit, about
30 GB free disk at the start (one shared disk for SeaweedFS data, staging and spill). Linux
6.18, Docker 29.9.1. Python 3.11.17, duckdb 1.5.6 (iceberg extension `890b78a9c`, httpfs
`4bc690d`, avro `f9d5902`), pyiceberg 0.12.0, pyarrow 25.0.1, boto3 1.43.117 (used only to
delete a dropped table's files, since Polaris has purge disabled). Polaris 1.8.0, SeaweedFS
4.48, by the digests pinned in `../harness/README.md`. Polars isn't used.

### Golden fixture: SCD1 / SCD2 × 4 partition specs (`check_golden.py`)

Fixture: `golden.py`, derived from `docs/write_modes.md` and `merge/{scd1,scd2,shared}.rs`.
It covers changed, unchanged, new and absent keys, a change that moves a row to another identity
partition, null-safe compare (NULL = NULL is unchanged, NULL → value is changed), a closed
historical SCD2 version that must never match, and additive schema evolution: `tier` is added
with PyIceberg on a DuckDB connection opened *before* the change, and is a compare column, so
non-null `tier` counts as a change, like Rust's `scd2_changed_predicate`.

The test that must fail first, `results/golden-fail-first.txt` (with `merge.py` stubbed): 8/8 FAIL.

Final run, `results/golden.txt` / `results/golden.json`:

| Check (each of: unpartitioned, identity(region), day(event_date), bucket(4,id)) | SCD1 | SCD2 |
|---|---|---|
| Exactly one new snapshot | **FAIL** (2: `overwrite` + `append`) | **FAIL** (2) |
| One catalog commit (one new `metadata.json`) | PASS | PASS |
| DuckDB reads the golden expected rows | PASS | PASS |
| PyIceberg reads the golden expected rows (position deletes honoured) | PASS | PASS |
| Every data file holds only rows of its partition value | PASS | PASS |
| Additive schema evolution (PyIceberg, before the merge) | PASS | PASS |
| SCD2 metrics closed=3 / inserted=4 / unchanged=2 | n/a | PASS |
| Rerun same batch: rows unchanged, 0 new snapshots | PASS | PASS |

The disk spill check isn't meaningful at golden size. It is covered by the scale runs.

### G3 at scale: 10M-row SCD2 batch into a 500M-row unpartitioned target (`scale.py`)

Batch mix (placeholder): 5M changed keys, 3M unchanged keys, 2M new keys. Each batch is staged
as local Parquet (156 MB), and each merge runs in a fresh child process. Peak RSS is the child's
`VmHWM`. Spill is the peak size of the temp directory, polled every 100 ms. Seeding the target
took 336 s (10 chunks of 50M rows, 590 data files).

`memory_limit=4GB`, `threads=4`, `results/scale-g3_500m_b-1-5-4GB.json`:

| Merge | Wall s | Peak RSS MB | Spill MB | Snapshots | Commits | Data files | Pos. delete files | Closed / inserted / dup current | Contract | PyIceberg sample = DuckDB | PyIceberg full count |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 72.1 | 4,359 | 0 | 2 | 1 | 624 | 6 | 5,000,000 / 7,000,000 / 0 | PASS | yes | 507,000,000 |
| 2 | 74.5 | 4,794 | 0 | 2 | 1 | 660 | 13 | 5,000,000 / 7,000,000 / 0 | PASS | yes | 514,000,000 |
| 3 | 75.0 | 5,270 | 0 | 2 | 1 | 691 | 20 | 5,000,000 / 7,000,000 / 0 | PASS | yes | 521,000,000 |
| 4 | 79.1 | 5,630 | 0 | 2 | 1 | 724 | 27 | 5,000,000 / 7,000,000 / 0 | PASS | yes | 528,000,000 |
| 5 | 77.6 | 6,079 | 0 | 2 | 1 | 757 | 34 | 5,000,000 / 7,000,000 / 0 | PASS | yes | 535,000,000 |
| rerun of 5 | 75.9 | 6,599 | 0 | 0 | 0 | 757 | 34 | 0 rows affected; 535,000,000 rows before and after | PASS | | |

Each merge adds 6–7 position delete files and 31–36 data files, and no equality deletes.
The "PyIceberg sample" compares all columns over 200k keys (400k rows) that the merge touched.
"Full count" is PyIceberg streaming every row id with deletes applied; it equals DuckDB's
`count(*)` each time.

Spill (same table, one more merge each), `results/scale-g3_500m_b-{6-6-2GB,7-7-1500MB,7-7-1GB}.json`:

| `memory_limit` | Result | Wall s | Peak RSS MB | Peak spill MB |
|---|---|---|---|---|
| 2GB | PASS (contract + PyIceberg sample) | 83.6 | 4,379 | 101 |
| 1500MB | **Out of Memory**, `max_temp_directory_size=15GB` reached | 102.1 | 4,327 | 14,305 |
| 1GB | **Out of Memory**, `max_temp_directory_size=15GB` reached | 75.2 | 2,702 | 14,305 |
| 1GB, first try, no cap | **No space left on device** (log `run-g3_500m_b-m6-1GB.log`) | | | >17 GB |

Partitioned target, `bucket(16, id)`, 100M rows, `results/scale-s100m_bucket16-*.json`:

| Batch | `memory_limit` | Result | Wall s | Peak RSS MB | Peak spill MB |
|---|---|---|---|---|---|
| 10M | 4GB | **Out of Memory** (`could not allocate block of size 76.5 MiB`), 3/3 attempts | 44–49 | ~4,400 | ~11,100 |
| 10M | 8GB | **Out of Memory** (`failed to pin block`) | 51.9 | 8,364 | 8,947 |
| 1M | 4GB | PASS (contract + PyIceberg sample), 2 snapshots / 1 commit, 16 position delete files | 15.2 | 1,713 | 0 |

### Baseline: PyIceberg `table.upsert` vs DuckDB SCD1 MERGE (`upsert_baseline.py`)

Unpartitioned target, batch = half updates, half inserts:

| Target / batch | Engine | Wall s | Peak RSS MB | Snapshots | Data / delete files after | Result rows correct |
|---|---|---|---|---|---|---|
| 1M / 100k | PyIceberg upsert | 7.3 | 523 | 3 | 4 / 0 (copy-on-write) | yes |
| 1M / 100k | DuckDB MERGE | 1.2 | 220 | 2 | 4 / 1 | yes |
| 10M / 1M | PyIceberg upsert | 101.5 | 1,865 | 3 | 14 / 0 | yes |
| 10M / 1M | DuckDB MERGE | 2.8 | 489 | 2 | 16 / 1 | yes |

## How the merges work (`merge.py`)

- **SCD1:** `MERGE INTO t USING read_parquet(batch) s ON keys WHEN MATCHED AND <changed> THEN
  UPDATE SET <non-key cols> WHEN NOT MATCHED THEN INSERT`. Rust updates every matched row
  unconditionally. The change guard gives the same rows, and makes an identical rerun write
  nothing.
- **SCD2:** one MERGE whose source is `batch UNION ALL (changed batch rows with a NULL merge
  key)`. `ON t.key = s.merge_key AND t.is_current`: a matched, changed row closes the current
  version (`is_current=false, valid_to=ts`); the NULL-key copy never matches, so it is inserted
  as the new current version (`valid_from=ts, valid_to=NULL`); new keys are inserted; unchanged
  keys match with no action. Rust does the same in two Delta merges. One timestamp is used for
  both close and insert.
- Change predicate: `IS DISTINCT FROM` on all non-key business columns, the same truth table as
  Rust's `(t <> s) OR (t IS NULL AND s IS NOT NULL) OR (t IS NOT NULL AND s IS NULL)`.

Deliberately not done: compaction / maintenance, Delta, concurrent writers (out of scope in
#598), and no tuning of DuckDB settings beyond `memory_limit`, `temp_directory`, `threads`
and `max_temp_directory_size`.

## Reproduce

```bash
cd spikes/python/harness
docker compose up -d --wait && python3 bootstrap.py
cd ../s2-merge
uv venv .venv && uv pip install --python .venv/bin/python -r requirements.txt

.venv/bin/python check_golden.py                     # golden SCD1/SCD2 x 4 partition specs
.venv/bin/python scale.py seed --table g3_500m_b --rows 500000000
.venv/bin/python scale.py run  --table g3_500m_b --target-rows 500000000 --batch-rows 10000000 \
    --merges 5 --memory-limit 4GB --verify --full-count --rerun
.venv/bin/python scale.py run  --table g3_500m_b --target-rows 500000000 --batch-rows 10000000 \
    --first-merge 6 --merges 1 --memory-limit 2GB --verify
.venv/bin/python scale.py run  --table g3_500m_b --target-rows 500000000 --batch-rows 10000000 \
    --first-merge 7 --merges 1 --memory-limit 1GB --verify        # also 1500MB
.venv/bin/python scale.py seed --table s100m_bucket16 --rows 100000000 --partition bucket
.venv/bin/python scale.py run  --table s100m_bucket16 --target-rows 100000000 --batch-rows 10000000 \
    --merges 2 --memory-limit 4GB --verify --full-count --rerun   # also --memory-limit 8GB, --batch-rows 1000000
.venv/bin/python upsert_baseline.py --target-rows 1000000 --batch-rows 100000
.venv/bin/python upsert_baseline.py --target-rows 10000000 --batch-rows 1000000
```

DuckDB downloads its `iceberg`, `httpfs` and `avro` extensions on first use. During this run
Docker Hub answered 429 for about 25 minutes. The harness images were pulled from
`mirror.gcr.io` by the exact digests pinned in `../harness/README.md`, then tagged with the
compose names.

## Files

- `golden.py`: the fixture. `check_golden.py`: the golden + partition + rerun checks.
- `merge.py`: the two MERGE statements. `common.py`: harness connections (from `smoke.py`).
- `scale.py`: seed / staged batches / measured merges. `upsert_baseline.py`: the baseline.
- `results/`: JSON and text output of every run reported above. `logs/`: raw run logs.
- `results/scale-g3_500m-rss-invalid.json`, `logs/run-g3_500m-rss-invalid.log`: a first G3 run
  on an earlier 500M table. Its peak RSS from merge 2 on (~12–13 GB) is **invalid**: it was
  measured with `wait4`'s `ru_maxrss`, which keeps the parent's high-water mark across
  fork/exec. Its contract and parity results (all PASS) and wall times (72–86 s) are valid.
  The reported table is the rerun with `VmHWM`.
