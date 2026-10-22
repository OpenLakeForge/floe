# Spike S4 (#600): exact unique check against a large Iceberg history, out of core

Throwaway spike code (parent #594, gate G6). It checks a staged Parquet batch against the key
columns of an Iceberg table in Polaris, in DuckDB, with `memory_limit` and a temp directory set.
The result must match Floe's Rust unique check on append exactly.

## Files

| File | What |
|---|---|
| `reference.py` | Brute-force Python version of the Rust contract (`checks/unique.rs` + `io/unique_seed/mod.rs`) |
| `unique.py` | The DuckDB check: one `COPY (...) TO` query, writes the flagged rows `(row_idx, kind)` to Parquet |
| `check_golden.py` | Small-scale exactness: `unique.py` vs `reference.py` on Iceberg tables in Polaris |
| `scale.py` | `seed` / `stage` / `verify` / `run` at 100M and 500M history rows |
| `summarize.py` | Prints `results/scale-*.jsonl` as the results table below |
| `common.py` | Harness connections, adapted from #598's `s2-merge/common.py` (PyIceberg imported lazily) |
| `results/`, `logs/` | Raw outputs of every run below, and the DuckDB profiles of 4 runs (`results/plans/`) |

## Contract matched (Rust `UniqueTracker` seeded from the target)

- On append, every row already in the target seeds the tracker. A target row with a NULL in any
  key column is not seeded.
- An incoming row with a NULL in any key column is skipped: it is never flagged and never seen.
- Incoming rows are visited in order. Every row whose key is in the target is flagged `target`,
  including the first occurrence. For a key not in the target, the first occurrence passes and
  every later one is flagged `batch`.
- Composite keys compare column by column. Floats compare by bit pattern: `-0.0` != `0.0`, and
  `NaN` == `NaN`.

`unique.py` filters out NULL keys and computes the distinct batch keys. It inner-joins them with
the history's key columns (a projection only) to find the keys already in the target, and uses
`row_number() OVER (PARTITION BY key ORDER BY file_row_number)` for the in-batch duplicates.
DuckDB's `=` treats `-0.0` and `0.0` as equal, so float key columns are compared through
`::VARCHAR`. ponytail: every NaN maps to `'nan'`, so NaNs with different payload bits match here
but not in Rust.

The DuckDB profiles show the shape of the join. The hash table is built on the distinct batch
keys (about 10M), and the Iceberg scan is the probe side. It streams the key columns only
(`Projections: id`, or `account, seq`) of every data file.

## Exactness

- **The test that must fail first:** `results/golden-fail-first.txt`, 46/46 FAIL, with
  `unique.unique_check` stubbed to `raise NotImplementedError`.
- **After the anti-join:** `results/golden.txt`, 46/46 PASS. The cases:
  - a hand-written fixture whose expected flags are written out in `check_golden.py`. The
    reference is checked against them first. The fixture covers duplicate keys inside the
    history, NULL in the history and in the batch for each key column, `target` vs `batch`, case
    sensitivity, `-0.0` vs `0.0` and `NaN`;
  - 5 randomised fixtures: 3,000 history rows and 2,000 batch rows, ~10% NULLs per column,
    duplicates on both sides;
  - each fixture with keys `[id]`, `[account, seq]`, `[id, account]` and `[f]` (a double), in
    both an unpartitioned and a `bucket(4, id)` table.
- **At scale, every row of the 10M batch is checked, not a sample.** `scale.py verify`
  recomputes the expected flags without DuckDB. PyArrow handles the batch (NULL filter, first
  occurrence per key), and a PyIceberg stream of the whole history's key columns is probed
  against the batch's distinct keys. Every `run` is compared row for row (`row_idx` and `kind`)
  with that output. At 1M history rows, the verifier itself was checked against `reference.py`
  (`verify --python-reference`: match for `[id]` and `[account, seq]`).
- Every run that completed was **exact**: 57 of 57, with 110,178 flagged rows (100,148
  `target`, 10,030 `batch`) for both keys. No run gave a wrong result. The failures are all
  DuckDB errors, listed in the table, and they write nothing.

## Workload

- **History:** 100M and 500M rows. Columns: `id BIGINT`, `account VARCHAR`, `seq BIGINT`, plus
  two payload columns (`amount DOUBLE`, `name VARCHAR`) that the check must not read. Row i has
  `id = i`, `account = 'a' || (i % 1000)` and `seq = i // 1000`. Seeded with DuckDB `INSERT`
  in chunks, because PyIceberg 0.12 cannot append to a bucketed table without `pyiceberg-core`.
- **Three layouts,** to separate the file-size confound #598 left open:

  | Layout | Seed chunk | Data files (100M / 500M) | Rows per file (median) | Size (100M / 500M) |
  |---|---|---|---|---|
  | unpartitioned | 50M | 118 / 590 | 860,160 | 967 / 4,836 MB |
  | `bucket(16, id)`, big files | 50M | 32 / 160 | ~3.1M | 1,105 / 5,526 MB |
  | `bucket(16, id)`, small files | 12.5M | 128 / 640 | ~0.78M | 1,105 / 5,526 MB |

- **Batch:** 10,000,000 rows, deterministic (`scale.py stage`). It has 1.0% collisions with
  the history, 0.1% in-batch duplicates, and 0.1% near misses (a history `seq` with an account
  the history doesn't have, which must not be flagged on `[account, seq]`). It also has 0.1%
  NULLs in each of `id`, `account` and `seq`. Staged Parquet: 180 MB.
- **Keys:** `[id]` (the bucket column) and `[account, seq]` (composite, not the bucket column).
- **Per run:** a child process with DuckDB only (no PyIceberg), 4 threads, `temp_directory` set,
  and `max_temp_directory_size` capped by the free disk (15GB at 100M; 8GB at 500M; 6GB for the
  last layout). Peak memory is the child's `VmHWM` and its peak `RssAnon`, polled every 100 ms,
  not `wait4`'s `ru_maxrss`. Spill is the peak size of the temp directory, polled every 100 ms.
  `memory_limit` is swept down from 4GB until the check fails.

## Results

Wall time includes the child's start-up and the catalog `ATTACH`. DuckDB's `GB`/`MB` are decimal,
so `VmHWM / limit` compares MiB with MiB. Spill sizes quoted as "GB" in this section are the
MiB figures from `results/summary.md` divided by 1,000 and rounded.

### Lowest working `memory_limit` (10M-row batch)

| History | Layout | `[id]` | `[account, seq]` |
|---|---|---|---|
| 100M | unpartitioned | 128MB (12 s, 1.6 GB spill) | 256MB (17 s, 2.4 GB spill) |
| 100M | bucket, big files | 128MB (12 s, 1.6 GB spill) | 256MB (20 s, 2.4 GB spill) |
| 100M | bucket, small files | 128MB (13 s, 1.6 GB spill) | 256MB (21 s, 2.5 GB spill) |
| 500M | unpartitioned | 128MB (45 s, 6.4 GB spill) | 1GB with an 8GB spill cap (49 s, 6.3 GB spill); 512MB with a 12GB cap (71 s, 8.6 GB spill) |
| 500M | bucket, big files | 128MB (43 s, 6.7 GB spill) | 1GB (58 s, 6.8 GB spill); 512MB **fails** even with a 12GB cap (`failed to offload data block ... 11.1 GiB/11.1 GiB used`) |
| 500M | bucket, small files | 1GB (lower limits not run: disk) | 2GB; 1GB fails at the 6GB spill cap (`5.5 GiB/5.5 GiB used`) |

At 64MB, `[id]` fails with `Out of Memory Error: failed to allocate data of size 16.0 MiB`. At
128MB, `[account, seq]` fails the same way (32.0 MiB). These are memory failures, not disk.
Below 1GB on `[account, seq]` at 500M, the failure is a full spill directory, so the minimum
depends on the free disk.

### Without spill (memory_limit 4GB / 2GB)

| History | Layout | Key | 4GB: wall s, VmHWM MiB | 2GB: wall s, VmHWM MiB |
|---|---|---|---|---|
| 100M | unpartitioned | `[id]` | 4.8, 1,445 | 4.4, 1,443 |
| 100M | unpartitioned | `[account, seq]` | 6.5, 1,978 | 7.5, 1,749 |
| 100M | bucket, big files | `[account, seq]` | 7.0, 2,110 | 8.2, 1,851 |
| 500M | unpartitioned | `[id]` | 18.6, 1,876 | 15.9, 1,878 |
| 500M | unpartitioned | `[account, seq]` | 24.4, 1,980 | 28.1, 1,721 |
| 500M | bucket, big files | `[id]` | 18.3, 1,946 | 15.9, 1,944 |
| 500M | bucket, big files | `[account, seq]` | 25.0, 2,698 | 31.0, 2,077 |
| 500M | bucket, small files | `[account, seq]` | 34.1, 2,704 | 39.2, 2,087 |

### Findings

1. **Exact:** 57 of 57 completed runs match the brute force on all 10M rows, for single and
   composite keys, at both history sizes and in every layout.
2. **Peak RSS against `memory_limit`:** RSS stays under the limit at 4GB in every
   configuration (VmHWM / limit 0.38–0.71). At 2GB the ratio is 0.76–1.09: over the limit for
   `[id]` at 500M bucketed (1,944–1,956 MiB) and for `[account, seq]` at 500M bucketed
   (2,077–2,087 MiB). At smaller limits RSS always goes over: 1.22–1.45× at 1GB, 1.65–1.78× at
   512MB, 1.93–2.28× at 256MB and 2.83–3.18× at 128MB. That is a roughly fixed 0.2–0.45 GiB on
   top of the limit (VmHWM ≈ 345–388 MiB at 128MB). Anonymous RSS is about 60–95 MiB below
   VmHWM (libraries). So a pod needs `memory_limit` + ~0.5 GiB.
3. **Unpartitioned vs bucketed:** unlike #598's MERGE, the bucketed history does not run out of
   memory. Wall time and memory match unpartitioned for `[id]`. For `[account, seq]` at 500M,
   bucketed needs +36% peak memory at 4GB (2,698 vs 1,980 MiB), spills more at 1GB (6.8 vs
   6.3 GB), and fails at 512MB with 11.1 GB of spill, where unpartitioned passes with 8.6 GB.
4. **File size is not the cause:** bucketed with ~0.78M-row files behaves like bucketed with
   ~3.1M-row files (100M: identical within noise; 500M `[account, seq]`: 2,704 vs 2,698 MiB at
   4GB). The cost comes from the bucketed layout itself, not from file size. The spike did not
   isolate the mechanism further.
5. **Memory is bounded by the batch, spill grows with the history.** Without spill, peak memory
   grows only modestly from 100M to 500M (`[id]`: 1,445 → 1,876 MiB). The hash table holds the
   batch keys. Once `memory_limit` is below what the hash table needs (~1.5–2 GB for 10M keys),
   DuckDB partitions the probe side to disk as well. So spill grows with the history: at 1GB,
   `[id]` spills 363 MB at 100M and 1,967 MB at 500M. At 128MB it spills 1.6 GB at 100M and
   6.4 GB at 500M. Spill does not stay flat as the history grows.

## Not measured / limits

- Only a 10M-row batch was measured. Unspilled memory scales with the batch's distinct keys,
  not the history, but no other batch size was run.
- Spill caps were set by the sandbox disk (about 30 GB free, shared with SeaweedFS and the
  tables), so no 500M run below 512MB was tried for `[account, seq]`. For the small-file
  bucketed 500M table only 4GB / 2GB / 1GB were run.
- Each configuration was run once (no repeats), so the wall times carry run-to-run noise.
- The report's unique *samples* (top 5 keys by count) are not produced. The output is the
  flagged rows and their kind, from which the counts follow.
- First-occurrence order is the row order of one staged file (`file_row_number`). A batch staged
  as several files needs a global row index.
- No Bloom or other approximate filter, and no persistent key index or sidecar (out of scope).

## Environment

- 4 vCPU Intel Xeon @ 2.10 GHz, 16 GB RAM, no swap, no cgroup memory limit. About 30 GB free
  disk at the start, shared by SeaweedFS, staging and spill. Linux 6.18.44, Docker 29.9.1.
- Python 3.11.17. **duckdb 1.5.6** (extensions: iceberg `890b78a9c`, httpfs `4bc690d`, avro
  `f9d5902`), pyiceberg 0.12.0, pyarrow 25.0.1. Polars and boto3 not used.
- Harness from #596: Polaris `apache/polaris:1.8.0` and SeaweedFS `chrislusf/seaweedfs:4.48`, at
  the digests pinned in `../harness/README.md`. Docker Hub answered 429 for SeaweedFS, so it was
  pulled from `mirror.gcr.io` by the same digest and tagged locally.

## Reproduce

```bash
cd spikes/python/harness && docker compose up -d --wait && python3 bootstrap.py && uv run smoke.py
cd ../s4-unique && uv venv .venv && uv pip install --python .venv/bin/python -r requirements.txt
.venv/bin/python check_golden.py                         # 46/46 PASS (stub unique.py first: 46/46 FAIL)

P=.venv/bin/python
$P scale.py seed --table h100m      --rows 100000000
$P scale.py seed --table h100m_b16  --rows 100000000 --partition bucket
$P scale.py seed --table h100m_b16s --rows 100000000 --partition bucket --chunk 12500000
$P scale.py seed --table h500m      --rows 500000000
$P scale.py seed --table h500m_b16  --rows 500000000 --partition bucket
$P scale.py seed --table h500m_b16s --rows 500000000 --partition bucket --chunk 12500000
# per table T with history size H (spill cap 15GB at 100M, 8GB at 500M, 6GB for h500m_b16s):
$P scale.py stage --table T --history-rows H --batch-rows 10000000
for C in "id" "account seq"; do
  $P scale.py verify --table T --cols $C
  $P scale.py run --table T --cols $C --stop-on-error --max-spill 8GB \
    --memory-limit 4GB --memory-limit 2GB --memory-limit 1GB --memory-limit 512MB \
    --memory-limit 256MB --memory-limit 128MB --memory-limit 64MB
done
$P scale.py run --table h500m     --cols account seq --memory-limit 512MB --max-spill 12GB
$P scale.py run --table h500m_b16 --cols account seq --memory-limit 512MB --max-spill 12GB
$P summarize.py                                          # results/summary.md
```

The full per-run table (71 runs) is in `results/summary.md`.
