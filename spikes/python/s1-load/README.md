# Spike S1: bounded-memory Iceberg load, one commit, crash-safe retry (#597)

Throwaway spike code for gates G1 and G2 of #594. It reads a CSV in bounded chunks with Python
Polars, casts every chunk to a fixed contract schema (no validation), and writes the chunks to
Iceberg through Polaris with PyIceberg in **one catalog commit**. The commit is tagged with
`floe.run_id` and `floe.input_hash` snapshot properties. A rerun that finds its run id skips the
write and only writes the "state promoted" marker, which stands in for Floe's incremental state.

| File | What it does |
|---|---|
| `common.py` | Harness connection settings, the contract schema and the casts |
| `gen_csv.py` | Deterministic CSV generator (`id` = start..start+N-1, so duplicates are easy to find) |
| `load.py` | The loader: four write paths, run-id guard, crash-point pauses, memory stats |
| `check.py` | Row count, distinct ids, snapshots, and orphaned files (S3 objects no metadata references) |
| `crash.py` | G2 driver: seed, run to a crash point, `kill -9`, rerun with the same run id, check |
| `g1.sh` | G1: runs `load.py` in `python:3.11-slim` under `docker run --memory=<limit> --memory-swap=<limit>` |

## Results

### G1: bounded memory (4 GB limit)

**Scaled down: 17.8 GB CSV (152,000,000 rows), not 20 GB.** The sandbox had 27 GB free disk.
A 20 GB attempt (19.9 GB, 170M rows, `single` path) was **not OOM-killed**, but it failed at
data file 150 of ~170. SeaweedFS turns its volumes read-only below 1% free disk (2.5 GB on this
252 GB volume): `volume 7 is read only ... No writable volumes`, `percent_free:0.947`. That run
recorded no memory stats. 17.8 GB is the largest size that fit beside its Parquet copy.

| Path | Limit | Rows | OOM | Exit | Peak anon RSS | Peak RSS (VmHWM) | cgroup peak | Catalog commits | Snapshots | Wall |
|---|---|---|---|---|---|---|---|---|---|---|
| `single` | 4 GB | 152,000,000 | no | 0 | 878 MB | 4,056 MB | 4,096 MB | 1 | **1** | 127.5 s |
| `chunked` | 4 GB | 152,000,000 | no | 0 | 910 MB | 4,059 MB | 4,096 MB | 1 | **152** | 149.7 s |
| `single` | 1.5 GB | 152,000,000 | no | 0 | 895 MB | 1,514 MB | 1,536 MB | 1 | **1** | 127.5 s |

- Each run starts on a fresh table (a first full load), with chunks of 1,000,000 rows.
  `check.py` confirms exactly 152,000,000 rows, 152,000,000 distinct ids and 0 orphaned files
  for every run.
- **Read the RSS columns together.** Polars memory-maps the CSV, so VmHWM and the cgroup peak
  include file-backed pages that the kernel reclaims under the limit. They grow until they reach
  the limit, whatever it is. The working set is anonymous memory (`RssAnon`, sampled every
  100 ms). It stays at 373–912 MB from the first chunk to the last, and it is the same for a 1 GB
  file (~850 MB). The 1.5 GB run shows that headroom: the same 17.8 GB load passes at 1.5 GB.
- `g1.sh` drops the host page cache before each run so that the container's cgroup is charged for
  the CSV pages it reads. Without that, the cgroup peak for a 1 GB file read 880 MB instead of
  2,078 MB.

### Commits and snapshots per run

Measured on a 1.16 GB CSV (10M rows, 10 chunks), with an overwrite run against a table that
already holds data. "Commits" counts both the client's `commit_table` calls and the new
`metadata.json` versions in the table's metadata log. They agree on every run.

| Path | How it writes | Overwrite: commits / snapshots | Append: commits / snapshots | Peak anon RSS |
|---|---|---|---|---|
| `chunked` | `tx.delete(AlwaysTrue())` + `tx.append(chunk)` per chunk | 1 / **N+1** (`delete` + N `append`) | 1 / **N** | ~860 MB |
| `stream` | `tx.delete(AlwaysTrue())` + one `tx.append(RecordBatchReader)` | 1 / 2 (`delete`, `append`) | 1 / 1 | ~1,500 MB |
| `single` | one snapshot producer (`overwrite()` / `fast_append()`), every chunk's data files added to it | 1 / **1** (`overwrite`) | 1 / **1** | ~870 MB |
| `add_files` | stage chunk Parquet under `data/staging/`, then `tx.delete` + `tx.add_files` | 1 / 2 (`delete`, `append`) | 1 / 1 | ~850 MB |

- Every path makes **exactly one catalog commit**. Only `single` makes exactly one snapshot in
  both modes. #594's G1 threshold asks for one snapshot, so `chunked` (the shape #597 proposed)
  fails that clause. It writes one snapshot per chunk (152 in the G1 run), although they all land
  in one atomic commit.
- `overwrite` on an empty table creates no `delete` snapshot. PyIceberg warns `Delete operation
  did not match any records`.
- `stream` uses only public API. Its memory is higher because PyIceberg bin-packs 512 MB of Arrow
  (`write.target-file-size-bytes`) before it writes a file.
- **Write path chosen: `single`.** It is the only path that meets "one commit, one snapshot" for
  both overwrite and append, and its memory matches `chunked`. Cost: it uses PyIceberg internals,
  `pyiceberg.io.pyarrow._dataframe_to_data_files` and `Transaction._scan()`. These are the same
  calls `Transaction.append` / `overwrite` make, but a PyIceberg upgrade can break them. If the
  port must stay on public API, use `stream`: one snapshot for append, two (`delete` + `append`)
  for overwrite.
- `add_files` (the fallback) was not needed for memory or for the single commit. It was measured
  anyway. It has the same snapshot shape as `stream`, and a killed run leaves its staged Parquet
  behind.

### G2: `kill -9` and rerun (same run id)

Measured on a 1.16 GB CSV (10,000,000 rows) with chunks of 1,000,000 rows. Overwrite tables are
first seeded with the same file. Append tables are seeded with 2,000,000 rows of disjoint ids, so
the expected total is exactly 12,000,000. The crash points:

- **read:** after the first chunk has been read and cast, before any data file is written by
  that chunk;
- **pre-commit:** after 3 chunks have gone to the writer, before `commit_transaction()`;
- **post-commit:** after the commit, before the state marker is written.

| Mode | Crash point | Rows after kill | Rerun | Rows / distinct / expected | Orphaned files (data + metadata) by path: `single` · `chunked` · `stream` · `add_files` |
|---|---|---|---|---|---|
| overwrite | read | 10M (seed intact) | written | 10M / 10M / 10M ✅ | 0+0 · 0+2 · 0+2 · 0+0 |
| overwrite | pre-commit | 10M (seed intact) | written | 10M / 10M / 10M ✅ | 3+0 · 3+8 · 0+2 · 3+0 |
| overwrite | post-commit | 10M (new data) | **skipped** (run id found) | 10M / 10M / 10M ✅ | 0 · 0 · 0 · 0 |
| append | read | 2M | written | 12M / 12M / 12M ✅ | 0 · 0 · 0 · 0 |
| append | pre-commit | 2M | written | 12M / 12M / 12M ✅ | 3+0 · 3+6 · 0+0 · 3+0 |
| append | post-commit | 12M | **skipped** (run id found) | 12M / 12M / 12M ✅ | 0 · 0 · 0 · 0 |

All 24 runs (4 paths × 6 cases) give the exact row count and no duplicates.

**Test that must fail first** (`crash.py --negative`, path `single`): append, killed after the
commit and before the marker, rerun with `--no-guard` → **22,000,000 rows, 12,000,000 distinct**:
10M duplicates. With the guard on, the same case gives 12,000,000 / 12,000,000. The run-id
guard is what prevents the duplicates.

**Orphaned files** are left only by runs killed before their commit. The killed run's catalog
state is untouched, but files it already wrote stay in S3:

- data files: 3 per pre-commit kill (the chunks already written) for `single`, `chunked` and
  `add_files` (staged Parquet under `data/staging/<run_id>-<attempt>/`). `stream` had flushed
  nothing yet because of its 512 MB bin-packing buffer.
- metadata files (manifest + manifest list): `chunked` and `stream` write them per snapshot
  *inside* the transaction, before the catalog commit. So even a kill "during read" leaves 2 from
  the `delete` that runs first. `single` and `add_files` write them only at commit and leave none.

Cleaning orphans up is table maintenance (out of scope, see the open question on #594).
`check.py` counts them as S3 objects under the table location that no metadata version,
manifest list, manifest or manifest entry (including deleted entries) references.

### What else turned up

- **`LazyFrame.collect_batches` does not apply backpressure.** With a slow consumer, the
  streaming engine reads ahead through the whole file. RSS grows with file size, but the growth is
  memory-mapped CSV pages (`RssFile`), not heap: `RssAnon` stayed at ~600–700 MB on a 3 GB
  file. `sink_batches` with a blocking queue behaves the same. Under a cgroup limit those pages
  are reclaimed (the 1.5 GB run). They still show up in naive RSS metrics, so the port should
  monitor anonymous memory.
- Polars 2.0 has no string → date/datetime cast (already recorded on #594). `common.py` uses
  `str.to_date` / `str.to_datetime` with explicit formats.
- `input_hash` fingerprints file name, size and mtime, not content (a `ponytail:` comment
  marks it). A retry under a new run id is not caught by this guard. Floe's claimed-file state
  would have to give a retry the same run id, or the guard would have to match on the hash.

## Environment

| | |
|---|---|
| CPU | 4 vCPU, Intel Xeon @ 2.10 GHz |
| RAM | 16 GB host, no swap; loader container limited by `--memory=X --memory-swap=X` (cgroup v1) |
| Disk | 252 GB volume, 27 GB free at start |
| Kernel / Docker | Linux 6.18.44, Docker 29.9.1, Compose v5.6.0 |
| Python | 3.11.17 (host venv and `python:3.11-slim`, `sha256:cce12316475d7d56d605f76e1d811d0dc4e7227f2d4afd29951521264f504ff8`) |
| Python packages | pyiceberg 0.12.0, pyarrow 25.0.1, polars 2.0.0 (polars-runtime-32 2.0.0), boto3 1.43.117 |
| Harness | `../harness` as on `chore/596-polaris-s3-harness`: Polaris 1.8.0, SeaweedFS 4.48 |

## Reproduce

From this directory, with the harness up (`cd ../harness && docker compose up -d --wait && python3 bootstrap.py`):

```bash
uv venv venv -p 3.11 && VIRTUAL_ENV=$PWD/venv uv pip install pyiceberg==0.12.0 pyarrow==25.0.1 polars==2.0.0 boto3==1.43.117
PY=venv/bin/python; DATA=/path/to/data   # needs ~1.3x the CSV size free, plus SeaweedFS's 1% reserve

# Snapshots per path (run each path twice per table so the second overwrite replaces data)
$PY gen_csv.py $DATA/g2.csv --gb 1
$PY gen_csv.py $DATA/g2-seed.csv --gb 0.2 --start 1000000000000
for path in chunked stream single add_files; do
  $PY load.py --csv $DATA/g2.csv --table paths_$path --mode overwrite --path $path --run-id r1 --state-dir state
  $PY load.py --csv $DATA/g2.csv --table paths_$path --mode append    --path $path --run-id r2 --state-dir state
  $PY load.py --csv $DATA/g2.csv --table paths_$path --mode overwrite --path $path --run-id r3 --state-dir state
done

# G2 (exits non-zero if any case has wrong rows or duplicates; --negative must report ok: false)
$PY crash.py --csv $DATA/g2.csv --seed-csv $DATA/g2-seed.csv --path single --state-dir state-neg --stats g2.jsonl --negative
for path in single chunked stream add_files; do
  $PY crash.py --csv $DATA/g2.csv --seed-csv $DATA/g2-seed.csv --path $path --state-dir state-$path --stats g2.jsonl
done

# G1 (root, to drop the page cache). Tear the harness down between runs to free disk.
$PY gen_csv.py $DATA/g1.csv --gb 17.8
./g1.sh venv/lib/python3.11/site-packages $DATA/g1.csv g1_single overwrite single 4g
$PY check.py --table g1_single --expected-rows 152000000
```
