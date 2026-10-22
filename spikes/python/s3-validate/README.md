# Spike S3 (#599): Python Polars validation path vs Rust `floe run` 0.7.0

Throwaway spike code for gates **G4** (performance) and **G5** (report parity) of #594.
Nothing here is shipped or imported by Floe.

| File | What it does |
|---|---|
| `floe_py.py` | Minimal Python/Polars validation path: CSV read, cast + mismatch handling, `not_null`, `unique` (per file and across files, composite keys), PII `hash`, Parquet accepted output, CSV rejected output (`__floe_row_index`, `__floe_errors`), run report and run summary. Unsupported config raises `NotImplementedError`. |
| `pii_hash.py` | PII `hash`: a hashlib loop and a columnar HMAC composed from DuckDB `sha256` over blobs. `python pii_hash.py --rows N` benchmarks both. |
| `gen_data.py` | Seeded data generator: CSV and benchmark `config.yml`. |
| `parity.py` | Runs configs through both sides and diffs reports, accepted rows and rejected CSVs. Exits 1 on any difference. |
| `bench.py` | Runs both sides `--repeat` times per size and reports median wall time and peak RSS. |
| `results/` | Raw output of the runs quoted below: benchmark JSON for polars 2.0.0 and 1.34.0, PII throughput, parity before the checks existed, and the final strict and `--ignore-writer-bytes` parity runs. |
| `fixtures/` | Parity configs: `casts` (every cast type plus chrono edge cases), `unique_pii` (two files, single and composite unique, NULL keys, HMAC), `mismatch` (missing and extra columns, `primary_key`, keyless SHA-256, `;` separator, quoted fields). |

## Reproduce

```bash
cd spikes/python/s3-validate
# Rust reference: the v0.7.0 release binary, checked against its .sha256
gh release download v0.7.0 -R OpenLakeForge/floe -p 'floe-v0.7.0-x86_64-unknown-linux-gnu.tar.gz*'
sha256sum -c floe-v0.7.0-x86_64-unknown-linux-gnu.tar.gz.sha256
tar -xzf floe-v0.7.0-x86_64-unknown-linux-gnu.tar.gz        # -> ./floe
uv venv -p 3.11 .venv && VIRTUAL_ENV=.venv uv pip install -r requirements.txt

.venv/bin/python parity.py --floe ./floe --ignore-writer-bytes # G5 verdict (writer byte metrics excluded, #594 ruling)
.venv/bin/python parity.py --floe ./floe                       # strict, for information
.venv/bin/python gen_data.py --rows 1000000 --out data
.venv/bin/python parity.py --floe ./floe data/1000000/config.yml   # parity at 1M rows
.venv/bin/python bench.py --floe ./floe --data data --threads 4    # G4: 1M, 5M, 20M x 3 runs
.venv/bin/python pii_hash.py --rows 5000000                    # PII throughput
```

## Benchmark contract

The same `config.yml` runs on both sides (`gen_data.py` writes it):
- CSV input with 9 columns: int64 ×3, string ×3, number, date and boolean.
- `policy.severity: reject`.
- `unique_keys: [[customer_id], [region, order_no]]`.
- `not_null` on `customer_id` and `amount`.
- PII `hash` with a key (HMAC-SHA256) on `email`.
- Parquet accepted output and CSV rejected output, with writes included in the timing.

The data has ~1% each of cast errors (`qty`), nulls (`amount`), duplicate `customer_id` and duplicate `(region, order_no)`. It is seeded through Polars' hash of the row index (`--seed 599`).

Deviations from the brief, both forced by Rust 0.7.0:
- **Rejected output is CSV, not Parquet.** The 0.7.0 binary rejects `sink.rejected.format: parquet` ("unsupported"; `io/format.rs:566` has only `csv`).
- **Row 0 always has a `qty` cast error.** Without it, 0.7.0 rejects the whole file as `csv_read_error` with `rows=0`: its header probe infers dtypes from the first ~100 rows, then fails on the first later bad value. Filed as #603. This moves one row out of ~10,000 bad ones and affects only Rust's ability to run at all.

Same machine, same thread budget: `POLARS_MAX_THREADS=4` for both, and DuckDB `threads=4` (`pii_hash.py` reads the same variable). Runs alternate rust/python, each in a fresh process with its outputs deleted first. Peak RSS is `ru_maxrss` of the single child process (`RUSAGE_CHILDREN`, measured from a fresh wrapper per run). Both sides run as one process; DuckDB is in-process.

## Environment

| | |
|---|---|
| CPU | Intel(R) Xeon(R) Processor @ 2.10GHz, 4 vCPU (1 thread/core) |
| Memory | 15.7 GB, no swap |
| OS | Ubuntu 24.04.5 LTS, Linux 6.18.44 x86_64, glibc 2.39 |
| Rust side | `floe 0.7.0 (5832a75)` release binary (Rust polars 0.52.0), sha256 `570706b0…bf88d84` verified |
| Python side | CPython 3.11.17, polars 2.0.0, pyarrow 26.0.0, duckdb 2.0.0, pyyaml 6.0.3 |

## Results

### G4: performance (median of 3 runs; all individual runs listed)

| Rows | Rust wall | Python wall | Wall ratio | Rust peak RSS | Python peak RSS | RSS ratio | Counts identical |
|---:|---:|---:|---:|---:|---:|---:|:-:|
| 1M | 4.75 s (5.48, 4.75, 4.36) | 2.09 s (2.09, 2.02, 2.15) | **0.44×** | 783 MB (787, 779, 783) | 801 MB (801, 829, 796) | **1.02×** | yes |
| 5M | 29.89 s (34.75, 29.79, 29.89) | 9.08 s (9.55, 8.97, 9.08) | **0.30×** | 3,339 MB (3331, 3365, 3339) | 2,514 MB (2367, 2584, 2514) | **0.75×** | yes |
| 20M | 143.43 s (158.03, 143.43, 143.31) | 43.46 s (43.78, 43.46, 43.37) | **0.30×** | 12,932 MB (12933, 12931, 12932) | 7,707 MB (7635, 7799, 7707) | **0.60×** | yes |

Counts at 20M on both sides: rows 20,000,000, accepted 19,215,038, rejected 784,962, errors 796,827.
The machine handled 20M rows on both sides. Rust's 12.9 GB peak is close to the 15.7 GB of RAM.

**G4: PASS.** Python is ≤ 1.3× Rust on wall time and peak RSS at every size: 0.30–0.44× wall, 0.60–1.02× RSS.

Why Python is faster here. The cause is Rust's code paths, not the language. Rust's own phase timings at 5M (`FLOE_PERF_PHASE_TIMINGS=1`, idle machine, entity elapsed 19.5 s):

| Phase | ms |
|---|---:|
| checks_validation | 9,723 |
| write_accepted | 3,866 |
| accept_reject_split | 2,983 |
| read_parse | 1,668 |
| write_rejected | 99 |

- `checks_validation` is mostly row loops in Rust: the `unique` check builds a `CompositeKey` per row into a `HashSet` (`checks/unique.rs:181-201, 371`), `build_errors_formatted_expr` walks every row (`checks/mod.rs:416-424`), and the boolean cast is a per-row closure (`io/format.rs:676`). The Python path does these as Polars expressions (`is_first_distinct`, joins, `concat_str`).
- `accept_reject_split` includes PII: Rust computes HMAC per value on one thread (`run/entity/pii.rs:145-160`). Python runs it in DuckDB on 4 threads.
- Memory: Rust keeps the raw string frame, the typed frame, a `Vec<bool>` and `Vec<Option<String>>` per row, and the `HashSet` of all keys alive together.
- In that perf run, Rust wrote the run summary 4.6 s after the entity finished (19.5 s). The benchmark's 5M median of 29.9 s also includes process start-up, the header probe and process exit, which the phase timer doesn't cover.

Engine-version control: Python Polars 1.34.0 bundles the same Rust polars 0.52 engine as the binary. Same contract, 3 runs per size:

| Rows | Rust wall | Python wall (polars 1.34.0) | Wall ratio | Rust peak RSS | Python peak RSS | RSS ratio |
|---:|---:|---:|---:|---:|---:|---:|
| 5M | 25.76 s (25.76, 25.81, 25.60) | 10.73 s (10.73, 12.38, 10.70) | 0.42× | 3,356 MB | 3,573 MB (3606, 3573, 3567) | 1.06× |
| 20M | 148.39 s (148.39, 152.54, 148.26) | 57.92 s (61.81, 57.92, 57.36) | 0.39× | 12,931 MB | 12,745 MB (12745, 12747, 12446) | 0.99× |

With the same engine, Python is still ~2.5× faster: the time comes from Rust's row loops and single-threaded HMAC, not from Polars versions. Memory is at parity, though, with 1.06× and 0.99×. **The RSS advantage in the main table comes from Polars 2.0, not from Python.** On the 0.52 engine, G4 still passes on memory, with little margin: 1.06× at 5M against the 1.3× threshold. Parity (`parity.py --ignore-writer-bytes`) also holds on polars 1.34.0.

### PII `hash` throughput (HMAC-SHA256; `pii_hash.py`, 4 threads)

| Rows | hashlib loop HMAC | DuckDB HMAC | hashlib SHA-256 (no key) | DuckDB SHA-256 (no key) |
|---:|---:|---:|---:|---:|
| 1M | 378,785 rows/s | 1,540,354 rows/s | 1,252,241 rows/s | 6,026,622 rows/s |
| 5M | 344,342 rows/s | 2,368,154 rows/s | 1,381,601 rows/s | 8,562,122 rows/s |
| 20M | 363,737 rows/s | 2,533,295 rows/s | 865,669 rows/s | 8,100,514 rows/s |

At 20M rows, HMAC takes 55.0 s with the hashlib loop and 7.9 s in DuckDB. The hashlib loop alone would be longer than the Python path's whole 43.5 s run.

**Method chosen: DuckDB (`sha256($opad || unhex(sha256($ipad || encode(v))))`).** It uses only approved dependencies and is the faster of the two at every size measured.
Byte-identical output is checked three ways:
- against Rust on the `unique_pii` fixture, accepted and rejected (`parity.py`);
- against Rust on all 1M rows of the benchmark data (`parity.py data/1000000/config.yml`);
- against the hashlib loop on every benchmark row (`identical_to_hashlib=True`).

Keys longer than 64 bytes are hashed first, per RFC 2104, and checked against hashlib.

### G5: report parity (`parity.py`)

Configs: `example/standalone/config.yml`, `fixtures/casts`, `fixtures/unique_pii`, `fixtures/mismatch`, and the 1M-row benchmark config.

Compared on every config:
- every report field except run ids, timestamps, durations and absolute paths, with key order included;
- accepted rows: schema and values;
- rejected CSVs, byte for byte.

Final strict run (`parity.py`, exit 1):

```
DIFF example/standalone/config.yml
    -    "total_bytes_written": 777,
    -    "avg_file_size_mb": 0.0007410049438476562,
    +    "total_bytes_written": 859,
    +    "avg_file_size_mb": 0.0008192062377929688,
DIFF fixtures/casts/config.yml                      2738 -> 2846 bytes (same two fields)
DIFF fixtures/unique_pii/config.yml                 2725 -> 2819 bytes (same two fields)
DIFF fixtures/mismatch/config.yml                   2654 -> 2754 bytes (same two fields)
DIFF data/1000000/config.yml                        43193095 -> 43535520 bytes (same two fields)
0/5 configs identical
```

These two fields are the only differences: no accepted, rejected or other report line differs. With `--ignore-writer-bytes` (exit 0): `5/5 configs identical`.

Before the error columns existed (`validate_file` accepting every row), the same script reported `0/4 configs identical` and exited 1. That was the test that had to fail first.

**G5: PASS** with writer-dependent byte metrics excluded, per the [ruling on #594](https://github.com/OpenLakeForge/floe/issues/594). `parity.py --ignore-writer-bytes` gives `5/5 configs identical`: accepted rows, rejected rows (`__floe_row_index`, `__floe_errors` byte for byte), every count, rule summary, unique-constraint sample and every other report field match.

The strict run is kept for information. Two report fields differ there: `accepted_output.total_bytes_written` and `avg_file_size_mb`.
- **Cause:** the two sides use different Parquet writer builds. Python Polars writes `created_by = "Polars (python) version 2.0.0 (build …)"` where Rust writes `"Polars"`, plus dictionary-page offsets. Compressed column chunks are the same size on the fixtures. At 1M rows the files differ by 0.8% (Rust 43,193,095 B vs Python 43,535,520 B).
- **Can't close it here:** neither Polars nor pyarrow lets the caller set `created_by`. The same field would also change between two Rust releases that bump Polars.
- **Ruled out of the contract on #594:** byte-size metrics are recorded but not compared, in this spike and in the future `conformance/` suite.

## Parity notes (what the Python path had to reproduce)

- **Casts.** Rust reads every column as a string, then casts with Polars 0.52 `NonStrict`. Polars 2.0 removed string→date and string→datetime casts, so `floe_py.py` re-expresses the 0.52 kernels:
  - date: chrono `NaiveDate::from_str`;
  - datetime: chrono's lenient partial parse of `%Y-%m-%dT%H:%M:%S%.f%:z`. Trailing garbage is accepted, the offset is ignored, minutes are required, seconds are optional and `:60` rolls over.

  Both are regexes plus `str.to_date`/`str.to_datetime`, checked on `fixtures/casts`. This is a real porting cost: a Python core needs a conformance table for these.
- **Error order** per row: `not_null` in required-column order, then `cast_error` in declared order, then `unique` in constraint order. A row can carry both `not_null` and `cast_error` for the same column.
- **Unique.** First occurrence wins, across files in sorted file order. Rows with any NULL key component are skipped. A row rejected for another reason still claims its key. The key comes from the typed value, so a cast failure means NULL, so the row is skipped. Samples: top 5 by count, ties broken by the `Debug` string of Rust's `CompositeKey` enum (`I64(10)` sorts before `I64(2)`). Zero counts and empty samples are omitted.
- **PII** is applied to the rejected rows too. `docs/pii.md` says the opposite (filed as #604).
- **Rejected parts.** The first file of a run writes `part-00000.csv`; later files append with uuid names.

## Not done (scope)

- Policies `warn` and `abort`.
- `normalize_columns` and non-default mismatch policies.
- PII strategies other than `hash`.
- Non-CSV inputs and write modes other than `overwrite`.
- History-aware unique (#600) and sinks (#597, #598).
- Accepted buffer flushes: one flush per entity here, while Rust flushes every 256 MB across files (`ponytail:` note in `write_accepted`).
- Float unique keys: Rust compares bit patterns; not handled.
