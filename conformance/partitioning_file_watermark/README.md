# partitioning_file_watermark

D30 (#885): `incremental_mode: file` with `source.partitioning` (`lookback_days: 1`). Three days, one batch each
(`2026/10/20`, `2026/10/23`, `2026/10/24`). The first run ingests all three and sets the state's watermark to
2026-10-24. The second run lists only from 2026-10-23: `2026/10/20` is not listed at all, the two files in the window
are skipped as `already_ingested`, and the run logs one `warn` event naming the window. `out/accepted` still holds the
first run's rows (the second run writes nothing).

The `runs` file cannot add files between runs, so a new batch and a late file are covered by
`tests/unit/run/test_run_partitioning.py` (`test_d30_*`).

Rust 0.7 rejects the config (`unknown field source.partitioning`), so `expected/` holds its failed last run
(run with `--log-format json` so it records); `expected_python/` holds the Python outputs of the last run.
