# partitioning_archive_full_refresh

D29 (#884): archive mode with `source.partitioning`. The first run archives `2026/10/24/f9e8/orders.csv` to
`archive/orders/2026/10/24/f9e8/orders__run-…__src-….csv`, keeping its partition sub-path; the second run
(`--full-refresh`) re-reads it from the archive and its rows keep `ingest_date = 2026-10-24` and `batch_id = f9e8`.

Rust 0.7 rejected the config (`unknown field source.partitioning`); the case runs with `--log-format json`.
