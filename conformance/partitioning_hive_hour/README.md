# partitioning_hive_hour

D28 (#883): `source.partitioning`. Hive-style folders with `{HH}`: `ingest_date` is a datetime (`2026-10-24 09:00`, then `13:00`), loaded in that order.

Rust 0.7 rejects the config (`unknown field source.partitioning`), so `expected/` holds its failed run
(run with `--log-format json` so it records); `expected_python/` holds the Python outputs, from
`check.py --impl python` on this case.
