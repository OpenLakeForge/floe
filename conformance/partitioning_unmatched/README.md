# partitioning_unmatched

D28 (#883): `source.partitioning`. `2026/10/24/orders.csv` has no batch folder, so it does not match the pattern: the run fails before the first
`entity_started`, with an error naming the file and the pattern, and nothing is written.

Rust 0.7 rejected the config (`unknown field source.partitioning`); the case runs with `--log-format json`.
