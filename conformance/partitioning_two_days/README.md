# partitioning_two_days

D28 (#883): `source.partitioning`. Two days, one hash batch each (`2026/10/24/f9e8`, `2026/10/25/a1b2`): 2026-10-24 loads first although its batch
sorts last by name, and every row gets the `ingest_date` and `batch_id` of its folder. `id` 1 on both days is
accepted (the primary key is `[id, ingest_date]`); the second `id` 2 of 2026-10-24 is rejected. One batch per day:
the order of two batches of one day follows their modification times, which a git checkout does not keep.

Rust 0.7 rejected the config (`unknown field source.partitioning`); the case runs with `--log-format json`.
