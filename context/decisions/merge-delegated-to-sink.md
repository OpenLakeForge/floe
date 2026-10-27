# Decision — Merge logic is delegated to the sink library (archived)

Archived: this Rust-era decision (Delta merges through the `deltalake` crate, no Iceberg merge) is kept with the Rust code at [OpenLakeForge/floe-rust](https://github.com/OpenLakeForge/floe-rust). In 0.8, Iceberg merges are one DuckDB `MERGE INTO`; see [`python-core-for-olf.md`](python-core-for-olf.md).
