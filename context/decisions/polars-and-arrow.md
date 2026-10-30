# Decision — Polars as DataFrame engine + Arrow as the inter-library format

> The Rust-era version of this record (crate versions, `RecordBatch` hand-off to `deltalake` and
> `iceberg-rust`) is archived at [OpenLakeForge/floe-rust](https://github.com/OpenLakeForge/floe-rust).
> The Python core keeps the decision; see [`python-core-for-olf.md`](python-core-for-olf.md).

## Context

Floe reads heterogeneous file formats (CSV, JSON, Parquet, ORC, Avro, XLSX, XML), validates rows
against a schema, and writes Parquet files and Iceberg tables. Column-level casts, null checks and
cross-row uniqueness need a DataFrame engine, and the writers (PyIceberg, DuckDB) each have their
own data model.

## Decision

Use **Polars** as the single DataFrame engine for reading and validation. Data crosses to the
writers as **Apache Arrow** through the Arrow C Data Interface: Polars → pyarrow → PyIceberg for
overwrite and append, and Arrow / staged Parquet → DuckDB for merges. Unique checks against
table history stay in Polars (a semi-join against a local cache of the table's key columns).

## Rationale

- Polars has native readers for most input formats, so Floe only adds its validation layer on top,
  and its expressions keep the data path free of per-row Python loops.
- The Arrow C Data Interface does not depend on library versions, so Polars, pyarrow, PyIceberg and
  DuckDB upgrade independently (the Rust core had to move every `arrow` major in lockstep).
- One Arrow schema is the boundary contract: the Iceberg table's schema is compared with the
  validated batch's Arrow schema, not with strings.

## Consequences

Validation code works on `polars.DataFrame`. Only the writers (`src/floe/write/`,
`src/floe/iceberg/`) convert to Arrow or stage Parquet for DuckDB.
