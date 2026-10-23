# Decision — Polars as DataFrame engine + Arrow as the inter-layer wire format

> **Superseded in part** by [`python-core-for-olf.md`](python-core-for-olf.md) (2026-10-23): the Python core exchanges Arrow data through the C Data Interface, so the Rust-specific points below (crate versions, `RecordBatch` types) apply only to the Rust core.

## Context

Floe needs to read heterogeneous file formats (CSV, JSON, Parquet, ORC, Avro, XLSX, XML), validate rows against a schema, and write to structured sinks (Parquet files, Delta Lake tables, Iceberg tables). A DataFrame engine is required for column-level type casting, null checks, and cross-row uniqueness evaluation.

The pipeline has a natural seam between the validation layer and the sink layer. Each sink library (`deltalake`, `iceberg`) speaks Apache Arrow — they accept `RecordBatch` streams natively. The question was: what is the internal representation, and how does data cross the seam?

## Decision

Use **Polars** as the single DataFrame engine inside `floe-core`. At the sink boundary, convert Polars DataFrames to **Apache Arrow RecordBatches** and pass them to the sink library directly.

## Rationale

**Why Polars:**
- Rust-native — no JVM, no GC, sub-millisecond startup.
- Batteries included: first-class readers for all supported input formats, with Polars handling low-level format parsing so Floe only adds a validation layer on top.
- LazyFrame API enables predicate and projection pushdown when reading Parquet or Delta for uniqueness seeding — only the columns needed for the check are materialised.
- The Arrow memory model is Polars' native columnar layout, so conversion to Arrow RecordBatches is zero-copy.

**Why Arrow at the sink boundary:**
- Both `deltalake` and `iceberg-rust` accept `RecordBatch` streams as their write interface. Passing Arrow directly avoids any intermediate serialisation.
- Keeping Arrow as the boundary contract means the sink libraries can evolve independently of Polars. If a future sink library does not speak Arrow, the conversion layer is isolated to that sink's implementation file.
- The same Arrow schema is used for schema validation between what Polars inferred and what the sink's existing table expects — mismatch detection happens at the type system level, not via string comparison.

## Consequences

All data flowing through the validation pipeline is `polars::DataFrame`. At write time, format writers (in `io/write/`) convert to Arrow and hand off to the sink library. No other layer of the codebase is aware of Arrow directly.
