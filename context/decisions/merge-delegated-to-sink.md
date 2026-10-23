# Decision — Merge logic is delegated to the sink library, not implemented in Floe

> **Superseded in part** by [`python-core-for-olf.md`](python-core-for-olf.md) (2026-10-23): Iceberg merges are now done by DuckDB `MERGE INTO`. The "Iceberg has no merge" rationale below applies only to the Rust core.

## Context

Some ingestion patterns require upsert or slowly-changing-dimension (SCD) semantics: instead of appending all incoming rows, you want to update existing records in the target table when a key matches, and insert new ones when it does not. This requires reading the existing target state, comparing keys, and producing an update plan.

The question was whether Floe should implement this merge logic itself (building a generic merge engine over Polars DataFrames) or whether it should delegate to the sink library.

## Decision

Merge logic is **owned entirely by the sink library**. Floe passes the validated incoming DataFrame and the user's merge configuration (key columns, SCD strategy) to the sink writer, which performs the merge using its own native mechanism.

For Delta Lake this means using the `deltalake` crate's `DeltaOps::merge` operation. For Parquet (plain files), merge write modes are not supported — the config validator rejects them at startup via `SinkFormat::supported_modes()`.

## Rationale

- **Iceberg has no merge.** The Apache Iceberg spec does not define a merge/upsert operation at the table level. The `iceberg-rust` crate has no merge primitive. Implementing a generic merge over Iceberg would mean Floe reading the entire existing table into memory, joining with the incoming data, and rewriting — this defeats the purpose of a table format and is not viable at scale. Iceberg therefore only supports `overwrite` and `append` write modes in Floe.
- **Delta's merge is battle-tested.** The `deltalake` crate exposes a merge builder that generates an optimised merge plan, handles transaction log atomicity, and produces correct results under concurrent writers. Reimplementing this in Floe would be duplicating work that the Delta maintainers have already solved correctly.
- **Each sink's merge semantics are format-specific.** Delta's ACID merge operates on Parquet part files with a transaction log. A hypothetical Iceberg merge would operate on snapshot manifests. There is no useful abstraction that spans both — a generic `MergeEngine` trait would either be too thin to be useful or too thick to be implementable without format-specific knowledge leaking up.

## Consequences

`MergeScd1` and `MergeScd2` write modes are only valid for the Delta sink. The config validator enforces this via the `SinkFormat::supported_modes()` registry — no hardcoded format-name checks. Adding merge support for a new sink requires only that sink's implementation to declare the modes as supported and handle them in its `write()` method.
