# Floe Documentation

Floe is a Polars-powered data contract runtime for reliable file ingestion. It
validates raw files or extracted datasets before they enter the trusted layer,
routing accepted rows to lakehouse sinks and rejected rows to quarantine with
audit reports.

## When Floe fits

Use Floe when you already have storage, an orchestrator, or a data platform, but
need a lightweight entry gate for file contracts and quality checks.

- Raw exports, partner feeds, API extracts, or object-storage drops need schema
  and quality validation before becoming trusted data.
- Existing platforms such as Databricks, Fabric-style lakehouses,
  Snowflake/Open Catalog, MotherDuck, Airflow, or Dagster need an embeddable
  ingestion gate rather than another platform.
- Extract/load tools such as dlt, ingestr, or Airbyte already move data, and
  Floe should validate and route the landed files or datasets.
- Accepted rows should land in Parquet, Delta Lake, Apache Iceberg, or DuckDB,
  while rejected rows and run evidence remain auditable.

## When Floe is not the right tool

- You need a connector catalog, CDC, or continuous replication engine.
- You need a transformation framework or dbt-style model layer.
- You need streaming ingestion from Kafka or another event bus.
- You need distributed compute across a cluster.

## Feature guide

| Capability | Documentation |
|---|---|
| Contracts and full YAML reference | [config.md](config.md) |
| Pipeline phases and execution details | [how-it-works.md](how-it-works.md) |
| Checks and policy behavior | [checks.md](checks.md) |
| Supported inputs, outputs, storage, and catalogs | [support-matrix.md](support-matrix.md) |
| Write modes and merge semantics | [write_modes.md](write_modes.md) |
| Parquet sink and sink tuning | [sinks/parquet.md](sinks/parquet.md), [sinks/options.md](sinks/options.md) |
| Delta sink and Unity Catalog | [sinks/delta.md](sinks/delta.md) |
| Iceberg sink with Glue or REST catalog | [sinks/iceberg.md](sinks/iceberg.md) |
| DuckDB and MotherDuck sink | [sinks/duckdb.md](sinks/duckdb.md) |
| S3, ADLS, and GCS storage | [storages/s3.md](storages/s3.md), [storages/adls.md](storages/adls.md), [storages/gcs.md](storages/gcs.md) |
| Incremental file state | [incremental.md](incremental.md) |
| Profiles and variables | [profiles.md](profiles.md), [variables.md](variables.md) |
| Multi-file projects, named sources, and domain defaults | [projects.md](projects.md) |
| PII masking | [pii.md](pii.md) |
| Run reports and logging | [report.md](report.md), [logging.md](logging.md) |
| OpenLineage integration | [lineage.md](lineage.md) |
| Python and notebooks | [python-bindings.md](python-bindings.md) |
| CLI usage and installation | [cli.md](cli.md), [installation.md](installation.md) |
| Manifest generation for orchestrators | [manifest.md](manifest.md) |
| Dagster and Airflow connectors | [Dagster README](../orchestrators/dagster-floe/README.md), [Airflow README](../orchestrators/airflow-floe/README.md) |

## Quick start

```bash
floe validate -c config.yml
floe run -c config.yml
```

Installation options are documented in [installation.md](installation.md). The
CLI reference, including `manifest generate`, `add-entity`, `state inspect`, and
`state reset`, is in [cli.md](cli.md).

## Current boundaries

- Floe runs as a lightweight single-node runtime using Rust, Polars, and Arrow.
- File-based incremental ingestion is implemented. Row-based incremental mode
  remains a contract-level value for future work.
- Floe reports write-time metrics and metadata for accepted outputs where the
  sink exposes them cheaply and reliably.
- Table optimization and maintenance remain external to Floe, for example Delta
  optimize/vacuum or Iceberg compaction jobs.
- Iceberg schema evolution and merge/upsert workflows are out of scope in
  current releases.
