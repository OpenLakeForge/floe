# Floe — Project Context

## What it is

Floe is a Python data contract runtime (Polars, PyIceberg, DuckDB) for reliable file ingestion. You
describe a data contract in a YAML config file; Floe reads raw files or extracted
datasets, enforces schema and quality rules, and writes clean rows to a sink —
routing invalid rows to a separate rejected output.

It is designed for data platform teams who need reliable, auditable file ingestion without running a Spark or Flink cluster.

## Core value proposition

- **No JVM, no cluster.** A lightweight runtime. Runs on a laptop, a Lambda function, or a Kubernetes job.
- **Declarative contracts.** Schema, quality rules, and routing are expressed in YAML — not code.
- **Audit trail by default.** Every run produces a JSON report with row counts, rejected file paths, schema evolution info, and timing.
- **Cloud-native storage.** S3 (and S3-compatible stores), ADLS, GCS and local, treated uniformly.
- **Open table targets.** Accepted rows land in Parquet or Apache Iceberg through a REST catalog (Polaris), including SCD1 merges.

## What it is NOT

- Not a streaming engine (no Kafka, no continuous processing).
- Not a transformation framework (no SQL, no dbt-style models).
- Not distributed — one process, one machine. Concurrency is handled via the CAS-based incremental state system, not worker coordination.

## Target users

- **Data engineers** building file ingestion pipelines (CSV drops, API exports, partner feeds).
- **Platform teams** standardising ingestion contracts across data products.
- **ML teams** needing validated, typed parquet landing zones.

## Supported formats

| Direction | Formats |
|---|---|
| Input | CSV · TSV · JSON · Parquet · ORC · Avro · XLSX · XML |
| Accepted output | Parquet · Apache Iceberg · Delta Lake (path-based) |
| Rejected output | CSV · Parquet · Apache Iceberg (quarantine table) |

## Supported storage

Local · AWS S3 (and S3-compatible stores) · Azure ADLS Gen2 and Google Cloud Storage (`adls` and `gcs` extras; no Iceberg on ADLS, no Iceberg merges on GCS, no manifests, `floe maintain` or remote profiles)

## Supported catalogs

Iceberg REST (Polaris) and AWS Glue (restored in 0.8.1; overwrite and append only)

Floe 0.8 dropped Delta, DuckDB/MotherDuck, fixed-width, Unity, ADLS and GCS (D1, D26 in
#619); ADLS, GCS and Delta (path-based, `overwrite` and `append`, local and S3) are back; Unity and Delta merges are not. The Rust implementation that had them is archived at
[OpenLakeForge/floe-rust](https://github.com/OpenLakeForge/floe-rust).

## Versioning

The project follows semantic versioning. A config's `version:` is the minimum Floe major.minor it targets, and a manifest's `spec_version` is the Floe major.minor that generated it (D21, D22). Breaking changes bump the minor version and are documented in `CHANGELOG.md`. See `context/bump-release.md`.
