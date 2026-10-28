![Floe logo](docs/assets/floe-banner.png)

# Floe

**Floe** is a Polars-powered data contract runtime for reliable file ingestion.
It validates raw files or extracted datasets before they enter your trusted
layer, routing accepted rows to lakehouse sinks and rejected rows to quarantine
with audit reports.

Use Floe when you already have a platform such as an Iceberg lakehouse on
Polaris or Snowflake Open Catalog, run by Dagster or Airflow, but you need a lightweight entry gate for file contracts, quality checks, rejected rows,
and run evidence.

Floe complements extract/load tools such as dlt, ingestr, and Airbyte: they get
data out of source systems; Floe decides what is allowed into trusted storage.

## What Floe does

- Defines source, schema, checks, accepted output, rejected output, write mode,
  metadata, and reporting in one human-readable YAML contract.
- Reads common file exports and extracted datasets from local or cloud storage.
- Applies schema, casting, nullability, and uniqueness checks before write.
- Writes accepted rows to Parquet or Apache Iceberg (REST catalog), with SCD1 merge on Iceberg.
- Writes invalid rows separately and emits deterministic JSON run reports.
- Runs as a CLI, Docker image, Python library, or orchestrated job.

## How it works

![Floe architecture](docs/assets/architecture.png)

Each `floe run` executes a deterministic gate per entity:

| Stage | What happens |
|---|---|
| **1. Resolve inputs** | Discover and download source files from local or cloud storage |
| **2. File-level checks** | Validate schema structure, file format, and headers |
| **3. Row-level checks** | Apply type casting and `not_null` checks row by row |
| **4. Entity-level checks** | Apply `unique` / primary-key checks across all input rows plus existing accepted data |
| **5. Write outputs** | Route valid rows to accepted sinks, invalid rows to rejected sinks, and write reports |

Floe is a Python package. It reads and validates with Polars, writes Iceberg
tables through PyIceberg, and runs Iceberg merges and unique checks against
table history through DuckDB, all on a single node.

- **Inputs:** CSV · TSV · JSON · Parquet · ORC · Avro · XLSX · XML
- **Accepted outputs:** Parquet · Apache Iceberg
- **Storage:** local · S3 (and S3-compatible stores)
- **Catalogs:** Iceberg REST (Polaris) · AWS Glue

Floe 0.8 dropped the Delta, DuckDB and fixed-width formats, the Unity
catalog, and ADLS and GCS storage. The Rust implementation that supported them
(0.7 and earlier) is archived at
[OpenLakeForge/floe-rust](https://github.com/OpenLakeForge/floe-rust).

## Feature index

| Capability | Start here |
|---|---|
| Contracts and full YAML reference | [docs/config.md](docs/config.md) |
| Pipeline phases and execution details | [docs/how-it-works.md](docs/how-it-works.md) |
| Checks: schema mismatch, cast, `not_null`, `unique` | [docs/checks.md](docs/checks.md) |
| Supported inputs, outputs, storage, and catalogs | [docs/support-matrix.md](docs/support-matrix.md) |
| Write modes: `overwrite`, `append`, `merge_scd1`, `merge_scd2` | [docs/write_modes.md](docs/write_modes.md) |
| Parquet and Iceberg sinks | [docs/sinks/parquet.md](docs/sinks/parquet.md), [docs/sinks/iceberg.md](docs/sinks/iceberg.md) |
| S3 storage | [docs/storages/s3.md](docs/storages/s3.md) |
| Incremental file state | [docs/incremental.md](docs/incremental.md) |
| Profiles and variables | [docs/profiles.md](docs/profiles.md), [docs/variables.md](docs/variables.md) |
| PII masking | [docs/pii.md](docs/pii.md) |
| Multi-file projects, named sources, and domain defaults | [docs/projects.md](docs/projects.md) |
| Reports, logs, and OpenLineage | [docs/report.md](docs/report.md), [docs/logging.md](docs/logging.md), [docs/lineage.md](docs/lineage.md) |
| Airflow and Dagster manifests | [docs/manifest.md](docs/manifest.md), [orchestrators/airflow-floe/README.md](orchestrators/airflow-floe/README.md), [orchestrators/dagster-floe/README.md](orchestrators/dagster-floe/README.md) |
| Python API and notebooks | [docs/python-bindings.md](docs/python-bindings.md) |
| Installation and CLI usage | [docs/installation.md](docs/installation.md), [docs/cli.md](docs/cli.md) |

## Install

Floe needs Python 3.11 or later.

```bash
pip install floe-python
floe --version
```

**Docker**

```bash
docker pull ghcr.io/openlakeforge/floe:latest
docker run --rm -v "$PWD:/work" ghcr.io/openlakeforge/floe:latest run -c /work/config.yml
```

→ [Full installation guide](docs/installation.md)

## Quick start

```bash
floe validate -c config.yml   # validate config and schema
floe run      -c config.yml   # run the pipeline
```

→ [Config reference](docs/config.md) · [Example config](example/config.yml)

For the full documentation entry point, see [docs/summary.md](docs/summary.md).

## License

MIT
