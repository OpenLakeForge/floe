![Floe logo](docs/assets/floe-banner.png)

# Floe

**Floe** is a Polars-powered data contract runtime for reliable file ingestion.
It validates raw files or extracted datasets before they enter your trusted
layer, routing accepted rows to lakehouse sinks and rejected rows to quarantine
with audit reports.

Use Floe when you already have a platform such as an Iceberg lakehouse on
Polaris or Snowflake Open Catalog (with static storage credentials: Floe does not use
catalog-vended credentials), run by Dagster or Airflow, but you need a lightweight entry gate for file contracts, quality checks, rejected rows,
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
tables through PyIceberg, and runs Iceberg merges through DuckDB, all on a
single node. Unique checks against table history semi-join each chunk with Polars
against a local cache of the table's key columns.

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
| What Floe is for, and a five-minute first run | [docs/get-started/introduction.md](docs/get-started/introduction.md), [docs/get-started/quickstart.md](docs/get-started/quickstart.md) |
| Data contracts: what a contract says and guarantees | [docs/concepts/data-contracts.md](docs/concepts/data-contracts.md) |
| Contracts and full YAML reference | [docs/reference/config.md](docs/reference/config.md) |
| Pipeline phases and execution details | [docs/concepts/how-it-works.md](docs/concepts/how-it-works.md) |
| Checks: schema mismatch, cast, `not_null`, `unique` | [docs/concepts/checks.md](docs/concepts/checks.md) |
| Supported inputs, outputs, storage, and catalogs | [docs/reference/support-matrix.md](docs/reference/support-matrix.md) |
| Write modes: `overwrite`, `append`, `merge_scd1`, `merge_scd2` | [docs/concepts/write-modes.md](docs/concepts/write-modes.md) |
| Parquet and Iceberg sinks | [docs/sinks/parquet.md](docs/sinks/parquet.md), [docs/sinks/iceberg.md](docs/sinks/iceberg.md) |
| S3 storage | [docs/storages/s3.md](docs/storages/s3.md) |
| Incremental file state | [docs/concepts/incremental.md](docs/concepts/incremental.md) |
| Profiles and variables | [docs/guides/profiles.md](docs/guides/profiles.md), [docs/guides/variables.md](docs/guides/variables.md) |
| PII masking | [docs/guides/pii.md](docs/guides/pii.md) |
| Multi-file projects, named sources, and domain defaults | [docs/guides/projects.md](docs/guides/projects.md) |
| Reports, logs, and OpenLineage | [docs/reference/report.md](docs/reference/report.md), [docs/reference/logging.md](docs/reference/logging.md), [docs/guides/lineage.md](docs/guides/lineage.md) |
| Airflow and Dagster manifests | [docs/guides/manifest.md](docs/guides/manifest.md), [orchestrators/airflow-floe/README.md](orchestrators/airflow-floe/README.md), [orchestrators/dagster-floe/README.md](orchestrators/dagster-floe/README.md) |
| Python API and notebooks | [docs/guides/python-api.md](docs/guides/python-api.md) |
| Installation and CLI usage | [docs/get-started/installation.md](docs/get-started/installation.md), [docs/reference/cli.md](docs/reference/cli.md) |
| AI agent skill for Floe contracts | [skills/building-floe-contracts/SKILL.md](skills/building-floe-contracts/SKILL.md) |

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

→ [Full installation guide](docs/get-started/installation.md)

## Quick start

```bash
floe validate -c config.yml   # validate config and schema
floe run      -c config.yml   # run the pipeline
```

→ [Config reference](docs/reference/config.md) · [Example config](example/config.yml)

The documentation starts at [docs/README.md](docs/README.md) and is published at
[openlakeforge.dev/floe](https://openlakeforge.dev/floe/).

## License

MIT
