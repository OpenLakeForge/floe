# Floe documentation

Floe is a data contract runtime for file ingestion. You describe what a
dataset must look like in a YAML contract; Floe checks every incoming file and
row against it, writes the rows that pass to Parquet or Apache Iceberg, puts
the rows that fail in quarantine with the reason, and leaves a JSON report of
every run.

New to Floe? Read the [Introduction](get-started/introduction.md), run the
[Quickstart](get-started/quickstart.md), then read
[Data contracts](concepts/data-contracts.md).

| I want to | Go to |
| --- | --- |
| Know what Floe is for and whether it fits | [Introduction](get-started/introduction.md) |
| Install it | [Installation](get-started/installation.md) |
| Validate my first file in five minutes | [Quickstart](get-started/quickstart.md) |
| Understand what a contract says and enforces | [Data contracts](concepts/data-contracts.md) |
| Know what happens, in order, during `floe run` | [How Floe works](concepts/how-it-works.md) |
| Know exactly when a row is rejected | [Checks and severity](concepts/checks.md) |
| Choose between overwrite, append and SCD merges | [Write modes](concepts/write-modes.md) |
| Ingest only the files that are new | [Incremental ingestion](concepts/incremental.md) |
| Organise many entities across files and domains | [Projects](guides/projects.md) |
| Run the same contracts in dev, uat and prod | [Environment profiles](guides/profiles.md), [Variables](guides/variables.md) |
| Mask personal data | [PII masking](guides/pii.md) |
| Run Floe from Dagster or Airflow | [Manifests](guides/manifest.md) |
| Call Floe from Python or a notebook | [Python API](guides/python-api.md) |
| Send lineage to Marquez or another OpenLineage backend | [OpenLineage](guides/lineage.md) |
| Write to Iceberg, Parquet or S3 | [Iceberg sink](sinks/iceberg.md), [Parquet sink](sinks/parquet.md), [S3 storage](storages/s3.md) |
| Look up a field, a flag or a report key | [Config](reference/config.md), [CLI](reference/cli.md), [Run report](reference/report.md), [Logging](reference/logging.md) |
| Check what formats, storages and catalogs are supported | [Support matrix](reference/support-matrix.md) |

Floe is the validation gate of [OpenLakeForge](https://openlakeforge.dev), where
it loads Bronze files into Silver Iceberg tables, but it is a standalone Python
package: nothing on these pages needs OpenLakeForge.

To change Floe itself, start with [CONTRIBUTING.md](../CONTRIBUTING.md) and
[AGENTS.md](../AGENTS.md).
