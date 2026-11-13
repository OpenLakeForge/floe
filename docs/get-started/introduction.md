# Introduction

Floe decides which rows of a raw file are allowed into the tables you trust.
It reads files exported by other systems, checks them against a data contract
written in YAML, and splits every run into three outputs: the accepted rows,
the rejected rows with the rule each one broke, and a report of what happened.

## The problem it solves

Data reaches a lakehouse as files: partner CSV drops, Excel exports, API
extracts landed by dlt or Airbyte, Parquet dumps of a database. Those files
change without warning. A column is renamed, a date arrives as text, an
identifier is suddenly empty, the same order appears twice. If nothing checks
them, the bad rows end up in the tables analysts query, and the error is found
weeks later, far from its cause.

Floe puts a gate between the raw files and the trusted layer. The rules of the
gate are a **contract**: the columns a dataset must have, their types, which
must be present, which must be unique, and what to do with a row or a file that
breaks them. The contract lives in Git next to the code that uses it, so a
change to a rule is reviewed like any other change.

## What a run does

```text
raw files ──▶ file checks ──▶ row checks ──▶ entity checks ──┬──▶ accepted rows  (Parquet or Iceberg)
 (local, S3)   columns,         types,         unique keys,    ├──▶ rejected rows  (CSV, Parquet or Iceberg, with reasons)
               headers          not_null       across files    └──▶ run report     (JSON)
```

1. **Resolve inputs.** Floe lists the files of each entity, locally or on S3,
   and skips the ones it has already ingested if incremental mode is on.
2. **File-level checks.** It reads only the header or schema of each file and
   compares the columns with the contract.
3. **Row-level checks.** It casts each value to its declared type and checks
   required columns.
4. **Entity-level checks.** It checks unique keys across every file of the run,
   and, when appending, against the rows already accepted.
5. **Write.** Accepted rows go to the accepted sink in one write per entity,
   rejected rows to the rejected sink, and a report is written for the run.

[How Floe works](../concepts/how-it-works.md) describes each phase in detail.

## Where it fits

Floe is one step in a data platform, not the platform:

| Step | Typical tool | Floe's role |
| --- | --- | --- |
| Get data out of a source system | dlt, ingestr, Airbyte, an SFTP drop | None: Floe reads what they land |
| **Validate and route the landed files** | **Floe** | Enforce the contract, quarantine bad rows, report |
| Store the trusted tables | Iceberg on S3 with Polaris or AWS Glue, or plain Parquet | Floe writes to it |
| Transform into models | dbt, SQL | None: downstream of Floe |
| Schedule and retry | Dagster, Airflow, cron | Runs `floe run` per entity, from a [manifest](../guides/manifest.md) |

In [OpenLakeForge](https://openlakeforge.dev), Floe is the step that turns
Bronze files into Silver Iceberg tables.

## Good fit

- Raw exports, partner feeds, API extracts or object-storage drops must be
  checked before they become trusted data.
- You already have storage, an orchestrator or a lakehouse (an Iceberg
  catalog such as Polaris, Snowflake Open Catalog or AWS Glue) and want a
  light, embeddable ingestion gate rather than another platform.
- You want rejected rows and run evidence kept and auditable, not dropped.
- You want rules as reviewed YAML, not as code scattered across jobs.

## Not the right tool

- You need a connector catalog, CDC or continuous replication: use an
  extract/load tool, then Floe on what it lands.
- You need transformations, joins or a model layer: that is dbt's job.
- You need streaming ingestion from Kafka or another event bus.
- You need distributed compute across a cluster. Floe runs in one process on
  one machine; concurrent runs coordinate only through their
  [incremental state](../concepts/incremental.md).

## How it is built

Floe is a pure-Python package, `floe-python` on PyPI, that installs the `floe`
command and the `floe` library. It reads and validates with Polars, writes
Iceberg tables through PyIceberg, and runs Iceberg merges through DuckDB, which
spills to disk. There is no JVM, no cluster and no server: it runs on a laptop,
in a container, in a Kubernetes job or inside a Dagster or Airflow task.

## Current boundaries

- **Inputs:** CSV, TSV, JSON, Parquet, ORC, Avro, XLSX, XML.
- **Accepted outputs:** Parquet and Apache Iceberg (REST or AWS Glue catalog).
- **Storage:** local and S3, including S3-compatible stores.
- File-based incremental ingestion is implemented; row-based incremental mode
  is not.
- Iceberg supports additive schema evolution (`add_columns`), `merge_scd1`
  and `merge_scd2`. Floe does not maintain tables during a run: `floe maintain`
  expires old snapshots and removes orphan files as a separate command.
- Floe uses static storage credentials, not catalog-vended credentials.
- Floe 0.8 dropped the Delta, DuckDB and fixed-width formats, the Unity
  catalog, and ADLS and GCS storage. The Rust implementation that supported
  them (0.7 and earlier) is archived at
  [OpenLakeForge/floe-rust](https://github.com/OpenLakeForge/floe-rust).

The [support matrix](../reference/support-matrix.md) lists every supported
combination.

## Next

- [Install Floe](installation.md).
- Run the [Quickstart](quickstart.md): one contract, one file, one command.
