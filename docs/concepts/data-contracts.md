# Data contracts

A Floe data contract is a YAML file that states what a dataset must look like
before it is trusted, and what happens to the data that does not. Floe enforces
it on every run. This page explains each part of a contract and the guarantees
it gives; the [Config reference](../reference/config.md) lists every field.

## Vocabulary

| Term | Meaning |
| --- | --- |
| **Config** | The YAML file you pass to `floe validate -c` and `floe run -c`. It holds one or more entities, and can include other files (see [Projects](../guides/projects.md)). |
| **Entity** | One dataset: one source of files, one schema, one accepted output. `orders`, `customers`. An entity is the unit Floe runs, reports on and commits. |
| **Contract** | The rules of an entity: its schema, its checks and its policy. In practice, the entity's block in the config. |
| **Domain** | An optional group of entities (`sales`, `finance`) that share defaults and an identity prefix: `sales.orders`. |
| **Accepted sink** | Where rows that pass go: a Parquet dataset or an Iceberg table. |
| **Rejected sink** | Where rows or files that fail go, with the reason: CSV, Parquet or an Iceberg quarantine table. |
| **Run** | One execution of `floe run`, identified by a run id, which writes one report. |

## Anatomy of a contract

```yaml
version: "0.10"                  # the minimum Floe release this config targets
domains:
  - name: sales                  # optional: groups entities, shares defaults
entities:
  - name: customer               # identity of the dataset
    domain: sales                # identity becomes sales.customer
    metadata:                    # optional: owner, description, tags
      owner: sales-data@example.com
      description: Customers exported nightly by the CRM

    source:                      # WHERE the raw files are, and how to read them
      format: csv
      path: ./in/customer
      options: { separator: ",", null_values: ["", "NULL"] }
      cast_mode: strict

    schema:                      # WHAT the data must look like
      mismatch:
        missing_columns: reject_file
        extra_columns: ignore
      primary_key: [customer_id]
      columns:
        - { name: customer_id, type: string, nullable: false }
        - { name: email, type: string }
        - { name: created_at, type: datetime, nullable: false }
        - { name: credit_limit, type: "decimal(12,2)" }

    policy:                      # HOW strict to be when a rule is broken
      severity: reject

    sink:                        # WHERE rows go, and how they are written
      write_mode: append
      accepted: { format: parquet, path: ./out/accepted/customer }
      rejected: { format: csv, path: ./out/rejected/customer }
      archive: { path: ./archive/customer }
```

Every entity has the same four blocks. The rest of this page takes them in
order.

## Source: where the files are

`source` says which files belong to the entity and how to parse them.

- `format`: `csv`, `tsv`, `json`, `parquet`, `orc`, `avro`, `xlsx` or `xml`.
- `path`: a file, a directory or a glob, locally or on S3 (with
  `storage`). A directory is read with the format's default glob (`*.csv`, ...).
- `options`: parsing options such as `separator`, `header`, `encoding`,
  `null_values`, the Excel `sheet` or the XML `row_tag`. The CSV separator
  defaults to `;`.
- `cast_mode`: `strict` (default) treats a value that does not fit its type
  as an error; `coerce` turns it into null.

A contract describes the data, not the transport: Floe does not fetch from
APIs or databases. An extract/load tool lands the files; the contract tells
Floe how to read them.

## Schema: what the data must look like

`schema` is the core of the contract. Each entry of `columns` declares an
output column:

| Field | Rule it creates |
| --- | --- |
| `name` | The column in the output. The input column of the same name is read, unless `source` names another (for nested JSON or XML, `user.address.city` or `order/@id`). |
| `type` | The value must cast to this type: `string`, `boolean`, integer and float types, `decimal(p,s)`, `date`, `datetime`. A value that does not cast is a `cast_error`. |
| `nullable: false` | The value must be present. A missing or null value is a `not_null` error. |
| `unique: true` | No two rows may share the value. Later duplicates are `unique` errors. |

Keys apply to whole rows:

- `primary_key: [a, b]` makes the columns required and unique together. It is
  also the merge key for `merge_scd1` and `merge_scd2`.
- `unique_keys: [[email], [id, country_code]]` declares one or more unique
  constraints, single or composite.

Two more settings say how strict to be about the file's shape:

- `mismatch.missing_columns`: when a declared column is absent from a file,
  `fill_nulls` (default) adds it as nulls, `reject_file` rejects the file.
- `mismatch.extra_columns`: when a file has columns the contract does not
  declare, `ignore` (default) drops them, `reject_file` rejects the file.
- `normalize_columns` matches columns after normalising names, for example to
  `snake_case`, so `Customer ID` in a file matches `customer_id`.

## Checks: what Floe verifies, and when

Floe checks a contract at three levels, cheapest first:

| Level | Check | Fails when |
| --- | --- | --- |
| File | schema mismatch | a declared column is missing or an undeclared one is present, and the contract says `reject_file` |
| Row | `cast_error` | a non-null raw value does not cast to the declared type |
| Row | `not_null` | a required column is null after the cast |
| Entity | `unique` | a value or key was already seen in this run, or (with `append`) already in the accepted output |

A file rejected at file level is not read row by row. Unique checks run across
every file of the entity in file order: the first occurrence is kept, later ones
are flagged. [Checks and severity](checks.md) gives the exact rules.

## Policy: how strict to be

`policy.severity` decides what a broken rule does:

| Severity | Rows that break a rule | Rows that pass | Run status |
| --- | --- | --- | --- |
| `warn` | Kept in the accepted output; errors reported | Accepted | `success_with_warnings`, exit `0` |
| `reject` | Written to the rejected sink with their errors | Accepted | `rejected`, exit `0` |
| `abort` | The whole file is rejected, and the entity stops reading | Rows of the files read before it are written; that file and later ones are not | `aborted`, exit `2` |

`reject` is the usual choice: good rows flow, bad rows wait in quarantine with
their reason, and the run does not fail. Use `abort` when a partial file is
worse than no file, for example a ledger that must balance. Use `warn` to
observe a new source before you enforce its contract. `reject` and `abort`
need a `sink.rejected`.

## Sinks: where rows go

`sink` has up to three targets and one write mode.

- `accepted` (required): `format: parquet` writes a dataset of
  `part-*.parquet` files; `format: iceberg` writes a table through a REST or
  AWS Glue catalog declared under `catalogs`. See
  [Iceberg sink](../sinks/iceberg.md) and [Parquet sink](../sinks/parquet.md).
- `rejected`: `csv` (default), `parquet` or an Iceberg quarantine table.
- `archive`: where input files are moved once their rows are committed.
- `write_mode`: `overwrite` (default), `append`, or, on Iceberg, `merge_scd1`
  (upsert by primary key) and `merge_scd2` (keep history). See
  [Write modes](write-modes.md).

Accepted rows are written once per entity, after every input file is read:
one pass of Parquet parts, or one Iceberg commit. On Iceberg that commit is
atomic: a run that fails on an error leaves the table as it was. A policy
`abort` is not an error in that sense: the files read before the aborted one
are still committed, so the table can change on a run that ends `aborted`.
Parquet has no transactions, so a crash during the write can leave partial
parts (see [Parquet sink](../sinks/parquet.md#parquet-is-not-transactional)).
Either way, input files are archived only after the write succeeded.

## What a rejected row looks like

A rejected row keeps its **raw** values, never cast, so you can see exactly
what arrived. Floe adds metadata columns:

| Column | Content |
| --- | --- |
| `__floe_row_index` | The row's position in its file, from 0 |
| `__floe_errors` | A JSON array of `{rule, column, message}`, one entry per broken rule |
| `__floe_run_id`, `__floe_source_uri` | The run and the file it came from (Parquet and Iceberg rejected sinks) |

A whole file rejected by a mismatch or by `abort` is copied as is to the
rejected path, with an error report next to it. Floe never logs raw values: the
report and the logs carry the rule, the column, the row index and the message.

## What a contract guarantees

For every run of an entity:

- every row in the accepted output passed every check of the contract (except
  under `warn`, which reports instead of enforcing);
- every row read is either accepted or rejected, and the report's counts add
  up: `rows_total = accepted_total + rejected_total`;
- every rejected row says which rule it broke;
- an Iceberg accepted table changes in one commit, or not at all;
- the report records what was read, what was written, and how long it took.

## Change a contract safely

- **Validate in CI.** `floe validate -c config.yml` checks the YAML, the
  types, the keys and the sink settings without reading any data, and fails
  with the field at fault. A config that needs a missing extra (`s3`,
  `iceberg`, `merge`, `xlsx`) fails with the `pip install` to run.
- **`version`** is the minimum Floe release the config targets. Floe accepts
  any version up to its own and keeps the semantics of older ones, so raising
  it is a deliberate step.
- **Adding a column** to an Iceberg table needs
  `schema.schema_evolution.mode: add_columns`; the default `strict` refuses a
  schema that differs from the table. Drops, renames and type changes are
  always refused.
- **Tightening a rule** (making a column required, adding a key) only affects
  new runs: rows already accepted are not re-checked. Run with
  `--full-refresh` to rebuild the output from every input.

## Design a contract

- Start from a real file: `floe add-entity -c config.yml --input
  ./in/orders.csv` infers the columns and types and writes an entity to edit.
- Declare types as precisely as the data allows: `decimal(p,s)` for money,
  `date` or `datetime` for time, `string` only for text.
- Mark as `nullable: false` only what downstream really needs, and give each
  entity a `primary_key` when it has one.
- Start a new source in `warn`, read the report, then switch to `reject`.
- Keep rejected output and reports: they are the evidence a data owner needs
  to fix the source.

## From one contract to a project

A single file with a few entities is enough to start. When contracts multiply,
share what they have in common: named `sources`, `domains` with `defaults`, one
file per entity, and environment profiles for paths and credentials. See
[Projects](../guides/projects.md), [Environment profiles](../guides/profiles.md)
and [Variables](../guides/variables.md).
