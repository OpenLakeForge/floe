# Delta sink

Floe writes Delta Lake tables for `sink.accepted.format: delta`: a path-based table on local or
S3 storage, with no catalog. The sink needs the `delta` extra (`deltalake`); `floe validate`
fails without it.

## Example config

```yaml
storages:
  default: "s3_out"
  definitions:
    - name: "s3_out"
      type: "s3"
      bucket: "my-bucket"
      region: "us-east-1"

entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/data/in/customers.csv"
    sink:
      write_mode: "append"
      accepted:
        format: "delta"
        path: "customer_delta"
        partition_by: ["country"]
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
        - name: "country"
          type: "string"
```

The table lives at `sink.accepted.path` on `sink.accepted.storage`, and is created on the first
write. Without a `sink.accepted.delta` block Databricks reads the table by its path; with one,
Floe also registers it in Unity Catalog (see [Unity Catalog](#unity-catalog)).

## Storage

- Local and S3 only. Delta on ADLS or GCS fails `floe validate` with `config_not_supported`.
- S3 options (`region`, `endpoint`, `path_style_access`) are passed to delta-rs; credentials
  come from the `AWS_*` environment, as for Floe's own S3 client. A plain `http://` endpoint
  is allowed.
- Commits use delta-rs's conditional put, so no unsafe-rename option (`AWS_S3_ALLOW_UNSAFE_RENAME`)
  is set or needed. The S3 integration tests run on SeaweedFS (`pytest -m s3` with `FLOE_S3_ENDPOINT` set) and cover
  create, overwrite, append, the retry guard (a commit that landed, retried) and racing appends
  of the same batch (one `success`, the others `already_committed`, one commit).

## Write modes

- `overwrite` removes every live file and adds the new ones in one commit.
- `append` adds files in one commit.
- Each run is one commit per entity, and the report's `accepted_output.table_version` is the
  table version of that commit. There is no `snapshot_id` for Delta.
- `merge_scd1` and `merge_scd2` fail `floe validate`.
- `schema.schema_evolution.mode: add_columns` fails `floe validate` (Iceberg only).
- The Delta table is never altered by Floe: a table Floe creates keeps delta-rs's minimal
  protocol, and no table property is changed.

See [write modes](../concepts/write-modes.md). Which backends are tested, and against what, is in the
[support matrix](../reference/support-matrix.md#tested-combinations).

## Retry guard

Every `append` commit carries a `txn` action `floe:<input_hash>` and `floe.run_id` and
`floe.input_hash` in `commitInfo`. A run that died after its commit is found on retry: its inputs are
reported as skipped with `skip_reason: already_committed` and no rows are added.

- Two runs appending the same batch at the same moment: the second one's commit conflicts, and
  it reports `already_committed`. Only one commit lands.
- An intentional reload of the same files after `floe state reset` appends again.
  This holds if the reload run itself dies and is retried: only a commit made since its claim counts.
- The guard reads the `txn`, which Delta expires after `delta.setTransactionRetentionDuration`.
  If a dead run's claim is older than that retention (or the property cannot be read), Floe
  cannot tell whether it committed and the run fails with `run_state_conflict` rather than guess;
  run `floe state reset` for the entity, then rerun.
- `policy.severity: abort` with an `append` is refused at run time (`run_not_supported`): an
  aborted file leaves a smaller batch committed than the hash the guard stamps names.

## Unique keys

`schema.unique_keys` and `schema.primary_key` are enforced on an `append` against the rows already
in the table: a row repeating an existing key is rejected. The existing keys are seeded by a
streamed `DeltaTable.scan` of the key columns to a local cache, so memory does not grow with the
table and column-mapped tables work. A key whose column the table lacks is skipped, as for
Iceberg. On `overwrite` the keys are checked within the batch only.

## Partitioning

`partition_by` lists identity partition columns (config column names). They are set when the
table is created and kept on later appends and overwrites. An existing table partitioned by
other columns fails the run with `run_schema_evolution` and is left untouched. `partition_spec`
is Iceberg-only, and setting both fails `floe validate`.

## Types and schema

- Delta has no unsigned integers: `uint8` and `uint16` are widened to `int32` and `uint32` to
  `int64`. A `uint64` column is refused (`run_not_supported`), before the table is created.
- `nullable: false` columns are required in the table.
- An existing table's columns must match the batch's names and types. Otherwise the run fails
  with `run_schema_evolution`; nothing is cast or added silently.

## Unsupported table features

A table that needs a feature delta-rs cannot write (for example catalog-managed tables) fails the
run with `run_not_supported`, and the table is never altered. Disable the feature on the table,
or write another table.

## Unity Catalog

`sink.accepted.delta: {catalog, schema, table}` with a `type: unity` catalog (`host`, `catalog`, `token`,
`create_schema_if_missing`) registers the table as an **external** Delta table, on S3 storage only. `token` is
an environment reference such as `${DATABRICKS_TOKEN}`, expanded when the table is registered.

> **Tested against a stub only.** Registration is tested against a local stub of the Unity Catalog REST API
> (`/api/2.1/unity-catalog`), not yet against a live Databricks workspace. Expect to verify it in yours.

- Floe commits the Delta table first, then registers it (`GET`, then `POST /api/2.1/unity-catalog/tables` with
  `table_type: EXTERNAL`, `data_source_format: DELTA`, the table's `s3://` location and its columns as Spark types).
  It does this on every successful run, so a run that died between the commit and the registration is registered
  by its retry, even when the retry reports `already_committed`.
- If the registration fails, the run fails and the Delta commit stays; fix the cause and run again.
- A table that already exists is checked, not changed: it must be an `EXTERNAL` `DELTA` table at the same
  location, else the run fails with `run_state_conflict`. Its columns are never updated, since Unity reads an
  external table's schema from the Delta log. Other API errors fail with `run_catalog_error`.
- With `create_schema_if_missing` (the default) the schema is created when absent; the catalog must exist.
- The report's `accepted_output` carries `delta_catalog_name`, `delta_schema` and `delta_table`.
- Managed tables and credential vending are not supported.

Workspace setup you do in Databricks before the first run:

- An **external location** covering the table's `s3://` path (and its storage credential).
- **External data access** enabled on the metastore.
- A token whose principal has `USE CATALOG` on the catalog; `USE SCHEMA`, `CREATE TABLE` and
  `EXTERNAL USE SCHEMA` on the schema; and `CREATE EXTERNAL TABLE` and `EXTERNAL USE LOCATION` on the
  external location. The two `EXTERNAL USE` privileges must be granted explicitly: ownership and
  `ALL PRIVILEGES` do not include them.
- With `create_schema_if_missing`, `CREATE SCHEMA` on the catalog as well. The catalog itself must already exist.

## Memory

The write is not memory-bounded: peak memory is about twice the compressed size of the entity's
accepted rows. Size the runner for the largest entity. See
[the decision](https://github.com/OpenLakeForge/floe/blob/main/context/decisions/delta-sink.md)
for the measurements and the upgrade path.

## Limitations

- No merges, no `add_columns`, no managed Unity tables, no ADLS or GCS (all follow-ups).
- Floe does not run `OPTIMIZE`, `VACUUM` or checkpoints.
