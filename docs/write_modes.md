# Write Modes

Floe resolves accepted/rejected write behavior from `sink.write_mode`.

Supported values:
- `overwrite` (default): existing output parts are replaced before writing new output.
- `append`: new output parts are added without deleting existing ones.
- `merge_scd1`: SCD1 upsert keyed by `schema.primary_key` on Iceberg accepted sinks (D2).
- `merge_scd2`: SCD2 merge keyed by `schema.primary_key`. `floe validate` accepts it on Iceberg
  sinks, but `floe run` does not support it yet and fails the entity (#654).

Parquet accepted sinks support `overwrite` and `append` only. The Delta and DuckDB sinks were
dropped in 0.8 (D1).

## Standard Iceberg writes (`append`, `overwrite`)

Each entity's accepted rows land in one Iceberg catalog commit.

- `schema.schema_evolution.mode: strict` (default): the incoming schema must match the table.
- `schema.schema_evolution.mode: add_columns` enables additive-only schema evolution (D13).
  - New source columns are added to the table as nullable columns.
  - Existing columns must remain compatible; missing columns, type changes and
    non-nullable → nullable changes are rejected.
  - Partition spec changes are rejected.
- `append` commits carry a fingerprint of the claimed input files, so a retry after a crash
  finds the batch already committed and skips the write (D5, D35).

## `merge_scd1` semantics

- Accepted sink format must be `iceberg`.
- `schema.primary_key` is required and is used as merge key.
- Source rows are validated for merge-key uniqueness during row checks.
  In `policy.severity=warn`, duplicate merge-key rows are rejected before merge so only unambiguous rows are merged.
- On key match: update non-key columns from source, only when a value changed, so rerunning a
  batch writes nothing.
- `sink.accepted.merge.ignore_columns` can exclude additional business columns from SCD1 update sets.
- On missing key: insert source row.
- The batch is staged as Parquet, then merged with one DuckDB `MERGE INTO` (merge-on-read:
  position delete files). A `bucket()`-partitioned table gets one MERGE per bucket inside one
  transaction, so still one catalog commit.
- Commit conflicts are retried a few times, then fail the entity.
- Strict schema handling remains the default behavior.
- `schema.schema_evolution.mode: add_columns` applies as for standard writes; merge-key columns
  cannot be introduced by schema evolution.
- `--full-refresh` is rejected with merge modes.
- Merge-on-read leaves position delete files; `floe maintain` expires old snapshots and removes
  orphan files (see [CLI](cli.md)).

## `merge_scd2` semantics

Not runnable on Iceberg in 0.8 (#654): validation accepts the mode and its
`sink.accepted.merge.scd2` options (system column names `__floe_is_current`,
`__floe_valid_from`, `__floe_valid_to` by default; custom names must be non-empty, unique, and
must not collide with schema columns), and `floe run` fails the entity.

## Rejected output behavior

`sink.write_mode` is shared with rejected output:
- `overwrite`: first rejected write in a run starts fresh, subsequent rejected files append.
- `append`: rejected files append.
- `merge_scd1`: rejected files append (merge logic applies only to the accepted sink).
- `merge_scd2`: rejected files append (merge logic applies only to the accepted sink).
