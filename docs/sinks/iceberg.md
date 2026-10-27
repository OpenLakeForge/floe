# Iceberg sink (accepted output)

Floe writes Iceberg tables for `sink.accepted.format: iceberg` through an
Iceberg REST catalog (e.g. Polaris).

**A catalog is required (0.8).** The sink needs a `sink.accepted.iceberg` block
that resolves to a `catalogs.definitions` entry, through `iceberg.catalog` or
`catalogs.default`. `floe validate` rejects a catalog-less Iceberg sink with
`sink.accepted.format=iceberg requires sink.accepted.iceberg with a catalog in 0.8`.
Floe 0.7's catalog-less filesystem tables are not supported: a path-only table
has no safe commit protocol for concurrent runs.

## Example config

```yaml
storages:
  default: "s3_out"
  definitions:
    - name: "s3_out"
      type: "s3"
      bucket: "my-bucket"
      region: "us-east-1"

catalogs:
  default: "polaris"
  definitions:
    - name: "polaris"
      type: "rest"
      uri: "https://polaris.example.com/api/catalog"
      warehouse: "lakehouse"

entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/data/in/customers.csv"
    sink:
      accepted:
        format: "iceberg"
        path: "customer_iceberg"
        iceberg: {} # uses catalogs.default
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
```

## Table location

The table is created in the catalog on its first write (and its namespace, if missing). Its
location is, in order:

- `sink.accepted.iceberg.location`, when set;
- `<warehouse_prefix>/<namespace>/<table>` on the catalog's `warehouse_storage`, when the catalog
  sets `warehouse_storage` or `warehouse_prefix`;
- otherwise `sink.accepted.path` on `sink.accepted.storage`.

The namespace defaults to `entity.domain`, then `default`; the table name to `entity.name`.
The storage must be S3: `floe validate` rejects a local accepted storage unless the catalog's
`warehouse_storage` points at S3.

## AWS credentials for S3 writes

Floe passes the S3 storage's `endpoint`, `region` and `path_style_access` to PyIceberg and
DuckDB. Credentials come from the standard AWS credential chain (environment variables, web
identity / IRSA, container credentials, instance metadata); the catalog does not vend them.

## REST catalog credentials

REST catalog definitions accept bearer tokens and OAuth2 client credentials:

```yaml
catalogs:
  default: "polaris"
  definitions:
    - name: "polaris"
      type: "rest"
      uri: "http://polaris:8181/api/catalog"
      credential: "client_credentials:${POLARIS_CLIENT_ID}:${POLARIS_CLIENT_SECRET}"
      warehouse: "lakehouse"
      oauth2_server_uri: "http://polaris:8181/api/catalog/v1/oauth/tokens"
      scope: "PRINCIPAL_ROLE:ALL"
```

**Prefer `${ENV_VAR}` placeholders in `credential`** over literal client IDs/secrets
in YAML. They are resolved from the OS environment at run time, including during
`floe run --manifest`, so runner environments can inject REST catalog secrets
without baking raw credentials into config or the manifest. The resolved credential
is held as a secret and is never written to reports or logs.

## Partition spec config

Iceberg accepted sinks can declare a partition spec in config, and Floe executes
it at write time for Iceberg table creation/data file layout:

```yaml
sink:
  accepted:
    format: "iceberg"
    path: "customer_iceberg"
    iceberg: {}
    partition_spec:
      - column: "event_date"
        transform: "day"
      - column: "country"
        transform: "identity"
      - column: "customer_id"
        transform: "bucket"
        num_buckets: 16
```

Current scope:
- partition columns must exist in `schema.columns`
- supported transforms: `identity`, `year`, `month`, `day`, `hour`, `bucket` (with `num_buckets`, D12)
- partition spec is applied to Iceberg table metadata and partitioned file writes
- writes validate partition-spec compatibility with existing tables (no implicit partition-spec evolution)

## Supported scope (current)

- Accepted sink only (`sink.accepted.format: iceberg`), through a REST catalog
- S3 storage
- `sink.write_mode`: `overwrite`, `append` and `merge_scd1` (D2); `merge_scd2` passes
  validation but fails at run time (#654). See [write modes](../write_modes.md).
- Each entity's write is one catalog commit. `overwrite` replaces every existing data file in
  that snapshot; `append` adds files; `merge_scd1` runs a DuckDB `MERGE INTO` (merge-on-read).
- `append` snapshots carry `floe.run_id` and `floe.input_hash`, so a retry finds a batch a dead
  run already committed and skips it (`skip_reason: already_committed`).
- Unique checks on `append` read the table's existing keys, and a commit conflict re-checks the
  keys of concurrently added files.
- `schema.schema_evolution.mode: add_columns` adds new columns as nullable (D13)
- Unsigned integer columns are widened (`uint8`/`uint16` → `int`, `uint32` → `long`); Iceberg
  has no unsigned types
- Run report `accepted_output` includes:
  - `table_root_uri`, `write_mode`, `files_written`, `parts_written`
  - `table_version` (Iceberg metadata version, when available)
  - `snapshot_id` (when a snapshot exists)
  - `iceberg_catalog_name`, `iceberg_namespace`, `iceberg_table`
  - file sizing metrics (`total_bytes_written`, `avg_file_size_mb`, `small_files_count`)
- Iceberg file sizing metrics are based on Iceberg data files only (metadata/manifests excluded)

## Limitations / non-goals (current)

- `strategy: drop` in `pii` is rejected: the table enforces the full declared schema
- Snapshot expiry and orphan file removal with `floe maintain` (see [CLI usage](../cli.md)); compaction is experimental (`--compact --experimental`)

## Integration tests

The Polaris and S3 tests are skipped by default. Start a local Polaris + S3 harness, then run
`FLOE_POLARIS_URI=http://localhost:8181/api/catalog uv run pytest -m polaris` (and set
`FLOE_S3_ENDPOINT` for the `-m s3` tests).

## Empty accepted dataset behavior

- The table is created on the first write even when no row is accepted.
- An `append` of no rows commits nothing.
- An `overwrite` of no rows removes every existing data file in one snapshot; on an empty table
  it commits nothing.
