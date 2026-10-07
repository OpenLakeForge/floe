# Config Reference

This document describes the Floe YAML configuration format and the meaning of
all supported options. See `example/config.yml` for a full working example.

## Structure at a glance

```yaml
version: "0.2"
metadata: { ... }
report:
  path: "/abs/or/relative/report/dir"
  storage: "s3_raw"
env:
  file: "metadata/env.dev.yml"
  vars:
    incoming_path: "/data/incoming"
domains:
  - name: "sales"
    incoming_dir: "{{incoming_path}}/sales"
entities:
  - name: "customer"
    domain: "sales"
    metadata: { ... }
    incremental_mode: "file"
    state:
      path: "./state/customer.json"
    source:
      format: "csv"
      path: "{{domain.incoming_dir}}/customer"
      options:
        header: true
        separator: ","
        encoding: "utf-8"
        null_values: ["", "NULL", "null"]
        recursive: false
        glob: "*.csv"
      cast_mode: "strict"
    sink:
      accepted:
        format: "parquet"
        path: "/path/to/accepted/"
      rejected:
        format: "csv"
        path: "/path/to/rejected/"
      archive:
        path: "/path/to/archive/"
    policy:
      severity: "reject"
    schema:
      normalize_columns:
        enabled: true
        strategy: "snake_case"
      schema_evolution:
        mode: "strict"
        on_incompatible: "fail"
      columns:
        - name: "customer_id"
          type: "string"
          nullable: false
          unique: true
```

## Top-level fields

- `version` (required)
  - String used to validate config compatibility.
  - Must be a numeric `major.minor` string such as `"0.1"`, `"0.2"`, or `"0.3"`.
  - Minimum supported version is `"0.1"`.
  - `schema.schema_evolution` requires `version >= "0.2"`.
- `metadata` (optional)
  - Free-form project metadata. Keys supported in schema: `project`,
    `description`, `owner`, `tags`.
- `report` (optional)
  - `report.path` is the base directory where run reports are written. If omitted,
    defaults to `"report"`.
  - `report.storage` (optional) selects the storage client used for reports.
    Defaults to `storages.default` when defined, otherwise `local`.
  - When `report.storage` is cloud-based, reports are written via temp upload.
    Relative `report.path` values resolve under the storage prefix.
  - Reports are written under:
    `report.path/run_<run_id>/run.summary.json` and
    `report.path/run_<run_id>/<entity.name>/run.json`.
  - Supports `{{var}}` templating (see "Templating & domains").
- `storages` (optional)
  - Defines named storage clients for `source.storage` and `sink.*.storage`.
  - If omitted, `local` is assumed.
- `catalogs` (optional)
  - Defines named external catalogs for Iceberg and Delta sinks.
  - Supported types: `glue`, `rest`, `unity`.
  - Keys:
    - `default` (optional): default catalog name used by `sink.accepted.iceberg.catalog` or `sink.accepted.delta.catalog` when omitted.
    - `definitions` (required): array of catalog definitions.
      - `name` (required)
      - `type` (required): `glue`, `rest`, or `unity`
      - **`glue`** — AWS Glue Data Catalog (Iceberg, S3-backed):
        - `region` (required)
        - `database` (required)
        - `warehouse_storage` (optional): storage name, must resolve to S3
        - `warehouse_prefix` (optional): prefix for deterministic table locations
      - **`rest`** — Iceberg REST catalog (Unity Catalog Iceberg endpoint, Polaris, Nessie, Snowflake Open Catalog, etc.):
        - `uri` (required): REST catalog base URI
        - `credential` (optional): `token:<pat>` or `client_credentials:<id>:<secret>`; `${ENV_VAR}` placeholders are resolved from the OS environment at run time, so manifest runs can keep REST catalog secrets out of the manifest artifact
        - `warehouse` (optional): warehouse/prefix hint forwarded to the REST catalog
        - `oauth2_server_uri` (optional): override OAuth2 server URL for client-credentials flow
        - `scope` (optional): OAuth2 scope
        - `warehouse_storage` (optional): storage name for table location resolution
        - `warehouse_prefix` (optional): prefix for deterministic table locations
      - **`unity`** — Databricks Unity Catalog (Delta Lake external table registration):
        - `host` (required): Databricks workspace URL, e.g. `https://my-workspace.azuredatabricks.net`
        - `catalog` (required): Unity catalog name
        - `schema` (required): default schema name
        - `token` (required): Personal Access Token; accepts a literal value or a single `${ENV_VAR}` reference resolved from the OS environment at run time
        - `create_schema_if_missing` (optional, default `false`): create the Unity schema if absent
- `lineage` (optional)
  - Enables OpenLineage event emission for this run.
  - `url` (required): base URL of the OpenLineage-compatible endpoint (e.g. `http://marquez:5000`)
  - `endpoint` (optional, default `api/v1/lineage`): path joined to `url` for POST requests
  - `namespace` (required): OpenLineage namespace for job identity
  - `dataset_namespace` (optional, defaults to `namespace`): namespace for accepted Iceberg output datasets
  - `api_key` (optional): Bearer token; supports `{{VAR}}` placeholder expansion
  - `timeout_secs` (optional, default `5`): HTTP request timeout in seconds
  - `producer` (optional): URI identifying this producer
  - See `docs/lineage.md` for full event reference and lifecycle details.
- `env` (optional)
  - Enables variable templating for string fields using `{{var}}` syntax.
  - `env.file` (optional) loads variables from a YAML file (path relative to the
    main config file).
  - `env.vars` (optional) provides inline variables and overrides `env.file`.
- `domains` (optional)
  - Named domain roots that can be referenced by entities via `entity.domain`.
  - Example entry: `{ name: "sales", incoming_dir: "{{incoming_path}}/sales" }`.
- `entities` (required)
  - Array of entity definitions (datasets). A single CLI run may process
    multiple entities.

## Entity fields

### `name` (required)
Logical dataset name. Used in report and output paths.

### `metadata` (optional)
Free-form entity metadata. Supported keys: `data_product`, `domain`, `owner`,
`description`, `tags`.

### `domain` (optional)
Reference to a domain defined in `domains`. When set, `{{domain.incoming_dir}}`
is available for templating within that entity.

### `incremental_mode` (optional)

- Controls incremental ingestion behavior for the entity.
- Supported values:
  - `none` (default)
  - `archive`
  - `file`
  - `row` (reserved contract value, not runtime-enabled in this release)
- `incremental_mode: archive` requires `sink.archive`.
- `incremental_mode: file` enables per-file state tracking and skip logic.
- For backward compatibility, `sink.archive` without `incremental_mode` is still
  treated as archive mode, but new configs should set `incremental_mode: archive`
  explicitly.

### `state` (optional)

- Entity-local state settings used by incremental ingestion.
- `state.path` (optional)
  - Overrides the file or object used to store entity state.
  - Supports local paths and cloud URIs for S3 (`s3://`), GCS (`gs://`), and ADLS (`abfs://` or `abfss://`).
  - Relative paths always resolve as local filesystem paths, regardless of the source
    storage context. Only explicit cloud URIs (`s3://`, `gs://`, `abfs://`, `abfss://`) use remote
    storage. A relative override with a cloud source will produce local state and will
    not benefit from remote claim coordination.
  - If omitted and `incremental_mode: file` is used, Floe derives the path under the
    source root as `.floe/state/<entity>/state.json`.
  - For cloud sources, the derived default is a remote object under the source root,
    for example `s3://bucket/prefix/incoming/.floe/state/customer/state.json`.

#### File incremental state and concurrency

`incremental_mode: file` writes state using schema `floe.state.file-ingest.v2`.
Existing `floe.state.file-ingest.v1` files are still readable; Floe rewrites state
as v2 the next time it updates the entity.

The durable `files` map tracks file URIs that have completed successfully. The
`claims` map tracks files currently owned by a run, with `run_id`, acquisition
time, expiry time, size, and mtime. The default claim TTL is one hour.

Before processing, Floe removes expired claims, skips previously processed files,
and skips files actively claimed by another run with an incremental warning in
the report. New pending files are claimed with an optimistic conditional state
write before accepted sink output is written. On success, the run promotes its
claims into `files`; on entity failure, it removes its claims so the files can be
retried. Conditional write conflicts are retried a small fixed number of times
and then fail with a clear concurrency error.

Remote state requires object-store permissions to read, write with conditional
preconditions, and delete the state object. Normal source listing permissions are
still needed for input discovery; state itself does not require listing except
where the selected cloud provider or IAM policy requires it for object access.

See [Incremental File Ingestion](incremental.md) for examples, CLI operations,
state file shape, and CAS behavior.

### `source` (required)

- `format` (required)
  - Supported: `csv`, `tsv`, `fixed`, `parquet`, `orc`, `json`, `xlsx`, `avro`, and `xml`.
  - Cloud inputs (S3/ADLS/GCS) use temp download + local read.
  - `json` supports NDJSON and JSON array modes.
  - `tsv` uses tab-delimited parsing (same options as `csv`).
  - `fixed` reads fixed-width text files using `schema.columns[].width`.
  - `xlsx` reads Excel worksheets using `source.options.sheet` + row offsets.
  - `xml` reads repeated records using `source.options.row_tag`.
- `path` (required)
  - Input location. Can be a file, a directory, or a glob pattern
    (example: `/data/in/*.csv`).
  - If a directory is provided, a glob is applied to select files.
  - Relative paths resolve against the config file directory.
  - Supports `{{var}}` templating (see "Templating & domains").
- `storage` (optional)
  - Name of the storage client to use for this source.
  - Defaults to `storages.default` when defined, otherwise `local`.
- `options` (optional)
  - Format-specific options.
  - Defaults if omitted:
    - `header`: `true`
    - `separator`: `";"` (or `"\t"` when `source.format` is `tsv`)
    - `encoding`: `"UTF8"`
    - `null_values`: `[""]` (empty fields treated as null by default)
    - `recursive`: `false`
    - `glob`: (none; default is based on `source.format`)
    - `json_mode`: `"array"`
  - `glob` (optional)
    - Used only when `source.path` is a directory.
    - Overrides the default file pattern for the source format:
      - `csv`: `*.csv`
      - `tsv`: `*.tsv`
      - `fixed`: `*.txt`, `*.fw`
      - `parquet`: `*.parquet`
      - `orc`: `*.orc`
      - `json`: `*.json`
      - `xlsx`: `*.xlsx`
      - `avro`: `*.avro`
      - `xml`: `*.xml`
    - If `source.path` itself contains a glob pattern, this option is ignored.
  - `recursive` (optional)
    - If `true`, directory globs include subdirectories (via `**/`).
  - `json_mode` (optional)
    - `array` (default): JSON array ingestion when `source.format: json`.
    - `ndjson`: newline-delimited JSON ingestion.
    - Nested JSON is supported via `schema.columns[].source` selectors.
    - JSON schema mismatch checks only consider top-level selector sources (no `.` or `[`).
    - For safety, JSON configs are limited to 1024 columns per entity.
  - `row_tag` (required, `source.format: xml`)
    - Element name used as row boundary (each matching element becomes one record).
  - `namespace` (optional, `source.format: xml`)
    - Namespace URI used to match `row_tag` and selector element tokens.
  - `value_tag` (optional, `source.format: xml`)
    - Optional descendant element used when extracting text content from selector targets.
  - `sheet` (optional, `source.format: xlsx`)
    - Sheet name to read (defaults to first sheet).
  - `header_row` (optional, `source.format: xlsx`)
    - 1-based row index for column headers (default: 1).
  - `data_row` (optional, `source.format: xlsx`)
    - 1-based row index where data starts (default: `header_row + 1`).
- `cast_mode` (optional)
  - `strict` (default): invalid values produce cast errors.
  - `coerce`: invalid values become null (and may still fail `not_null`).

### `sink` (required)

- `write_mode` (optional)
  - `overwrite` (default): remove existing dataset parts, then write new ones.
  - `append`: add new dataset parts without deleting existing ones.
  - `merge_scd1`: upsert mode keyed by `schema.primary_key` for Delta or DuckDB accepted sinks.
    - updates matching keys (SCD1) and inserts new keys
    - requires `sink.accepted.format: delta` or `duckdb`
    - requires non-empty `schema.primary_key`
    - optional merge behavior can be configured in `sink.accepted.merge`
    - source rows must be unique on `schema.primary_key` (duplicates abort the entity merge)
  - `merge_scd2`: history mode keyed by `schema.primary_key` for Delta or DuckDB accepted sinks.
    - closes changed current rows and inserts new current versions
    - requires `sink.accepted.format: delta` or `duckdb`
    - requires non-empty `schema.primary_key`
    - optional merge behavior can be configured in `sink.accepted.merge`
    - source rows must be unique on `schema.primary_key` (duplicates abort the entity merge)
  - Applies to both accepted and rejected outputs.
- `accepted` (required)
  - `format`: `parquet`, `delta`, `iceberg`, or `duckdb`.
    - `delta`: Delta Lake transactional sink; optional Unity Catalog registration via `sink.accepted.delta`.
    - `iceberg`: local/S3/GCS filesystem-catalog sink, with optional AWS Glue or REST catalog registration; append/overwrite supported; no schema evolution.
    - `duckdb`: local `.duckdb` file or MotherDuck (`md:`) database; `overwrite`/`append`/`merge_scd1`/`merge_scd2` via native `MERGE INTO`. Configured via `sink.accepted.duckdb`. See [DuckDB sink](sinks/duckdb.md).
- `path`: output directory for accepted records.
  - Supports `{{var}}` templating (see "Templating & domains").
  - `storage` (optional)
    - Name of the storage client to use for this sink target.
    - Defaults to `storages.default` when defined, otherwise `local`.
  - `options` (optional)
    - Sink tuning options (format support varies).
    - `compression`: `snappy`, `gzip`, `zstd`, `uncompressed`
    - `row_group_size`: positive integer (rows per row group)
    - `max_size_per_file`: positive integer bytes (default: 256MB)
      - Parquet: split accepted parquet into parts at write time.
      - Delta: mapped to the Delta writer target file size.
      - Also bounds in-process accepted-row memory: Floe flushes the
        accepted buffer to the sink whenever the accumulated estimated
        in-memory size meets this threshold (or at end of entity). The
        first flush uses the configured `write_mode`; subsequent flushes
        within the same run are forced to `append`. A file's archive
        call (when `sink.archive` is enabled) always runs before its
        rows are added to the buffer, so a file's accepted data only
        ever reaches the sink after that file's archive has succeeded.
      - **Partial-output trade-off (applies to all non-merge sinks)**:
        because flushes commit synchronously while Phase B is still
        iterating inputs, a failure on a *later* file (read /
        validation / rejected-write / archive) can leave the *earlier*
        files' accepted rows already committed to the sink — and in
        `overwrite` mode the first flush may already have replaced the
        previous dataset. The run report and incremental state are not
        committed in that case, so the same inputs are re-processed on
        the next run.
      - For **Parquet** that committed prefix is non-transactional part
        files in the accepted directory; downstream consumers reading
        the directory directly may see the torn state.
      - For **Delta** and **Iceberg** each individual flush is
        transactional (Delta commit / Iceberg snapshot), so no
        single flush exposes a half-written file — but a failure on a
        later file does *not* roll back the earlier flush's commit.
        Readers can still observe a committed prefix of the batch
        between the failure and the next run, and the re-run will
        process the same inputs again (cross-run unique constraints
        or merge modes are what protect against duplication, not the
        sink's per-commit atomicity).
      - This is an explicit design choice — keeping memory bounded
        means accepting non-atomic batches. Merge modes (`merge_scd1`
        / `merge_scd2`) keep the previous accumulate-then-write
        behaviour and ignore this memory ceiling because they require
        the full per-entity dataset, so they do not exhibit the
        cross-file partial-batch exposure.
  - `partition_by` (optional, `sink.accepted.format: delta`)
    - Identity partition columns (array of schema column names).
    - `floe validate` checks the columns exist in `schema.columns`.
    - Runtime wiring is implemented for Delta accepted writes.
  - `partition_spec` (optional, `sink.accepted.format: iceberg`)
    - Array of partition spec entries.
    - Each entry supports:
      - `column` (required): schema column name
      - `transform` (optional, default `identity`): `identity`, `year`, `month`, `day`, `hour`
    - `floe validate` checks column existence and supported transforms.
    - Runtime wiring is implemented for Iceberg accepted writes (table partition spec + partitioned file layout).
  - `merge` (optional, Delta and DuckDB merge modes only)
    - Supported only when:
      - `sink.accepted.format: delta` or `duckdb`
      - `sink.write_mode: merge_scd1` or `merge_scd2`
    - `ignore_columns` (optional)
      - List of schema business columns excluded from merge update/compare behavior.
      - Columns must exist in `schema.columns`.
      - `schema.primary_key` columns are not allowed.
    - `compare_columns` (optional; most relevant for `merge_scd2`)
      - Explicit list of schema business columns used for change detection.
      - Columns must exist in `schema.columns`.
      - `schema.primary_key` columns are not allowed.
      - If omitted in `merge_scd2`, Floe compares all non-key business columns minus `ignore_columns`.
    - `scd2` (optional, `merge_scd2` only)
      - `current_flag_column` (default `__floe_is_current`)
      - `valid_from_column` (default `__floe_valid_from`)
      - `valid_to_column` (default `__floe_valid_to`)
      - Custom names must be non-empty, unique, and must not collide with business schema columns.
  - `iceberg` (optional, `sink.accepted.format: iceberg`)
    - Enables Iceberg catalog-specific options.
    - `catalog` (optional)
      - Catalog name from `catalogs.definitions` (type `glue` or `rest`).
      - If omitted, `catalogs.default` is used when set.
    - `namespace` (optional)
      - Iceberg namespace identifier reported with the run.
      - Defaults to `entity.domain` when present, otherwise the catalog `database` / REST warehouse value.
    - `table` (optional)
      - Logical table name for the catalog registration.
      - Defaults to `entity.name` (normalized).
    - `location` (optional)
      - Overrides the Iceberg table root location.
      - Resolved via `warehouse_storage` when defined, otherwise via the sink storage.
  - `delta` (optional, `sink.accepted.format: delta`)
    - Enables Unity Catalog registration after the Delta write.
    - `catalog` (optional)
      - Catalog name from `catalogs.definitions` (must be type `unity`).
      - If omitted, `catalogs.default` is used when set.
    - `schema` (optional)
      - Unity schema identifier. Defaults to `entity.domain` (normalized), then the catalog-level `schema`.
    - `table` (optional)
      - Unity table name. Defaults to `entity.name` (normalized).
  - `duckdb` (optional, required when `sink.accepted.format: duckdb`)
    - `table` (required)
      - Target table name. May be schema-qualified (`"schema.table"`).
    - `schema` (optional)
      - Target schema; defaults to `main`.
    - `connection` (optional)
      - MotherDuck connection string (`md:<database>`). When set, the target is MotherDuck and
        `path`/`storage` must be omitted. A non-`md:` connection string is rejected.
      - When omitted, the target is the local `.duckdb` file at `sink.accepted.path`
        (object-store paths are rejected — use MotherDuck instead).
    - `token` (optional, MotherDuck only)
      - Literal value or a single `${ENV_VAR}` reference, resolved at connect time. Never logged.
    - See [DuckDB sink](sinks/duckdb.md) for full semantics.
  - Accepted output reports may include file sizing metrics (`files_written`,
    `total_bytes_written`, `avg_file_size_mb`, `small_files_count`) when collected by the writer.
    - Parquet: populated from written parquet files.
    - Delta (local): populated from committed Delta log `add` actions.
    - Delta (S3/GCS/ADLS): best-effort via object_store commit-log read; metrics remain nullable if collection fails after a successful write.
    - Iceberg: populated from written Iceberg data files (metadata/manifests excluded from size totals).
  - `table_version` / `snapshot_id` in reports are sink-format specific (for example Delta table version, Iceberg metadata version + snapshot ID).
  - Compaction/optimization/maintenance remains external to Floe (for Parquet/Delta/Iceberg datasets).
    - Examples: Delta optimize/vacuum, Iceberg compaction/maintenance jobs.
  - `schema.schema_evolution` is strict by default.
    - Runtime support in this phase is Delta-only and additive-only.
    - `mode: add_columns` is implemented for Delta `append`, `overwrite`, `merge_scd1`, and `merge_scd2`.
    - Unsupported changes such as drops, renames, type changes, and non-nullable-to-nullable drift still fail the write.
    - When enabled, Floe audits the outcome in the entity report `schema_evolution` block and emits a `schema_evolution_applied` lifecycle event when columns are added.
- `rejected` (required when `policy.severity: reject`)
  - `format`: `csv` (v0.1).
- `path`: output directory for rejected rows.
  - Supports `{{var}}` templating (see "Templating & domains").
  - Rejected outputs are written as dataset parts (`part-*.csv`).
- `archive` (optional)
- `path`: directory where raw input files are archived after ingestion.
  - If omitted, archiving is disabled.
  - Supports `{{var}}` templating (see "Templating & domains").

### `pii` (optional)

Column-level PII masking applied to accepted output before sink write.

```yaml
pii:
  columns:
    - name: email
      strategy: hash
    - name: credit_card
      strategy: mask
      mask_pattern: "{first4}****{last4}"
    - name: ssn
      strategy: redact
      redact_value: "***"
    - name: notes
      strategy: drop
```

- `columns` (required): array of PII column transforms
  - `name` (required): schema column name (must match `schema.columns[].name`)
  - `strategy` (required): `hash`, `drop`, `nullify`, `redact`, or `mask`
  - `mask_pattern` (required for `strategy: mask`): literal output string with optional `{firstN}` / `{lastN}` tokens
  - `redact_value` (optional, `strategy: redact`): replacement string (default `[REDACTED]`)

See `docs/pii.md` for full strategy reference and masking pattern syntax.

### `policy` (required)

- `severity` (required)
  - `warn`: keep rows, log violations.
  - `reject`: reject rows with violations (first unique kept, duplicates rejected).
  - `abort`: abort the entire file on first violation.

### `schema` (required)

- `normalize_columns` (optional)
  - `enabled`: boolean toggle.
  - `strategy`: `snake_case`, `lower`, `camel_case`, `none`.
  - When enabled, both schema column names and input column names are normalized
    before checks. If normalization causes a name collision, the run fails.
- `schema_evolution` (optional, v0.2)
  - Omitted by default, Floe behaves as `mode: strict` and `on_incompatible: fail`.
  - `mode`: `strict` or `add_columns`.
  - `on_incompatible`: `fail`.
  - `mode: add_columns` requires `sink.accepted.format: delta`.
  - Current implementation is additive-only:
    - new columns may be appended to existing Delta schemas
    - existing columns must remain compatible
    - merge-key columns cannot be introduced by evolution during Delta merge modes
    - adding columns to already partitioned Delta tables is rejected in this phase
- `primary_key` (optional)
  - Array of schema column names.
  - Primary key columns are always treated as required (`not_null`) at runtime.
  - Config validation rejects `nullable: true` on primary key columns.
  - `primary_key` is also enforced as a composite unique constraint.
  - In `sink.write_mode: merge_scd1` or `merge_scd2`, `primary_key` is the mandatory merge key.
- `unique_keys` (optional)
  - Array of unique constraints, each constraint being an array of schema column names.
  - Supports single-column and multi-column uniqueness:
    - `unique_keys: [["email"], ["id","country_code"]]`
  - If `unique_keys` is present, legacy `columns[].unique` flags are ignored.
  - Null semantics: a row is ignored for a constraint if any key component is null.
- `columns` (required)
  - Array of column definitions.
  - `name` (required): target column name in the output schema.
  - `source` (optional): source field selector (including nested JSON/XML extraction).
    - When omitted, defaults to the value of `name`.
    - For CSV/Parquet/ORC/XLSX/Avro/fixed, use an input column name.
    - For JSON, selectors may include dot notation and `[index]` (example: `user.names[0]`).
    - For XML, selectors support element paths with `.` or `/`, with optional terminal
      attribute selection (example: `order.customer.@id` or `order/customer/@id`).
    - When `source` is set, `normalize_columns` applies to the source selector for matching,
      but the output column name remains the explicit `name`.
    - When `source` is set, validation summaries and row error logs include the source
      value alongside the column name.
  - `type` (required): logical type. Accepted values are case-insensitive and
    normalized by removing `-` and `_`.
  - `nullable` (optional): default `true`.
  - `unique` (optional, legacy): default `false`.
    - Legacy single-column unique. Prefer `schema.unique_keys`.
  - `width` (required for `source.format: fixed`): number of characters to read.
  - `trim` (optional, `source.format: fixed`): trim whitespace for the column (default `true`).

## Supported column types

The parser accepts the following type names (case-insensitive, `-` and `_`
ignored):

- `string`: `string`, `str`, `text`
- `boolean`: `boolean`, `bool`
- `integer`: `int8`, `int16`, `int32`, `int64`, `int`, `integer`, `long`
- `unsigned integer`: `uint8`, `uint16`, `uint32`, `uint64`
- `number` (float64): `number`, `float64`, `float`, `double`, `decimal`
- `float32`: `float32`
- `date`: `date`
- `datetime`: `datetime`, `timestamp`
- `time`: `time`

For more details about checks and severity behavior, see `docs/checks.md`.
Execution order and pipeline phases are documented in `docs/how-it-works.md`.

## Templating & domains

Floe supports simple placeholder substitution in string fields using `{{var}}`
syntax. Variables are resolved in this order (lowest to highest precedence):

1) `env.file` variables (YAML map)
2) `env.vars` inline variables (override file values)

If an entity sets `domain: "<name>"`, the following is also available:

- `{{domain.incoming_dir}}` resolved from the matching domain entry.

Unresolved placeholders (or unknown domains) are configuration errors.
