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

## AWS Glue catalog on S3 (optional)

```yaml
storages:
  default: "local_fs"
  definitions:
    - name: "local_fs"
      type: "local"
    - name: "s3_out"
      type: "s3"
      bucket: "my-bucket"
      region: "us-east-1"

catalogs:
  default: "glue_main"
  definitions:
    - name: "glue_main"
      type: "glue"
      region: "us-east-1"
      database: "lakehouse"
      warehouse_storage: "s3_out"
      warehouse_prefix: "warehouse/iceberg"

entities:
  - name: "customer"
    domain: "sales"
    source:
      format: "csv"
      path: "/data/in/customers.csv"
    sink:
      write_mode: "append"
      accepted:
        format: "iceberg"
        path: "unused/customer_iceberg" # data location is derived from the catalog warehouse
        storage: "s3_out"
        iceberg:
          catalog: "glue_main"
          table: "customer"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
```

## AWS credentials for S3 / Glue writes

Floe resolves AWS credentials for Glue/S3 Iceberg writes through the standard AWS SDK
credential chain. The supported sources (in the SDK's default priority order) include:

- Static environment variables (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` /
  `AWS_SESSION_TOKEN`).
- Web identity / IRSA (`AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN`).
- **EKS Pod Identity / ECS container credentials**
  (`AWS_CONTAINER_CREDENTIALS_FULL_URI` + `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`).
- EC2 instance metadata (IMDS).

The Iceberg writer's underlying object-store layer does not, on its own, understand EKS Pod
Identity container credentials and would otherwise fall back to EC2 IMDS. Floe resolves the
credentials up front through the AWS SDK and hands them to the writer, so EKS Pod Identity and
the other container-credential modes work for both the Glue catalog metadata and the S3 data
and metadata writes — no extra configuration is required. To use explicit keys instead, set
the `AWS_*` environment variables.

## ADLS Gen2 (filesystem catalog)

Iceberg tables on Azure Data Lake Storage Gen2 use filesystem catalog semantics.

### Authentication

The Iceberg write path (OpenDAL/reqsign) and the metadata listing path both use
the same credential resolution order:

1. **Account key** — set `AZURE_STORAGE_ACCOUNT_KEY` in the environment. Floe
   forwards it as `adls.account-key` to the storage layer. Simplest option for
   local development and CI.
2. **SAS token** — set `AZURE_STORAGE_SAS_TOKEN`. Floe forwards it as
   `adls.sas-token`; the token must have at least List + Read permissions on
   the container.
3. **Service principal** — set `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, and
   `AZURE_CLIENT_SECRET`. Reqsign reads these automatically; no extra config
   needed in the floe manifest.
4. **Workload identity / managed identity** — picked up automatically from the
   Azure environment.

> **Note on Azure CLI (`az login`)**: Azure CLI works for management-plane
> operations (e.g. `az storage account keys list`) but is not consumed by the
> Iceberg OpenDAL storage path. Use account key, SAS, service-principal, workload
> identity, or managed identity credentials for Iceberg data and metadata IO.

```yaml
storages:
  default: "local_fs"
  definitions:
    - name: "local_fs"
      type: "local"
    - name: "adls_out"
      type: "adls"
      account: "mystorageaccount"
      container: "lakehouse"
      prefix: "warehouse/iceberg"

entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/data/in/customers.csv"
    sink:
      write_mode: "append"
      accepted:
        format: "iceberg"
        path: "customer_iceberg"
        storage: "adls_out"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
```

REST catalog (Polaris, Nessie, Snowflake) is also supported with ADLS data
locations — set `sink.accepted.iceberg.catalog` to a REST catalog definition and
the `warehouse_storage` field to the ADLS storage definition.

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
is held in a redacting `Secret` wrapper and is never written to reports or logs.

## Partition spec config

Iceberg accepted sinks can declare a partition spec in config, and Floe executes
it at write time for Iceberg table creation/data file layout:

```yaml
sink:
  accepted:
    format: "iceberg"
    path: "/data/out/customer_iceberg"
    partition_spec:
      - column: "event_date"
        transform: "day"
      - column: "country"
        transform: "identity"
```

Current scope:
- partition columns must exist in `schema.columns`
- supported transforms: `identity`, `year`, `month`, `day`, `hour`
- partition spec is applied to Iceberg table metadata and partitioned file writes
- append validates partition-spec compatibility with existing tables (no implicit partition-spec evolution)
- compaction/optimization remains external to Floe

## Supported scope (current)

- Accepted sink only (`sink.accepted.format: iceberg`)
- Local, S3, or GCS storage (`sink.accepted.storage` must resolve to `local`, `s3`, or `gcs`)
- `sink.write_mode`: `overwrite` and `append`
- Scalar column types only (for example: string, bool, signed ints, floats, date/time/datetime)
- Append creates a new Iceberg snapshot/metadata version
- Overwrite replaces logical table contents by recreating table metadata at the same root (no cleanup/GC)
- S3/GCS writes use direct Iceberg/object_store `FileIO`
- S3 supports optional AWS Glue catalog registration (`sink.accepted.iceberg.catalog`)
- Glue mode uses S3 for actual table data/metadata location
- Run report `accepted_output` includes:
  - `table_root_uri`, `write_mode`, `files_written`, `parts_written`, `part_files`
  - `table_version` (Iceberg metadata version, when available)
  - `snapshot_id` (when a snapshot exists)
  - `iceberg_catalog_name`, `iceberg_database`, `iceberg_namespace`, `iceberg_table` (Glue mode)
  - file sizing metrics (`total_bytes_written`, `avg_file_size_mb`, `small_files_count`)
- Iceberg file sizing metrics are based on Iceberg data files only (metadata/manifests excluded)

## Limitations / non-goals (current)

- No schema evolution
- No merge/upsert
- No cleanup/garbage collection of orphaned files/metadata
- Snapshot expiry only, with `floe maintain` (Python core, see [CLI usage](../cli.md)); no compaction

## Glue catalog scope and limitations

- Catalog type supported by Floe today: AWS Glue (`catalogs.definitions[].type: glue`)
- Glue-backed Iceberg requires S3-backed table locations (no GCS/ADLS Glue mode)
- Floe creates/updates the Glue table entry for the configured Iceberg table location
- The target Glue database must already exist
- Floe does not manage Glue database lifecycle or table maintenance workflows

## Manual S3 integration test

- A gated test exists in `crates/floe-core/tests/integration/iceberg_s3_run.rs`.
- It is skipped by default. To run it:
  - set AWS credentials in your environment
  - set `FLOE_RUN_MANUAL_S3_ICEBERG_TEST=1`
  - optionally set `FLOE_TEST_S3_BUCKET` (default: `floe-test`)
  - optionally set `FLOE_TEST_S3_REGION` (falls back to `AWS_REGION`, then `us-east-1`)
  - optionally set `FLOE_TEST_S3_PREFIX` for a custom cleanup prefix

## Manual GCS integration test

- A gated test exists in `crates/floe-core/tests/integration/iceberg_gcs_run.rs`.
- It is skipped by default. To run it:
  - set Google Cloud credentials for ADC (Application Default Credentials)
  - set `FLOE_RUN_MANUAL_GCS_ICEBERG_TEST=1`
  - optionally set `FLOE_TEST_GCS_BUCKET` (default: `floe-test`)
  - optionally set `FLOE_TEST_GCS_PREFIX` for a custom cleanup prefix

## Manual AWS Glue integration test (S3-backed)

- A gated test exists in `crates/floe-core/tests/integration/iceberg_glue_run.rs`.
- It is skipped by default. To run it:
  - set AWS credentials with S3 + Glue permissions
  - ensure the target Glue database already exists (default: `floe_test`)
  - set `FLOE_RUN_MANUAL_GLUE_ICEBERG_TEST=1`
  - optionally set `FLOE_TEST_S3_BUCKET` (default: `floe-test`)
  - optionally set `FLOE_TEST_GLUE_REGION` or `FLOE_TEST_S3_REGION` (falls back to `AWS_REGION`)
  - optionally set `FLOE_TEST_GLUE_DATABASE`
  - optionally set `FLOE_TEST_GLUE_PREFIX` for a custom cleanup prefix

## Empty accepted dataset behavior

- Overwrite/create with an empty accepted dataframe creates the table metadata layout
  (no data files, no snapshot yet).
- Append with an empty accepted dataframe is a no-op for table contents.
