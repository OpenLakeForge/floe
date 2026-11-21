# ADLS storage

This document describes current Azure Data Lake Storage Gen2 (ADLS) support in Floe. Floe reads and
writes through the Azure Blob endpoint of the storage account, with `obstore`.

## Requirements

Install the extra: `pip install 'floe-python[adls]'`. A config with a `type: adls` storage fails
validation without it.

Credentials come from the environment, never from the config. The `obstore` Azure store reads the
usual variables, for example:

- `AZURE_STORAGE_ACCOUNT_KEY` (account key)
- `AZURE_STORAGE_SAS_TOKEN` (shared access signature)
- `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and `AZURE_CLIENT_SECRET` (service principal)
- `AZURE_FEDERATED_TOKEN_FILE` (workload identity), or a managed identity on Azure compute

The identity needs read, write and delete on the container's blobs: Floe writes the incremental
state, archives sources by copy then delete, and removes superseded files.

## Config example

```yaml
storages:
  default: local
  definitions:
    - name: local
      type: local
    - name: lake
      type: adls
      account: mystorageaccount
      container: raw
      prefix: data

entities:
  - name: customers
    source:
      format: csv
      path: inbox/customers
      storage: lake
    sink:
      accepted:
        format: parquet
        path: outbound/customers
        storage: lake
      rejected:
        format: csv
        path: outbound/customers_rejected
        storage: lake
    policy:
      severity: reject
    schema:
      columns:
        - name: customer_id
          type: string
```

`account` and `container` are required; `prefix` is optional. Resolved URIs are formed as:

```
abfss://<container>@<account>.dfs.core.windows.net/<prefix>/<path>
```

## URI forms

A path is relative to the storage (and its `prefix`), or an explicit `abfss://` URI of the same
container and account. Nothing else is accepted: `az://`, `abfs://` and `https://` URIs fail
validation, and an `abfss://` URI of another container or account fails with a mismatch error.

An object key has no leading `/` and no empty segment (`a//b`): Floe refuses such a path instead
of letting the store name another object.

## Emulators

`endpoint` points Floe at an Azure-compatible service such as Azurite, for example
`http://127.0.0.1:10000/devstoreaccount1`. Plain `http://` is allowed for an endpoint. The URIs
Floe reports stay `abfss://<container>@<account>.dfs.core.windows.net/...`. Without `endpoint`, Floe
uses `https://<account>.blob.core.windows.net`.

## Supported behavior

- Inputs: CSV, TSV, JSON (array/ndjson), ORC, Parquet, XLSX, Avro and XML, by prefix listing plus
  suffix filtering, downloaded to a temp directory and read locally.
- Outputs: accepted Parquet parts, rejected CSV or Parquet parts, and JSON reports, each built in
  memory then uploaded.
- Incremental state and `sink.archive`.
- The state file is coordinated by compare-and-swap on the blob's ETag, see
  [the decision record](https://github.com/OpenLakeForge/floe/blob/main/context/decisions/incremental-state-cas.md).

## Prefix matching

A source path is a prefix, as on S3. A literal prefix without a trailing `/` matches by string
prefix, so `data/inbox` also matches `data/inbox/a.csv` and `data/inbox_old/b.csv`; add the `/` to
match the directory only. A glob in the path needs a non-empty literal prefix before the first
wildcard. Listing is recursive and sorted by key.

## Limitations

- Iceberg on ADLS writes through PyIceberg's fsspec FileIO (`adlfs`, installed with the `adls`
  extra). Merges and compaction attach DuckDB with an Azure secret; only `overwrite` and `append`
  are tested against Azurite.
- Delta on ADLS is refused with `config_not_supported`.
- `floe manifest generate` and `floe manifest replay` refuse a config that defines
  any `adls` (or `gcs`) storage, even an unused one.
- A manifest (`--manifest`, `manifest generate -o`) and a profile (`-p`) are read and written from
  local paths or `s3://` only, not `abfss://`.
- Parquet and ORC inputs use a temp download plus a local read, as on S3.

## Contributors

`python-integration.yml` runs the `adls` tests against Azurite in the `object-stores` job:
`uv run pytest -m adls tests/integration`, with `FLOE_AZURITE_ENDPOINT` and
`AZURE_STORAGE_ACCOUNT_KEY` set.
