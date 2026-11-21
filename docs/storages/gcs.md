# GCS storage

This document describes current Google Cloud Storage (GCS) support in Floe. Floe reads and writes
objects with `obstore`; Iceberg tables are written through PyIceberg's PyArrow file IO.

## Requirements

Install the extra: `pip install 'floe-python[gcs]'`. A config with a `type: gcs` storage fails
validation without it.

Credentials come from the environment, never from the config: Application Default Credentials,
for example:

- `GOOGLE_APPLICATION_CREDENTIALS` (path to a service account key file)
- `GOOGLE_SERVICE_ACCOUNT` or `GOOGLE_SERVICE_ACCOUNT_KEY`
- the metadata server on Google Cloud compute, or `gcloud auth application-default login` locally

The identity needs read, write and delete on the bucket's objects: Floe writes the incremental
state, archives sources by copy then delete, and removes superseded files.

## Config example

```yaml
storages:
  default: local
  definitions:
    - name: local
      type: local
    - name: gcs_raw
      type: gcs
      bucket: my-bucket
      prefix: raw/data

entities:
  - name: customers
    source:
      format: csv
      path: inbox/customers
      storage: gcs_raw
    sink:
      accepted:
        format: parquet
        path: outbound/customers
        storage: gcs_raw
      rejected:
        format: csv
        path: outbound/customers_rejected
        storage: gcs_raw
    policy:
      severity: reject
    schema:
      columns:
        - name: customer_id
          type: string
```

`bucket` is required; `prefix` is optional. Resolved URIs are formed as:

```
gs://<bucket>/<prefix>/<path>
```

## URI forms

A path is relative to the storage (and its `prefix`), or an explicit `gs://` URI of the same
bucket. Nothing else is accepted: `gcs://` and other schemes fail validation, and a `gs://` URI of
another bucket fails with a bucket mismatch.

An object key has no leading `/` and no empty segment (`a//b`): Floe refuses such a path instead
of letting the store name another object.

## Emulators

`endpoint` points Floe at a GCS-compatible service, for example `http://127.0.0.1:4443`. Plain `http://` is allowed for an endpoint. For Iceberg, Floe passes it to
PyIceberg as `gcs.service.host`; without `endpoint`, PyIceberg uses Application Default Credentials.

fake-gcs-server does not work with Floe today: its latest release (1.56.1) has no XML-API upload
route and ignores the generation-precondition header that `obstore` sends (#1342).

## Supported behavior

- Inputs: CSV, TSV, JSON (array/ndjson), ORC, Parquet, XLSX, Avro and XML, by prefix listing plus
  suffix filtering, downloaded to a temp directory and read locally.
- Outputs: accepted Parquet parts, rejected CSV or Parquet parts, and JSON reports, each built in
  memory then uploaded. Accepted and rejected Iceberg tables with `overwrite` or `append`, through
  an Iceberg catalog whose table data lives on the GCS storage (also as the catalog's
  `warehouse_storage`).
- Incremental state and `sink.archive`.
- The state file is coordinated by compare-and-swap on the object's generation, see
  [the decision record](https://github.com/OpenLakeForge/floe/blob/main/context/decisions/incremental-state-cas.md).

## Prefix matching

A source path is a prefix, as on S3. A literal prefix without a trailing `/` matches by string
prefix, so `data/inbox` also matches `data/inbox/a.csv` and `data/inbox_old/b.csv`; add the `/` to
match the directory only. A glob in the path needs a non-empty literal prefix before the first
wildcard. Listing is recursive and sorted by key.

## Limitations

- Iceberg merges (`merge_scd1`, `merge_scd2`) and compaction on GCS fail the run with
  `config_not_supported`: DuckDB's GCS secret takes HMAC keys only, not Application Default
  Credentials. `floe validate` does not catch it.
- Delta on GCS is refused with `config_not_supported`.
- `floe manifest generate` and `floe manifest replay` refuse a config that defines
  any `gcs` (or `adls`) storage, even an unused one.
- A manifest (`--manifest`, `manifest generate -o`) and a profile (`-p`) are read and written from
  local paths or `s3://` only, not `gs://`.
- Parquet and ORC inputs use a temp download plus a local read, as on S3.
