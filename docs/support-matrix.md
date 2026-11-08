# Floe Support Matrix (0.8)

This matrix reflects **current behavior** of Floe 0.9 (not aspirational). Floe 0.9 supports
local and S3 storage. S3 inputs are downloaded to a temp directory and read locally; Parquet
parts, rejected CSV and reports are built in memory, then uploaded; Iceberg tables are
written through PyIceberg (and DuckDB for merges) directly to S3.

## Dropped in 0.8

These 0.7 features are rejected by `floe validate` with a "not supported in 0.8" error (D1, D26):

| Dropped | Instead |
|---|---|
| Fixed-width input (`source.format: fixed`) | Convert to CSV upstream |
| Delta Lake accepted sink and Unity Catalog | Iceberg |
| DuckDB / MotherDuck accepted sink | Iceberg (merges run through DuckDB) |
| Catalog-less Iceberg tables (filesystem catalog) | Iceberg REST catalog |
| ADLS and GCS storage | S3 or an S3-compatible store |

The 0.7 implementation is archived at
[OpenLakeForge/floe-rust](https://github.com/OpenLakeForge/floe-rust).

## Inputs

| Format | Local | S3 | Notes |
|---|---|---|---|
| CSV | ✅ | ✅ (temp) | Suffix filter `.csv` |
| TSV | ✅ | ✅ (temp) | Suffix filter `.tsv` |
| JSON (array) | ✅ | ✅ (temp) | `source.options.json_mode=array` (default) |
| JSON (ndjson) | ✅ | ✅ (temp) | `source.options.json_mode=ndjson` |
| Parquet | ✅ | ✅ (temp) | Suffix filter `.parquet` |
| ORC | ✅ | ✅ (temp) | Suffix filter `.orc` |
| Avro | ✅ | ✅ (temp) | Suffix filter `.avro` |
| XLSX | ✅ | ✅ (temp) | `source.options.sheet` (name), `header_row`/`data_row` (1-based) |
| XML | ✅ | ✅ (temp) | `source.options.row_tag` required; selectors in `schema.columns[].source`; DOCTYPEs are refused |

Notes:
- File extensions are matched case-insensitively: `.csv`, `.tsv`, `.parquet`, `.orc`, `.avro`,
  `.xlsx`, `.xml`, and `.json`, `.jsonl`, `.ndjson`, `.djson` for JSON.
- S3 inputs are resolved by **prefix listing** plus suffix filtering, then downloaded to temp
  files. A glob in an S3 path needs a non-empty literal prefix before the first wildcard.
- `source.options.glob` and `recursive` apply to local directory inputs.
- `.floe/` paths and the entity's archive location are never listed as inputs (D23, D31).
- Nested JSON values (objects/arrays) are rejected.

## Outputs

| Output | Local | S3 | Notes |
|---|---|---|---|
| Accepted: Parquet | ✅ | ✅ (upload) | Writes `part-*.parquet`; `overwrite` and `append` |
| Accepted: Iceberg | ❌ | ✅ (REST or Glue catalog) | `overwrite`, `append`, `merge_scd1` and `merge_scd2` in one catalog commit per entity; merges need a REST catalog |
| Rejected: CSV | ✅ | ✅ (upload) | Dataset parts `part-*.csv`; the default |
| Rejected: Parquet | ✅ | ✅ (upload) | Dataset parts `rows/part-*.parquet`, bounded by size; raw-value columns plus `__floe_*` metadata, see [Config](config.md). `output.rejected_path` is the `rows` directory |
| Rejected: Iceberg | ❌ | ✅ (REST or Glue catalog) | Opt-in quarantine table through the accepted sink's catalog binding: all-string declared columns plus `__floe_*` metadata, one append commit per entity run; at-least-once (a retry appends again) and not atomic with the accepted commit. See [Config](config.md) |
| Reports: JSON | ✅ | ✅ (upload) | |

Notes:
- `sink.write_mode` applies to accepted and rejected outputs (`overwrite`, `append`,
  `merge_scd1`, `merge_scd2`). Merge modes need an Iceberg accepted sink.
- An Iceberg accepted sink requires a REST or AWS Glue catalog in `catalogs.definitions` (D26)
  and S3 storage for the table data.
- Iceberg `partition_spec` supports `identity`, `year`, `month`, `day`, `hour` and
  `bucket` (with `num_buckets`, D12).
- `schema.schema_evolution.mode: add_columns` is Iceberg-only and additive-only (D13).
- Iceberg `append` commits carry an input fingerprint, so a retry after a crash does not write
  the same batch twice (D5).
- Iceberg merges are merge-on-read and leave position delete files. `floe maintain` expires old
  snapshots and removes orphan files; see [CLI](cli.md).
- PII `strategy: drop` is rejected for Iceberg sinks (the table enforces the full declared schema).

## Accepted-output report metadata

- `accepted_output` in each entity report can include:
  - `files_written`, `parts_written`, `part_files`
  - `table_version` and `snapshot_id` (Iceberg)
  - `total_bytes_written`, `avg_file_size_mb`, `small_files_count`
  - merge metrics for `merge_scd1` (see [report.md](report.md))

## Cloud storage behavior

- Storage paths resolve to canonical URIs: `s3://<bucket>/<prefix>/<path>`.
- Prefix listing is stable and lexicographically sorted.
- S3-compatible stores (MinIO, SeaweedFS, …) work through `AWS_ENDPOINT_URL_S3` /
  `AWS_ENDPOINT_URL`; see [storages/s3.md](storages/s3.md).

## Fault scenarios (Iceberg and S3)

Tested against local services (Polaris + SeaweedFS in `python-integration.yml`, moto for Glue), nightly
and on demand. Anything else is untested against a real cloud.

| Scenario | Status |
|---|---|
| Commit conflict on a REST catalog (stale writer retries on the fresh table) | Automated (Polaris) |
| Rival copy of the same batch is not appended twice | Automated (Polaris) |
| Writer failure mid-stream commits nothing; the retry lands once | Automated (Polaris) |
| Read-after-commit: a fresh reader sees the new snapshot | Automated (Polaris) |
| Glue catalog: overwrite, append, per-domain databases | Automated (moto, not real AWS) |
| Real AWS S3 + Glue, cloud-backed Polaris | Manual: needs credentials and costs money |
| Credential expiry mid-run | Manual: the local harness cannot expire a credential |
| Missing permissions (denied catalog or bucket access) | Manual |
| Object-store partial failure (a part uploaded, then the connection drops) | Manual |

## Scale (200 entities)

`tests/scale/` generates a split project (`example/project/` layout) of 200 small entities in 5 domains:
3 named sources (CSV, JSON, Parquet), domain defaults for the sinks and `incremental_mode: file`, and 2 input
files per entity with 3 rejected rows out of 20. `pytest -m scale tests/scale -rP` runs it nightly and on demand
(`python-integration.yml`) and prints the table below. Measured on 4 vCPUs, local disk, Parquet sinks:

| Step | Runtime | Peak RSS |
|---|---|---|
| `floe --version` (cold start) | 0.4 s | 101 MiB |
| `floe validate` (200 entities) | 0.6 s | 105 MiB |
| `floe manifest generate` (200 entities) | 0.6 s | 107 MiB |
| `floe run`, 400 files, 4000 rows, 3400 accepted, 600 rejected, 4 entities at a time | 3.0 s | 189 MiB |
| the same, one entity at a time | 5.6 s | not measured |
| restart: all 400 files already ingested, nothing read | 1.8 s | 154 MiB |
| retry after 3 new files: only those 3 are read | 1.8 s | 181 MiB |
| `floe run --entities <one>` of the same project | 0.9 s | 178 MiB |

A run costs about 0.4 s of start-up plus about 12 ms per entity here. The 10-entity Iceberg subset
(`-m "polaris and scale"`, nightly in the `polaris` job) asserts one snapshot per table after the first run, none
added by a restart, one added by a retry with a new file, and the rejected counts of the run report; its timings
are in the nightly log, not measured locally (no Docker). Times are one machine's, and noisy: the tests assert
counts and a 600 MiB RSS ceiling, not runtimes.

## Config keys (storage)

```yaml
storages:
  default: local
  definitions:
    - name: local
      type: local
    - name: s3_raw
      type: s3
      bucket: my-bucket
      region: eu-west-1
      prefix: data
```

Storage references:
- `entities[].source.storage`
- `entities[].sink.accepted.storage`
- `entities[].sink.rejected.storage`
