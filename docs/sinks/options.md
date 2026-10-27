# Sink options

Floe supports `sink.accepted.options` for tuning accepted output writers.

- Parquet applies `compression`, `row_group_size`, and `max_size_per_file`.
- Iceberg uses only `max_size_per_file`, to set the report's small-file threshold
  (`small_files_count` counts files under half of it). Iceberg maintenance runs with
  `floe maintain`, not through this block.

## Parquet options

- `compression`: `snappy`, `gzip`, `zstd`, `uncompressed`
- `row_group_size`: positive integer (rows per row group)
- `max_size_per_file`: positive integer bytes (default: 256MB; split accepted parquet into parts)

`compression` defaults to Polars' default (`zstd`).

Notes:

- For Parquet, this controls write-time file splitting.
- Parquet datasets get no compaction or maintenance from Floe; Iceberg tables get it from
  `floe maintain` (see [CLI usage](../cli.md)).

Example:

```yaml
sink:
  accepted:
    format: "parquet"
    path: "/data/out/customers"
    options:
      compression: "zstd"
      row_group_size: 50000
      max_size_per_file: 268435456
```
