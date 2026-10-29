# Parquet Sink (Accepted Output)

Floe can write accepted output as Parquet files on local or S3 storage.

Example:
```yaml
entities:
  - name: customers
    source:
      format: csv
      path: /data/in/customers.csv
    sink:
      accepted:
        format: parquet
        path: /data/out/customers
```

Semantics:
- `sink.accepted.format: parquet` writes one or more part files under `sink.accepted.path`.
- Write mode comes from `sink.write_mode`:
  - `overwrite`: write the new parts as `part-00000.parquet`, `part-00001.parquet`, ... and remove the old `part-*.parquet` files once every new part was written
  - `append`: add new `part-<uuid>.parquet` files alongside existing ones.
  - `merge_scd1` / `merge_scd2`: not supported for Parquet; use the [Iceberg](iceberg.md) sink.

## File splitting

Large accepted datasets are split into multiple part files. The split threshold is controlled by
`sink.accepted.options.max_size_per_file` (default 256 MB), measured on the in-memory size of
the rows. The entity's accepted rows are written once, after every input file is read.

```yaml
sink:
  accepted:
    format: parquet
    path: /data/out/orders
    options:
      compression: zstd
      row_group_size: 50000
      max_size_per_file: 268435456   # 256 MB
```

Available options:
- `compression`: `snappy`, `gzip`, `zstd` (default, Polars' default), `uncompressed`
- `row_group_size`: rows per row group (positive integer)
- `max_size_per_file`: part size threshold in bytes (default: 268435456 / 256 MB)

## Limitations

### Parquet is not transactional

Parquet has no built-in transaction or atomic-commit semantics. Floe does not implement a
write-ahead log or two-phase commit for Parquet output. This has two practical consequences:

**Incomplete files on write failure**

If a failure occurs during a Parquet write (process crash, out-of-disk, network error on remote
storage), the in-progress part file is left on disk in an incomplete or corrupt state. There is no
rollback — incomplete files must be cleaned up manually before the next run or they will coexist
with valid output files in the accepted directory.

**Overwrite is not atomic**

`overwrite` first writes every new part under a `_staging-<id>-part-NNNNN.parquet` name, which
readers of `part-*.parquet` do not see. Only when all of them were written does it copy them to
`part-NNNNN.parquet`, delete the old parts that were not replaced, then delete the staged files.
A write failure leaves the previous parts untouched (the staged files are removed). A crash or a
storage error during the final copy and delete steps can leave a mix of new and old parts (rows
duplicated or missing) and orphaned `_staging-*` files, which are safe to delete. The run report
and incremental state are not committed in that case, so Floe re-processes the same inputs on
the next run and a re-run restores a clean directory.

**If you need transactional accepted writes, use the [Iceberg](iceberg.md) sink instead.**
Each entity's Iceberg write is one atomic catalog commit.
