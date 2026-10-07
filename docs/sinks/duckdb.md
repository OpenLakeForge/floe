# DuckDB Sink (Accepted Output)

Floe can write accepted output into a [DuckDB](https://duckdb.org) database — either a
**local `.duckdb` file** or a **[MotherDuck](https://motherduck.com)** (managed remote DuckDB)
database. Writes use DuckDB's bundled engine (DuckDB 1.5.0, native `MERGE INTO`) and Arrow
ingestion, so source `RecordBatch`es feed DuckDB directly with no intermediate file.

## Availability (companion distribution)

The DuckDB sink is **not** compiled into the default `floe` CLI, Docker image, or PyPI
wheel — its bundled native build is too large for the standard release artifacts. It
ships as a separate companion that the lean `floe` transparently delegates to. Use any
of:

- the `ghcr.io/openlakeforge/floe-duckdb` Docker image, or
- a `floe-duckdb` companion binary on your `PATH` (lean `floe run` auto-re-execs it when
  a config targets a DuckDB sink), or
- the off-PyPI `floe-duckdb` Python wheel
  (`pip install floe-duckdb --index-url https://openlakeforge.github.io/floe/simple/`).

See [installation → DuckDB support](../installation.md#duckdb-support-companion-distribution)
for details. A lean build still *validates* and round-trips a DuckDB-sink config; only the
write path requires the companion.

## Local file target

```yaml
entities:
  - name: customers
    source:
      format: csv
      path: /data/in/customers.csv
    sink:
      write_mode: overwrite          # overwrite | append | merge_scd1 | merge_scd2
      accepted:
        format: duckdb
        path: /data/out/warehouse.duckdb   # local file only
        duckdb:
          table: customers           # required; may be schema-qualified ("main.customers")
          schema: main               # optional; default "main"
```

## MotherDuck target

Object stores (S3 / GCS / ADLS) **cannot** host a read-write `.duckdb` file — DuckDB performs
random in-place range overwrites that object storage does not support. Remote writes therefore go
through MotherDuck, which is addressed by a `duckdb.connection` string instead of a filesystem path:

```yaml
entities:
  - name: customers
    source:
      format: csv
      path: /data/in/customers.csv
    sink:
      write_mode: merge_scd1
      accepted:
        format: duckdb
        # no `path` and no `storage` for a MotherDuck target
        duckdb:
          connection: "md:analytics"        # MotherDuck database
          table: customers
          token: "${MOTHERDUCK_TOKEN}"       # literal or single ${ENV} reference
```

**Prefer a `${ENV_VAR}` reference for `token`** rather than a literal value in YAML, so the
secret stays out of source control and shared config. A literal value is also accepted but
discouraged, and only a `${ENV_VAR}` reference is preserved in the generated manifest (a literal
token is dropped during manifest sanitization). The resolved token is held in a redacting
`Secret` wrapper, passed to DuckDB through the connection configuration (never as SQL), and is
never written to reports or logs. If `token` is omitted, DuckDB falls back to the ambient
`motherduck_token` environment variable.

## Write modes

Write mode comes from `sink.write_mode`:

- `overwrite`: replaces **only the target table** (`CREATE OR REPLACE TABLE`). Other tables in the
  same `.duckdb` database are left untouched. Subsequent flushes within the same run append.
- `append`: creates the table if absent, then inserts rows (`INSERT INTO ... BY NAME`).
- `merge_scd1`: upsert source rows using `schema.primary_key` as the merge key (native
  `MERGE INTO`).
  - matched keys: update non-key, non-ignored columns from source
  - missing keys: insert source rows
- `merge_scd2`: close changed current rows (`__floe_is_current = false`, `__floe_valid_to = now()`)
  and insert new current versions. System columns: `__floe_is_current`, `__floe_valid_from`,
  `__floe_valid_to`.

Each buffer flush is applied inside a single DuckDB transaction (`BEGIN`/`COMMIT`), so readers
never observe a torn write. The same partial-commit caveat documented for the other soft-buffered
sinks applies across flushes — see [Parquet sink limitations](parquet.md#limitations).

## Connection handling

DuckDB is single-writer per database. Floe keeps one connection per database open for the lifetime
of the process (keyed by the canonical file path or `md:` string) and serializes writes through it,
so repeated flushes and multiple entities targeting the same database are correct by construction.
In the normal CLI flow the process exits after a run, releasing the file lock.

If you embed Floe as a **library** and want to read a local `.duckdb` file in the same process
after a run, call `floe_core::io::write::duckdb::close_cached_connections()` first to release the
write lock.

## Merge mode notes

- Requires `schema.primary_key` in config (non-empty).
- Source duplicates on the merge key are handled during row checks before the write.
- `sink.accepted.merge.compare_columns` / `ignore_columns` are honored for change detection.
- Merge metrics (`inserted_count`, `updated_count`, `closed_count`, row counts) are reported in
  the entity report `accepted_output`, derived from `MERGE INTO ... RETURNING merge_action`.

## Validation

- `sink.accepted.format: duckdb` requires a `sink.accepted.duckdb` block with a non-empty `table`.
- A MotherDuck `connection` (`md:<database>`) and a filesystem `storage` are mutually exclusive.
- Object-store file paths (S3 / GCS / ADLS) are rejected with a message recommending MotherDuck.
- A `duckdb.connection` that is not a MotherDuck (`md:`) string is rejected.
- Merge modes require `schema.primary_key`.

## Unique key deduplication

`schema.unique_keys` works end-to-end across runs. Before each run, Floe scans the target table
to seed the uniqueness tracker with existing keys, so rows that were written in a previous run
are correctly detected as duplicates in the current run. This requires the target table to be
readable — if it does not exist yet, seeding is skipped and all incoming rows are treated as new.

## Limitations

- No object-store read-write `.duckdb` files (physically unsupported by DuckDB); use MotherDuck.
- **Schema evolution is not supported.** `schema.schema_evolution` is ignored for DuckDB sinks.
  Add, rename, or remove columns manually before running, or use `write_mode: overwrite` to
  replace the table structure on each run.
- DuckDB-native partitioning is not used; `partition_by` does not apply to DuckDB sinks.
- DuckDB is a sink only — Floe does not read from DuckDB as a source.
- Rejected output is not written to DuckDB (accepted only).
