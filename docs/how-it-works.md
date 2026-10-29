# How Floe Works

Floe is a contract-first ingestion gate. For each entity, it validates raw files
or extracted datasets before they enter the trusted layer, writes valid rows to
the accepted sink, routes invalid rows to the rejected sink, and records what
happened in machine-readable reports.

The runtime is a lightweight single-node Python package. Floe reads and validates
with Polars, writes Iceberg tables through PyIceberg (one catalog commit per
entity), and runs Iceberg merges through DuckDB, which spills to disk. With
`append`, a unique check against the target's history reads the target's key
columns once per entity into a local file, and each chunk checks its keys
against that file. The libraries exchange data as Arrow, so no extra
serialization step sits between them.

This document describes the canonical ingestion pipeline Floe uses for each
entity. The order is deterministic and is reflected in reports.

## Pipeline phases

### A) Entity planning (entity-level)

1. Resolve input files/objects (local directory/glob or storage prefix).
   - In normal runs, resolved cloud objects are then downloaded to temp files.
   - In dry-run, resolution is list-only for cloud inputs (no downloads, no writes).
2. Resolve storage targets for accepted/rejected/report outputs.
3. If `incremental_mode: file` is enabled, load the entity state file/object and
   skip any file URI already recorded as previously ingested.
   - If the current size/mtime still matches the recorded state, Floe skips the file silently.
   - If the size/mtime changed, Floe still skips the file and emits an
     `incremental_file_changed` warning instead of reprocessing it.
   - If another run has an active claim on the file, Floe skips it for this run
     and emits an `incremental_file_claimed` warning.
   - Remaining pending files are claimed with a conditional state update before
     accepted sink output is written.
4. Prepare output directories if needed.

### B) File-level prechecks (per file)

Floe inspects only the file header/schema before reading full data:

- **CSV/TSV**: header row (or the first row for headerless CSV/TSV).
- **Parquet**: schema metadata.
- **ORC**: schema metadata.
- **NDJSON**: the first valid JSON object line.
- **Avro**: writer schema fields.
- **XLSX**: configured sheet header row.
- **XML**: fields from the first matching `row_tag` element.

Then it applies the schema mismatch policy:

- Missing columns: `fill_nulls` or `reject_file`
- Extra columns: `ignore` or `reject_file`

If `reject_file` is triggered, the file is rejected/aborted according to
`policy.severity`, and **row-level validation does not run** for that file.

### C) Row-level validation (per file)

For files that passed prechecks:

1. Read full data into a dataframe.
2. Apply casts (`cast_mode`).
3. Apply `not_null` checks.
4. Build per-row error lists and counters.

### D) Entity-level uniqueness (across files)

After row-level validation, uniqueness is evaluated across the **entire entity**
in file order. The first occurrence is accepted; later duplicates are flagged.
With `append`, keys already in the accepted sink count as seen,
so a row that duplicates the existing history is flagged too.

### E) Entity-level accepted output (across files)

Accepted rows from input files are written to the accepted sink in file order,
once per entity. For Parquet sinks, Floe writes a dataset directory containing
`part-*.parquet` (`sink.accepted.options.max_size_per_file` splits it into
parts). For Iceberg sinks, `overwrite` and `append` land in one PyIceberg
transaction, and `merge_scd1` stages the batch as Parquet, then runs one DuckDB
`MERGE INTO`; either way the entity is one catalog commit. With `sink.archive`,
input files are archived only after that commit succeeds (D32).

### F) Incremental state commit

When `incremental_mode: file` is enabled, Floe first claims pending file URIs in
state. Local state uses atomic file replacement. Remote state on S3 uses optimistic
conditional writes so concurrent runners do not silently
overwrite one another.

When the entity finishes successfully, Floe promotes this run's claims into the
durable processed-file map with observed size/mtime metadata. Later runs reuse
that state to skip any file URI already present in the state file. If the entity
does not finish successfully, Floe removes this run's claims so those files can
be retried. Claims expire after one hour so a later runner can recover work from
a crashed process.

The recorded metadata is used only to detect that a previously ingested file has
changed since the earlier run. In that case Floe emits an
`incremental_file_changed` warning, but it still does not reprocess the file
automatically.

## Severity behavior

- **warn**: keep rows, emit warnings and error reports.
- **reject**: reject rows with violations; write rejected output.
- **abort**: abort the file when violations occur. The entity is reported
  `aborted` and the run continues with the next entity (D36); the run still
  exits `2`.

## Report mapping

- `files[].mismatch`: file-level precheck outcome (missing/extra + action).
- `files[].validation`: row-level + entity-level checks (cast/not_null/unique).
- `accepted_output`: entity-level accepted dataset summary (path + parts).
- `results`: totals aggregated across files and checks.

See `docs/report.md` for the report schema and `docs/checks.md` for rule details.
