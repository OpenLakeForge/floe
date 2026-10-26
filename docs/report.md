# Floe Run Report

This document describes the JSON run report emitted by `floe run`
and the status/exit code rules.

## Goals

- Provide a deterministic, machine-readable summary of each run.
- Avoid logging raw row values (PII risk).
- Support multiple input files and entities under the same run.

Reports only include aggregate counts and row indices for examples; raw row
values are excluded.

## Report directory layout

Reports are written under the configured report directory:

```
<report.path>/run_<run_id>/run.summary.json
<report.path>/run_<run_id>/<entity.name>/run.json
```

`run_id` is a filename-safe ISO-like string: `YYYY-MM-DDTHH-MM-SSZ`
(colons replaced by dashes). The report JSON must include the full
`report_file` path.

Each entity produces its own `run.json`. The run summary aggregates all entities.

Golden example files live under `example/report/run_2026-01-19T10-23-45Z/`.

## Top-level sections

### Run summary (`run.summary.json`)

- `spec_version`: Config version string.
- `tool`: Tool name/version and optional git info.
- `run`: Run ID, timestamps, duration, status, and exit code.
- `config`: Config path, version, and metadata (free-form).
- `report`: Report base path and the full summary report file path.
- `results`: Totals for files, rows, accepted/rejected rows, warnings, errors.
- `entities`: Per-entity totals + entity report file paths.

### Entity report (`<entity>/run.json`)

- `spec_version`: Config version string.
- `entity`: Entity name and metadata (free-form).
- `source`: Input format, path, options, cast mode, read plan, and resolved inputs.
- `sink`: Accepted/rejected/archive paths and formats.
- `policy`: Severity.
- `accepted_output`: Entity-level accepted output summary (path, row counts, write metadata, and file metrics).
- `schema_evolution`: Entity-level schema-evolution summary for the accepted write path.
- `results`: Totals for files, rows, accepted/rejected rows, warnings, errors.
- `files`: Per-file outcomes and validation summary.

### `accepted_output` (entity report)

`accepted_output` is the main place to inspect write-time sink metadata after a run.

Common fields:
- `path`: accepted sink target URI/path used for the run
- `table_root_uri`: table/dataset root URI (defaults to `path` when not format-specific)
- `write_mode`: resolved write mode (`overwrite` or `append`)
  - possible values: `overwrite`, `append`, `merge_scd1`, `merge_scd2`
- `accepted_rows`: total accepted rows written for the entity
- `files_written`: number of accepted data files written (format-specific semantics; `null` when unknown)
- `parts_written`: writer part count (may differ from `files_written`, especially for table formats)
- `part_files`: capped list of output data-file basenames (when collected)

Format-specific metadata:
- `table_version`: version identifier when the sink exposes one
  - Delta: Delta table version
  - Iceberg: metadata version (parsed from metadata filename)
- `snapshot_id`: Iceberg snapshot id (when a snapshot exists)
- `iceberg_catalog_name`, `iceberg_database`, `iceberg_namespace`, `iceberg_table`:
  present for Iceberg runs using Glue catalog registration

Write-time file sizing metrics (optional):
- `total_bytes_written`
- `avg_file_size_mb`
- `small_files_count`

Accepted-output metrics contract:

| Report value | Meaning |
| --- | --- |
| `null` | Unknown or not computable |
| `0` | Computed exactly and the exact value is zero |
| `> 0` | Computed exactly and the exact value is non-zero |

This applies to `files_written`, `total_bytes_written`, `avg_file_size_mb`, and
`small_files_count`. Floe does not use sentinel values such as `-1`.

Merge-specific metrics (optional, Delta or DuckDB `merge_scd1` / `merge_scd2`):
- `merge_key`: merge key columns (`schema.primary_key`)
- `inserted_count`
- `updated_count` (`merge_scd1`: updated rows, `merge_scd2`: closed previous current rows)
- `closed_count` (`merge_scd2` only): rows closed from current target set
- `unchanged_count` (`merge_scd2` only): source rows that matched active target rows with no data change
- `target_rows_before`
- `target_rows_after`
- `merge_elapsed_ms`

### `schema_evolution` (entity report)

- `enabled`: `true` when schema evolution is active for the entity write path
- `mode`: resolved config mode (`strict` or `add_columns`)
- `applied`: `true` when Floe actually added Delta columns during the write
- `added_columns`: ordered list of Delta columns added in that write
- `incompatible_changes_detected`: `true` when Floe detected non-additive changes and failed the write

Notes:
- Current runtime support is Delta-only and additive-only.
- Floe writes an explicit no-op shape (`applied=false`, empty `added_columns`) when
  `mode=add_columns` is enabled but the target schema already matches the write schema.
- This block is populated for entity reports even when the write path remains strict.

Example:

```json
{
  "schema_evolution": {
    "enabled": true,
    "mode": "add_columns",
    "applied": true,
    "added_columns": ["email", "country_code"],
    "incompatible_changes_detected": false
  }
}
```

## Lifecycle event for schema evolution

When Floe adds Delta columns, it emits a structured `schema_evolution_applied` lifecycle event.

JSON envelope example (`--log-format json`):

```json
{
  "schema": "floe.log.v1",
  "level": "info",
  "event": "schema_evolution_applied",
  "run_id": "2026-03-11T16-31-00Z",
  "entity": "customer",
  "mode": "add_columns",
  "added_columns": ["email"],
  "ts_ms": 1741710660000
}
```

Text example (`--log-format text`):

```text
schema_evolution_applied entity=customer mode=add_columns added_columns=email
```

Notes:
- Metrics are populated when the writer can collect them cheaply and reliably.
- Delta metrics are derived from committed Delta log `add` actions for the committed version.
- Remote Delta metrics are collected best-effort after the write; if collection fails,
  the run still succeeds; `part_files` may be empty and accepted-output metrics remain `null`.
- Iceberg file sizing metrics count Iceberg data files only (not metadata/manifests).


## Status and exit code rules

Per-file status:
- `success`: No errors for the file.
- `rejected`: Rows were rejected and written to the rejected output.
- `aborted`: The file triggered abort behavior.
- `failed`: Technical error (I/O, parsing, unexpected failure).
- `skipped`: The file was not read. `skip_reason` says why:
  - `already_ingested`: `incremental_mode: file` state already holds the file.
  - `already_ingested_changed`: the same, but its size or mtime changed (with a warning).
  - `active_claim`: another live run has claimed the file.
  - `already_committed` (0.8): an `append` to Iceberg retried a batch whose rows a dead run
    already committed (the snapshot's `floe.input_hash` matches), so the file is marked done
    without being read or written again. See [Incremental](incremental.md#retries-of-an-append).

Run status:
- `success`: All files succeeded.
- `success_with_warnings`: No rejected/aborted/failed files, but warnings were emitted.
- `rejected`: At least one file was rejected and none failed/aborted.
- `aborted`: At least one file aborted and none failed.
- `failed`: Any file failed.

Exit codes:
- `0`: success, success_with_warnings, or rejected.
- `2`: aborted (policy).
- `1`: failed (technical error).

## Validation payloads

Validation summaries report aggregated counts only (no row-level examples).

Aggregations:
- `validation.errors`/`warnings` are totals for the file.
- `validation.rules[]` aggregates violations by rule and column.

Example rule summary:

```json
{
  "rule": "not_null",
  "severity": "reject",
  "violations": 12,
  "columns": [
    {"column": "customer_id", "violations": 12, "source": "CUSTOMER-ID"}
  ]
}
```

Row-level error details are written to the rejected error report JSON
(`*_reject_errors.json`) when available; reports do not include examples.
When a column has an explicit `source`, row error logs include a `source`
field (JSON) or an extra column/annotation (CSV/text) alongside `column`.
