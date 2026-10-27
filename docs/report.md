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

(`<entity.name>` is `<domain>/<name>` for an entity with a qualified id, see [config](config.md#entity-identity).)

When every entity of the run belongs to one domain (D37, 0.8), the run directory sits
under that domain (`<report.path>/<domain>/run_<run_id>/`), so a domain's runs are listed together, and `run.summary.json`
carries `run.domain`. A run spanning several domains, entities without a domain, or a
pre-0.3 domain holding `.`, `/` or `\` (not a safe directory name) keeps `<report.path>/run_<run_id>/` and has no `run.domain`. `run_finished`'s
`summary_uri` and `entity_report_uris` point at the actual location; `report_base`
stays `<report.path>`. Floe 0.7 always wrote `<report.path>/run_<run_id>/`.

`run_id` is a filename-safe ISO-like string: `YYYY-MM-DDTHH-MM-SSZ`
(colons replaced by dashes). The report JSON must include the full
`report_file` path.

Each entity produces its own `run.json`. The run summary aggregates all entities.

Golden example files live under `example/report/run_2026-01-19T10-23-45Z/`.

## Top-level sections

### Run summary (`run.summary.json`)

- `spec_version`: Config version string.
- `tool`: Tool name/version and optional git info.
- `run`: Run ID, `domain` (only when the run has a single domain, D37), timestamps, duration, status, and exit code.
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
  - Iceberg: metadata version (parsed from metadata filename)
- `snapshot_id`: Iceberg snapshot id (when a snapshot exists)
- `iceberg_catalog_name`, `iceberg_namespace`, `iceberg_table`: present for Iceberg runs

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

Merge-specific metrics (Iceberg `merge_scd1`):
- `merge_key`: merge key columns (`schema.primary_key`)
- `inserted_count`
- `updated_count`
- `merge_elapsed_ms`

### `schema_evolution` (entity report)

- `enabled`, `applied`, `incompatible_changes_detected`: always `false` in 0.8
- `mode`: resolved config mode (`strict` or `add_columns`)

`mode: add_columns` does add new columns to an Iceberg table (D13), but 0.8 does not record
them here and emits no `schema_evolution_applied` event yet.

Notes:
- Metrics are populated when the writer can collect them cheaply and reliably.
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

An entity aborted by `policy.severity: abort` fails only that entity (D36, 0.8): its `run.json`
is written with status `aborted`, and the run continues with the next entity. The run summary
lists every entity, and the run status and exit code stay `aborted` / `2`. A technical error
(storage, catalog, credentials, memory) still stops the run at that entity, with no summary.
Floe 0.7 stopped the run at the first aborted entity.

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
