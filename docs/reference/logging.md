# Logging

Floe can emit structured run events for orchestrators like Dagster/Airflow.

## Modes

### Default (no flag)

- Intended for interactive CLI use.
- Prints a human summary at the end of the run.

### `--log-format json`

- Intended for orchestrators.
- **stdout is NDJSON only**: one JSON object per line.
- **stderr contains human output** (summary, warnings, errors).

Each JSON line includes:
- `schema: "floe.log.v1"` (stable)
- `level: "info" | "warn" | "error"` (stable)
- `event: ...` plus event-specific fields

The final line on stdout is always the `run_finished` event.

## Event types

- `run_started`: run metadata (`run_id`, `config`, `report_base`, `ts_ms`); inside a Dagster run also `dagster_run_id` and `dagster_job_name`, read from `DAGSTER_RUN_ID` and `DAGSTER_JOB_NAME` (absent when unset)
- `entity_started`: entity name (`name`, `ts_ms`)
- `file_started`: entity/input pair (`entity`, `input`, `ts_ms`)
- `file_finished`: file outcome and counts (`status`, `rows`, `accepted`, `rejected`, `elapsed_ms`, `ts_ms`)
- `schema_evolution_applied`: additive schema-evolution audit event (`entity`, `mode`, `added_columns`, `ts_ms`); emitted once per entity run in which `add_columns` adds columns to an Iceberg table
- `entity_finished`: entity totals (`status`, `files`, `rows`, `accepted`, `rejected`, `warnings`, `errors`, `timings_ms`, `ts_ms`); `timings_ms` is the entity report's object (`list_ms`, `download_wait_ms`, `read_validate_ms`, `rejected_write_ms`, `accepted_write_ms`, `archive_ms`), or `null` for a failed entity; a failed entity also has `error_code` when the error carries a typed `code`
- `run_finished`: final run totals and exit code (`status`, `exit_code`, `files`, `rows`, `accepted`, `rejected`, `warnings`, `errors`, `summary_uri`, `report_write_ms`, `ts_ms`); `report_write_ms` is the time to write `run.summary.json`

### `timings_ms`

Cumulative wall-clock milliseconds per phase of one entity, summed over its files; the boundaries are in [report.md](report.md#timings_ms-entity-report). The phases overlap (reading, validating and the accepted write interleave chunk by chunk, and downloads run ahead), so `download_wait_ms` is only the time spent waiting, and the sum is not the entity's elapsed time. `null` means the phase did not run; `0` means it ran in under a millisecond. The Iceberg commit is part of `accepted_write_ms`.
- `log`: structured warning/error line (`log_level`, `code`, `message`, optional `entity`, optional `input`, `ts_ms`)
  - `error_code` (optional): the typed `code` of the error behind the failure-report log; absent when the error has none. `code` stays `run_failure_report`.
  - `code` is `null` or a stable snake_case tag. Today: `heartbeat_failed` (warn): renewing the incremental claims failed; the run continues and the next beat retries, but the claims expire after an hour without a successful renewal.

### `--log-format off` (default)

- Suppresses all structured event output.
- Human summary is still printed to stderr at the end of the run.
- **OpenLineage events are still posted** when `lineage:` is configured — log
  format does not gate lineage emission.
- Lineage HTTP errors (401, timeouts) are emitted as warnings to stderr even
  when log output is otherwise suppressed.

### `--log-format text`

- Prints the same lifecycle events as plain text lines.
- Also prints the human end-of-run summary.

## Examples

### Orchestrator parsing (conceptual)

```bash
floe run -c config.yml --log-format json | jq -cr 'select(.event == "run_finished")'
```

### Split streams

```bash
# machine events
floe run -c config.yml --log-format json > events.ndjson

# human logs
floe run -c config.yml --log-format json 2> run.log
```

### Schema evolution event

```json
{"schema":"floe.log.v1","level":"info","event":"schema_evolution_applied","run_id":"2026-03-11T16-31-00Z","entity":"customer","mode":"add_columns","added_columns":["email"],"ts_ms":1741710660000}
```
