# Logging (Orchestrator-safe)

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

- `run_started`: run metadata (`run_id`, `config`, `report_base`, `ts_ms`)
- `entity_started`: entity name (`name`, `ts_ms`)
- `file_started`: entity/input pair (`entity`, `input`, `ts_ms`)
- `file_finished`: file outcome and counts (`status`, `rows`, `accepted`, `rejected`, `elapsed_ms`, `ts_ms`)
- `schema_evolution_applied`: additive schema-evolution audit event (`entity`, `mode`, `added_columns`, `ts_ms`); reserved, not emitted in 0.8
- `entity_finished`: entity totals (`status`, `files`, `rows`, `accepted`, `rejected`, `warnings`, `errors`, `ts_ms`)
- `run_finished`: final run totals and exit code (`status`, `exit_code`, `files`, `rows`, `accepted`, `rejected`, `warnings`, `errors`, `summary_uri`, `ts_ms`)
- `log`: structured warning/error line (`log_level`, `code`, `message`, optional `entity`, optional `input`, `ts_ms`)

`schema_evolution_applied` is part of the event contract, but 0.8 does not emit it yet, even when `add_columns` adds columns to an Iceberg table.

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
