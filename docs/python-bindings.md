# Python API

Floe is a Python library: `import floe` gives the same contract-first ingestion
gate as the `floe` command, from Python scripts, Jupyter notebooks, tests, or
orchestrators. Use it when you want Floe inside an existing Python workflow
without deploying a service or starting a separate platform instance.

The package is pure Python and needs Python 3.11 or later.

```bash
pip install floe-python
```

The public API is what `floe.__all__` exports: the functions below, the
`RunOutcome`, `RootConfig` and `EntityConfig` result types, the typed exceptions,
the `RunEvent` TypedDicts, and `floe.__version__`.

---

## The core functions

| Function | Returns | When to use |
|---|---|---|
| `floe.validate(config_path)` | `None` | Pre-flight config check before running |
| `floe.run(config_path)` | `RunOutcome` | Execute the full ingestion pipeline |
| `floe.run_manifest(manifest_path)` | `RunOutcome` | Run from a generated manifest, as `floe run --manifest` |
| `floe.build_manifest(config_path, profile_path=None)` | `dict` | The manifest `floe manifest generate --deterministic` writes, built in memory |
| `floe.load_config(config_path)` | `RootConfig` | Inspect entities and settings without running |
| `floe.extract_config_env_vars(config_path)` | `dict[str, str]` | List `{{VAR}}` placeholders in the config |
| `floe.maintain(config_path, older_than_days=7, retain_last=10, orphan_delay_days=3, dry_run=False, compact=False, experimental=False)` | `list[dict]` | Expire old Iceberg snapshots and remove orphan files (compaction with `compact=True, experimental=True`), as `floe maintain` |
| `floe.inspect_entity_state(config_path, entity_name)` / `floe.reset_entity_state(config_path, entity_name)` | `dict` / `bool` | Incremental state, as `floe state inspect` / `floe state reset` (see [below](#incremental-state)) |
| `floe.set_observer(callback)` / `floe.clear_observer()` | `bool` / `None` | Receive run events as they happen (see [below](#observing-runs-in-real-time)) |

`run`, `run_manifest` and `maintain` also take `entities=[...]` to select entities;
`run` and `run_manifest` take `dry_run`, `run_id` and `full_refresh`, like the CLI
flags of the same names. `validate`, `run` and `maintain` take `profile_path` and
`profile_vars` (see [Working with profiles](#working-with-profiles)).

A quick example for each:

```python
import floe

# Check the config is valid before kicking off a scheduled run
floe.validate("orders.yml")

# Dry run — see what would be processed without moving any data
outcome = floe.run("orders.yml", dry_run=True)
for preview in outcome.dry_run_previews:
    print(preview["name"], "→", preview["scanned_files"], "files")

# Inspect what entities are defined (useful in notebooks or CI)
config = floe.load_config("orders.yml")
print(config.entity_names)          # ["orders", "returns"]
print(config.entities[0].source_format)  # "csv"

# See which env vars need to be set before the pipeline can run
placeholders = floe.extract_config_env_vars("orders.yml")
# e.g. {"report_root": "", "incoming_root": ""}
```

---

## Reading results

`floe.run()` returns a `RunOutcome`. The most useful properties:

```python
outcome = floe.run("orders.yml")

# Top-level summary
summary = outcome.summary
print(summary["run"]["status"])              # "success" | "success_with_warnings" | "failed"
print(summary["results"]["rows_total"])      # total rows scanned
print(summary["results"]["accepted_total"])  # rows written to accepted sink

# Per-entity status lives in summary["entities"], not entity_reports
for entity in summary["entities"]:
    print(entity["name"], entity["status"])  # "success" | "rejected" | "failed" | …

# Per-entity detail reports (schema, file-level results, write metadata)
for report in outcome.entity_reports:
    name = report["entity"]["name"]
    acc  = report["results"]["accepted_total"]
    rej  = report["results"]["rejected_total"]
    print(f"{name}: {acc} accepted, {rej} rejected")

# Quick dict for pandas or downstream processing
data = outcome.to_dict()
```

### Jupyter integration

In a Jupyter notebook, `RunOutcome` renders as a color-coded HTML table automatically — no extra code required:

```python
# In a notebook cell, just end with the variable name:
outcome = floe.run("orders.yml")
outcome   # renders inline HTML table with per-entity status rows
```

The `_repr_html_()` method is called automatically by Jupyter. Status colors: green = `success`, orange = `success_with_warnings`, red = anything else.

---

## Observing runs in real time

floe-python supports a single global observer callback that receives structured events as they happen. You register it before `run()`, then clear it after.

```python
import floe

def on_event(event: dict) -> None:
    kind = event["event"]
    if kind == "entity_finished":
        print(f"  {event['name']}: {event['accepted']} accepted, {event['rejected']} rejected")
    elif kind == "run_finished":
        print(f"Run {event['run_id']}: {event['status']}")

floe.set_observer(on_event)
outcome = floe.run("orders.yml")
floe.clear_observer()
```

The callback is **swappable between calls** — calling `set_observer` again replaces the current one. If your callback raises an exception it is silently ignored so the run is never aborted by observer code.

### Event types

| Event | Key fields |
|---|---|
| `run_started` | `run_id`, `config`, `report_base`, `ts_ms` |
| `entity_started` | `run_id`, `name`, `ts_ms` |
| `file_started` | `run_id`, `entity`, `input`, `ts_ms` |
| `file_finished` | `run_id`, `entity`, `input`, `status`, `rows`, `accepted`, `rejected`, `elapsed_ms` |
| `schema_evolution_applied` | `run_id`, `entity`, `mode`, `added_columns` |
| `entity_finished` | `run_id`, `name`, `status`, `files`, `rows`, `accepted`, `rejected`, `warnings`, `errors` |
| `run_finished` | `run_id`, `status`, `exit_code`, `files`, `rows`, `accepted`, `rejected`, `summary_uri` |
| `log` | `run_id`, `log_level`, `code`, `message`, `entity`, `input` |

All events have an `event` key (string discriminator) and `ts_ms` (Unix timestamp in milliseconds). Each event type is exported as a TypedDict (`floe.RunStartedEvent`, …, `floe.LogEvent`; `floe.RunEvent` is their union), defined in [`src/floe/run/events.py`](../src/floe/run/events.py).

---

## Working with profiles

Profiles let you override config variables, storage credentials, or lineage settings without editing the YAML. There are two ways to provide them:

```python
# Option 1: load a profile YAML file (a local path or an s3:// URI)
floe.run("orders.yml", profile_path="prod.yml")

# Option 2: pass overrides inline (useful in notebooks and CI)
floe.run("orders.yml", profile_vars={
    "report_root": "/data/reports",
    "incoming_root": "s3://my-bucket/incoming",
})

# validate() also accepts profile overrides
floe.validate("orders.yml", profile_vars={"report_root": "/tmp"})
# Note: load_config() is profile-unaware — it takes only config_path
```

`profile_vars` and `profile_path` can be combined — inline vars take precedence over the file. See [docs/profiles.md](profiles.md) for the full profile format.

---

## Incremental state

When a pipeline uses `incremental_mode: file`, floe tracks which files have already been ingested. Two helper functions let you inspect and reset that state:

```python
# See what floe has already processed for an entity
state = floe.inspect_entity_state("orders.yml", "orders")
print(state["incremental_mode"])  # "file"
print(state["state"])             # dict with ingested file list, or None

# Clear state to force a full re-ingest on the next run
deleted = floe.reset_entity_state("orders.yml", "orders")
print(deleted)  # True if state existed and was removed
```

---

## Error handling

floe-python raises typed exceptions so you can handle config problems and run failures separately:

```python
try:
    outcome = floe.run("orders.yml")
except floe.FloeConfigError as e:
    # YAML is invalid or a required field is missing
    print(f"Config error: {e}")
except floe.FloeStorageError as e:
    # Could not reach S3 or read a local path
    print(f"Storage error: {e}")
except floe.FloeRunError as e:
    # Pipeline ran but encountered a fatal data error
    print(f"Run failed: {e}")
except floe.FloeError as e:
    # Catch-all for any other floe error
    print(f"Floe error: {e}")
```

Exception hierarchy:

```
FloeError
├── FloeConfigError   — config parse or validation failure
├── FloeRunError      — pipeline execution failure
├── FloeStorageError  — cloud storage or local I/O failure
└── FloeIoError       — lower-level I/O error
```

---

## Threads

`floe.run()` runs its entities one after another in the calling thread and calls
the observer from that thread, so the callback blocks the run while it executes.
Keep it fast, and use a queue to hand off work:

```python
import queue
q: queue.Queue = queue.Queue()

floe.set_observer(q.put_nowait)  # fast: just enqueues
outcome = floe.run("orders.yml")
floe.clear_observer()

while not q.empty():
    print(q.get_nowait())  # process events after the run
```

The observer is global to the process: runs in parallel threads share it.

---

## Developing Floe

To work on the library itself, see [`CONTRIBUTING.md`](../CONTRIBUTING.md):

```bash
git clone https://github.com/OpenLakeForge/floe
cd floe
uv sync --all-groups
uv run pytest
```
