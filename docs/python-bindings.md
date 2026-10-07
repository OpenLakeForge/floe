# Using floe from Python

`floe-python` wraps the Floe Rust engine via [PyO3](https://pyo3.rs) and
[maturin](https://www.maturin.rs), so the same contract-first ingestion gate can
run from Python scripts, Jupyter notebooks, tests, or orchestrators. Use it when
you want Floe inside an existing Python workflow without deploying a service or
starting a separate platform instance.

A single ABI3 wheel covers Python 3.10–3.13.

```bash
pip install floe-python
```

---

## The core functions

Four functions cover the main embedded and notebook use cases:

| Function | Returns | When to use |
|---|---|---|
| `floe.validate(config_path)` | `None` | Pre-flight config check before running |
| `floe.run(config_path)` | `RunOutcome` | Execute the full ingestion pipeline |
| `floe.load_config(config_path)` | `RootConfig` | Inspect entities and settings without running |
| `floe.extract_config_env_vars(config_path)` | `dict[str, str]` | List `{{VAR}}` placeholders in the config |

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

The `_repr_html_()` method is called automatically by Jupyter. Status colors: green = Success, orange = SuccessWithWarnings, red = Failure.

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

All events have an `event` key (string discriminator) and `ts_ms` (Unix timestamp in milliseconds). The full TypedDict definitions are in [`python/floe/_floe.pyi`](../crates/floe-python/python/floe/_floe.pyi).

---

## Working with profiles

Profiles let you override config variables, storage credentials, or lineage settings without editing the YAML. There are two ways to provide them:

```python
# Option 1: load a profile YAML file
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
    # Could not reach S3/GCS/ADLS or read a local path
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

## Threading and the GIL

`floe.run()` and `floe.validate()` release the Python GIL while Rust is working, so they are safe to call from threads managed by `asyncio`, `concurrent.futures`, or a Dagster worker. You can run multiple pipelines in parallel across threads.

The observer callback is called from the Rust thread and must reacquire the GIL. Keep callbacks fast and non-blocking — avoid heavy pandas operations or network calls inside the callback. Use a queue if you need to hand off work:

```python
import queue, threading
q: queue.Queue = queue.Queue()

floe.set_observer(q.put_nowait)  # fast: just enqueues
outcome = floe.run("orders.yml")
floe.clear_observer()

while not q.empty():
    print(q.get_nowait())  # process events after the run
```

---

## Build from source

If you want to build the native extension yourself (e.g., to test a local floe-core change):

```bash
# Install maturin
pip install maturin

# Clone the repo and build in development mode
git clone https://github.com/OpenLakeForge/floe
cd floe/crates/floe-python
maturin develop          # builds and installs into your current virtualenv
```

For a release-quality wheel:

```bash
maturin build --release  # produces a .whl in target/wheels/
```

The extension is compiled as `floe._floe` (a C extension module) and re-exported through `floe/__init__.py`. The ABI3 flag (`abi3-py310`) means the same `.so` file works on Python 3.10, 3.11, 3.12, and 3.13.
