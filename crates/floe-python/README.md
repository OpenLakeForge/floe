# floe-python

Python bindings for [floe](https://github.com/OpenLakeForge/floe) — a high-performance data ingestion and quality engine written in Rust.

Run floe pipelines at full Rust speed directly from Python notebooks, scripts, or orchestrators.

## Installation

```bash
pip install floe-python
```

## Quick start

```python
import floe

# Validate a pipeline config
floe.validate("pipeline.yml")

# Run the full pipeline
outcome = floe.run("pipeline.yml")
print(outcome.summary["results"])

# Dry run — see which files would be processed
outcome = floe.run("pipeline.yml", dry_run=True)
for preview in outcome.dry_run_previews:
    print(preview["name"], preview["scanned_files"])

# Run a subset of entities
outcome = floe.run("pipeline.yml", entities=["customers", "orders"])

# Live progress in Jupyter
floe.set_observer(lambda e: print(f"[{e['event']}]", e.get("name", "")))
outcome = floe.run("pipeline.yml")
floe.clear_observer()

# Profile overrides (variables + catalog config)
floe.run("pipeline.yml", profile_path="profile.yml")
floe.run("pipeline.yml", profile_vars={"output_bucket": "s3://my-bucket"})

# Inspect / reset incremental state
state = floe.inspect_entity_state("pipeline.yml", "customers")
floe.reset_entity_state("pipeline.yml", "customers")
```

## Error handling

```python
try:
    floe.validate("config.yml")
except floe.FloeConfigError as e:
    print(f"Config problem: {e}")
except floe.FloeRunError as e:
    print(f"Run failed: {e}")
except floe.FloeError as e:
    print(f"Floe error: {e}")
```

## API reference

| Function | Description |
|---|---|
| `validate(config_path, *, entities, profile_vars, profile_path)` | Validate a config file |
| `run(config_path, *, entities, dry_run, run_id, profile_vars, profile_path)` | Execute the pipeline |
| `load_config(config_path)` | Parse config into a `RootConfig` object |
| `extract_config_env_vars(config_path)` | List placeholder variables in the config |
| `inspect_entity_state(config_path, entity_name)` | Inspect incremental state for an entity |
| `reset_entity_state(config_path, entity_name)` | Delete incremental state for an entity |
| `set_observer(callback)` | Register a live-event callback |
| `clear_observer()` | Remove the current callback |

## Jupyter

`RunOutcome` renders as a color-coded HTML table automatically in Jupyter — no extra code needed. Just end a cell with the variable:

```python
outcome = floe.run("orders.yml")
outcome  # renders inline HTML table with per-entity status, accepted/rejected counts
```

Use `outcome.to_dict()` to turn results into a plain dict for pandas:

```python
import pandas as pd
df = pd.DataFrame(outcome.entity_reports)
```

## Observing progress

Register a callback to receive live events as the run proceeds:

```python
floe.set_observer(lambda e: print(f"[{e['event']}]", e.get("name", e.get("entity", ""))))
outcome = floe.run("orders.yml")
floe.clear_observer()
```

Event types: `run_started`, `entity_started`, `file_started`, `file_finished`, `schema_evolution_applied`, `entity_finished`, `run_finished`, `log`. See the [full guide](../../docs/python-bindings.md#observing-runs-in-real-time) for all event fields.

## Profile overrides

Override config variables or cloud credentials without editing the YAML:

```python
floe.run("orders.yml", profile_vars={"incoming_root": "s3://my-bucket/incoming"})
floe.run("orders.yml", profile_path="prod.yml")
```

## Building from source

```bash
pip install maturin
git clone https://github.com/OpenLakeForge/floe
cd floe/crates/floe-python
maturin develop
```

## License

Apache 2.0

---

→ Full API reference and examples: [docs/python-bindings.md](../../docs/python-bindings.md)
