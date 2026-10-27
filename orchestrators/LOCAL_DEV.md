# Local Orchestrators Setup (Dagster + Airflow)

This guide sets up one shared Python virtual environment from the repo root:
- `.venv-orchestrators` for both `orchestrators/dagster-floe` and `orchestrators/airflow-floe`

Recommended working directory for all commands in this guide:
- repo root (`floe/`)

Using repo root keeps relative paths stable for:
- `orchestrators/*/example/config.yml`
- Airflow DAG folder path
- Floe input/output paths in examples

## Prerequisites

- Python 3.12 installed (recommended for Airflow stability)
- Floe CLI available (`floe --version`) or use `uv run floe` from the repo root

## 1) Create a shared venv (once)

From `floe/`:

```bash
python3.12 -m venv .venv-orchestrators
source .venv-orchestrators/bin/activate
pip install --upgrade pip
pip install -e orchestrators/dagster-floe[dev]
pip install -e orchestrators/airflow-floe[dev]
```

`uv` variant:

```bash
uv venv .venv-orchestrators --python 3.12
source .venv-orchestrators/bin/activate
uv pip install -e orchestrators/dagster-floe[dev]
uv pip install dagster-webserver
uv pip install -e orchestrators/airflow-floe[dev]
```

## 2) Run Dagster example

From `floe/`:

```bash
source .venv-orchestrators/bin/activate
export FLOE_MANIFEST_DIR="$PWD/orchestrators/dagster-floe/example/manifests"
# optional single-manifest override:
# export FLOE_MANIFEST="$PWD/orchestrators/dagster-floe/example/manifest.dagster.json"
dagster dev -w orchestrators/dagster-floe/workspace.yaml
```

Open Dagster UI at `http://127.0.0.1:3000`.

## 3) Run Airflow example (without default examples)

From `floe/` in another terminal:

```bash
source .venv-orchestrators/bin/activate
export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/orchestrators/airflow-floe/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES="False"
export FLOE_CMD="floe"
export FLOE_MANIFEST="$PWD/orchestrators/airflow-floe/example/manifest.airflow.json"
# optional multi-manifest mode (1 manifest => 1 DAG):
# export FLOE_MANIFEST_DIR="$PWD/orchestrators/airflow-floe/example/manifests"
```

If your Airflow DB was initialized previously with examples enabled, reset once:

```bash
airflow db reset -y
```

Start Airflow:

```bash
airflow standalone
```

Open Airflow UI at `http://127.0.0.1:8080`.

- Username: `admin`
- Password: generated in terminal (or in `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`)

Trigger DAG:
- `floe_example_operator`
  - If `FLOE_MANIFEST_DIR` is set, Airflow creates one DAG per manifest (`floe_<manifest_name>`).

After at least one successful run, check Airflow Assets UI:
- assets are declared from `FLOE_MANIFEST` at parse time
- materialization metadata is published when run tasks finish
- task logs include streamed Floe NDJSON/stdout/stderr lines

Deactivate when done:

```bash
deactivate
```

## 4) Optional: use the local checkout instead of installed `floe`

In Airflow env:

```bash
export FLOE_CMD="uv run --project /path/to/floe floe"
```

## 5) Troubleshooting

- Task stuck in `queued`:
  - verify scheduler is alive:

```bash
airflow jobs check --job-type SchedulerJob --allow-multiple --limit 5
```

- If scheduler crashes on Python 3.14:
  - use Python 3.12 for `.venv-orchestrators`.

- DAGs not visible:
  - confirm `AIRFLOW__CORE__DAGS_FOLDER` points to `orchestrators/airflow-floe/dags`
  - unpause DAGs if needed:

```bash
airflow dags unpause floe_example_operator
```
