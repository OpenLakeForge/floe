# Floe — Orchestrator Integrations

## Overview

Floe ships two orchestrator packages: `dagster-floe` and `airflow-floe`. Both follow the same pattern: they invoke the Floe CLI as a subprocess and parse structured JSON events from stdout to determine outcome. Neither package embeds Floe logic — they are thin adapters that translate orchestrator concepts (Dagster assets, Airflow operators) into Floe CLI invocations.

Both packages live under `orchestrators/` and are published as independent Python packages.

### Shared modules & drift guard

A few modules are deliberately kept **byte-identical** between the two packages
(`databricks_client.py`, `k8s_status.py`). Because the packages publish to PyPI
independently with no shared runtime dependency, these copies can silently
diverge — and historically did (issue #394). A CI guard,
`scripts/check_orchestrator_drift.py` (run by `.github/workflows/orchestrator-drift.yml`),
fails the build if any designated shared module differs between
`floe_dagster/` and `airflow_floe/`. When changing one of these modules, apply
the identical change to both copies.

`manifest.py` and the `*_runner.py` modules are **intentionally**
framework-specific (Dagster `Failure` vs Airflow logging/operators, different
manifest feature sets) and are not part of the drift guard.

---

## The Manifest

The manifest is a JSON file that decouples the Floe config from orchestrator-specific execution concerns. It tells the orchestrator how to invoke Floe and what entities exist.

Schema: `orchestrators/schemas/floe.manifest.schema.json`, versioned with Floe releases. `spec_version` is the Floe major.minor that generated the manifest.

### Structure

```json
{
  "generated_at_ts_ms": 1700000000000,
  "floe_version": "0.x.y",
  "spec_version": "0.8",
  "manifest_id": "abc123",
  "config_uri": "local://config.yml",
  "config_checksum": "sha256:...",
  "report_base_uri": "local://report/",
  "execution": {
    "entrypoint": "floe",
    "base_args": ["run", "-c", "{config_uri}", "--log-format", "json"],
    "per_entity_args": ["--entities", "{entity_name}"],
    "log_format": "json",
    "result_contract": {
      "run_finished_event": true,
      "summary_uri_field": "summary_uri",
      "exit_codes": { "success": "0", "partial": "2", "failed": "1" }
    },
    "defaults": {
      "env": {},
      "workdir": null
    }
  },
  "runners": {
    "default": "local",
    "definitions": {
      "local": { "type": "local_process" },
      "k8s": {
        "type": "kubernetes_job",
        "image": "my-registry/floe:0.4.0",
        "namespace": "data-platform",
        "service_account": "floe-runner",
        "resources": { "cpu": "500m", "memory_mb": 512 },
        "env": { "AWS_REGION": "eu-west-1" },
        "secrets": [
          { "name": "AWS_ACCESS_KEY_ID", "secret_name": "aws-creds", "key": "access-key-id" }
        ]
      }
    }
  },
  "entities": [
    {
      "name": "orders",
      "group_name": "bronze",
      "source_format": "csv",
      "accepted_sink_uri": "s3://bucket/bronze/orders/",
      "rejected_sink_uri": "s3://bucket/rejected/orders/",
      "asset_key": ["bronze", "orders"],
      "runner": null
    }
  ]
}
```

### Template tokens

`base_args` and `per_entity_args` support three substitution tokens expanded at runtime:

| Token | Replaced with |
|---|---|
| `{config_uri}` | Resolved path or URI to the Floe config |
| `{entity_name}` | The entity being processed (only in `per_entity_args`) |
| `{run_id}` | The orchestrator run ID (optional) |

### Generating the manifest

```bash
floe manifest generate -c config.yml -o manifest.dagster.json
```

The `manifest generate` subcommand reads the Floe config and emits a manifest with one entity entry per entity defined in the config. The `execution` block defaults are suitable for local subprocess execution; edit `runners.definitions` to add Kubernetes or Databricks targets.

Pass `--profile profiles/prod.yml` (or `-p profiles/prod.yml`) to generate a manifest for a specific environment profile.

### Config URI resolution

The manifest stores `config_uri` as either:
- A `local://` URI resolved relative to the manifest file
- An absolute path
- A cloud URI (`s3://`, `gs://`, `abfss://`)

`resolve_config_uri(manifest_path, config_uri)` in `manifest.py` handles all three forms.

---

## Profiles

Profiles are YAML overlays applied on top of the base Floe config. They let you parameterize environment-specific values (storage URIs, credentials, catalog names) without duplicating the full config.

```bash
floe run -c config.yml --profile profiles/prod.yml
# short alias:
floe run -c config.yml -p profiles/prod.yml
```

Both `run`, `validate`, and `manifest generate` accept `--profile` / `-p`.

Profile parsing in `src/floe/config/profile.py`:
- `parse_profile(path)` reads and validates the profile YAML (unknown keys are rejected)
- `apply_profile(root, profile)` merges it into the config, which is what Floe executes against

In a manifest `base_args`, you can embed the profile path as a literal arg since the manifest itself is environment-specific:
```json
"base_args": ["run", "-c", "{config_uri}", "-p", "profiles/prod.yml", "--log-format", "json"]
```

---

## dagster-floe

### How it works

`load_floe_assets(manifest_path, runner)` in `assets.py` is the entry point. It:

1. Loads the manifest via `load_manifest(manifest_path)`.
2. Resolves the config URI relative to the manifest file.
3. Creates one Dagster `@asset` per entity in `manifest.entities`.
4. Returns a `Definitions` object Dagster can register.

Each `@asset` function, when materialized:
1. Calls `runner.run_floe_entity(config_uri, run_id, entity_name, ...)`.
2. Parses NDJSON events from stdout looking for the `run_finished` event.
3. Reads the summary JSON at `finished.summary_uri` to extract per-entity row counts.
4. Raises `Failure` if exit code != 0 or status is not `success`.
5. Yields `MaterializeResult` with row count metadata.

### Runner

`InProcessRunner` (in `runner.py`) is the default when the `floe` Python core is importable (`build_runner_from_env`; `FLOE_RUNNER=local` opts out). It runs a local `floe run --manifest` execution through `floe.run_manifest` in the Dagster process, renders the observed events as the same NDJSON stdout, and turns a `FloeError` into exit code 1 with the message on stderr. In-process runs are serialized by a lock, because `floe.set_observer` is process-global. Anything else (a timeout, `defaults.env`/`workdir`, extra CLI flags, a non-`local_process` runner) falls back to `LocalRunner`.

`LocalRunner` invokes the CLI as a subprocess and returns a `RunResult(stdout, stderr, exit_code)`.

`LocalRunner` also handles routing to `kubernetes_job` and `databricks_job` runner types by delegating to `kubernetes_runner.py` and `databricks_runner.py` respectively.

```python
from floe_dagster.assets import load_floe_assets
from floe_dagster.runner import LocalRunner

defs = load_floe_assets("manifest.dagster.json", runner=LocalRunner())
```

### Definitions module

`definitions.py` exports `build_definitions(manifest_path)` and `build_definitions_from_manifest_dir(manifest_dir)`. The example `definitions.py` shows the typical setup: check `FLOE_MANIFEST_DIR` env var, fall back to a bundled example manifest.

### Runner types

| Type | Description |
|---|---|
| `local_process` | Subprocess on the Dagster worker node |
| `kubernetes_job` | Launches a Kubernetes Job, polls until completion |
| `databricks_job` | Submits a Databricks job run, polls until completion |

Each entity can override the runner via `entity.runner`; otherwise `runners.default` is used.

### Kubernetes runner fields

When `type = kubernetes_job`, the manifest runner definition supports:

| Field | Type | Description |
|---|---|---|
| `image` | string | Container image (required) |
| `namespace` | string | Kubernetes namespace (required) |
| `service_account` | string | ServiceAccount to attach to the runner Pod |
| `resources.cpu` | string | CPU request in Kubernetes quantity notation (e.g. `500m`) |
| `resources.memory_mb` | integer | Memory request in mebibytes |
| `resources.cpu_limit` | string | CPU limit (none when omitted) |
| `resources.memory_limit_mb` | integer | Memory limit in mebibytes (none when omitted) |
| `job_scope` | `entity` \| `domain` | `entity` (default): one Job per entity. `domain`: see below |
| `domain_resources` | object | Resources per domain name; used by `domain` Jobs in place of `resources` (no key-wise merge) |
| `env` | object | Plain env-var key→value pairs injected into the container |
| `secrets` | array | Kubernetes Secret key references: `[{name, secret_name, key}]` |
| `labels` | object | Pod labels added to the defaults (`app`, `floe-job`); the defaults win on a clash, because `floe-job` is how the pod is found again |
| `annotations` | object | Pod template annotations |
| `image_pull_policy` | string | Container `imagePullPolicy` |
| `security_context` | object | Container `securityContext`, verbatim |
| `node_selector` | object | Pod `nodeSelector` |
| `tolerations` | array | Pod `tolerations`, verbatim |
| `backoff_limit` | integer >= 0 | Job `backoffLimit` (default `0`) |
| `env_from` | array | Container `envFrom`, verbatim, e.g. `[{secretRef: {name: floe-env}}]` |

These fields are populated automatically when `floe manifest generate` is given a profile with a `kubernetes_job` runner block. The `dagster-floe[kubernetes]` optional extra expresses the Kubernetes client requirement.

### Job scope (`job_scope`)

- **`entity`** (default, also when unset): one Job per entity, named `floe-<entity>-<id>`. Pod start is paid once per entity; each entity has its own timeout, TTL and failure. Use it for a few large entities, or when entities need isolation.
- **`domain`**: entities of one domain whose runner has `job_scope: domain` form one subsettable multi-asset per (domain, runner), so an entity with another runner gets its own Job. Materializing it launches **one** Job named `floe-<domain>-<id>` running `floe run --manifest <uri> --entities a,b,c` for the selected entities in manifest order, one after another, so Polaris never sees overlapping commits from the Job. The Job is sized from `domain_resources[<domain>]`, else `resources`; `timeout_seconds`, TTL and polling apply to the one Job. Use it when pod start dominates (many small entities). Select the accepted assets: a rejected asset is materialized only when it is selected too, and the asset job selects both. The `floe/table` op tag (#924) is not set on a domain asset.

Dagster still reports per entity (materialization, rows, rejected counts, report URI, asset checks) from the run's `run_finished` event and summary.

**After a failure** each entity is reported from its own status in the run summary: `rejected` and `success_with_warnings` entities are materialized, an `aborted` / `failed` entity fails, and an entity absent from the summary never ran and is reported as *not run* (listed in the step failure's `not_run_entities`), not as its own failure. Since D36 (#959) a policy `severity: abort` fails only its entity and the run continues with the next one. A global failure (access, memory, any raised `FloeError`) still aborts the run. Re-executing the step reruns the selected entities; subset it to rerun only some. Report URIs come from `run_finished` (`summary_uri`, `entity_report_uris`), never built by hand (#960).

---

## airflow-floe

### How it works

`FloeRunOperator(BaseOperator)` in `operators.py` is the main Airflow operator.

```python
FloeRunOperator(
    task_id="ingest_orders",
    config_path="config.yml",
    entities=["orders"],
)
```

`template_fields = ("config_path", "entities")` — both are Jinja-templatable in Airflow DAGs.

On `execute()`:
1. Builds CLI args via `FloeRunHook.build_args()`.
2. Launches subprocess with `subprocess.Popen`, capturing stdout and stderr in parallel threads (so logs stream in real time to the Airflow task log).
3. Parses the `run_finished` NDJSON event from stdout.
4. Returns an XCom payload.

### XCom payload

Schema: `floe.airflow.run.v1`

```json
{
  "schema": "floe.airflow.run.v1",
  "run_id": "...",
  "status": "success",
  "exit_code": 0,
  "files": 3,
  "rows": 1000,
  "accepted": 980,
  "rejected": 20,
  "warnings": 5,
  "errors": 0,
  "summary_uri": "local://report/.../summary.json",
  "config_uri": "config.yml",
  "floe_log_schema": "floe.log.v1",
  "finished_at_ts_ms": 1700000000000,
  "entity": "orders"
}
```

If `entities` has more than one entry, the `entity` field is omitted (multi-entity run).

### Manifest-driven routing

`FloeRunOperator` accepts an optional `manifest_context: DagManifestContext`. When provided:
- Uses `manifest.execution` to build args (template tokens, entrypoint, etc.)
- Resolves the runner per entity and routes to `kubernetes_job` or `databricks_job` if specified
- Emits Airflow asset events via `outlet_events` in the task context

Without a manifest context the operator falls back to a plain `floe run -c <config> --log-format json` invocation.

### Stdout/stderr threading

Both streams are consumed in background threads while the process runs, so neither blocks. Both are buffered and also forwarded to the Airflow task logger (`log.info` for stdout, `log.warning` for stderr). After the process exits, the full stdout buffer is scanned for the `run_finished` event.

---

## What both orchestrators require from Floe

1. **`--log-format json`** — Floe must emit NDJSON to stdout. The manifest enforces this (`execution.log_format` must be `"json"`).
2. **`run_finished` event** — A structured `{"event": "run_finished", ...}` line on stdout signals completion and carries the summary URI, row counts, and final status.
3. **Exit codes** — `0` for success, non-zero for failure. The orchestrators check exit code first; the `run_finished` event provides additional context.
