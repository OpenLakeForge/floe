# Floe x Airflow Integration Spec

## 1. Goal

Airflow consumes a static Floe manifest (`floe.manifest.v1`) and executes Floe runs without parsing Floe YAML configs directly at DAG parse-time.

The connector must:

- load topology from manifest at parse-time
- execute using manifest execution contract at run-time
- select runner from manifest runner contract
- publish normalized run payloads and asset materialization metadata

## 2. Source of truth

### 2.1 Manifest schema

- Schema id: `floe.manifest.v1`
- Canonical schema file: `orchestrators/schemas/floe.manifest.schema.json`

### 2.2 Manifest generation

Current CLI command:

```bash
floe manifest generate -c <config> --output <manifest_path>
```

Recommended deployment flow:

1. Generate manifest in CI/CD.
2. Validate manifest against JSON schema.
3. Deploy DAGs + manifest together.

## 3. Parse-time behavior in Airflow

At import time, Airflow connector:

1. loads manifest (`FloeManifestHook`)
2. builds static entity list and assets from `entities[]`
3. resolves config path/URI from `config_uri`

Multi-manifest mode:

- when `FLOE_MANIFEST_DIR` is set, connector loads all `*.manifest.json` and builds one DAG per manifest (`floe_<manifest_name>`)
- fallback to single-manifest mode when directory is empty/missing

No Floe subprocess is invoked during DAG parse.

Fallback mode is supported:

- if manifest is missing/invalid, DAG can still run with `FLOE_CONFIG` (no assets loaded)

## 4. Run-time execution contract

The connector executes commands from `manifest.execution`, not from hardcoded CLI args.

### 4.1 Command assembly

For a run task, final command is built as:

`[entrypoint] + base_args(rendered) + per_entity_args(rendered if entities selected)`

Placeholder support:

- `{config_uri}` -> resolved config path/URI used by the task
- `{entity_name}` -> comma-joined selected entities for current task

Current requirement:

- `execution.log_format` must be `json` (connector parses NDJSON `floe.log.v1`)

### 4.2 Result parsing

Connector expects `run_finished` event from NDJSON stream and extracts summary URI from:

- `execution.result_contract.summary_uri_field`

The run payload pushed by operator remains:

- schema: `floe.airflow.run.v1`
- stdout/stderr lines are streamed into Airflow task logs while preserving NDJSON parsing

## 5. Runner contract

Runner selection source:

- `entities[].runner` if provided
- otherwise `runners.default`

Runner lookup:

- selected runner name must exist in `runners.definitions`

### 5.1 Supported now

- `type: local_process`
  - executed by `FloeRunOperator` using local subprocess

### 5.2 Planned

- `type: kubernetes_pod` / `kubernetes_job`
  - map to Airflow Kubernetes operators
- `type: ecs_task`
  - map to Airflow ECS operator

Current fail-fast behavior:

- unsupported runner types raise explicit task error
- mixed runner names in a single `FloeRunOperator` invocation are rejected

## 6. Asset materialization

Static assets are created from manifest at parse-time.

On run completion, operator enriches outlet events with per-entity metrics using:

- `run_finished` payload
- optional `summary_uri` loaded summary file
- per-entity report reference (`entity_report_file`, typically `{run_id}/{entity}/run.json`)

## 7. Error policy

Task fails when:

- manifest schema/required fields are invalid
- runner resolution fails
- unsupported runner type is selected
- NDJSON cannot be parsed
- `run_finished` event is missing

## 8. Connector modules

- `src/airflow_floe/manifest.py`
  - manifest models + loader (`floe.manifest.v1`)
- `src/airflow_floe/manifest_discovery.py`
  - multi-manifest discovery and deterministic DAG id derivation
- `src/airflow_floe/runtime.py`
  - context build, URI/path handling, run summary loading
- `src/airflow_floe/operators.py`
  - run operator/hook, execution + runner resolution, payload emission
- `src/airflow_floe/hooks.py`
  - manifest context hook for DAG wiring

## 9. Open roadmap

1. Remote report loading (cloud URIs).
   - Support `summary_uri` and entity report loading for `s3://`, `gs://`, `abfs://` in addition to local/file.
   - Keep current local behavior unchanged and fail clearly when remote credentials are missing.
2. Runner backend implementations from manifest contract.
   - Implement `kubernetes_pod` / `kubernetes_job` and `ecs_task` mappings.
   - Preserve `local_process` as dev default.
3. Strict manifest validation at parse-time.
   - Validate every loaded manifest against `orchestrators/schemas/floe.manifest.schema.json`.
   - Fail fast with explicit validation errors in DAG import diagnostics.
4. Multi-manifest robustness.
   - Add explicit collision policy for DAG ids and asset keys across manifests.
   - Expose clear warnings/errors when conflicts occur.
5. Run payload and metadata contract hardening.
   - Stabilize `floe.airflow.run.v1` fields and document backward-compatible evolution.
   - Ensure per-entity metadata always includes `entity_report_file` when resolvable.
6. Error policy and retry semantics.
   - Define retryable vs terminal failures for NDJSON parse, missing `run_finished`, summary load issues, and runner errors.
   - Align task states and exit-code interpretation with `execution.result_contract.exit_codes`.
7. Connector CI e2e coverage.
   - Add end-to-end tests for single-manifest and multi-manifest DAG registration.
   - Add run-time tests for streamed logs + asset metadata enrichment paths.
