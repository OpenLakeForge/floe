# Floe x Dagster Integration Spec

## Goal

Dagster must consume Floe manifests (`floe.manifest.v1`) as the single orchestration contract, without parsing Floe YAML at runtime.

Production target model:

- 1 config per domain
- 1 manifest per config
- 1 Dagster job per manifest
- entity-level assets inside each job

## Source of truth

- Manifest schema id: `floe.manifest.v1`
- Canonical schema file: `orchestrators/schemas/floe.manifest.schema.json`
- Generation command:

```bash
floe manifest generate -c <config> --output <manifest_path>
```

## Implemented today

### Parse-time

- Loads a single manifest file or a manifest directory (`*.manifest.json`).
- Builds one Dagster asset per `entities[]` item.
- Asset key comes from `entity.asset_key`.
- Group comes from `entity.group_name`.

### Jobs

- Generates one Dagster `define_asset_job(...)` per manifest.
- Job selection is constrained to assets from that manifest only.
- Stable job naming based on `manifest_id`.
- Duplicate protection:
  - duplicate asset keys across manifests => error
  - duplicate generated job names => error

### Run-time execution

- Uses `manifest.execution` (no hardcoded CLI contract).
- Command rendering supports placeholders:
  - `{config_uri}`
  - `{entity_name}`
  - `{run_id}`
- Applies `execution.defaults.env` and `execution.defaults.workdir` in local runner.
- Enforces JSON log mode (`execution.log_format == json`).

### Runner contract

- Runner selected by:
  - `entity.runner`, fallback `runners.default`
- Current supported runner type:
  - `local_process`
- Unsupported runner types fail fast.

### Metadata and reports

- Parses NDJSON stdout and extracts final `run_finished`.
- Loads local summary from `summary_uri` when available.
- Attaches run/entity stats in Dagster materialization metadata.

### Validation

- Strict JSON Schema validation at manifest load time.
- Invalid payloads fail before asset/job construction.

### Code organization

- Connector package logic: `src/floe_dagster/`
- Repo-only example wiring: `example/definitions.py`

## Remaining work

1. Non-local runner adapters.
Use the same manifest contract for `kubernetes_*` and `ecs_task`.

2. Remote summary loading.
Read `summary_uri` for cloud backends (`s3://`, `gs://`, `abfs://`) to enrich metadata outside local mode.

3. Single-process fan-out mode (optional optimization).
Allow one Floe run for multiple selected entities, then fan out results to corresponding assets.

4. Dagster checks mapping.
Map Floe quality results to `AssetCheckResult`.

5. Multi-manifest operations polish.
Add explicit policy for cross-manifest collisions and optional selection/filter mechanisms.

## Non-goals for this phase

- No backward compatibility bridge from `floe.plan.v1`.
- No direct YAML parsing in connector runtime.
