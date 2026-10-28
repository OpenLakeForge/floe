# Floe + Dagster

This folder contains the Dagster connector for Floe.

For local setup of both Dagster and Airflow with isolated virtual environments, see:
- `orchestrators/LOCAL_DEV.md`

Current model:
- Parse-time reads Floe manifests (`floe.manifest.schema.json`), not Floe YAML.
- Supports single manifest and multi-manifest directory loading.
- Builds one asset per entity and one job per manifest.
- Runs Floe with manifest execution contract and JSON logs.
- Publishes run/entity metadata from NDJSON + summary.

## What Is Implemented

- Manifest-first orchestration (`floe.manifest.schema.json`).
- Strict schema validation at manifest load time.
- Asset generation from `entities[]` (`asset_key`, `group_name` respected).
- One upstream source asset per entity (`<entity>_source`), or one shared `sources/<project>/<name>` asset per
  named source of the manifest `sources` index (group `sources`; `<project>` is `manifest_name`, else `manifest_id`).
- Manifests load from a local path or a bucket URI (`s3://...`, `remote` extra; AWS default credential chain).
- One Dagster job per manifest (stable job names).
- Multi-manifest loading (`*.manifest.json`) with collision checks.
- Local runner (`local_process`) support.
- `execution.defaults.env` and `execution.defaults.workdir` support.
- Floe quality outcomes exposed as Dagster native Asset Checks (`cast_error`, `not_null`, `unique`, `schema_mismatch`, `file_status`) from Floe reports.

## Install

Prereqs:
- Floe 0.8 (`pip install floe-python`, **or** Docker with the `ghcr.io/openlakeforge/floe` image).
- Python 3.10+.

```bash
pip install dagster-floe
pip install "dagster-floe[floe]"   # also installs floe-python>=0.8,<0.9 for the in-process runner
```

## Supported Floe versions

| dagster-floe | Floe | Manifest `spec_version` |
| --- | --- | --- |
| 0.3.x | 0.8 | `0.8` |
| 0.2.x | 0.7 and earlier | see 0.2.x docs |

## Development install (from this repo)

```bash
python3 -m venv orchestrators/dagster-floe/.venv
source orchestrators/dagster-floe/.venv/bin/activate
pip install -e orchestrators/dagster-floe[dev]
```

## Generate a manifest

```bash
floe manifest generate \
  -c orchestrators/dagster-floe/example/config.yml \
  --output orchestrators/dagster-floe/example/manifest.dagster.json
```

## Manifest file or config: two ways to load

```python
from floe_dagster.assets import load_floe_assets
from floe_dagster.definitions import build_definitions, build_job_run_config_from_manifest

# 1. A manifest file generated beforehand (`floe manifest generate`)
assets = load_floe_assets(manifest_path="manifests/sales.json", runner=runner, domains=["sales"])

# 2. The Floe config (+ profile): the manifest is built in memory (`pip install "dagster-floe[floe]"`)
assets = load_floe_assets(config_path="/app/floe.yml", profile_path="/app/prod.yml", runner=runner, domains=["sales"])
run_config = build_job_run_config_from_manifest(config_path="/app/floe.yml", profile_path="/app/prod.yml")
defs = build_definitions(config_path="/app/floe.yml", profile_path="/app/prod.yml")
```

`manifest_path=` and `config_path=` are mutually exclusive, and both give the same assets and jobs.

- **Manifest file**: for CI/CD promotion (generate the manifest once, deploy it as an artifact), for Airflow, or
  when the Dagster image does not ship the Floe config or the `floe` Python core.
- **Config**: for an image that ships its config and the `floe` Python core. There's no separate
  `manifest generate` and upload step. The manifest is built in memory with `floe.build_manifest`, the same
  code path as `floe manifest generate`. Runs take `floe run -c <config> [-p <profile>] --entities ...` in
  place of `--manifest <uri>`, both in process and as a local subprocess or `kubernetes_job` pod. The config
  and profile paths must therefore exist where the run happens; for a `kubernetes_job`, that's the run image.

In config mode, pass `manifest_name="sales"` (to `load_floe_assets`, `build_definitions` or
`build_job_run_config_from_manifest`) to keep the op name of a `job_scope: domain` asset stable. Without it the name
derives from the config's path and checksum, so every edit of the config renames the op and breaks its history in
Dagster. The job name (`floe_<manifest_id>_job`) still derives from the manifest id; pin it with
`build_definitions(job_name=...)`.

## Run the example (repo-only)

```bash
cd orchestrators/dagster-floe
FLOE_MANIFEST_DIR=./example/manifests dagster dev
```

The example workspace loads `example/definitions.py`, which wires local example files/manifest to the reusable connector APIs.
The repository example includes two manifests by domain:
- `example/manifests/hr.manifest.json`
- `example/manifests/sales.manifest.json`

## Asset checks

Every accepted-output asset automatically gets quality checks registered in Dagster — no extra config needed. After each run, the connector reads the Floe run report and publishes pass/fail results for each entity.

| Check | Fails when |
| --- | --- |
| `file_status` | One or more input files had a failed or error-level processing status (e.g., unreadable file, parse failure) |
| `cast_error` | Type-casting failures exceeded the entity's policy threshold |
| `not_null` | Null values found in non-nullable columns |
| `unique` | Duplicate values found in uniqueness-constrained columns |
| `schema_mismatch` | Incoming file schema is incompatible with the declared schema |

Checks appear in the Dagster UI under each asset, linked to the run that produced them. A `WARN`-severity violation marks the check as a warning; `REJECT` or `ABORT` marks it as a failure.

## Reading manifests and reports from S3

`dagster-floe` reads `s3://` manifests and Floe run reports with `fsspec` (install the `remote`
extra). It configures S3 the same way the Floe run does, from the environment:

| Variable | Effect |
| --- | --- |
| `AWS_ENDPOINT_URL_S3`, else `AWS_ENDPOINT_URL` | Custom endpoint (MinIO, SeaweedFS, Ceph, ...). |
| `AWS_S3_FORCE_PATH_STYLE=true` (or `1`) | Path-style addressing, which most S3-compatible stores need. |
| `AWS_REGION`, `AWS_ACCESS_KEY_ID`, ... | The standard AWS chain, as for any boto3 client. |

Set these on the code-location pod (and on the run pods) so Dagster reads exactly where Floe wrote.
There is no need to replace the connector's reader: earlier versions (0.2.x) passed no endpoint or
path-style options, which is why some deployments patched `floe_dagster.assets._fsspec_read_text`.
From 0.3.0, remove that patch and set the variables above.

## Lineage integration

The connector automatically injects two environment variables into every floe subprocess:

- `DAGSTER_RUN_ID` — the current Dagster run ID
- `DAGSTER_JOB_NAME` — the Dagster job name

When lineage is enabled and both variables are present, floe attaches a **parent facet** to its OpenLineage run event. This facet declares that the Floe run is a child of the Dagster run, making the lineage graph show the full Dagster → Floe pipeline hierarchy.

The Floe run event's own `job.name` is derived from `lineage.job_name` in the embedded manifest lineage config, or from the config file stem. To control it explicitly, add `job_name` to your lineage block when generating the manifest:

```yaml
# prod.yml (profile)
lineage:
  url: "http://marquez:5000"
  namespace: "my-data-platform"
  job_name: "orders-pipeline"   # sets job.name on the Floe OpenLineage run event
```

```bash
floe manifest generate -c orders.yml -p prod.yml --output manifests/orders.json
```

See [docs/lineage.md](../../docs/lineage.md) for the full lineage config reference.

## Notes

- This connector does **not** parse YAML directly; it consumes Floe manifests (`spec_version` `0.8`), read from a file or built by `floe.build_manifest` in config mode.
- Connector logic lives under `src/floe_dagster/`; local wiring for demo lives in `example/definitions.py`.
- For local development without an installed `floe` command, you can point `LocalRunner` to a custom command, e.g.:
  - `LocalRunner("python -m floe")`
- Manifest runner support in connector is currently `local_process` only.
- `build_runner_from_env()` runs Floe in-process (`InProcessRunner`, through `floe.run_manifest`) when the `floe` Python core is installed in the Dagster environment, otherwise as a `floe` subprocess (`LocalRunner`). Set `FLOE_RUNNER=local` to force the subprocess. A timeout (`FLOE_RUN_TIMEOUT_SECONDS`) also runs as a subprocess, which can be killed. In-process runs are serialized within a process.
- For local setup commands, use `orchestrators/LOCAL_DEV.md`.
- Design notes and future work: `orchestrators/dagster-floe/INTEGRATION_SPEC.md`

## What Is Not Implemented Yet

- Kubernetes/ECS runner adapters.
- Cloud summary loading (`s3://`, `gs://`, `abfs://`).
- Single-process multi-entity fan-out execution mode.

## Releasing

This repo is a monorepo. Floe and this connector are versioned and tagged independently:

- Floe CLI release tags: `vX.Y.Z`
- Dagster connector release tags: `dagster-floe-vX.Y.Z` (triggers the PyPI publish workflow)

Example:

```bash
git checkout main
git pull
git tag dagster-floe-v0.1.0
git push origin dagster-floe-v0.1.0
```
