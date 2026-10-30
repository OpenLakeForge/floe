# Environment Profiles

Environment Profiles let you define per-environment configuration values
(variables, runner settings) in a standalone YAML file.
Profile variables feed into the same `{{VAR}}` placeholder substitution used in
Floe config files, giving you a clean separation between logic (the config) and
environment-specific values (the profile).

## Machine-readable schema

A JSON-Schema-compatible YAML schema is provided at [`schemas/profile.schema.yaml`](../schemas/profile.schema.yaml).
It covers every field defined in this document with types, `required` /
`additionalProperties` constraints, and inline descriptions.

**AI-assisted authoring** — paste the contents of `schemas/profile.schema.yaml` into
your AI assistant (or point it at the file) and ask it to generate a valid
profile for your environment.  The schema enumerates all allowed values (e.g.
`runner.type: local|kubernetes_job|databricks_job`) and marks required fields, so generated output can be
validated immediately with a JSON Schema validator or `floe validate --profile <file>`.

## Schema v1

```yaml
apiVersion: floe/v1          # required; must be exactly "floe/v1"
kind: EnvironmentProfile     # required; must be exactly "EnvironmentProfile"
metadata:                    # required
  name: dev                  # required; non-empty string identifier
  description: "..."         # optional
  env: dev                   # optional; free-form environment label
  tags: [development, local] # optional; string list
execution:                   # optional
  runner:
    type: local              # required when execution is present
  orchestration:             # optional; see "Running entities in parallel"
    strategy: parallel       # optional; "sequential" (default) or "parallel"
    max_concurrent_entities: 4  # optional; integer >= 1
variables:                   # optional; flat string → string map
  KEY: value
validation:                  # optional
  strict: false              # optional bool; default behaviour is determined by the config
```

### Required fields

| Field | Notes |
|-------|-------|
| `apiVersion` | Must equal `floe/v1`. |
| `kind` | Must equal `EnvironmentProfile`. |
| `metadata.name` | Non-empty string. |

All other fields are optional.

---

## Profile location

A profile is a local path, an `s3://` URI or `env:NAME`, for `-p` / `--profile` on every command and for `profile_path=`
in the Python API:

```bash
floe run -c config.yml -p s3://my-bucket/profiles/prod.yml
```

An `s3://` profile is read with the AWS default credential chain (`AWS_ENDPOINT_URL_S3` and
`AWS_S3_FORCE_PATH_STYLE` for S3-compatible stores), as an `s3://` manifest is. A missing object fails with
`profile not found: <uri>`. `floe manifest generate -p s3://...` records the URI as `profile_uri`, and the
SHA-256 of the object as `profile_checksum`. The config (`-c`) is still a local path.

`-p env:NAME` reads the profile YAML from environment variable `NAME`, so a container or Job needs no profile file:

```bash
FLOE_PROFILE_CONTENT="$(cat profile.yml)" floe run -c config.yml -p env:FLOE_PROFILE_CONTENT
```

An unset or empty variable fails with a `FloeConfigError` naming it. The profile behaves exactly as a file one
(`${VAR}` references are expanded the same way). A profile never rebases paths, so a relative path it supplies
(e.g. through `variables`) resolves against the config's directory, as for a file profile; there is no profile
directory to resolve against. `execution.runner` is validated only: `floe run` always runs in-process and never launches
a runner, so a Job started with its own profile does not start another Job. `floe manifest generate -p env:NAME`
records `env:NAME` as `profile_uri` and the SHA-256 of the variable's content as `profile_checksum`.

---

## Variable precedence and resolution

When variable sources are merged at runtime the following precedence applies
(highest wins):

```
config variables  >  env file variables  >  profile variables
```

- **Profile variables** – defined in `variables:` of the profile file.
- **Env file variables** – loaded from the YAML file named by `env.file:` in the main Floe YAML config.
- **Config variables** – defined in `env.vars:` of the main Floe YAML config.

Variable values may themselves contain `${VAR}` references that are
expanded recursively (nested resolution).  Unresolved placeholders and
circular references both produce an immediate, actionable error before
the run begins.

For the full resolution rules and nested reference examples see
**[`docs/variables.md`](variables.md)**.

---

## Examples

### dev.yml

```yaml
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: dev
  env: dev
execution:
  runner:
    type: local
variables:
  CATALOG: dev_catalog
  SCHEMA: dev_bronze
  TABLE_PREFIX: dev
  INCOMING_DIR: /tmp/floe/incoming
  OUTPUT_DIR: /tmp/floe/output
  REPORT_DIR: /tmp/floe/reports
validation:
  strict: false
```

### uat.yml

```yaml
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: uat
  env: uat
execution:
  runner:
    type: local
variables:
  CATALOG: uat_catalog
  SCHEMA: uat_bronze
  TABLE_PREFIX: uat
  INCOMING_DIR: /data/uat/incoming
  OUTPUT_DIR: /data/uat/output
  REPORT_DIR: /data/uat/reports
validation:
  strict: true
```

### prod.yml

```yaml
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: prod
  env: prod
execution:
  runner:
    type: local
variables:
  CATALOG: prod_catalog
  SCHEMA: prod_bronze
  TABLE_PREFIX: prod
  INCOMING_DIR: /data/prod/incoming
  OUTPUT_DIR: /data/prod/output
  REPORT_DIR: /data/prod/reports
validation:
  strict: true
```

### databricks.yml (runner MVP)

```yaml
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: prod-dbx
  env: prod
execution:
  runner:
    type: databricks_job
    workspace_url: https://adb-1234.5.azuredatabricks.net
    existing_cluster_id: 1111-222222-abc123
    config_uri: dbfs:/floe/configs/prod.yml         # passed to floe CLI as `-c`
    python_file_uri: dbfs:/floe/bin/floe_entry.py   # spark_python_task python_file
    job_name: floe-{domain}-{env}                   # rendered before job lookup/create
    auth:
      service_principal_oauth_ref: env://DATABRICKS_TOKEN
variables:
  CATALOG: prod_catalog
validation:
  strict: true
```

`config_uri` and `python_file_uri` are intentionally separate: the first is the
floe YAML config the CLI is invoked with (`-c`), the second is the Python entry
script Databricks executes as `spark_python_task.python_file`. They are usually
distinct artifacts.

Databricks auth expectations for orchestrator runtimes:

- `auth.service_principal_oauth_ref` is required in the profile/manifest
  contract and must be of the form `env://<VAR>` (schema-enforced). When set,
  it is the source of truth and **takes precedence** over any
  `FLOE_DATABRICKS_OAUTH_TOKEN` fallback.
- If `auth.service_principal_oauth_ref` is not set, the runtime falls back to
  `FLOE_DATABRICKS_OAUTH_TOKEN`.
- Other reference schemes (for example `secret://...`) are rejected at runtime
  and at schema-validation time. Resolve them upstream into an env var before
  the orchestrator process starts.

Per-run env vars and `existing_cluster_id`:

- Databricks does not allow per-run OS env vars on a `spark_python_task`
  attached to an existing cluster. `runner.env_parameters` is therefore not
  delivered per-run; values that the floe CLI reads from the environment
  (e.g. `FLOE_ENV`) must be configured at cluster level. The orchestrator
  process still reads `env_parameters.FLOE_ENV` to render `{env}` in
  `job_name`, but does not push it to the running task.

### kubernetes.yml (pod options)

A `kubernetes_job` runner can shape the Job's pod. Every option is optional and is carried into the
manifest runner definition only when set; dagster-floe applies them (airflow-floe ignores them).

```yaml
execution:
  runner:
    type: kubernetes_job
    image: registry/floe:0.8
    namespace: floe-prod
    command: [python, -m, floe]              # container command, a string or a list
    labels: {olf.io/flow: ingest}            # added to the defaults (`app`, `floe-job`), which win on a clash
    annotations: {team: data}                # pod template annotations
    image_pull_policy: IfNotPresent
    security_context: {runAsNonRoot: true}   # container securityContext, verbatim
    node_selector: {pool: batch}
    tolerations: [{key: batch, operator: Exists, effect: NoSchedule}]   # verbatim
    backoff_limit: 2                         # Job backoffLimit, default 0
    poll_interval_seconds: 2                 # seconds between Job status reads, default 10
    env_from: [{secretRef: {name: floe-env}}]   # container envFrom, verbatim
```

`labels`, `annotations` and `node_selector` are string maps, `backoff_limit` is a non-negative integer,
and `security_context`, `tolerations` and `env_from` are copied as written: Kubernetes validates them.

`poll_interval_seconds` is an integer of at least 1 (default 10): dagster-floe reads the Job's status that
often, so a Job that finishes early is only noticed at the next poll. A lower value such as `2` shortens
short Jobs at the cost of more Kubernetes API reads.

`job_scope` (`entity` by default, or `domain`) sets how many Jobs a run launches. `entity` starts one Job
per entity: each pays pod start and one poll interval, but they are isolated and run in parallel through
Dagster's executor. `domain` starts one Job per domain: pod start and polling are paid once, but its
entities run one after another.

---

## Using profile variables in a Floe config

Profile variables are consumed by the standard `{{VAR}}` placeholder syntax in
the main config file.  The example below shows how catalog and schema values
defined in a profile can drive path construction.

```yaml
version: "0.8"
report:
  path: "{{REPORT_DIR}}"
entities:
  - name: customers
    source:
      format: csv
      path: "{{INCOMING_DIR}}/customers.csv"
    sink:
      accepted:
        format: parquet
        path: "{{OUTPUT_DIR}}/{{TABLE_PREFIX}}_customers.parquet"
    policy:
      severity: warn
    schema:
      columns:
        - name: customer_id
          type: string
          nullable: false
```

---

## Validation errors

The parser and validator return actionable errors for common problems:

| Situation | Error message (example) |
|-----------|------------------------|
| Wrong `apiVersion` | `profile.apiVersion: expected "floe/v1", got "floe/v2"` |
| Wrong `kind` | `profile.kind: expected "EnvironmentProfile", got "SomethingElse"` |
| Missing `metadata` | `profile.metadata is required` |
| Missing `metadata.name` | `profile.metadata.name is required` |
| Unknown field | `unknown field profile.unknownField` |
| Unknown runner | `profile.execution.runner.type: unknown runner "kubernetes"; known runners: local, kubernetes_job, databricks_job` |
| Missing `python_file_uri` | `profile.execution.runner.python_file_uri is required for databricks_job` |
| Unresolved variable | `profile variable "PATH_VAR" contains unresolved placeholder: ${UNDEFINED}` |

---

## File layout convention

Place profiles alongside the main config or in a dedicated `profiles/`
directory:

```
my-project/
├── config.yml          # main Floe config
└── profiles/
    ├── dev.yml
    ├── uat.yml
    └── prod.yml
```

See `example/profiles/` in this repository for ready-to-use templates.
The machine-readable field reference is in `schemas/profile.schema.yaml`.

## Environment catalog overrides

Profiles can carry a `catalogs:` block with the same catalog definition shape as
main Floe configs. When a command is run with both `--config` and `--profile`,
the profile `catalogs:` block replaces the config-level `catalogs:` block for
that invocation. This keeps entity logic in the shared config while moving
Iceberg catalog endpoints and Glue databases (e.g. `database: dev_{domain}_silver`) into
environment-specific files.

```yaml
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: prod
catalogs:
  default: prod_polaris
  definitions:
    - name: prod_polaris
      type: rest
      uri: https://polaris.example.com/api/catalog
      warehouse: bronze
      credential: "client_credentials:${POLARIS_CLIENT_ID}:${POLARIS_CLIENT_SECRET}"
      warehouse_storage: lake_s3
      warehouse_prefix: iceberg
```

Notes:

- `warehouse_storage` still references a storage definition from the main config.
- Profile-only validation (`floe validate --profile profiles/prod.yml`) checks the
  profile schema, known keys, runner contracts, variable placeholder syntax,
  duplicate catalog names, and `catalogs.default` references.
- Full config validation (`floe validate -c config.yml --profile profiles/prod.yml`)
  additionally checks catalog/storage compatibility and entity sink bindings.

## Running entities in parallel

`execution.orchestration` also sets how many entities one `floe run` process runs at once
(`floe run -c … -p …`, `floe run --manifest …` generated with the profile, and `floe.run(…, profile_path=…)`).
Dagster reads the same fields to cap its own executor.

```yaml
execution:
  runner: {type: local}
  orchestration: {strategy: parallel, max_concurrent_entities: 4}
```

- **Opt-in.** Only `strategy: parallel` runs entities in parallel threads. `sequential`, no `orchestration`
  block, or `max_concurrent_entities: 1` run them one after another in config order, as before.
  `max_concurrent_entities` defaults to `min(4, CPU count)` when `strategy: parallel` leaves it unset.
- **Never at the same time:** entities with the same Iceberg table (catalog, namespace, table), the same
  accepted path or the same incremental state file; and `merge_scd1` / `merge_scd2` entities, which run
  one at a time. They wait for each other while other entities proceed.
- **Order.** `entity_started`, `file_*` and `entity_finished` events of different entities interleave, and
  observers (including your `set_observer` callback) are called from worker threads. The
  reports, the `run.summary.json` entity list and the console summary stay in config order.
  Every entity's inputs are still listed before the first `entity_started`.
- **Failure.** A policy `abort` fails only its entity. Any other error stops new entities from starting,
  lets the running ones finish (they release or promote their incremental claims), then raises the first
  error in config order. An interrupt (Ctrl-C) is handled the same way: the running entities finish first.
- **Ephemeral disk.** Each running entity may hold up to its download prefetch window (8 files, about
  512 MiB) in a temporary directory. Size ephemeral storage for `workers × 512 MiB` plus the largest file.

Existing profiles that already say `strategy: parallel` for Dagster now also run the entities of a
`domain` Job in parallel inside its `floe run`.

## Hardening assessment and plan

Current profile feature gaps and fixes:

1. **No standalone profile validation path** — previously `floe validate` always
   required a config. Fixed by allowing `floe validate --profile <file>` to parse
   and validate an `EnvironmentProfile` by itself.
2. **Environment-specific catalogs forced into shared configs** — catalog targets
   are often environment-specific. Fixed by adding `profile.catalogs` and applying
   it as an invocation-scoped replacement for config-level `catalogs`.
3. **Partial profile validation only** — profile parsing rejected unknown keys and
   checked runner/variable basics, but catalog duplicates/default references had
   no profile-level guard. Fixed with profile catalog validation.
4. **Validation order inconsistency** — manifest generation validated the config
   before loading the profile, so profile-provided catalogs could not satisfy
   catalog bindings. Fixed by loading the profile first and passing profile
   catalogs into config validation.
5. **Remaining hardening work** — profile `validation.strict` is parsed but not
   enforced, profile paths are local-only while config supports URI resolution,
   and profile catalog validation cannot fully verify `warehouse_storage` without
   a config. Future work should wire `strict` into schema checks, reuse config URI
   resolution for profile loading, and add a combined validation report that
   clearly separates profile-only errors from config/profile integration errors.
