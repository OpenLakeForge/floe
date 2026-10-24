# Environment Profiles

Environment Profiles let you define per-environment configuration values
(variables, runner settings, validation strictness) in a standalone YAML file.
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
validated immediately with `cargo test` or a JSON Schema validator.

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

For the full resolution rules, nested reference examples, and the Rust
API see **[`docs/variables.md`](variables.md)**.

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
Glue, REST Iceberg, or Unity Catalog endpoints into environment-specific files.

```yaml
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: prod
catalogs:
  default: prod_glue
  definitions:
    - name: prod_glue
      type: glue
      region: eu-west-1
      database: bronze
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
