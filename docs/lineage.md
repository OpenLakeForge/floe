# OpenLineage Integration

Floe can emit run and entity lifecycle events to any
[OpenLineage](https://openlineage.io)-compatible HTTP endpoint (Marquez,
Atlan, OpenMetadata, Astronomer, etc.).

## Configuration

Add a `lineage` block at the root of your config file:

```yaml
lineage:
  url: "http://marquez:5000"
  endpoint: "api/v1/lineage"         # optional, default shown
  namespace: "my-floe-namespace"
  dataset_namespace: "my-floe-namespace"  # optional, defaults to namespace
  api_key: "{{OPENLINEAGE_API_KEY}}"   # optional — Bearer token
  timeout_secs: 5                       # optional, default 5
  producer: "https://github.com/myorg/floe"  # optional
  job_name: "orders_ingestion"          # optional
```

| Field          | Required | Description |
|----------------|----------|-------------|
| `url`          | yes      | Base URL of the OpenLineage-compatible endpoint |
| `endpoint`     | no       | Path joined to `url` for POST requests (default: `api/v1/lineage`) |
| `namespace`    | yes      | OpenLineage namespace used for job identity |
| `dataset_namespace` | no   | Namespace for accepted Iceberg output datasets; defaults to `namespace` |
| `api_key`      | no       | Bearer token for the `Authorization` header. When unset, Floe falls back to the `OPENLINEAGE_API_KEY` environment variable at run time. |
| `timeout_secs` | no       | HTTP request timeout in seconds (default: `5`) |
| `producer`     | no       | URI identifying this producer. Defaults to the versioned release URL for the current build (e.g. `https://github.com/OpenLakeForge/floe/releases/tag/v0.4.2`). |
| `max_failures` | no       | Consecutive failures before the circuit opens (default: `3`) |
| `job_name`     | no       | Stable OpenLineage job name for top-level `RunStarted`/`RunFinished` events. Defaults to the config file stem (e.g. `orders.yml` → `orders`), fallback `floe-run`. Use this to group multiple runs under the same Marquez job node. |

`api_key` supports `{{VAR}}` placeholder expansion via the same profile and
env-vars mechanism used for the rest of the config.

### Runtime credential resolution (`OPENLINEAGE_API_KEY`)

When `lineage.api_key` is not set, Floe resolves the Bearer token from the
`OPENLINEAGE_API_KEY` environment variable when the lineage observer is
constructed — the same variable the OpenLineage Python client and `dbt-ol` read.
Resolution order:

1. An explicit `lineage.api_key` (including a `{{VAR}}` placeholder resolved
   during config templating) is used as-is.
2. Otherwise, `OPENLINEAGE_API_KEY` from the process environment is used.
3. If neither is present, events are sent unauthenticated.

**Recommended for orchestrated runners and manifest replay.** Omit
`lineage.api_key` from configs and profiles and inject `OPENLINEAGE_API_KEY` into
the runner environment (for example, from a Kubernetes Secret). The credential
then stays in the pod environment and is never serialized into a generated
manifest, Git, or object storage. This mirrors how OpenLineage-based tools such
as `dbt-ol` supply the OpenMetadata ingestion token.

> An unresolved `{{VAR}}` placeholder that survives into a generated manifest —
> for example a profile-only `api_key` that was never templated — is treated as
> absent, so replay falls back to `OPENLINEAGE_API_KEY` instead of sending the
> literal placeholder as the token.

## Events emitted

For each Floe run, Floe posts OpenLineage `RunEvent` objects to
`POST <url>/<endpoint>`, where `endpoint` defaults to `api/v1/lineage`:

For OpenMetadata's native OpenLineage endpoint:

```yaml
lineage:
  url: http://openmetadata:8585
  endpoint: api/v1/openlineage/lineage
  namespace: dagster            # stable job identity
  dataset_namespace: iceberg    # catalog-service dataset namespace
```

`dataset_namespace` governs the accepted Iceberg output dataset; source and
non-Iceberg sink datasets retain their physical storage namespaces.

| Floe lifecycle        | OpenLineage event type | Notes                                                                  |
|-----------------------|------------------------|------------------------------------------------------------------------|
| Run begins            | `START`                | Top-level run job                                                      |
| Entity begins         | `START`                | Per-entity job `<namespace>.<entity>`                                  |
| Entity finishes (ok)  | `COMPLETE`             | Inputs = source dataset; outputs = accepted (+ rejected) sink datasets |
| Entity finishes (err) | `FAIL`                 | Same input/output structure as COMPLETE                                |
| Run finishes (ok)     | `COMPLETE`             | Top-level run job                                                      |
| Run finishes (err)    | `FAIL`                 | Top-level run job                                                      |

### Datasets on entity COMPLETE/FAIL events

Each entity event carries the actual data movement as OpenLineage datasets:

- **`inputs`** — one dataset named after `source.path`
- **`outputs`** — one dataset named after the accepted sink; plus one named
  after the rejected sink when a rejected sink is configured

This produces a lineage graph in Marquez (and compatible tools) of the form:

```text
source path  →  <namespace>.<entity> job  →  accepted sink path
                                          →  rejected sink path (when present)
```

Manifest replay (`floe run --manifest`) emits the same `inputs`/`outputs`
datasets as a direct config run — the entities embedded in the manifest carry
the resolved source and sink identities used to build them.

### Facets on entity COMPLETE/FAIL events

Attached to the **accepted output dataset**:

- **`SchemaDataset`** (`schema`) — column names and types from `schema.columns`
- **`DataQualityMetrics`** (`dataQualityMetrics`) — accepted row counts for the output dataset
- **`FloeQualityRun`** — `entity`, `rejectionRate`, `files`, `rows`, `accepted`, `rejected`, `warnings`, `errors`
- **`ColumnLineageDatasetFacet`** (`columnLineage`) — output columns mapped to
  source fields when configured, or to the same source field name by default

When a rejected sink is configured, the rejected output dataset carries its own
`DataQualityMetrics` facet with rejected row counts.

### ParentRun facet (Airflow / Dagster)

When Floe is invoked from an Airflow task or Dagster job, the parent run
context is auto-detected from environment variables and attached as a `parent`
run facet to all run events — both the top-level run (`START`/`COMPLETE`/`FAIL`)
and each entity-level job (`START`/`COMPLETE`/`FAIL`):

| Orchestrator | Environment variables read                                                    |
|--------------|-------------------------------------------------------------------------------|
| Airflow      | `AIRFLOW_CTX_DAG_RUN_ID`, `AIRFLOW_CTX_DAG_ID`, `AIRFLOW_CTX_TASK_ID`       |
| Dagster      | `DAGSTER_RUN_ID`, `DAGSTER_JOB_NAME`                                          |

## Resilience: circuit breaker and retry

Floe uses a circuit breaker to avoid blocking long pipelines when the lineage
endpoint is down or slow.

**Retry behaviour** — transient failures (connection errors, HTTP 5xx, 429) are
retried up to 3 times with 0 / 100 / 500 ms backoff before counting as a
failure. Non-retryable 4xx errors (e.g. 401 bad API key) are counted immediately
without retrying.

**Circuit breaker** — after `max_failures` consecutive failures (default 3,
configurable via `lineage.max_failures`), the circuit opens for the remainder of
the run. Subsequent events are skipped immediately with no HTTP calls. A single
`lineage_circuit_open` warning is emitted when the circuit trips.

**Recovery** — the circuit resets at the start of each new run (`RunStarted`),
so a recovered endpoint is retried in the next pipeline execution without
restarting the process.

A `lineage_http_error` warning is emitted whenever an event is dropped (whether
retried or not) so silent event loss is always visible in operator logs.

## Fail-silent behaviour

HTTP errors that do not trip the circuit breaker are emitted as `warn`-level
log events and do not affect the run outcome. The ingestion continues normally.

Warnings are always surfaced to stderr even when `--log-format off` is used,
so misconfigured endpoints are always visible in operator logs.

## Lifecycle correctness

Floe ensures every run produces a well-formed OpenLineage lifecycle:

- A `START` event is always emitted before any `COMPLETE` or `FAIL` event for
  both the top-level run and each entity job.
- If `run_with_base` fails during config validation or context construction
  (before execution begins), a `START` event is emitted in the failure path so
  consumers never see a bare `FAIL` without a prior `START`.
- If `run_with_base` fails after the run has already started (e.g. during
  input resolution), the existing `START` is not re-emitted.

## Log format and lineage

Lineage emission is independent of `--log-format`. Even with
`--log-format off` (the default), OpenLineage events are posted normally.

## Example config

```yaml
version: "0.3"
lineage:
  url: "http://localhost:5000"
  namespace: "data-platform"
  api_key: "{{MARQUEZ_API_KEY}}"
  timeout_secs: 10

entities:
  - name: orders
    source:
      format: csv
      path: /data/orders/
    sink:
      accepted:
        format: parquet
        path: /lake/orders/
      rejected:
        format: csv
        path: /lake/orders/rejected/
    policy:
      severity: reject
    schema:
      columns:
        - name: order_id
          type: string
        - name: amount
          type: number
```
