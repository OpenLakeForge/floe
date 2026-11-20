# Airflow for Floe 1.0: assessment and plan

Issue #1264. Investigation only; the follow-up issues below are proposals for the planner. Facts
about Airflow cite a URL; a fact that could not be confirmed says so. Facts about Floe come from
the code at `main` (6711174) and say where.

## 1. Latest Airflow and what matters to Floe

**Latest: Apache Airflow 3.4.0, released 2026-11-06** ([release notes](https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html),
[PyPI](https://pypi.org/project/apache-airflow/): "Requires Python `!=3.15, >=3.11`"). The latest
`apache-airflow-providers-cncf-kubernetes` is **11.0.0, 2026-11-09**, requires `apache-airflow>=2.11.0`
and Python >=3.11 ([PyPI](https://pypi.org/pypi/apache-airflow-providers-cncf-kubernetes/json),
[changelog](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/changelog.html)).

Since 3.1.0 (2025-09-25), the changes that touch Floe ([release notes](https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html)):

| Release | Change | Meaning for Floe |
|---|---|---|
| 3.2.0 (2026-04-07) | Task-level exceptions and serde move into the Task SDK, so "workers no longer depend on `airflow-core`"; old imports still work with warnings. Asset partitioning. | `operators.py` imports `BaseOperator` from `airflow.models.baseoperator` (a deprecation shim, see the pyright ignore there). Whether that import survives 3.4 is **not confirmed**; CI must tell us (follow-up 1). |
| 3.2.2 (2026-05-29) | Fixes a triggerer race that "caused deferred tasks to stall indefinitely". | Deferring is only safe from 3.2.2 on. A deferrable Floe operator should require `>=3.2.2`. |
| 3.3.0 (2026-07-06) | Task and asset state store, pluggable retry policies, `on_kill()` on `BaseTrigger`, async XCom accessors for triggers. | Nothing Floe must adopt. `on_kill()` lets a trigger delete a Job on cancel. |
| 3.3.1 (2026-08-12) | Deferred tasks "fail instead of hanging when saved state can't be resumed". | Same: deferral is stable enough for 1.0 on 3.3+. |
| 3.4.0 (2026-11-06) | Python 3.10 removed. `Dataset` alias removed (use `airflow.sdk.Asset`). Asset-triggered Dags run once per event unless `batch_asset_events=True`. Experimental non-Python Dag importers. | `runtime.py` already imports `airflow.sdk.Asset`. airflow-floe declares `requires-python >=3.10` and a 3.10 classifier; Airflow 3.4 cannot run on 3.10, and Floe itself needs 3.11 (`AGENTS.md`). |

What the Airflow docs say about the four things the issue asks about:

- **Asset events with metadata.** A task attaches JSON-serializable `extra` to an event with
  `outlet_events[asset].extra = {...}` or by yielding `Metadata(asset, {...})`; "Asset event extra
  information can only contain JSON-serializable values"; event `extra` annotates a specific data
  change, such as a row count, as opposed to `Asset(..., extra=)` which describes the asset
  ([assets](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html)).
  For a classic operator the page says to override `execute`, or use `pre_execute`/`post_execute`.
  `AssetAlias` declares an outlet before the URIs are known. That is not needed: Floe knows every
  accepted sink URI at parse time from the manifest.
  **Not confirmed:** whether events are registered when the task fails. The docs read do not say.
- **Deferrable operators.** `self.defer(trigger=..., method_name=...)` releases the worker; a
  `triggerer` runs the async `BaseTrigger.run`, which yields a `TriggerEvent`; a worker resumes at
  `method_name`. `run` must not block ("use non-blocking calls such as `asyncio.sleep`"), and the
  trigger "must not come from a Dag bundle", so it has to live in an installed package such as
  airflow-floe. Deferred tasks "release their worker slot"
  ([deferring](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html)).
- **Kubernetes.** `KubernetesPodOperator` has `deferrable=True` (the triggerer, not the operator,
  watches pod start) and `logging_interval` to fetch logs while deferred. `do_xcom_push=True` adds a
  sidecar and reads `/airflow/xcom/return.json`; XComs are "pushed only for tasks that reach
  success". `KubernetesJobOperator` has `wait_until_job_complete` and "also supports deferrable
  mode" ([operators](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html)).
  The page does not document `backoff_limit` or `ttl_seconds_after_finished` for the Job operator.
- **Data-quality results elsewhere.** I found no Airflow-wide data-quality result object. The
  common-sql check operators fail the task when a check fails (documented for
  `SQLThresholdCheckOperator`; the page does not describe per-check results or XCom for the others)
  ([common-sql](https://airflow.apache.org/docs/apache-airflow-providers-common-sql/stable/operators.html)).
  The OpenLineage provider emits events for every operator and, for supported ones, may add "data
  quality metrics" ([supported classes](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/supported_classes.html)).
  So the idiomatic channels are: fail the task, task logs, and asset event `extra`.

## 2. airflow-floe 0.4.0 vs dagster-floe

From the code (`orchestrators/airflow-floe/src/airflow_floe/`, `orchestrators/dagster-floe/src/floe_dagster/`).
"Gap" rows are what the 1.0 plan has to close or state.

| Feature | dagster-floe | airflow-floe 0.4.0 |
|---|---|---|
| Manifest-driven runs (`floe run --manifest`) | Renders `{manifest_uri}` (`manifest.py:_render_token`) and runs `--manifest`. | **Broken for generated manifests.** `operators.py:_render_tokens` renders only `{config_uri}`. A manifest from current Floe (0.10) has `base_args: ["run", "--manifest", "{manifest_uri}", ...]` (`src/floe/manifest.py:_execution`). Reproduced on `example/manifest.airflow.json`: `build_args` returns `['floe','run','--manifest','{manifest_uri}','--log-format','json','--quiet','--entities','orders']`. Its tests only use the old `-c {config_uri}` form. |
| Task granularity | Asset per entity; multi-asset per (domain, runner) for `job_scope: domain`. | Same shape: `plan_floe_tasks` makes one task per entity, or per (domain, runner) for `job_scope: domain`; `max_active_tasks` from `execution.orchestration`. Parity. |
| Kubernetes runner | Job built from the manifest, pod log followed while it runs, many pod fields (`labels`, `annotations`, `node_selector`, `tolerations`, `security_context`, `image_pull_policy`, `env_from`, `backoff_limit`, `poll_interval_seconds`). | Job built from `image`, `namespace`, `service_account`, `resources`, `env`, `secrets`, `command`/`args`, TTL and timeout only; `backoffLimit` is hard-coded 0 (`build_k8s_job_spec`); the other pod fields are ignored (CHANGELOG 0.3.0). Blocking `time.sleep` poll in the worker slot; logs read once after the Job ends. No `kubernetes` extra in `pyproject.toml`. |
| Failed backend run fails the task | Raises `Failure`. | **Gap.** `run_kubernetes_job` and `run_databricks_job` return a payload with `backend_status` and never raise on a failed or timed-out Job; `FloeRunOperator.execute` does not check it, so the task is green. Only the local path raises (`CalledProcessError`). |
| Per-entity results | One `MaterializeResult` per entity, from the run summary and from `entity_finished` while the Job runs (#1227). Failed, aborted and not-run entities are told apart. | Asset event `extra` per entity (status, files, rows, accepted, rejected, warnings, errors), written after the whole task. The summary is read only from a local path; `_resolve_summary_path` returns `None` for `s3://` ("Remote summary loading is out of scope for MVP"), so with remote reports the event falls back to run-level counts. A local run that exits non-zero (partial/aborted, exit 2) raises before any event is written, so its succeeded entities get no event. |
| Check results | `AssetCheckResult` per rule, schema mismatch and file status, with severity and `max_rejection_rate` (`asset_checks.py`). | None. |
| Report and failure URIs | `entity_report_uris` from `run_finished`, never built by hand (#960); `run.failure.json` read from the `run_failure_report` event. | `summary_uri` and `entity_report_uris` go into the XCom payload. The per-entity `entity_report_file` is built by hand as `<summary dir>/<entity>/run.json` when the summary has no `report_file` (`runtime.py:_resolve_entity_report_file`). No failure report. |
| Lineage | Floe emits OpenLineage itself; Dagster forwards `DAGSTER_RUN_ID` and `DAGSTER_JOB_NAME` into the Job so the parent run is set (`kubernetes_runner.py`). | Floe reads `AIRFLOW_CTX_DAG_ID`, `AIRFLOW_CTX_TASK_ID` and `AIRFLOW_CTX_DAG_RUN_ID` (`src/floe/lineage/events.py`). A local subprocess inherits the task environment; **whether Airflow 3 sets those variables in the task process is not confirmed**. Nothing forwards them to a Kubernetes or Databricks Job. Asset per entity: `Asset(name="floe.<asset_key>", uri=<accepted sink>)`. |
| Tests | Real Dagster in CI. | CI installs the package `--no-deps` and runs `unittest` (`.github/workflows/airflow-floe.yml`); Airflow is not installed, so the fallback `BaseOperator` and `Asset` classes are what the tests exercise. Nothing has run against Airflow 3.1 to 3.4 in CI. |

## 3. Recommended 1.0 target

Decisions, each with the reason.

1. **Keep one `FloeRunOperator`; the manifest decides the grain.** Default is one task per entity
   (own retries, own timeout, entity outcome as that task's outlet); `job_scope: domain` gives one
   task per (domain, runner) that runs its entities one after another, which saves pod starts when
   entities are small. This is already built (`plan_floe_tasks`), is the same switch dagster-floe
   uses, and matches the manifest as the one contract. A per-domain operator class would add a
   second public API for the same choice. No new operator.
2. **Run through `floe run --manifest`, as the manifest's own `execution.base_args` say.** Render
   `{manifest_uri}`. Locally it is the manifest path. For `kubernetes_job` and `databricks_job` the
   Job cannot see the Airflow host's file, so the manifest must be a remote URI (`floe manifest
   generate -o s3://...` substitutes it; `docs/reference/cli.md`); a Job with an unrendered token
   must fail loudly instead of running with a literal `{manifest_uri}`. This is a bug fix, not a new
   feature, and everything else builds on it.
3. **Kubernetes: keep Floe's own Job runner and make waiting deferrable with a small Floe trigger;
   do not adopt `KubernetesPodOperator` for 1.0.** Reasons: (a) KPO runs one Pod; Floe relies on
   Job `backoffLimit`, TTL and `activeDeadlineSeconds`, and shares the Job builder and status
   mapping byte-for-byte with dagster-floe (`k8s_status.py`); (b) KPO's result channel is the XCom
   sidecar reading `/airflow/xcom/return.json`, which Floe does not write, so we would parse
   `run_finished` from logs anyway; (c) it adds `apache-airflow-providers-cncf-kubernetes` as a
   dependency, which needs approval on an issue (`AGENTS.md` §3), and its pod options would have to
   be mapped from the manifest a second time. `KubernetesJobOperator` is the closer fit, but its Job
   options are not documented in the page I read, so it is a comparison point inside the deferrable
   issue, not an assumption. Shape: `FloeRunOperator(deferrable=True)` creates the Job, calls
   `self.defer(...)` on a `FloeK8sJobTrigger` that polls the Job status with the existing
   `map_k8s_job_status` (sync client through `asyncio.to_thread`, so no new dependency), and the
   resume method reads the pod log on the worker and parses `run_finished`. The trigger returns only
   status and Job name, never the log. Requires Airflow >=3.2.2 (see section 1). The synchronous
   path stays the default, so a deployment with no triggerer keeps working.
4. **Report every entity outcome as an asset event with metadata, and fail the task honestly.**
   Per entity: status, counts, `entity_report_uri` (from `run_finished`, not built by hand),
   `failure_reason`, rejected sink URI, `max_rejection_rate` and a check summary (rule failures by
   severity, schema mismatches, file statuses), the same facts dagster-floe turns into asset checks.
   Emit for `success`, `success_with_warnings` and `rejected`, not for `failed`, `aborted` or
   not-run, like dagster-floe; a non-zero run still raises afterwards. Airflow has no check-result
   object (section 1), so `extra` plus task failure is the contract. Read remote summaries with
   `fsspec` as dagster-floe does (`remote` extra) instead of returning `None`. Streaming events while
   a Job runs (#1227) is **not** in the 1.0 target: it needs a long-lived log follower that does not
   fit a deferred task, and the end-of-run path is correct.
5. **Lineage: forward the Airflow run context to Jobs.** Same as dagster-floe forwards `DAGSTER_*`.
   Cheap, and it makes the ParentRun facet work on Kubernetes.

**Build order** (each step stands alone; later ones assume the earlier ones):

1. Follow-ups 1, 2 and 3: fix `{manifest_uri}`, fix a failed Job not failing the task, run the tests
   against real Airflow (independent PRs, any order). Nothing else is trustworthy before these.
2. Follow-up 4: report entities on a non-zero exit (builds on the raise path of follow-up 2).
3. Follow-up 5: richer asset event metadata and remote summaries.
4. Follow-ups 6 and 7: Kubernetes parity and lineage context.
5. Follow-up 8: deferrable Kubernetes wait.
6. Follow-up 9: release PR (unfreeze, see below).

## 4. Unfreeze recommendation

**Unfreeze airflow-floe and release it with Floe, as dagster-floe is.** Evidence that it has
already been drifting: 0.4.0 does not work with the manifests current Floe generates (the
`{manifest_uri}` token, section 2), and its CI never imports Airflow, so nothing caught it. The
manifest schema is vendored and the two vendored modules are byte-checked
(`tests/test_vendored_copies.py`), but those checks cover files, not behaviour. The CHANGELOG
already pairs a Floe release with a dagster-floe and an airflow-floe version; keep doing that, with
the connector's version bumped in a release PR of its own (`context/bump-release.md`).

Support level to state at 1.0:

- **Beta until follow-ups 1 to 4 have landed**, because 0.4.0 has two correctness gaps (the token and
  the green failed Job). After them, **supported on Airflow 3.1 to 3.4**, tested in CI against the
  lowest (3.1) and the latest (3.4) of that range.
- **Not in the 1.0 promise:** Airflow 2.x (out of scope in #1264), deferrable waiting for Databricks
  Jobs, entity events streamed while a Job runs, and Airflow-native check results.
- **Python:** `requires-python >=3.11`, dropping the 3.10 classifier, because Airflow 3.4 and Floe both
  need 3.11.

## Proposed follow-up issues

Sizes are rough counts of non-test lines under `orchestrators/`, within the `AGENTS.md` §3 budget
(at most 300 lines and 8 files). The planner creates the issues; none exists yet.

1. **`fix(airflow-floe): render {manifest_uri} so generated manifests run`**
   Render `{manifest_uri}` in `execution.base_args` with the manifest path for a local run; for
   `kubernetes_job` and `databricks_job`, raise if the token is still unrendered. No new field.
   Files: `operators.py`, `runtime.py` (the manifest path already lives on `DagManifestContext`).
   Size: about 30 lines, 2 files.
   Fails first: `FloeRunHook().build_args(...)` on `example/manifest.airflow.json` returns a list
   with no `{` in it (today it contains `'{manifest_uri}'`).

2. **`fix(airflow-floe): a failed or timed-out Kubernetes or Databricks job fails the task`**
   Raise `AirflowException` when the payload's `backend_status` is not succeeded or Floe's `status`
   is `failed` or `aborted`, after the asset events are written. Files: `operators.py`. Size: about
   25 lines, 1 file.
   Fails first: `FloeRunOperator.execute` with a fake jobs API whose Job has a `Failed` condition
   raises; today it returns the payload.

3. **`ci(airflow-floe): run the connector tests against Airflow 3.1 and 3.4`**
   Install `apache-airflow` in the workflow (matrix 3.1 and the latest `<4`), use the real
   `BaseOperator` and `Asset`, and settle the `airflow.models.baseoperator` import question.
   Files: `.github/workflows/airflow-floe.yml`, `orchestrators/airflow-floe/pyproject.toml` (a `dev`
   constraint), at most `operators.py` for the import. Size: under 10 src lines, 3 files.
   Fails first: a test that builds `build_floe_tasks` with the real `airflow.sdk.Asset` as `outlets`
   and asserts `isinstance(task, airflow.sdk.BaseOperator)`; it cannot run today because Airflow is
   not installed.

4. **`fix(airflow-floe): report the succeeded entities when a run exits non-zero`**
   On a non-zero exit, parse `run_finished` and the summary, write asset events for entities that
   succeeded, then raise. Files: `operators.py`, `runtime.py`. Size: about 80 lines, 2 files.
   Fails first: a local run exiting 2 whose summary has one `success` and one `failed` entity writes
   an event for the first and raises; today nothing is written.

5. **`feat(airflow-floe): richer asset event metadata and remote summaries`**
   Add `entity_report_uri` from `entity_report_uris`, `failure_reason`, the rejected sink URI,
   `max_rejection_rate` and a check summary to the event `extra`; stop hand-building the report path;
   read `s3://` summaries through `fsspec`; skip `failed` and `aborted` entities. Files:
   `runtime.py`, `operators.py`, a new `checks.py` (about 100 lines, one self-contained concern).
   Size: about 220 lines, 3 files. Needs approval on the issue for the `remote` extra (`fsspec`),
   as dagster-floe has it.
   Fails first: `build_asset_event_extra` for a summary with one failed rule returns a `checks` entry
   naming it with its severity; today the key is absent.

6. **`feat(airflow-floe): Kubernetes pod options and backoff_limit like dagster-floe`**
   Parse and apply `labels`, `annotations`, `node_selector`, `tolerations`, `security_context`,
   `image_pull_policy`, `env_from`, `backoff_limit` and `poll_interval_seconds` from the runner
   definition, and add the `kubernetes` extra. No manifest field is new: all exist in the schema.
   Files: `kubernetes_runner.py`, `manifest.py`, `pyproject.toml`. Size: about 150 lines, 3 files.
   Fails first: `build_k8s_job_spec` for a runner with `node_selector` has it under
   `template.spec.nodeSelector`; today it is dropped.

7. **`feat(airflow-floe): forward the Airflow run context to Kubernetes Jobs for lineage`**
   Add `AIRFLOW_CTX_DAG_ID`, `AIRFLOW_CTX_TASK_ID`, `AIRFLOW_CTX_DAG_RUN_ID` to the Job's env, the
   names `src/floe/lineage/events.py` reads. Files: `kubernetes_runner.py`, `operators.py`. Size:
   about 30 lines, 2 files.
   Fails first: the Job spec built for a task with a run context carries those three env vars.

8. **`feat(airflow-floe): deferrable wait for Kubernetes Jobs`**
   `FloeRunOperator(deferrable=True)` submits the Job, defers to a new `FloeK8sJobTrigger`
   (`asyncio.to_thread` over the existing status mapping, `on_kill` deletes the Job), and resumes to
   parse the pod log. Requires Airflow >=3.2.2 for this mode. Compare `KubernetesJobOperator` in the
   issue before building and record the choice. Files: `operators.py`, `kubernetes_runner.py`
   (split submit from wait), new `triggers.py`. Size: about 220 lines, 3 files. Over budget? Split
   into a behaviour-neutral prep PR (submit/wait split) and the trigger.
   Fails first: with `deferrable=True`, `execute` raises `TaskDeferred` after creating the Job; today
   there is no such argument.

9. **`chore(airflow-floe): release 0.5 and state the Airflow support level`**
   Bump to 0.5.0, `requires-python >=3.11`, CHANGELOG, README support table, update
   `docs/reference/support-matrix.md` to the level reached. Release PR only, after 1 to 7 (8 if it
   ships). Files: `pyproject.toml`, `CHANGELOG.md`, `README.md`, docs. Size: under 20 src lines.
   Fails first: none (release); the existing vendored-copy tests must pass.

Not proposed: streaming entity events from a running Job (#1227 equivalent), deferrable Databricks,
Airflow 2.x. Each would be its own Feature issue after 1.0.
