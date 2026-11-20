"""Operators for Airflow-Floe connector."""

from __future__ import annotations

import io
import re
import shlex
import subprocess
import threading
from typing import Any
from urllib.parse import urlparse

from .databricks_runner import run_databricks_job
from .kubernetes_runner import run_kubernetes_job, sized_for_domain
from .manifest import ManifestExecution, ManifestRunnerDefinition
from .runtime import (
    DagManifestContext,
    build_asset_event_extra,
    load_run_summary,
    parse_run_finished,
    summary_entities_by_name,
)

try:
    from airflow.models.baseoperator import BaseOperator  # pyright: ignore[reportMissingImports] -- Airflow 3 re-exports it through a lazy deprecation shim pyright cannot follow
except Exception:  # pragma: no cover - fallback for local unit tests without Airflow

    class BaseOperator:  # type: ignore[override]
        template_fields: tuple[str, ...] = ()

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            del args, kwargs


def _split_cmd(command: str) -> list[str]:
    parts = shlex.split(command)
    if not parts:
        raise ValueError("FLOE_CMD cannot be empty")
    return parts


class FloeRunHook:
    """Execute `floe run` and return normalized Airflow run payload."""

    def __init__(self, floe_cmd: str | None = None) -> None:
        self.floe_cmd = floe_cmd

    def build_args(
        self,
        config_path: str,
        entities: list[str] | None = None,
        *,
        execution: ManifestExecution | None = None,
        manifest_uri: str | None = None,
        run_id: str | None = None,
    ) -> list[str]:
        if execution is None:
            cmd = self.floe_cmd or "floe"
            args = [*_split_cmd(cmd), "run", "-c", config_path, "--log-format", "json"]
            if entities:
                args.extend(["--entities", ",".join(entities)])
            return args

        command = self.floe_cmd or execution.entrypoint
        args = [*_split_cmd(command)]
        args.extend(_render_tokens(execution.base_args, config_path, manifest_uri, run_id))
        if entities:
            args.extend(
                _render_entity_tokens(
                    execution.per_entity_args, config_path, entities, manifest_uri, run_id
                )
            )
        return args

    def run(
        self,
        config_path: str,
        entities: list[str] | None = None,
        *,
        execution: ManifestExecution | None = None,
        runner_definition: ManifestRunnerDefinition | None = None,
        manifest_uri: str | None = None,
        run_id: str | None = None,
        log_stdout: Any | None = None,
        log_stderr: Any | None = None,
    ) -> dict[str, Any]:
        if runner_definition is not None and runner_definition.runner_type == "kubernetes_job":
            args = self.build_args(
                config_path,
                entities=entities,
                execution=execution,
                manifest_uri=_remote_manifest_uri(manifest_uri),
                run_id=run_id,
            )
            return run_kubernetes_job(
                args,
                config_path,
                entities,
                runner=runner_definition,
            )
        if runner_definition is not None and runner_definition.runner_type == "databricks_job":
            args = self.build_args(
                config_path,
                entities=entities,
                execution=execution,
                manifest_uri=_remote_manifest_uri(manifest_uri),
                run_id=run_id,
            )
            return run_databricks_job(
                cmd_args=args,
                runner=runner_definition,
                entities=entities,
            )
        if runner_definition is not None and runner_definition.runner_type != "local_process":
            raise NotImplementedError(
                "unsupported runner type for Airflow FloeRunOperator: "
                f"{runner_definition.runner_type!r} — wire in a connector-layer "
                "adapter before using non-local runner types"
            )
        if execution is not None and execution.log_format != "json":
            raise ValueError(
                "unsupported execution.log_format for Airflow FloeRunOperator: "
                f"{execution.log_format}. Expected 'json'."
            )

        args = self.build_args(
            config_path,
            entities=entities,
            execution=execution,
            manifest_uri=manifest_uri,
            run_id=run_id,
        )
        process = subprocess.Popen(
            args,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1,
        )
        stdout_text, stderr_text = _capture_and_log_process_output(
            process,
            log_stdout=log_stdout,
            log_stderr=log_stderr,
        )
        return_code = process.wait()
        if return_code != 0:
            raise subprocess.CalledProcessError(
                returncode=return_code,
                cmd=args,
                output=stdout_text,
                stderr=stderr_text,
            )

        run_finished = parse_run_finished(stdout_text)
        summary_uri_field = (
            execution.result_contract.summary_uri_field if execution is not None else "summary_uri"
        )
        payload: dict[str, Any] = {
            "schema": "floe.airflow.run.v1",
            "run_id": run_finished["run_id"],
            "status": run_finished["status"],
            "exit_code": run_finished["exit_code"],
            "files": run_finished["files"],
            "rows": run_finished["rows"],
            "accepted": run_finished["accepted"],
            "rejected": run_finished["rejected"],
            "warnings": run_finished["warnings"],
            "errors": run_finished["errors"],
            "summary_uri": run_finished.get(summary_uri_field),
            "config_uri": config_path,
            "floe_log_schema": "floe.log.v1",
            "finished_at_ts_ms": run_finished["ts_ms"],
        }
        if entities and len(entities) == 1:
            payload["entity"] = entities[0]
        if "report_base" in run_finished:
            payload["report_base"] = run_finished["report_base"]
        if "entity_report_uris" in run_finished:
            payload["entity_report_uris"] = run_finished["entity_report_uris"]
        return payload


class FloeRunOperator(BaseOperator):  # pyright: ignore[reportGeneralTypeIssues] -- BaseOperator is unresolved above
    """Run Floe CLI and push normalized run payload to XCom.

    Manifest-driven routing
    -----------------------
    The operator reads ``runners`` from the Airflow manifest and routes
    accordingly:

    * ``"local_process"`` (or no manifest) — subprocess path via
      :class:`FloeRunHook`.
    * Any other runner type — raises :exc:`NotImplementedError`; wire in a
      connector-layer adapter before using non-local runner types.
    """

    template_fields = ("config_path", "entities")

    def __init__(
        self,
        *,
        config_path: str,
        entities: list[str] | None = None,
        floe_cmd: str | None = None,
        manifest_context: DagManifestContext | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.config_path = config_path
        self.entities = entities or []
        self.floe_cmd = floe_cmd
        self.manifest_context = manifest_context

    def execute(self, context: dict[str, Any] | None = None) -> dict[str, Any]:
        hook = FloeRunHook(floe_cmd=self.floe_cmd)
        execution, runner_definition = self._resolve_execution_contract()
        # Under Airflow 3.x, BaseOperator.log (LoggingMixin.log) is always a bound
        # logger, so these resolve to its info/warning methods and the operator
        # streams floe's stdout/stderr to the task log. The `else None` guard is
        # only reached by the no-Airflow fallback BaseOperator used in unit tests
        # (which has no `log` attribute).
        logger = getattr(self, "log", None)
        log_stdout = logger.info if logger is not None else None
        log_stderr = logger.warning if logger is not None else None
        payload = hook.run(
            self.config_path,
            entities=self.entities,
            execution=execution,
            runner_definition=runner_definition,
            manifest_uri=(
                self.manifest_context.manifest_path if self.manifest_context is not None else None
            ),
            run_id=_floe_run_id(
                context, getattr(self, "task_id", "")
            ),  # the no-Airflow stub has no task_id
            log_stdout=log_stdout,
            log_stderr=log_stderr,
        )
        self._emit_asset_events(payload, context)
        return payload

    def _resolve_execution_contract(
        self,
    ) -> tuple[ManifestExecution | None, ManifestRunnerDefinition | None]:
        if self.manifest_context is None or self.manifest_context.manifest is None:
            return None, None

        manifest = self.manifest_context.manifest
        execution = manifest.execution
        runners = manifest.runners

        entity_names = (
            self.entities if self.entities else list(self.manifest_context.entities_by_name.keys())
        )
        requested_runner_names = set()
        domains = set()
        for entity_name in entity_names:
            entity = self.manifest_context.entities_by_name.get(entity_name)
            if entity is None:
                continue
            requested_runner_names.add(entity.runner or runners.default)
            domains.add(entity.domain)

        if not requested_runner_names:
            requested_runner_names.add(runners.default)
        if len(requested_runner_names) > 1:
            raise ValueError(
                "multiple runner names are not supported in one FloeRunOperator call: "
                f"{sorted(requested_runner_names)}"
            )

        runner_name = next(iter(requested_runner_names))
        runner_definition = runners.definitions.get(runner_name)
        if runner_definition is None:
            raise ValueError(f"runner '{runner_name}' not found in manifest.runners.definitions")
        if runner_definition.job_scope == "domain" and len(domains) == 1:
            runner_definition = sized_for_domain(runner_definition, next(iter(domains)))

        return execution, runner_definition

    def _emit_asset_events(self, payload: dict[str, Any], context: dict[str, Any] | None) -> None:
        if self.manifest_context is None:
            return
        if not isinstance(context, dict):
            return
        outlet_events = context.get("outlet_events")
        if outlet_events is None or not hasattr(outlet_events, "get"):
            return

        summary = load_run_summary(payload.get("summary_uri"), self.config_path)
        summary_entities = summary_entities_by_name(summary)

        if self.entities:
            target_entities = self.entities
        else:
            target_entities = list(self.manifest_context.entities_by_name.keys())

        for entity_name in target_entities:
            manifest_entity = self.manifest_context.entities_by_name.get(entity_name)
            if manifest_entity is None:
                continue
            asset = self.manifest_context.assets_by_entity.get(entity_name)
            if asset is None:
                continue
            event = outlet_events.get(asset)
            if event is None:
                continue
            event.extra = build_asset_event_extra(
                entity=manifest_entity,
                run_finished=payload,
                summary_entity=summary_entities.get(entity_name),
                config_path=self.config_path,
            )


def orchestration_max_active_tasks(context: DagManifestContext) -> int | None:
    """DAG `max_active_tasks` for `execution.orchestration`; `None` leaves Airflow's default.

    `sequential` is always one task at a time, whatever `max_concurrent_entities` says.
    """
    orchestration = context.manifest.execution.orchestration if context.manifest else None
    if orchestration is None:
        return None
    if orchestration.strategy == "sequential":
        return 1
    return orchestration.max_concurrent_entities


def plan_floe_tasks(context: DagManifestContext) -> list[tuple[str, list[str]]]:
    """`(task_id, entities)` per task: one per entity, or one per (domain, runner) for `job_scope: domain`."""
    manifest = context.manifest
    if manifest is None:
        return []
    plan: dict[str, list[str]] = {}
    for entity in manifest.entities:
        runner_name = entity.runner or manifest.runners.default
        runner = manifest.runners.definitions[runner_name]
        if runner.runner_type == "kubernetes_job" and runner.job_scope == "domain":
            task_id = f"floe_domain_{entity.domain or 'default'}_{runner_name}"
        else:
            task_id = f"floe_{entity.name}"
        plan.setdefault(task_id, []).append(entity.name)
    return list(plan.items())


def build_floe_tasks(
    context: DagManifestContext, *, floe_cmd: str | None = None
) -> list[FloeRunOperator]:
    """Create the planned `FloeRunOperator`s; call it inside a DAG definition.

    Pass `max_active_tasks=orchestration_max_active_tasks(context)` to the DAG.
    """
    if (
        context.manifest is None
    ):  # manifest failed to load: the `FLOE_CONFIG` fallback runs everything
        return [
            FloeRunOperator(task_id="run_floe", config_path=context.config_path, floe_cmd=floe_cmd)
        ]
    return [
        FloeRunOperator(
            task_id=task_id,
            entities=entities,
            config_path=context.config_path,
            floe_cmd=floe_cmd,
            manifest_context=context,
            outlets=[context.assets_by_entity[name] for name in entities],
        )
        for task_id, entities in plan_floe_tasks(context)
    ]


def _remote_manifest_uri(manifest_uri: str | None) -> str | None:
    """The manifest URI a remote job can read; `None` for a local path of the Airflow worker."""
    if manifest_uri is not None and urlparse(manifest_uri).scheme not in ("", "local", "file"):
        return manifest_uri
    return None


def _floe_run_id(context: dict[str, Any] | None, task_id: str) -> str | None:
    """`<dag run id>_<task id>` (like dagster-floe's `<run id>_<op>`), limited to `[A-Za-z0-9_.-]`."""
    dag_run_id = context.get("run_id") if isinstance(context, dict) else None
    if not dag_run_id:
        return None
    return re.sub(r"[^A-Za-z0-9_.-]", "_", f"{dag_run_id}_{task_id}")


def _render(token: str, values: dict[str, str | None]) -> str:
    for name, value in values.items():
        if value is not None:
            token = token.replace(f"{{{name}}}", value)
    unrendered = re.search(r"\{[^{}]*\}", token)
    if unrendered:
        raise ValueError(
            f"unrendered token {unrendered.group()} in manifest execution args: {token!r} "
            "(for kubernetes_job/databricks_job, {manifest_uri} needs a remote manifest URI)"
        )
    return token


def _render_tokens(
    tokens: list[str],
    config_uri: str,
    manifest_uri: str | None = None,
    run_id: str | None = None,
) -> list[str]:
    values = {"config_uri": config_uri, "manifest_uri": manifest_uri, "run_id": run_id}
    return [_render(token, values) for token in tokens]


def _render_entity_tokens(
    tokens: list[str],
    config_uri: str,
    entities: list[str],
    manifest_uri: str | None = None,
    run_id: str | None = None,
) -> list[str]:
    values = {
        "run_id": run_id,
        "config_uri": config_uri,
        "manifest_uri": manifest_uri,
        "entity_name": ",".join(entities),
    }
    return [_render(token, values) for token in tokens]


def _capture_and_log_process_output(
    process: subprocess.Popen[str],
    *,
    log_stdout: Any | None = None,
    log_stderr: Any | None = None,
) -> tuple[str, str]:
    stdout_pipe = process.stdout
    stderr_pipe = process.stderr
    if stdout_pipe is None or stderr_pipe is None:
        raise ValueError("failed to capture process stdout/stderr")

    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()

    def _consume(pipe: Any, buffer: io.StringIO, logger: Any | None, prefix: str) -> None:
        for line in pipe:
            buffer.write(line)
            if logger is not None:
                stripped = line.rstrip("\n")
                if stripped:
                    logger("%s %s", prefix, stripped)
        pipe.close()

    stdout_thread = threading.Thread(
        target=_consume,
        args=(stdout_pipe, stdout_buffer, log_stdout, "[floe/stdout]"),
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=_consume,
        args=(stderr_pipe, stderr_buffer, log_stderr, "[floe/stderr]"),
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()
    stdout_thread.join()
    stderr_thread.join()

    return stdout_buffer.getvalue(), stderr_buffer.getvalue()
