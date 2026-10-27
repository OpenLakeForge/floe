"""Databricks runner adapter for airflow-floe."""

from __future__ import annotations

import os
import time
from typing import Any

from .databricks_client import DatabricksJobSpec, DatabricksJobsClient
from .manifest import ManifestRunnerDefinition


def run_databricks_job(
    *,
    cmd_args: list[str],
    runner: ManifestRunnerDefinition,
    entities: list[str] | None,
) -> dict[str, Any]:
    workspace_url = runner.workspace_url
    existing_cluster_id = runner.existing_cluster_id
    config_uri = runner.config_uri
    python_file_uri = runner.python_file_uri
    if (
        not workspace_url
        or not existing_cluster_id
        or not config_uri
        or not python_file_uri
    ):
        raise ValueError(
            "databricks_job runner requires workspace_url, existing_cluster_id, "
            "config_uri and python_file_uri"
        )

    auth_ref = _extract_oauth_ref(runner.auth)
    oauth_token = _resolve_oauth_token(auth_ref)
    domain = _resolve_domain(entities)
    env_name = _resolve_env_name(runner.env_parameters)
    job_name = _render_job_name(
        runner.job_name or "floe-{domain}-{env}", domain=domain, env=env_name
    )

    client = DatabricksJobsClient(
        http_client=_RequestsHttpClient(),
        workspace_url=workspace_url,
        oauth_bearer_token=oauth_token,
    )

    spec = DatabricksJobSpec(
        workspace_url=workspace_url,
        existing_cluster_id=existing_cluster_id,
        python_file_uri=python_file_uri,
        job_name=job_name,
        parameters=list(cmd_args),
        poll_interval_seconds=runner.poll_interval_seconds or 10,
        timeout_seconds=runner.timeout_seconds or 3600,
    )
    job_id: int | None = None
    run_id: int | None = None
    try:
        job_id = client.ensure_domain_job(spec)
        run_id = client.run_now(job_id=job_id)
        terminal = client.poll_run_to_terminal(
            run_id=run_id,
            poll_interval_seconds=spec.poll_interval_seconds,
            timeout_seconds=spec.timeout_seconds,
        )
    except TimeoutError as exc:
        return _infra_failure_payload(
            status="timeout",
            failure_reason=str(exc),
            workspace_url=workspace_url,
            config_uri=config_uri,
            run_id=run_id,
            job_id=job_id,
            error_type="timeout",
            entities=entities,
        )
    except Exception as exc:  # noqa: BLE001
        return _infra_failure_payload(
            status="failed",
            failure_reason=str(exc) or exc.__class__.__name__,
            workspace_url=workspace_url,
            config_uri=config_uri,
            run_id=run_id,
            job_id=job_id,
            error_type=exc.__class__.__name__,
            entities=entities,
        )
    status = _map_databricks_status(
        life_cycle_state=terminal.state,
        result_state=terminal.result_state,
    )
    failure_reason = _resolve_failure_reason(
        status=status,
        result_state=terminal.result_state,
        state_message=terminal.state_message,
    )

    backend_metadata = _backend_metadata(
        workspace_url=workspace_url,
        run_id=run_id,
        job_id=job_id,
        life_cycle_state=terminal.state,
        result_state=terminal.result_state,
        state_message=terminal.state_message,
        run_page_url=terminal.run_page_url,
    )

    payload: dict[str, Any] = {
        "schema": "floe.airflow.run.v1",
        "run_id": str(run_id) if run_id is not None else "",
        "status": status,
        "exit_code": 0 if status == "success" else 1,
        "files": 0,
        "rows": 0,
        "accepted": 0,
        "rejected": 0,
        "warnings": 0,
        "errors": 0,
        "summary_uri": None,
        "config_uri": config_uri,
        "floe_log_schema": "floe.log.v1",
        "finished_at_ts_ms": int(time.time() * 1000),
        "backend_type": "databricks",
        "backend_run_id": run_id,
        "backend_status": terminal.result_state or terminal.state,
        "backend_metadata": backend_metadata,
    }
    if failure_reason:
        payload["failure_reason"] = failure_reason
    if entities and len(entities) == 1:
        payload["entity"] = entities[0]
    return payload


def _resolve_domain(entities: list[str] | None) -> str:
    if not entities:
        return "default"
    dotted = [entity for entity in entities if entity and "." in entity]
    if not dotted:
        return "default"
    domains = {entity.split(".", 1)[0] for entity in dotted}
    if len(domains) > 1:
        raise ValueError(
            "databricks_job runner expects entities from one domain per run; "
            f"got domains: {sorted(domains)}"
        )
    return next(iter(domains), "default")


def _resolve_env_name(env_parameters: dict[str, str] | None) -> str:
    if env_parameters and env_parameters.get("FLOE_ENV"):
        return env_parameters["FLOE_ENV"]
    return os.environ.get("FLOE_ENV") or "default"


def _render_job_name(template: str, *, domain: str, env: str) -> str:
    return template.replace("{domain}", domain).replace("{env}", env)


def _extract_oauth_ref(auth: dict[str, str] | None) -> str | None:
    return (auth or {}).get("service_principal_oauth_ref")


def _resolve_oauth_token(auth_ref: str | None) -> str:
    # Prefer a declared `env://VAR` reference over the bare-fallback env so
    # that misconfiguration (declared ref pointing at a missing var) is loud
    # instead of silently masked by FLOE_DATABRICKS_OAUTH_TOKEN.
    if auth_ref:
        if auth_ref.startswith("env://"):
            env_var = auth_ref[len("env://") :]
            resolved = os.environ.get(env_var)
            if resolved:
                return resolved
            raise ValueError(
                f"databricks oauth token env var '{env_var}' "
                f"(from auth.service_principal_oauth_ref) not found"
            )
        raise ValueError(
            "databricks oauth reference is not directly resolvable by airflow-floe; "
            "use auth.service_principal_oauth_ref=env://<VAR> or set "
            "FLOE_DATABRICKS_OAUTH_TOKEN with no auth.service_principal_oauth_ref"
        )
    fallback = os.environ.get("FLOE_DATABRICKS_OAUTH_TOKEN")
    if fallback:
        return fallback
    raise ValueError(
        "databricks_job runner requires auth.service_principal_oauth_ref "
        "(env://<VAR>) or FLOE_DATABRICKS_OAUTH_TOKEN fallback"
    )


def _backend_metadata(
    *,
    workspace_url: str,
    run_id: int | None,
    job_id: int | None,
    life_cycle_state: str | None,
    result_state: str | None,
    state_message: str | None,
    run_page_url: str | None,
    error_type: str | None = None,
) -> dict[str, Any]:
    return {
        "backend_type": "databricks",
        "backend_run_id": run_id,
        "workspace_url": workspace_url,
        "job_id": job_id,
        "life_cycle_state": life_cycle_state,
        "result_state": result_state,
        "state_message": state_message,
        "run_page_url": run_page_url,
        "error_type": error_type,
    }


def _infra_failure_payload(
    *,
    status: str,
    failure_reason: str,
    workspace_url: str,
    config_uri: str,
    run_id: int | None,
    job_id: int | None,
    error_type: str,
    entities: list[str] | None,
) -> dict[str, Any]:
    backend_metadata = _backend_metadata(
        workspace_url=workspace_url,
        run_id=run_id,
        job_id=job_id,
        life_cycle_state=None,
        result_state=None,
        state_message=failure_reason,
        run_page_url=None,
        error_type=error_type,
    )
    payload: dict[str, Any] = {
        "schema": "floe.airflow.run.v1",
        "run_id": str(run_id) if run_id is not None else "",
        "status": status,
        "exit_code": 1,
        "files": 0,
        "rows": 0,
        "accepted": 0,
        "rejected": 0,
        "warnings": 0,
        "errors": 1,
        "summary_uri": None,
        "config_uri": config_uri,
        "floe_log_schema": "floe.log.v1",
        "finished_at_ts_ms": int(time.time() * 1000),
        "backend_type": "databricks",
        "backend_run_id": run_id,
        "backend_status": None,
        "failure_reason": failure_reason,
        "backend_metadata": backend_metadata,
    }
    if entities and len(entities) == 1:
        payload["entity"] = entities[0]
    return payload


def _map_databricks_status(
    *, life_cycle_state: str | None, result_state: str | None
) -> str:
    normalized_result = (result_state or "").strip().upper()
    normalized_lifecycle = (life_cycle_state or "").strip().upper()

    if normalized_result == "SUCCESS":
        return "success"
    if normalized_result == "TIMEDOUT":
        return "timeout"
    if normalized_result in {"CANCELED", "CANCELLED"}:
        return "canceled"
    if normalized_result:
        return "failed"

    if normalized_lifecycle == "SKIPPED":
        return "canceled"
    return "failed"


def _resolve_failure_reason(
    *, status: str, result_state: str | None, state_message: str | None
) -> str | None:
    if status == "success":
        return None
    if state_message:
        return state_message
    if result_state:
        return str(result_state)
    return None


class _RequestsHttpClient:
    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
        timeout_seconds: int | None = None,
    ) -> dict[str, Any]:
        import requests

        response = requests.request(
            method,
            url,
            headers=headers,
            json=json_body,
            timeout=timeout_seconds or 30,
        )
        response.raise_for_status()
        return response.json() if response.text else {}
