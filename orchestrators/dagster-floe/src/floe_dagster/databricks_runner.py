"""Databricks runner adapter for dagster-floe."""

from __future__ import annotations

import os
from typing import Any

from .databricks_client import DatabricksJobsClient, DatabricksJobSpec, DatabricksRunResult
from .k8s_status import STATUS_CANCELED, STATUS_FAILED, STATUS_SUCCEEDED, STATUS_TIMEOUT
from .manifest import ManifestRunnerDefinition
from .runner import RunResult


def run_databricks_job(
    cmd_args: list[str],
    *,
    entity: str,
    runner: ManifestRunnerDefinition,
    client: DatabricksJobsClient | None = None,
) -> RunResult:
    workspace_url = runner.workspace_url
    existing_cluster_id = runner.existing_cluster_id
    config_uri = runner.config_uri
    python_file_uri = runner.python_file_uri
    if not workspace_url or not existing_cluster_id or not config_uri or not python_file_uri:
        raise ValueError(
            "databricks_job runner requires workspace_url, existing_cluster_id, config_uri and python_file_uri"
        )

    auth_ref = _extract_oauth_ref(runner.auth)
    oauth_token = _resolve_oauth_token(auth_ref)
    domain = _resolve_domain(entity)
    env_name = _resolve_env_name(runner.env_parameters)

    dbx_client = client or DatabricksJobsClient(
        http_client=_RequestsHttpClient(),
        workspace_url=workspace_url,
        oauth_bearer_token=oauth_token,
    )
    spec = DatabricksJobSpec(
        workspace_url=workspace_url,
        existing_cluster_id=existing_cluster_id,
        python_file_uri=python_file_uri,
        job_name=_render_job_name(runner.job_name or "floe-{domain}-{env}", domain=domain, env=env_name),
        parameters=list(cmd_args),
        poll_interval_seconds=runner.poll_interval_seconds or 10,
        timeout_seconds=runner.timeout_seconds or 3600,
    )

    job_id: int | None = None
    run_id: int | None = None
    try:
        job_id = dbx_client.ensure_domain_job(spec)
        run_id = dbx_client.run_now(job_id=job_id)
        terminal = dbx_client.poll_run_to_terminal(
            run_id=run_id,
            poll_interval_seconds=spec.poll_interval_seconds,
            timeout_seconds=spec.timeout_seconds,
        )
    except TimeoutError as exc:
        return _infra_failure_result(
            status=STATUS_TIMEOUT,
            failure_reason=str(exc),
            stderr=str(exc),
            workspace_url=workspace_url,
            job_id=job_id,
            run_id=run_id,
            error_type="timeout",
        )
    except Exception as exc:  # noqa: BLE001
        return _infra_failure_result(
            status=STATUS_FAILED,
            failure_reason=str(exc) or exc.__class__.__name__,
            stderr=str(exc),
            workspace_url=workspace_url,
            job_id=job_id,
            run_id=run_id,
            error_type=exc.__class__.__name__,
        )

    status = _map_status(terminal)
    backend_metadata = _backend_metadata(
        workspace_url=workspace_url,
        terminal=terminal,
        job_id=job_id,
    )

    if status != STATUS_SUCCEEDED:
        return RunResult(
            stdout="",
            stderr=terminal.state_message or "Databricks run failed",
            exit_code=1,
            status=status,
            failure_reason=terminal.state_message,
            backend_metadata=backend_metadata,
        )

    return RunResult(
        stdout="",
        stderr="",
        exit_code=0,
        status=status,
        failure_reason=None,
        backend_metadata=backend_metadata,
    )


def _map_status(terminal: DatabricksRunResult) -> str:
    result_state = (terminal.result_state or "").upper()
    life_cycle_state = (terminal.state or "").upper()

    if result_state == "SUCCESS":
        return STATUS_SUCCEEDED
    if result_state in {"CANCELED", "CANCELLED"}:
        return STATUS_CANCELED
    if result_state == "TIMEDOUT":
        return STATUS_TIMEOUT
    if result_state:
        return STATUS_FAILED

    if life_cycle_state == "SKIPPED":
        return STATUS_CANCELED
    return STATUS_FAILED


def _resolve_domain(entity: str) -> str:
    if not entity or "." not in entity:
        return "default"
    return entity.split(".", 1)[0]


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
                f"databricks oauth token env var '{env_var}' (from auth.service_principal_oauth_ref) not found"
            )
        raise ValueError(
            "databricks oauth reference is not directly resolvable by dagster-floe; "
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
    terminal: DatabricksRunResult,
    job_id: int | None,
) -> dict[str, Any]:
    return {
        "backend_type": "databricks",
        "backend_run_id": terminal.run_id,
        "workspace_url": workspace_url,
        "job_id": job_id,
        "life_cycle_state": terminal.state,
        "result_state": terminal.result_state,
        "state_message": terminal.state_message,
        "run_page_url": terminal.run_page_url,
        "error_type": None,
    }


def _infra_failure_result(
    *,
    status: str,
    failure_reason: str,
    stderr: str,
    workspace_url: str,
    job_id: int | None,
    run_id: int | None,
    error_type: str,
) -> RunResult:
    backend_metadata: dict[str, Any] = {
        "backend_type": "databricks",
        "backend_run_id": run_id,
        "workspace_url": workspace_url,
        "job_id": job_id,
        "life_cycle_state": None,
        "result_state": None,
        "state_message": failure_reason,
        "run_page_url": None,
        "error_type": error_type,
    }
    return RunResult(
        stdout="",
        stderr=stderr,
        exit_code=1,
        status=status,
        failure_reason=failure_reason,
        backend_metadata=backend_metadata,
    )


class _RequestsHttpClient:
    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        json_body: dict[str, str] | None = None,
        timeout_seconds: int | None = None,
    ) -> dict:
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
