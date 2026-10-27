"""Databricks Jobs API client abstraction for connector runtimes.

Primary auth model: service principal OAuth bearer token supplied by caller.
"""

from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any, Protocol
from urllib.parse import urlencode


# Bumped whenever floe materially changes the job-settings shape it produces.
# Stored on the Databricks job as a tag so we can detect drift without comparing
# server-normalized payloads (which trigger spurious resets every run).
FLOE_SETTINGS_VERSION = "1"

_FIND_JOB_MAX_PAGES = 200


class HttpClient(Protocol):
    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
        timeout_seconds: int | None = None,
    ) -> dict[str, Any]:
        ...


@dataclass(frozen=True)
class DatabricksJobSpec:
    workspace_url: str
    existing_cluster_id: str
    python_file_uri: str
    job_name: str
    parameters: list[str]
    poll_interval_seconds: int = 10
    timeout_seconds: int = 3600


@dataclass(frozen=True)
class DatabricksRunResult:
    run_id: int
    state: str
    result_state: str | None
    state_message: str | None
    run_page_url: str | None


class DatabricksJobsClient:
    def __init__(
        self,
        *,
        http_client: HttpClient,
        workspace_url: str,
        oauth_bearer_token: str,
    ) -> None:
        self._http = http_client
        self._workspace_url = workspace_url.rstrip("/")
        self._headers = {
            "Authorization": f"Bearer {oauth_bearer_token}",
            "Content-Type": "application/json",
        }

    def ensure_domain_job(self, spec: DatabricksJobSpec) -> int:
        expected = self._build_job_settings(spec)
        expected_marker = expected["tags"]["floe_settings_version"]

        existing = self._find_job_by_name(spec.job_name)
        if existing is None:
            created = self._call(
                "POST",
                "/api/2.1/jobs/create",
                {"name": spec.job_name, **expected},
            )
            return int(created["job_id"])

        job_id = int(existing["job_id"])
        current_marker = (
            (existing.get("settings") or {})
            .get("tags", {})
            .get("floe_settings_version")
        )
        if current_marker != expected_marker:
            self._call(
                "POST",
                "/api/2.1/jobs/reset",
                {"job_id": job_id, "new_settings": {"name": spec.job_name, **expected}},
            )
        return job_id

    def run_now(self, *, job_id: int) -> int:
        # NOTE: Per-run env vars are intentionally not sent here. Databricks
        # `job_parameters` are not surfaced as OS env or argv to a
        # `spark_python_task`. Per-run inputs (entity, run id, etc.) must be
        # encoded into `spec.parameters` (task-level argv).
        response = self._call("POST", "/api/2.1/jobs/run-now", {"job_id": job_id})
        return int(response["run_id"])

    def poll_run_to_terminal(
        self,
        *,
        run_id: int,
        poll_interval_seconds: int,
        timeout_seconds: int,
    ) -> DatabricksRunResult:
        deadline = time.monotonic() + timeout_seconds
        while True:
            run = self._call("GET", "/api/2.1/jobs/runs/get", {"run_id": run_id})
            state = run.get("state") or {}
            life_cycle_state = str(state.get("life_cycle_state") or "")
            result_state = state.get("result_state")
            state_message = state.get("state_message")
            if life_cycle_state in {"TERMINATED", "INTERNAL_ERROR", "SKIPPED"}:
                return DatabricksRunResult(
                    run_id=run_id,
                    state=life_cycle_state,
                    result_state=result_state,
                    state_message=state_message,
                    run_page_url=run.get("run_page_url"),
                )
            if time.monotonic() >= deadline:
                raise TimeoutError(f"databricks run {run_id} did not reach terminal state within {timeout_seconds}s")
            time.sleep(max(1, poll_interval_seconds))

    def _find_job_by_name(self, job_name: str) -> dict[str, Any] | None:
        limit = 25
        offset = 0
        page_token: str | None = None
        for _ in range(_FIND_JOB_MAX_PAGES):
            params: dict[str, Any] = {"name": job_name, "limit": limit}
            if page_token:
                params["page_token"] = page_token
            else:
                params["offset"] = offset

            response = self._call("GET", "/api/2.1/jobs/list", params)
            for item in response.get("jobs", []):
                settings = item.get("settings") or {}
                if settings.get("name") == job_name:
                    return item

            next_token = response.get("next_page_token")
            has_more = bool(response.get("has_more"))
            if isinstance(next_token, str) and next_token:
                page_token = next_token
                continue
            if has_more:
                offset += limit
                continue
            return None
        raise RuntimeError(
            f"databricks jobs/list pagination exceeded {_FIND_JOB_MAX_PAGES} pages "
            f"while searching for job_name={job_name!r}"
        )

    def _build_job_settings(self, spec: DatabricksJobSpec) -> dict[str, Any]:
        spark_python_task = {
            "python_file": spec.python_file_uri,
            "parameters": list(spec.parameters),
        }
        task = {
            "task_key": "floe_main",
            "existing_cluster_id": spec.existing_cluster_id,
            "spark_python_task": spark_python_task,
        }
        return {
            "max_concurrent_runs": 1,
            "tasks": [task],
            "tags": {
                "managed_by": "floe",
                "runner_type": "databricks_job",
                "floe_settings_version": FLOE_SETTINGS_VERSION,
            },
        }

    def _call(self, method: str, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"{self._workspace_url}{path}"
        if method == "GET":
            query = urlencode(payload, doseq=True)
            url = f"{url}?{query}" if query else url
            return self._http.request(method, url, headers=self._headers)
        return self._http.request(method, url, headers=self._headers, json_body=payload)
