from __future__ import annotations

import pytest

from floe_dagster.databricks_client import DatabricksRunResult
from floe_dagster.databricks_runner import run_databricks_job
from floe_dagster.k8s_status import STATUS_CANCELED, STATUS_FAILED, STATUS_SUCCEEDED, STATUS_TIMEOUT
from floe_dagster.manifest import ManifestRunnerDefinition


class FakeDatabricksClient:
    def __init__(self, terminal: DatabricksRunResult | None = None, exc: Exception | None = None) -> None:
        self._terminal = terminal
        self._exc = exc
        self.last_run_now: dict[str, object] | None = None
        self.last_spec = None

    def ensure_domain_job(self, spec):  # type: ignore[no-untyped-def]
        self.last_spec = spec
        if self._exc is not None:
            raise self._exc
        return 123

    def run_now(self, *, job_id):  # type: ignore[no-untyped-def]
        self.last_run_now = {"job_id": job_id}
        return 456

    def poll_run_to_terminal(self, *, run_id, poll_interval_seconds, timeout_seconds):  # type: ignore[no-untyped-def]
        del run_id, poll_interval_seconds, timeout_seconds
        if self._exc is not None:
            raise self._exc
        assert self._terminal is not None
        return self._terminal


def _runner(**overrides) -> ManifestRunnerDefinition:
    base = dict(
        runner_type="databricks_job",
        image=None,
        namespace=None,
        service_account=None,
        env=None,
        command=None,
        args=None,
        timeout_seconds=60,
        ttl_seconds_after_finished=None,
        poll_interval_seconds=1,
        secrets=None,
        workspace_url="https://adb.example.com",
        existing_cluster_id="1111-222222-abc123",
        config_uri="dbfs:/floe/configs/prod.yml",
        python_file_uri="dbfs:/floe/bin/floe_entry.py",
        job_name="floe-sales-prod",
        auth={"service_principal_oauth_ref": "env://DATABRICKS_TOKEN"},
        env_parameters={"FLOE_ENV": "prod"},
    )
    base.update(overrides)
    return ManifestRunnerDefinition(**base)


def test_run_databricks_job_success_sets_structured_backend_metadata(monkeypatch) -> None:
    monkeypatch.setenv("DATABRICKS_TOKEN", "token")
    client = FakeDatabricksClient(
        terminal=DatabricksRunResult(
            run_id=456,
            state="TERMINATED",
            result_state="SUCCESS",
            state_message="ok",
            run_page_url="https://example/run/456",
        )
    )

    result = run_databricks_job(["floe", "run", "-c", "cfg.yml"], entity="orders", runner=_runner(), client=client)

    assert result.exit_code == 0
    assert result.status == STATUS_SUCCEEDED
    assert result.failure_reason is None
    assert result.backend_metadata["backend_type"] == "databricks"
    assert result.backend_metadata["backend_run_id"] == 456
    assert result.backend_metadata["job_id"] == 123
    assert result.backend_metadata["run_page_url"] == "https://example/run/456"
    assert client.last_run_now == {"job_id": 123}
    # cmd_args should be passed as task-level parameters (env_parameters
    # cannot reach a spark_python_task on existing clusters).
    assert client.last_spec.parameters == ["floe", "run", "-c", "cfg.yml"]
    assert client.last_spec.python_file_uri == "dbfs:/floe/bin/floe_entry.py"


def test_run_databricks_job_failure_maps_to_failed(monkeypatch) -> None:
    monkeypatch.setenv("DATABRICKS_TOKEN", "token")
    client = FakeDatabricksClient(
        terminal=DatabricksRunResult(
            run_id=456,
            state="TERMINATED",
            result_state="FAILED",
            state_message="task failed",
            run_page_url="https://example/run/456",
        )
    )

    result = run_databricks_job(["floe", "run"], entity="orders", runner=_runner(), client=client)

    assert result.exit_code == 1
    assert result.status == STATUS_FAILED
    assert result.failure_reason == "task failed"
    assert result.backend_metadata["backend_type"] == "databricks"
    assert result.backend_metadata["backend_run_id"] == 456


def test_run_databricks_job_canceled_maps_to_canceled(monkeypatch) -> None:
    monkeypatch.setenv("DATABRICKS_TOKEN", "token")
    client = FakeDatabricksClient(
        terminal=DatabricksRunResult(
            run_id=456,
            state="TERMINATED",
            result_state="CANCELED",
            state_message="run canceled",
            run_page_url="https://example/run/456",
        )
    )

    result = run_databricks_job(["floe", "run"], entity="orders", runner=_runner(), client=client)

    assert result.exit_code == 1
    assert result.status == STATUS_CANCELED
    assert result.failure_reason == "run canceled"


def test_run_databricks_job_timeout_error_returns_timeout_status(monkeypatch) -> None:
    monkeypatch.setenv("DATABRICKS_TOKEN", "token")
    client = FakeDatabricksClient(exc=TimeoutError("run 456 timed out"))

    result = run_databricks_job(["floe", "run"], entity="orders", runner=_runner(), client=client)

    assert result.exit_code == 1
    assert result.status == STATUS_TIMEOUT
    assert result.failure_reason == "run 456 timed out"
    assert result.backend_metadata["backend_type"] == "databricks"
    assert result.backend_metadata["error_type"] == "timeout"


def test_run_databricks_job_infra_failure_preserves_status_context(monkeypatch) -> None:
    monkeypatch.setenv("DATABRICKS_TOKEN", "token")
    client = FakeDatabricksClient(exc=RuntimeError("jobs api unavailable"))

    result = run_databricks_job(["floe", "run"], entity="orders", runner=_runner(), client=client)

    assert result.exit_code == 1
    assert result.status == STATUS_FAILED
    assert result.failure_reason == "jobs api unavailable"
    assert result.backend_metadata["backend_type"] == "databricks"
    assert result.backend_metadata["error_type"] == "RuntimeError"


def test_run_databricks_job_renders_job_name_placeholders(monkeypatch) -> None:
    monkeypatch.setenv("DATABRICKS_TOKEN", "token")

    client = FakeDatabricksClient(
        terminal=DatabricksRunResult(
            run_id=456,
            state="TERMINATED",
            result_state="SUCCESS",
            state_message="ok",
            run_page_url="https://example/run/456",
        )
    )
    run_databricks_job(
        ["floe", "run"],
        entity="sales.orders",
        runner=_runner(job_name="floe-{domain}-{env}"),
        client=client,
    )
    assert client.last_spec is not None
    assert client.last_spec.job_name == "floe-sales-prod"


def test_auth_ref_takes_precedence_over_fallback(monkeypatch) -> None:
    monkeypatch.setenv("DATABRICKS_TOKEN", "from-auth-ref")
    monkeypatch.setenv("FLOE_DATABRICKS_OAUTH_TOKEN", "from-fallback")
    captured: dict[str, object] = {}

    class CapturingClient(FakeDatabricksClient):
        pass

    # Patch construction inside runner to capture token
    import floe_dagster.databricks_runner as runner_mod

    original_cls = runner_mod.DatabricksJobsClient

    def fake_cls(*, http_client, workspace_url, oauth_bearer_token):
        captured["token"] = oauth_bearer_token
        return CapturingClient(
            terminal=DatabricksRunResult(
                run_id=1,
                state="TERMINATED",
                result_state="SUCCESS",
                state_message="ok",
                run_page_url=None,
            )
        )

    monkeypatch.setattr(runner_mod, "DatabricksJobsClient", fake_cls)
    try:
        run_databricks_job(["floe", "run"], entity="orders", runner=_runner())
    finally:
        monkeypatch.setattr(runner_mod, "DatabricksJobsClient", original_cls)

    assert captured["token"] == "from-auth-ref"


def test_auth_ref_missing_env_raises_loudly(monkeypatch) -> None:
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    monkeypatch.setenv("FLOE_DATABRICKS_OAUTH_TOKEN", "fallback")
    with pytest.raises(ValueError, match="DATABRICKS_TOKEN"):
        run_databricks_job(["floe", "run"], entity="orders", runner=_runner())


def test_non_env_auth_scheme_is_rejected(monkeypatch) -> None:
    monkeypatch.setenv("FLOE_DATABRICKS_OAUTH_TOKEN", "fallback")
    runner = _runner(auth={"service_principal_oauth_ref": "secret://kv/dbx"})
    with pytest.raises(ValueError, match="env://"):
        run_databricks_job(["floe", "run"], entity="orders", runner=runner)


def test_fallback_used_when_no_auth_ref(monkeypatch) -> None:
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    monkeypatch.setenv("FLOE_DATABRICKS_OAUTH_TOKEN", "fallback")
    runner = _runner(auth=None)
    captured: dict[str, object] = {}

    import floe_dagster.databricks_runner as runner_mod

    original_cls = runner_mod.DatabricksJobsClient

    def fake_cls(*, http_client, workspace_url, oauth_bearer_token):
        captured["token"] = oauth_bearer_token
        return FakeDatabricksClient(
            terminal=DatabricksRunResult(
                run_id=1,
                state="TERMINATED",
                result_state="SUCCESS",
                state_message="ok",
                run_page_url=None,
            )
        )

    monkeypatch.setattr(runner_mod, "DatabricksJobsClient", fake_cls)
    try:
        run_databricks_job(["floe", "run"], entity="orders", runner=runner)
    finally:
        monkeypatch.setattr(runner_mod, "DatabricksJobsClient", original_cls)

    assert captured["token"] == "fallback"
