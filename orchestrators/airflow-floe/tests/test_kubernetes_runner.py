"""Tests for Kubernetes Job runner (airflow-floe connector, T7).

Coverage:
- build_k8s_job_spec: manifest -> K8s Job spec mapping (minimal + full)
- map_k8s_job_status: all terminal/intermediate states
- run_kubernetes_job: success / failure / timeout integration with mocked k8s clients
- FloeRunOperator routing: kubernetes_job type delegates to kubernetes runner
"""

from __future__ import annotations

import json
import sys
import types
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Airflow stub (same pattern as other test files)
# ---------------------------------------------------------------------------


def _install_airflow_stub() -> None:
    if "airflow.sdk" in sys.modules:
        return

    airflow_module = types.ModuleType("airflow")
    sdk_module = types.ModuleType("airflow.sdk")

    class Asset:  # pragma: no cover
        def __init__(self, name: str, uri: str, group: str | None = None) -> None:
            self.name = name
            self.uri = uri
            self.group = group

    sdk_module.Asset = Asset
    airflow_module.sdk = sdk_module
    sys.modules["airflow"] = airflow_module
    sys.modules["airflow.sdk"] = sdk_module


_install_airflow_stub()
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from airflow_floe.kubernetes_runner import (  # noqa: E402
    STATUS_CANCELED,
    STATUS_FAILED,
    STATUS_RUNNING,
    STATUS_SUBMITTED,
    STATUS_SUCCEEDED,
    STATUS_TIMEOUT,
    build_k8s_job_spec,
    map_k8s_job_status,
    run_kubernetes_job,
)
from airflow_floe.manifest import (  # noqa: E402
    ManifestRunnerDefinition,
    ManifestRunnerResources,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _minimal_runner(
    *,
    image: str = "floe-runner:latest",
    namespace: str = "data",
    timeout_seconds: int = 300,
) -> ManifestRunnerDefinition:
    return ManifestRunnerDefinition(
        runner_type="kubernetes_job",
        image=image,
        namespace=namespace,
        service_account=None,
        resources=None,
        env=None,
        command=None,
        args=None,
        timeout_seconds=timeout_seconds,
        ttl_seconds_after_finished=None,
        poll_interval_seconds=None,
        secrets=None,
    )


def _full_runner() -> ManifestRunnerDefinition:
    return ManifestRunnerDefinition(
        runner_type="kubernetes_job",
        image="floe-runner:1.2.3",
        namespace="production",
        service_account="floe-sa",
        resources=ManifestRunnerResources(cpu="500m", memory_mb=1024),
        env={"LOG_LEVEL": "INFO", "REGION": "us-east-1"},
        command=["python"],
        args=["-m", "floe"],
        timeout_seconds=600,
        ttl_seconds_after_finished=120,
        poll_interval_seconds=5,
        secrets=[
            {"name": "DB_PASSWORD", "secret_name": "db-creds", "key": "password"},
        ],
    )


def _make_job_status(
    *,
    active: int = 0,
    succeeded: int = 0,
    failed: int = 0,
    conditions: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Build a plain-dict job object accepted by map_k8s_job_status."""
    return {
        "status": {
            "active": active,
            "succeeded": succeeded,
            "failed": failed,
            "conditions": conditions or [],
        }
    }


def _make_mock_jobs_api(
    *,
    poll_responses: list[dict[str, Any]],
) -> MagicMock:
    """Return a mock BatchV1Api whose read_namespaced_job cycles through responses."""
    api = MagicMock()
    api.create_namespaced_job.return_value = None
    api.read_namespaced_job.side_effect = poll_responses
    return api


def _make_mock_core_api(*, logs: str = "") -> MagicMock:
    api = MagicMock()
    pods_mock = MagicMock()
    pod_mock = MagicMock()
    pod_mock.metadata.name = "floe-orders-abc12345-pod"
    pods_mock.items = [pod_mock]
    api.list_namespaced_pod.return_value = pods_mock
    api.read_namespaced_pod_log.return_value = logs
    return api


def _run_finished_line(**overrides: Any) -> str:
    event = {
        "schema": "floe.log.v1",
        "event": "run_finished",
        "run_id": "run-k8s-1",
        "status": "success",
        "exit_code": 0,
        "files": 2,
        "rows": 100,
        "accepted": 95,
        "rejected": 5,
        "warnings": 0,
        "errors": 0,
        "summary_uri": "local:///tmp/report/run.summary.json",
        "ts_ms": 1739500000000,
    }
    event.update(overrides)
    return json.dumps(event)


# ---------------------------------------------------------------------------
# build_k8s_job_spec
# ---------------------------------------------------------------------------


class BuildJobSpecTests(unittest.TestCase):
    def test_minimal_spec(self) -> None:
        runner = _minimal_runner()
        spec = build_k8s_job_spec("floe-orders-abc123", ["floe", "run", "-c", "/cfg.yml"], runner)

        self.assertEqual(spec["apiVersion"], "batch/v1")
        self.assertEqual(spec["kind"], "Job")
        self.assertEqual(spec["metadata"]["name"], "floe-orders-abc123")
        self.assertEqual(spec["metadata"]["namespace"], "data")

        container = spec["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(container["name"], "floe-runner")
        self.assertEqual(container["image"], "floe-runner:latest")
        self.assertEqual(container["command"], ["floe"])
        self.assertEqual(container["args"], ["run", "-c", "/cfg.yml"])
        self.assertNotIn("env", container)
        self.assertNotIn("resources", container)
        self.assertNotIn("serviceAccountName", spec["spec"]["template"]["spec"])

    def test_timeout_sets_active_deadline(self) -> None:
        runner = _minimal_runner(timeout_seconds=600)
        spec = build_k8s_job_spec("floe-run-test", ["floe"], runner)
        self.assertEqual(spec["spec"]["activeDeadlineSeconds"], 600)

    def test_no_timeout_omits_active_deadline(self) -> None:
        runner = ManifestRunnerDefinition(
            runner_type="kubernetes_job",
            image="img:v1",
            namespace="ns",
            service_account=None,
            resources=None,
            env=None,
            command=None,
            args=None,
            timeout_seconds=None,
            ttl_seconds_after_finished=None,
            poll_interval_seconds=None,
            secrets=None,
        )
        spec = build_k8s_job_spec("floe-run-test", ["floe"], runner)
        self.assertNotIn("activeDeadlineSeconds", spec["spec"])

    def test_full_spec_with_all_optional_fields(self) -> None:
        runner = _full_runner()
        spec = build_k8s_job_spec("floe-full-abc", ["floe", "run", "--entities", "orders"], runner)

        container = spec["spec"]["template"]["spec"]["containers"][0]

        # Command override replaces the binary; args prefix the rendered arguments
        self.assertEqual(container["command"], ["python"])
        self.assertEqual(container["args"], ["-m", "floe", "run", "--entities", "orders"])

        # Service account
        self.assertEqual(
            spec["spec"]["template"]["spec"]["serviceAccountName"], "floe-sa"
        )

        # Resources
        self.assertEqual(container["resources"]["requests"]["cpu"], "500m")
        self.assertEqual(container["resources"]["limits"]["cpu"], "500m")
        self.assertEqual(container["resources"]["requests"]["memory"], "1024Mi")
        self.assertEqual(container["resources"]["limits"]["memory"], "1024Mi")

        # Env vars
        env = {e["name"]: e for e in container["env"]}
        self.assertEqual(env["LOG_LEVEL"]["value"], "INFO")
        self.assertEqual(env["REGION"]["value"], "us-east-1")

        # Secret ref
        self.assertIn("DB_PASSWORD", env)
        secret_entry = env["DB_PASSWORD"]
        self.assertEqual(secret_entry["valueFrom"]["secretKeyRef"]["name"], "db-creds")
        self.assertEqual(secret_entry["valueFrom"]["secretKeyRef"]["key"], "password")

        # TTL
        self.assertEqual(spec["spec"]["ttlSecondsAfterFinished"], 120)

    def test_backoff_limit_is_zero(self) -> None:
        spec = build_k8s_job_spec("floe-x", ["floe"], _minimal_runner())
        self.assertEqual(spec["spec"]["backoffLimit"], 0)

    def test_pod_restart_policy_is_never(self) -> None:
        spec = build_k8s_job_spec("floe-x", ["floe"], _minimal_runner())
        self.assertEqual(spec["spec"]["template"]["spec"]["restartPolicy"], "Never")

    def test_pod_label_includes_job_name(self) -> None:
        spec = build_k8s_job_spec("floe-orders-xyz", ["floe"], _minimal_runner())
        labels = spec["spec"]["template"]["metadata"]["labels"]
        self.assertEqual(labels["floe-job"], "floe-orders-xyz")

    def test_missing_image_raises(self) -> None:
        runner = _minimal_runner(image="")
        with self.assertRaises(ValueError, msg="should require image"):
            build_k8s_job_spec("floe-x", ["floe"], runner)

    def test_missing_namespace_raises(self) -> None:
        runner = _minimal_runner(namespace="")
        with self.assertRaises(ValueError, msg="should require namespace"):
            build_k8s_job_spec("floe-x", ["floe"], runner)

    def test_empty_cmd_args_produces_no_command(self) -> None:
        spec = build_k8s_job_spec("floe-x", [], _minimal_runner())
        container = spec["spec"]["template"]["spec"]["containers"][0]
        self.assertNotIn("command", container)
        self.assertNotIn("args", container)

    def test_single_cmd_arg_is_command_only(self) -> None:
        spec = build_k8s_job_spec("floe-x", ["floe"], _minimal_runner())
        container = spec["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(container["command"], ["floe"])
        self.assertNotIn("args", container)


# ---------------------------------------------------------------------------
# map_k8s_job_status
# ---------------------------------------------------------------------------


class MapK8sJobStatusTests(unittest.TestCase):
    def test_none_job_is_canceled(self) -> None:
        self.assertEqual(map_k8s_job_status(None), STATUS_CANCELED)

    def test_complete_condition_is_succeeded(self) -> None:
        job = _make_job_status(
            succeeded=1, conditions=[{"type": "Complete", "status": "True", "reason": ""}]
        )
        self.assertEqual(map_k8s_job_status(job), STATUS_SUCCEEDED)

    def test_succeeded_count_without_condition_is_succeeded(self) -> None:
        job = _make_job_status(succeeded=1)
        self.assertEqual(map_k8s_job_status(job), STATUS_SUCCEEDED)

    def test_failed_condition_is_failed(self) -> None:
        job = _make_job_status(
            failed=1, conditions=[{"type": "Failed", "status": "True", "reason": "BackoffLimitExceeded"}]
        )
        self.assertEqual(map_k8s_job_status(job), STATUS_FAILED)

    def test_failed_deadline_exceeded_is_timeout(self) -> None:
        job = _make_job_status(
            failed=1, conditions=[{"type": "Failed", "status": "True", "reason": "DeadlineExceeded"}]
        )
        self.assertEqual(map_k8s_job_status(job), STATUS_TIMEOUT)

    def test_failed_count_without_condition_is_failed(self) -> None:
        job = _make_job_status(failed=1)
        self.assertEqual(map_k8s_job_status(job), STATUS_FAILED)

    def test_active_pod_is_running(self) -> None:
        job = _make_job_status(active=1)
        self.assertEqual(map_k8s_job_status(job), STATUS_RUNNING)

    def test_no_counts_is_submitted(self) -> None:
        job = _make_job_status()
        self.assertEqual(map_k8s_job_status(job), STATUS_SUBMITTED)

    def test_job_with_none_status_is_submitted(self) -> None:
        self.assertEqual(map_k8s_job_status({"status": None}), STATUS_SUBMITTED)

    def test_condition_complete_false_does_not_succeed(self) -> None:
        # condition present but status=False — should not count as terminal
        job = _make_job_status(
            active=1, conditions=[{"type": "Complete", "status": "False", "reason": ""}]
        )
        self.assertEqual(map_k8s_job_status(job), STATUS_RUNNING)


# ---------------------------------------------------------------------------
# run_kubernetes_job
# ---------------------------------------------------------------------------


class RunKubernetesJobTests(unittest.TestCase):
    def _runner(self, *, timeout_seconds: int = 300, poll_interval_seconds: int = 1) -> ManifestRunnerDefinition:
        return ManifestRunnerDefinition(
            runner_type="kubernetes_job",
            image="floe-runner:latest",
            namespace="data",
            service_account=None,
            resources=None,
            env=None,
            command=None,
            args=None,
            timeout_seconds=timeout_seconds,
            ttl_seconds_after_finished=None,
            poll_interval_seconds=poll_interval_seconds,
            secrets=None,
        )

    def test_success_returns_normalized_payload(self) -> None:
        logs = _run_finished_line()
        jobs_api = _make_mock_jobs_api(
            poll_responses=[
                _make_job_status(active=1),
                _make_job_status(
                    succeeded=1,
                    conditions=[{"type": "Complete", "status": "True", "reason": ""}],
                ),
            ]
        )
        core_api = _make_mock_core_api(logs=logs)

        with patch("time.sleep"):
            payload = run_kubernetes_job(
                ["floe", "run", "-c", "/cfg.yml"],
                "/cfg.yml",
                ["orders"],
                runner=self._runner(),
                jobs_api=jobs_api,
                core_api=core_api,
            )

        self.assertEqual(payload["schema"], "floe.airflow.run.v1")
        self.assertEqual(payload["run_id"], "run-k8s-1")
        self.assertEqual(payload["status"], "success")
        self.assertEqual(payload["backend_type"], "kubernetes")
        self.assertIn("backend_run_id", payload)
        self.assertEqual(payload["entity"], "orders")
        self.assertEqual(payload["config_uri"], "/cfg.yml")
        self.assertEqual(payload["files"], 2)
        self.assertEqual(payload["accepted"], 95)

    def test_failure_returns_failed_status(self) -> None:
        jobs_api = _make_mock_jobs_api(
            poll_responses=[
                _make_job_status(
                    failed=1,
                    conditions=[
                        {"type": "Failed", "status": "True", "reason": "BackoffLimitExceeded"}
                    ],
                ),
            ]
        )
        core_api = _make_mock_core_api(logs="")  # no run_finished event

        with patch("time.sleep"):
            payload = run_kubernetes_job(
                ["floe", "run", "-c", "/cfg.yml"],
                "/cfg.yml",
                ["orders"],
                runner=self._runner(),
                jobs_api=jobs_api,
                core_api=core_api,
            )

        self.assertEqual(payload["schema"], "floe.airflow.run.v1")
        self.assertEqual(payload["status"], STATUS_FAILED)
        self.assertEqual(payload["backend_type"], "kubernetes")
        self.assertEqual(payload["backend_status"], STATUS_FAILED)
        self.assertIsNotNone(payload["backend_metadata"])
        self.assertIsNone(payload["summary_uri"])

    def test_timeout_returns_timeout_status(self) -> None:
        # Job stays active indefinitely; connector-side timeout triggers
        runner = self._runner(timeout_seconds=1, poll_interval_seconds=1)
        always_active = _make_job_status(active=1)
        jobs_api = _make_mock_jobs_api(
            poll_responses=[always_active] * 10
        )
        core_api = _make_mock_core_api(logs="")

        # Patch time.monotonic to simulate deadline exceeded after first poll
        times = [0.0, 0.0, 2.0]  # start, deadline calc, next check > deadline
        with patch("time.sleep"), patch("time.monotonic", side_effect=times):
            payload = run_kubernetes_job(
                ["floe", "run", "-c", "/cfg.yml"],
                "/cfg.yml",
                ["orders"],
                runner=runner,
                jobs_api=jobs_api,
                core_api=core_api,
            )

        self.assertEqual(payload["status"], STATUS_TIMEOUT)
        self.assertEqual(payload["backend_type"], "kubernetes")
        self.assertEqual(payload["backend_status"], STATUS_TIMEOUT)
        self.assertIn("failure_reason", payload)

    def test_k8s_deadline_exceeded_condition_returns_timeout(self) -> None:
        # K8s itself signals DeadlineExceeded via job condition
        jobs_api = _make_mock_jobs_api(
            poll_responses=[
                _make_job_status(
                    failed=1,
                    conditions=[{"type": "Failed", "status": "True", "reason": "DeadlineExceeded"}],
                ),
            ]
        )
        core_api = _make_mock_core_api(logs="")

        with patch("time.sleep"):
            payload = run_kubernetes_job(
                ["floe", "run", "-c", "/cfg.yml"],
                "/cfg.yml",
                ["orders"],
                runner=self._runner(),
                jobs_api=jobs_api,
                core_api=core_api,
            )

        self.assertEqual(payload["status"], STATUS_TIMEOUT)
        self.assertTrue(str(payload.get("failure_reason", "")).startswith("DeadlineExceeded"))

    def test_no_run_finished_event_uses_infra_status(self) -> None:
        # Logs exist but contain no run_finished event
        jobs_api = _make_mock_jobs_api(
            poll_responses=[_make_job_status(succeeded=1)]
        )
        core_api = _make_mock_core_api(logs="some unexpected output\n")

        with patch("time.sleep"):
            payload = run_kubernetes_job(
                ["floe", "run", "-c", "/cfg.yml"],
                "/cfg.yml",
                None,
                runner=self._runner(),
                jobs_api=jobs_api,
                core_api=core_api,
            )

        self.assertEqual(payload["status"], STATUS_SUCCEEDED)
        self.assertNotIn("entity", payload)  # entities=None

    def test_plural_entities_omit_entity_key(self) -> None:
        logs = _run_finished_line()
        jobs_api = _make_mock_jobs_api(
            poll_responses=[_make_job_status(succeeded=1)]
        )
        core_api = _make_mock_core_api(logs=logs)

        with patch("time.sleep"):
            payload = run_kubernetes_job(
                ["floe", "run", "-c", "/cfg.yml"],
                "/cfg.yml",
                ["orders", "customers"],
                runner=self._runner(),
                jobs_api=jobs_api,
                core_api=core_api,
            )

        self.assertNotIn("entity", payload)

    def test_new_run_finished_fields_passed_through_to_payload(self) -> None:
        logs = _run_finished_line(
            report_base="s3://bucket/reports",
            entity_report_uris={"orders": "s3://bucket/reports/run_1/entities/orders.report.json"},
        )
        jobs_api = _make_mock_jobs_api(
            poll_responses=[_make_job_status(succeeded=1)]
        )
        core_api = _make_mock_core_api(logs=logs)

        with patch("time.sleep"):
            payload = run_kubernetes_job(
                ["floe", "run", "-c", "/cfg.yml"],
                "/cfg.yml",
                ["orders"],
                runner=self._runner(),
                jobs_api=jobs_api,
                core_api=core_api,
            )

        self.assertEqual(payload["report_base"], "s3://bucket/reports")
        self.assertIn("orders", payload["entity_report_uris"])

    def test_new_fields_absent_when_not_in_event(self) -> None:
        logs = _run_finished_line()  # fixture has no report_base / entity_report_uris
        jobs_api = _make_mock_jobs_api(
            poll_responses=[_make_job_status(succeeded=1)]
        )
        core_api = _make_mock_core_api(logs=logs)

        with patch("time.sleep"):
            payload = run_kubernetes_job(
                ["floe", "run", "-c", "/cfg.yml"],
                "/cfg.yml",
                ["orders"],
                runner=self._runner(),
                jobs_api=jobs_api,
                core_api=core_api,
            )

        self.assertNotIn("report_base", payload)
        self.assertNotIn("entity_report_uris", payload)

    def test_job_submitted_to_correct_namespace(self) -> None:
        runner = self._runner()
        jobs_api = _make_mock_jobs_api(
            poll_responses=[_make_job_status(succeeded=1)]
        )
        core_api = _make_mock_core_api()

        with patch("time.sleep"):
            run_kubernetes_job(
                ["floe"],
                "/cfg.yml",
                ["orders"],
                runner=runner,
                jobs_api=jobs_api,
                core_api=core_api,
            )

        call_kwargs = jobs_api.create_namespaced_job.call_args
        self.assertEqual(call_kwargs.kwargs.get("namespace") or call_kwargs[1].get("namespace"), "data")

    def test_job_name_derived_from_entity(self) -> None:
        jobs_api = _make_mock_jobs_api(
            poll_responses=[_make_job_status(succeeded=1)]
        )
        core_api = _make_mock_core_api()

        with patch("time.sleep"):
            payload = run_kubernetes_job(
                ["floe"],
                "/cfg.yml",
                ["my_entity"],
                runner=self._runner(),
                jobs_api=jobs_api,
                core_api=core_api,
            )

        self.assertTrue(payload["backend_run_id"].startswith("floe-my-entity-"))

    def test_create_job_called_once(self) -> None:
        jobs_api = _make_mock_jobs_api(
            poll_responses=[_make_job_status(succeeded=1)]
        )
        core_api = _make_mock_core_api()

        with patch("time.sleep"):
            run_kubernetes_job(
                ["floe"],
                "/cfg.yml",
                ["orders"],
                runner=self._runner(),
                jobs_api=jobs_api,
                core_api=core_api,
            )

        jobs_api.create_namespaced_job.assert_called_once()


# ---------------------------------------------------------------------------
# FloeRunOperator routing for kubernetes_job
# ---------------------------------------------------------------------------


class OperatorKubernetesRoutingTests(unittest.TestCase):
    """Verify FloeRunOperator delegates kubernetes_job runner type to k8s adapter."""

    def _write_k8s_manifest(self, base: Path, config_path: Path) -> Path:
        manifest_path = base / "manifest.k8s.json"
        payload = {
            "spec_version": "0.8",
            "generated_at_ts_ms": 1739500000000,
            "floe_version": "0.3.0",
            "config_uri": str(config_path),
            "config_checksum": None,
            "entities": [
                {
                    "name": "orders",
                    "domain": "sales",
                    "group_name": "sales",
                    "source_format": "csv",
                    "accepted_sink_uri": f"local://{base}/out/accepted/orders",
                    "rejected_sink_uri": None,
                    "asset_key": ["sales", "orders"],
                    "runner": None,
                }
            ],
            "execution": {
                "entrypoint": "floe",
                "base_args": ["run", "-c", "{config_uri}", "--log-format", "json"],
                "per_entity_args": ["--entities", "{entity_name}"],
                "log_format": "json",
                "result_contract": {
                    "run_finished_event": True,
                    "summary_uri_field": "summary_uri",
                    "exit_codes": {"0": "success_or_rejected", "1": "technical_failure"},
                },
                "defaults": {"env": {}, "workdir": None},
            },
            "runners": {
                "default": "k8s",
                "definitions": {
                    "k8s": {
                        "type": "kubernetes_job",
                        "image": "floe-runner:latest",
                        "namespace": "data",
                        "service_account": None,
                        "resources": None,
                        "env": None,
                        "command": None,
                        "args": None,
                        "timeout_seconds": 300,
                        "ttl_seconds_after_finished": None,
                        "poll_interval_seconds": 1,
                        "secrets": None,
                    }
                },
            },
        }
        manifest_path.write_text(json.dumps(payload), encoding="utf-8")
        return manifest_path

    def test_kubernetes_job_runner_delegates_to_k8s_adapter(self) -> None:
        import tempfile

        from airflow_floe.operators import FloeRunOperator
        from airflow_floe.runtime import build_dag_manifest_context

        expected_payload = {
            "schema": "floe.airflow.run.v1",
            "run_id": "run-k8s-1",
            "status": "success",
            "backend_type": "kubernetes",
            "backend_run_id": "floe-orders-abc12345",
        }

        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            config_path = base / "config.yml"
            config_path.write_text("version: v1\n", encoding="utf-8")
            manifest_path = self._write_k8s_manifest(base, config_path)
            manifest_context = build_dag_manifest_context(str(manifest_path))

            operator = FloeRunOperator(
                task_id="run_k8s",
                config_path=str(config_path),
                entities=["orders"],
                manifest_context=manifest_context,
            )

            with patch(
                "airflow_floe.operators.run_kubernetes_job",
                return_value=expected_payload,
            ) as k8s_mock:
                result = operator.execute({})

        k8s_mock.assert_called_once()
        call_kwargs = k8s_mock.call_args
        # First positional arg is cmd_args
        cmd_args = call_kwargs.args[0] if call_kwargs.args else call_kwargs[0][0]
        self.assertIsInstance(cmd_args, list)
        self.assertIn("floe", cmd_args)
        self.assertEqual(result, expected_payload)

    def test_unknown_runner_type_still_raises_not_implemented(self) -> None:
        import tempfile

        from airflow_floe.operators import FloeRunOperator
        from airflow_floe.runtime import build_dag_manifest_context

        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            config_path = base / "config.yml"
            config_path.write_text("version: v1\n", encoding="utf-8")

            manifest_path = base / "manifest.json"
            payload = {
                "spec_version": "0.8",
                "generated_at_ts_ms": 1739500000000,
                "floe_version": "0.3.0",
                "config_uri": str(config_path),
                "config_checksum": None,
                "entities": [
                    {
                        "name": "orders",
                        "domain": "sales",
                        "group_name": "sales",
                        "source_format": "csv",
                        "accepted_sink_uri": f"local://{base}/out/accepted/orders",
                        "rejected_sink_uri": None,
                        "asset_key": ["sales", "orders"],
                        "runner": None,
                    }
                ],
                "execution": {
                    "entrypoint": "floe",
                    "base_args": ["run", "-c", "{config_uri}"],
                    "per_entity_args": ["--entities", "{entity_name}"],
                    "log_format": "json",
                    "result_contract": {
                        "run_finished_event": True,
                        "summary_uri_field": "summary_uri",
                        "exit_codes": {"0": "success_or_rejected"},
                    },
                    "defaults": {"env": {}, "workdir": None},
                },
                "runners": {
                    "default": "custom",
                    "definitions": {
                        "custom": {
                            "type": "ecs_task",
                            "image": None,
                            "namespace": None,
                            "service_account": None,
                            "resources": None,
                            "env": None,
                            "command": None,
                            "args": None,
                            "timeout_seconds": None,
                            "ttl_seconds_after_finished": None,
                            "poll_interval_seconds": None,
                            "secrets": None,
                        }
                    },
                },
            }
            manifest_path.write_text(json.dumps(payload), encoding="utf-8")
            manifest_context = build_dag_manifest_context(str(manifest_path))

            operator = FloeRunOperator(
                task_id="run_ecs",
                config_path=str(config_path),
                manifest_context=manifest_context,
            )
            with self.assertRaises(NotImplementedError) as cm:
                operator.execute({})
            self.assertIn("ecs_task", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
