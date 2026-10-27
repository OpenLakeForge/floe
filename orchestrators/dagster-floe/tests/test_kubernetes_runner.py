from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock, patch

from floe_dagster.kubernetes_runner import (
    STATUS_FAILED,
    STATUS_RUNNING,
    STATUS_SUBMITTED,
    STATUS_SUCCEEDED,
    STATUS_TIMEOUT,
    build_k8s_job_spec,
    map_k8s_job_status,
    run_kubernetes_job,
    sized_for_domain,
)
from floe_dagster.manifest import ManifestRunnerDefinition, load_manifest
from floe_dagster.runner import LocalRunner


def _runner() -> ManifestRunnerDefinition:
    return ManifestRunnerDefinition(
        runner_type="kubernetes_job",
        image="floe-runner:latest",
        namespace="data",
        service_account=None,
        env={"LOG_LEVEL": "INFO"},
        command=None,
        args=None,
        timeout_seconds=60,
        ttl_seconds_after_finished=30,
        poll_interval_seconds=1,
        secrets=None,
    )


def _job(active: int = 0, succeeded: int = 0, failed: int = 0, conditions=None):
    return {
        "status": {
            "active": active,
            "succeeded": succeeded,
            "failed": failed,
            "conditions": conditions or [],
        }
    }


def test_build_k8s_job_spec_minimal_mapping() -> None:
    spec = build_k8s_job_spec("floe-orders-abc", ["floe", "run", "-c", "cfg.yml"], _runner())
    container = spec["spec"]["template"]["spec"]["containers"][0]
    assert spec["metadata"]["namespace"] == "data"
    assert container["image"] == "floe-runner:latest"
    assert container["command"] == ["floe"]
    assert container["args"] == ["run", "-c", "cfg.yml"]


def test_status_mapping_states() -> None:
    assert map_k8s_job_status(_job(active=1)) == STATUS_RUNNING
    assert map_k8s_job_status(_job(succeeded=1)) == STATUS_SUCCEEDED
    assert map_k8s_job_status(_job(failed=1)) == STATUS_FAILED
    assert map_k8s_job_status({"status": None}) == STATUS_SUBMITTED


def test_run_kubernetes_job_success() -> None:
    jobs_api = MagicMock()
    jobs_api.create_namespaced_job.return_value = None
    jobs_api.read_namespaced_job.side_effect = [
        _job(active=1),
        _job(succeeded=1, conditions=[{"type": "Complete", "status": "True"}]),
    ]

    core_api = MagicMock()
    pods = MagicMock()
    pod = MagicMock()
    pod.metadata.name = "pod-1"
    pods.items = [pod]
    core_api.list_namespaced_pod.return_value = pods
    core_api.read_namespaced_pod_log.return_value = '{"schema":"floe.log.v1","event":"run_finished","status":"success"}'

    with patch("time.sleep"):
        result = run_kubernetes_job(
            ["floe", "run", "-c", "cfg.yml"],
            entity="orders",
            runner=_runner(),
            jobs_api=jobs_api,
            core_api=core_api,
        )

    assert result.exit_code == 0
    assert result.status == STATUS_SUCCEEDED
    assert result.failure_reason is None
    assert result.backend_metadata["backend_type"] == "kubernetes"
    assert "run_finished" in result.stdout


def test_run_kubernetes_job_surfaces_timeout_reason() -> None:
    jobs_api = MagicMock()
    jobs_api.create_namespaced_job.return_value = None
    jobs_api.read_namespaced_job.side_effect = [
        _job(
            failed=1,
            conditions=[
                {
                    "type": "Failed",
                    "status": "True",
                    "reason": "DeadlineExceeded",
                    "message": "Job was active longer than specified deadline",
                }
            ],
        ),
    ]

    core_api = MagicMock()
    pods = MagicMock()
    pod = {
        "metadata": {"name": "pod-1"},
        "status": {
            "container_statuses": [
                {"state": {"terminated": {"reason": "Error", "exit_code": 137}}}
            ]
        },
    }
    pods.items = [pod]
    core_api.list_namespaced_pod.return_value = pods
    core_api.read_namespaced_pod_log.return_value = ""

    with patch("time.sleep"):
        result = run_kubernetes_job(
            ["floe", "run", "-c", "cfg.yml"],
            entity="orders",
            runner=_runner(),
            jobs_api=jobs_api,
            core_api=core_api,
        )

    assert result.status == STATUS_TIMEOUT
    assert result.failure_reason.startswith("DeadlineExceeded")
    assert result.backend_metadata["job_failure_reason"].startswith("DeadlineExceeded")


def test_run_kubernetes_job_surfaces_image_pull_reason() -> None:
    jobs_api = MagicMock()
    jobs_api.create_namespaced_job.return_value = None
    jobs_api.read_namespaced_job.side_effect = [
        _job(failed=1, conditions=[{"type": "Failed", "status": "True", "reason": "BackoffLimitExceeded"}]),
    ]

    core_api = MagicMock()
    pods = MagicMock()
    pod = {
        "metadata": {"name": "pod-1"},
        "status": {
            "container_statuses": [
                {"state": {"waiting": {"reason": "ImagePullBackOff"}}}
            ]
        },
    }
    pods.items = [pod]
    core_api.list_namespaced_pod.return_value = pods
    core_api.read_namespaced_pod_log.return_value = ""

    with patch("time.sleep"):
        result = run_kubernetes_job(
            ["floe", "run", "-c", "cfg.yml"],
            entity="orders",
            runner=_runner(),
            jobs_api=jobs_api,
            core_api=core_api,
        )

    assert result.status == STATUS_FAILED
    assert result.failure_reason == "ImagePullBackOff"
    assert result.backend_metadata["container_reason"] == "ImagePullBackOff"


def test_build_k8s_job_spec_renders_resource_requests() -> None:
    runner = replace(_runner(), resources={"cpu": "500m", "memory_mb": 2048})
    spec = build_k8s_job_spec("floe-orders-abc", ["floe", "run"], runner)
    container = spec["spec"]["template"]["spec"]["containers"][0]
    assert container["resources"] == {"requests": {"cpu": "500m", "memory": "2048Mi"}}


def test_build_k8s_job_spec_without_resources_is_unchanged() -> None:
    for resources in (None, {}, {"cpu": None, "memory_mb": None}):
        runner = replace(_runner(), resources=resources)
        spec = build_k8s_job_spec("floe-orders-abc", ["floe", "run"], runner)
        assert "resources" not in spec["spec"]["template"]["spec"]["containers"][0]


def test_manifest_runner_definition_parses_resources() -> None:
    runner = ManifestRunnerDefinition.from_dict(
        {"type": "kubernetes_job", "resources": {"cpu": "1", "memory_mb": 512}}
    )
    assert runner.resources == {"cpu": "1", "memory_mb": 512}


def test_run_kubernetes_job_surfaces_oom_killed_reason() -> None:
    jobs_api = MagicMock()
    jobs_api.read_namespaced_job.side_effect = [
        _job(failed=1, conditions=[{"type": "Failed", "status": "True", "reason": "BackoffLimitExceeded"}]),
    ]
    core_api = MagicMock()
    pods = MagicMock()
    pods.items = [
        {
            "metadata": {"name": "pod-1"},
            "status": {
                "container_statuses": [
                    {"state": {"terminated": {"reason": "OOMKilled", "exit_code": 137}}}
                ]
            },
        }
    ]
    core_api.list_namespaced_pod.return_value = pods
    core_api.read_namespaced_pod_log.return_value = ""

    with patch("time.sleep"):
        result = run_kubernetes_job(
            ["floe", "run"], entity="orders", runner=_runner(), jobs_api=jobs_api, core_api=core_api
        )

    assert result.failure_reason == "OOMKilled (exit_code=137)"


def test_build_k8s_job_spec_sets_requests_and_limits() -> None:
    runner = replace(
        _runner(),
        resources={"cpu": "1", "memory_mb": 512, "cpu_limit": "2", "memory_limit_mb": 1024},
    )
    container = build_k8s_job_spec("floe-x", ["floe"], runner)["spec"]["template"]["spec"]["containers"][0]
    assert container["resources"] == {
        "requests": {"cpu": "1", "memory": "512Mi"},
        "limits": {"cpu": "2", "memory": "1024Mi"},
    }


def test_sized_for_domain_replaces_resources_only_for_a_listed_domain() -> None:
    runner = replace(
        _runner(),
        resources={"cpu": "1"},
        domain_resources={"sales": {"memory_mb": 4096}},
    )
    assert sized_for_domain(runner, "sales").resources == {"memory_mb": 4096}
    assert sized_for_domain(runner, "finance").resources == {"cpu": "1"}
    assert sized_for_domain(runner, None).resources == {"cpu": "1"}


def test_run_floe_entities_creates_one_job_named_for_the_domain() -> None:
    execution = load_manifest(Path(__file__).parent / "fixtures" / "manifest.json").execution
    runner = replace(_runner(), domain_resources={"sales": {"cpu": "2"}})
    jobs_api, core_api = MagicMock(), MagicMock()
    jobs_api.read_namespaced_job.return_value = _job(
        succeeded=1, conditions=[{"type": "Complete", "status": "True"}]
    )
    core_api.list_namespaced_pod.return_value.items = []

    with (
        patch("floe_dagster.kubernetes_runner._make_batch_client", return_value=jobs_api),
        patch("floe_dagster.kubernetes_runner._make_core_client", return_value=core_api),
    ):
        result = LocalRunner().run_floe_entities(
            config_uri="cfg.yml",
            run_id="r1",
            domain="sales",
            entities=["orders", "returns", "refunds"],
            execution=execution,
            runner_definition=runner,
            manifest_uri="s3://m/manifest.json",
        )

    assert result.exit_code == 0
    jobs_api.create_namespaced_job.assert_called_once()
    body = jobs_api.create_namespaced_job.call_args.kwargs["body"]
    assert body["metadata"]["name"].startswith("floe-sales-")
    container = body["spec"]["template"]["spec"]["containers"][0]
    assert container["args"][container["args"].index("--entities") + 1] == "orders,returns,refunds"
    assert container["resources"] == {"requests": {"cpu": "2"}}
