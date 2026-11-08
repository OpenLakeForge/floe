"""Manifest settings dagster-floe honours: orchestration, job_scope, domain_resources, resource limits (#1009)."""

from __future__ import annotations

import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from typing import Any


def _install_airflow_stub() -> None:
    if "airflow.sdk" in sys.modules:
        return
    airflow_module = types.ModuleType("airflow")
    sdk_module = types.ModuleType("airflow.sdk")

    class Asset:  # pragma: no cover - simple import stub
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

from airflow_floe.kubernetes_runner import build_k8s_job_spec
from airflow_floe.manifest import ManifestRunnerDefinition, ManifestRunnerResources
from airflow_floe.operators import (
    FloeRunOperator,
    build_floe_tasks,
    orchestration_max_active_tasks,
    plan_floe_tasks,
)
from airflow_floe.runtime import build_dag_manifest_context, build_dag_manifest_context_or_empty

_K8S: dict[str, Any] = {"type": "kubernetes_job", "image": "floe:1", "namespace": "ns"}


def _entity(name: str, domain: str, runner: str | None = None) -> dict[str, Any]:
    return {
        "name": name,
        "domain": domain,
        "group_name": domain,
        "source_format": "csv",
        "accepted_sink_uri": f"./out/{name}",
        "rejected_sink_uri": None,
        "asset_key": [domain, name],
        "runner": runner,
    }


def _manifest(
    *,
    orchestration: dict[str, Any] | None = None,
    runner: dict[str, Any] | None = None,
    entities: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "spec_version": "0.9",
        "generated_at_ts_ms": 1,
        "config_uri": "config.yml",
        "execution": {
            "entrypoint": "floe",
            "base_args": ["run", "-c", "{config_uri}", "--log-format", "json"],
            "per_entity_args": ["--entities", "{entity_name}"],
            "log_format": "json",
            "result_contract": {
                "run_finished_event": True,
                "summary_uri_field": "summary_uri",
                "exit_codes": {"0": "ok"},
            },
            "defaults": {"env": {}, "workdir": None},
            **({"orchestration": orchestration} if orchestration is not None else {}),
        },
        "runners": {"default": "k8s", "definitions": {"k8s": {**_K8S, **(runner or {})}}},
        "entities": entities or [_entity("a", "sales"), _entity("b", "sales"), _entity("c", "hr")],
    }


def _context(payload: dict[str, Any]):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "manifest.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return build_dag_manifest_context(str(path))


def _runner(resources: ManifestRunnerResources | None) -> ManifestRunnerDefinition:
    return ManifestRunnerDefinition(
        runner_type="kubernetes_job",
        image="floe:1",
        namespace="ns",
        service_account=None,
        resources=resources,
        env=None,
        command=None,
        args=None,
        timeout_seconds=None,
        ttl_seconds_after_finished=None,
        poll_interval_seconds=None,
        secrets=None,
    )


def _resources(spec: dict[str, Any]) -> dict[str, Any]:
    return spec["spec"]["template"]["spec"]["containers"][0]["resources"]


class ResourceLimitsTests(unittest.TestCase):
    def test_limits_come_from_limit_fields_not_requests(self) -> None:
        resources = ManifestRunnerResources.from_dict(
            {"cpu": "500m", "memory_mb": 512, "cpu_limit": "2", "memory_limit_mb": 1024}
        )
        got = _resources(build_k8s_job_spec("j", ["floe"], _runner(resources)))
        self.assertEqual(got["requests"], {"cpu": "500m", "memory": "512Mi"})
        self.assertEqual(got["limits"], {"cpu": "2", "memory": "1024Mi"})

    def test_requests_alone_set_no_limits(self) -> None:
        resources = ManifestRunnerResources.from_dict({"cpu": "500m", "memory_mb": 512})
        got = _resources(build_k8s_job_spec("j", ["floe"], _runner(resources)))
        self.assertNotIn("limits", got)

    def test_limits_alone_set_no_requests(self) -> None:
        resources = ManifestRunnerResources.from_dict({"memory_limit_mb": 256})
        got = _resources(build_k8s_job_spec("j", ["floe"], _runner(resources)))
        self.assertEqual(got, {"limits": {"memory": "256Mi"}})


class DomainResourcesTests(unittest.TestCase):
    def _spec_resources(self, operator: FloeRunOperator) -> Any:
        _, runner = operator._resolve_execution_contract()
        assert runner is not None
        return runner.resources

    def _op(self, ctx: Any, entities: list[str]) -> FloeRunOperator:
        return FloeRunOperator(
            task_id="t", config_path="c.yml", entities=entities, manifest_context=ctx
        )

    def test_domain_scoped_task_uses_domain_resources(self) -> None:
        ctx = _context(
            _manifest(
                runner={
                    "job_scope": "domain",
                    "resources": {"cpu": "1"},
                    "domain_resources": {"sales": {"cpu": "4", "memory_limit_mb": 2048}},
                }
            )
        )
        self.assertEqual(
            self._spec_resources(self._op(ctx, ["a", "b"])),
            ManifestRunnerResources(cpu="4", memory_mb=None, memory_limit_mb=2048),
        )

    def test_unlisted_domain_keeps_runner_resources(self) -> None:
        ctx = _context(
            _manifest(
                runner={
                    "job_scope": "domain",
                    "resources": {"cpu": "1"},
                    "domain_resources": {"sales": {"cpu": "4"}},
                }
            )
        )
        self.assertEqual(self._spec_resources(self._op(ctx, ["c"])).cpu, "1")

    def test_entity_scope_ignores_domain_resources(self) -> None:
        ctx = _context(
            _manifest(
                runner={"resources": {"cpu": "1"}, "domain_resources": {"sales": {"cpu": "4"}}}
            )
        )
        self.assertEqual(self._spec_resources(self._op(ctx, ["a"])).cpu, "1")


class JobScopeTests(unittest.TestCase):
    def test_default_is_one_task_per_entity(self) -> None:
        self.assertEqual(
            plan_floe_tasks(_context(_manifest())),
            [("floe_a", ["a"]), ("floe_b", ["b"]), ("floe_c", ["c"])],
        )

    def test_domain_scope_is_one_task_per_domain(self) -> None:
        plan = plan_floe_tasks(_context(_manifest(runner={"job_scope": "domain"})))
        self.assertEqual(
            plan, [("floe_domain_sales_k8s", ["a", "b"]), ("floe_domain_hr_k8s", ["c"])]
        )

    def test_fallback_context_without_manifest_runs_one_task(self) -> None:
        ctx = build_dag_manifest_context_or_empty("missing.json", default_config_path="c.yml")
        tasks = build_floe_tasks(ctx)
        self.assertEqual([t.entities for t in tasks], [[]])

    def test_domain_scope_needs_a_kubernetes_runner(self) -> None:
        payload = _manifest(runner={"type": "local_process", "job_scope": "domain"})
        self.assertEqual(len(plan_floe_tasks(_context(payload))), 3)

    def test_entity_with_its_own_runner_gets_its_own_task(self) -> None:
        payload = _manifest(runner={"job_scope": "domain"})
        payload["runners"]["definitions"]["big"] = {**_K8S, "job_scope": "domain"}
        payload["entities"][1] = _entity("b", "sales", runner="big")
        self.assertEqual(
            plan_floe_tasks(_context(payload)),
            [
                ("floe_domain_sales_k8s", ["a"]),
                ("floe_domain_sales_big", ["b"]),
                ("floe_domain_hr_k8s", ["c"]),
            ],
        )


class OrchestrationTests(unittest.TestCase):
    def _max(self, orchestration: dict[str, Any] | None) -> int | None:
        return orchestration_max_active_tasks(_context(_manifest(orchestration=orchestration)))

    def test_no_orchestration_is_unconstrained(self) -> None:
        self.assertIsNone(self._max(None))

    def test_sequential_is_one_whatever_the_cap(self) -> None:
        self.assertEqual(self._max({"strategy": "sequential", "max_concurrent_entities": 4}), 1)

    def test_max_concurrent_entities_caps_tasks(self) -> None:
        self.assertEqual(self._max({"strategy": "parallel", "max_concurrent_entities": 3}), 3)

    def test_parallel_without_cap_is_unconstrained(self) -> None:
        self.assertIsNone(self._max({"strategy": "parallel"}))


if __name__ == "__main__":
    unittest.main()
