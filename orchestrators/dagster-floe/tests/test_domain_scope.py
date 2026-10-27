from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from dagster import AssetKey, AssetSelection, materialize

from floe_dagster.assets import build_floe_asset_defs
from floe_dagster.definitions import build_definitions_from_manifest_paths
from floe_dagster.runner import LocalRunner

FIXTURE = Path(__file__).parent / "fixtures" / "manifest.json"
NAMES = ["orders", "returns", "refunds"]


def _manifest(tmp_path: Path, *, job_scope: str | None = "domain") -> Path:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    template = next(e for e in payload["entities"] if e["name"] == "orders")
    payload["entities"] = []
    for name in NAMES:
        entity = copy.deepcopy(template)
        entity["name"] = name
        entity["asset_key"] = ["sales", name]
        payload["entities"].append(entity)
    payload["runners"]["definitions"]["k8s"]["job_scope"] = job_scope
    payload["runners"]["definitions"]["k8s"]["domain_resources"] = {"sales": {"cpu": "2", "memory_mb": 4096}}
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _summary(tmp_path: Path, statuses: dict[str, str]) -> str:
    summary = {
        "entities": [
            {"name": n, "status": s, "results": {"rows_total": 10, "accepted_total": 10}, "report_file": None}
            for n, s in statuses.items()
        ]
    }
    path = tmp_path / "summary.json"
    path.write_text(json.dumps(summary), encoding="utf-8")
    return str(path)


def _run(tmp_path: Path, statuses: dict[str, str], *, exit_code: int = 0, selection=None, **manifest_kw: Any):
    """Materialize with the Kubernetes client mocked; returns the result and the Job API mock."""
    manifest = _manifest(tmp_path, **manifest_kw)
    jobs_api, core_api = MagicMock(), MagicMock()
    ok = {"type": "Complete", "status": "True"}
    jobs_api.read_namespaced_job.return_value = {
        "status": {"succeeded": int(exit_code == 0), "failed": int(exit_code != 0), "conditions": [ok] if exit_code == 0 else []}
    }
    finished = {"event": "run_finished", "run_id": "r1", "status": "success", "exit_code": exit_code, "summary_uri": _summary(tmp_path, statuses)}
    core_api.list_namespaced_pod.return_value.items = [{"metadata": {"name": "pod-1"}}]
    core_api.read_namespaced_pod_log.return_value = json.dumps(finished)
    with (
        patch("floe_dagster.kubernetes_runner._make_batch_client", return_value=jobs_api),
        patch("floe_dagster.kubernetes_runner._make_core_client", return_value=core_api),
    ):
        assets, sources, _ = build_floe_asset_defs(manifest_path=str(manifest), runner=LocalRunner())
        result = materialize([*assets, *sources], selection=selection, raise_on_error=False)
    return result, jobs_api


def _entities_arg(jobs_api: MagicMock) -> str:
    args = jobs_api.create_namespaced_job.call_args.kwargs["body"]["spec"]["template"]["spec"]["containers"][0]["args"]
    return args[args.index("--entities") + 1]


def _materialized(result) -> set[str]:
    return {e.asset_key.to_user_string() for e in result.get_asset_materialization_events()}


def test_domain_scope_runs_the_entities_in_one_job_with_a_materialization_each(tmp_path: Path) -> None:
    result, jobs_api = _run(tmp_path, dict.fromkeys(NAMES, "success"))

    assert result.success
    jobs_api.create_namespaced_job.assert_called_once()
    assert _entities_arg(jobs_api) == "orders,returns,refunds"
    assert _materialized(result) == {"sales/orders", "sales/returns", "sales/refunds"}
    args = jobs_api.create_namespaced_job.call_args.kwargs["body"]["spec"]["template"]["spec"]["containers"][0]["args"]
    assert args[args.index("--run-id") + 1].startswith(f"{result.run_id}_floe_")  # one Floe run id per group


def test_domain_scope_subset_runs_only_the_selected_entities(tmp_path: Path) -> None:
    selection = [AssetKey(["sales", "orders"]), AssetKey(["sales", "refunds"])]
    result, jobs_api = _run(tmp_path, {"orders": "success", "refunds": "success"}, selection=selection)

    assert result.success
    jobs_api.create_namespaced_job.assert_called_once()
    assert _entities_arg(jobs_api) == "orders,refunds"
    assert _materialized(result) == {"sales/orders", "sales/refunds"}


def test_domain_scope_failed_entity_fails_alone_and_the_ones_before_it_materialize(tmp_path: Path) -> None:
    """`floe run` stops at the first abort (until #959): entities after it never ran, so they are not failed."""
    result, jobs_api = _run(tmp_path, {"orders": "success", "returns": "aborted"}, exit_code=2)

    assert not result.success
    jobs_api.create_namespaced_job.assert_called_once()
    assert _materialized(result) == {"sales/orders"}
    [failure] = [e for e in result.all_events if e.event_type_value == "STEP_FAILURE"]
    message = failure.event_specific_data.error.message
    assert "entities returns; not run: refunds" in message


def test_entity_scope_is_unchanged_when_job_scope_is_unset(tmp_path: Path) -> None:
    result, jobs_api = _run(tmp_path, {"orders": "success"}, job_scope=None, selection=[AssetKey(["sales", "orders"])])

    assert result.success
    assert jobs_api.create_namespaced_job.call_args.kwargs["body"]["metadata"]["name"].startswith("floe-orders-")
    assert _entities_arg(jobs_api) == "orders"


def test_an_entity_with_its_own_runner_gets_its_own_domain_asset(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path)
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["runners"]["definitions"]["k8s_big"] = payload["runners"]["definitions"]["k8s"]
    payload["entities"][-1]["runner"] = "k8s_big"
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    assets, _, _ = build_floe_asset_defs(manifest_path=str(manifest), runner=LocalRunner())

    assert sorted(a.node_def.name.split("__", 1)[1] for a in assets) == ["sales__k8s", "sales__k8s_big"]


def test_each_output_keeps_its_own_entitys_source(tmp_path: Path) -> None:
    assets, _, _ = build_floe_asset_defs(manifest_path=str(_manifest(tmp_path)), runner=LocalRunner())
    [domain] = assets
    assert domain.asset_deps[AssetKey(["sales", "orders"])] == {AssetKey(["sales", "orders_source"])}
    assert domain.asset_deps[AssetKey(["sales", "refunds"])] == {AssetKey(["sales", "refunds_source"])}


def test_a_checks_only_selection_still_runs_the_entity(tmp_path: Path) -> None:
    assets, _, _ = build_floe_asset_defs(manifest_path=str(_manifest(tmp_path)), runner=LocalRunner())
    selection = AssetSelection.checks(*(k for k in assets[0].check_keys if k.asset_key == AssetKey(["sales", "orders"])))
    result, jobs_api = _run(tmp_path, {"orders": "success"}, selection=selection)

    assert result.success
    assert _entities_arg(jobs_api) == "orders"
    assert _materialized(result) == set()
    assert len(result.get_asset_check_evaluations()) == 5


def test_a_later_entity_aborting_does_not_fail_the_checks_of_an_earlier_one(tmp_path: Path) -> None:
    result, _ = _run(tmp_path, {"orders": "success", "returns": "aborted"}, exit_code=2)
    orders = [c for c in result.get_asset_check_evaluations() if c.asset_key == AssetKey(["sales", "orders"])]
    assert orders
    assert all(c.passed for c in orders)


def test_a_failed_backend_fails_the_step_even_when_every_entity_succeeded(tmp_path: Path) -> None:
    result, _ = _run(tmp_path, dict.fromkeys(NAMES, "success"), exit_code=1)

    assert not result.success
    assert _materialized(result) == {"sales/orders", "sales/returns", "sales/refunds"}  # outcomes are kept


def test_two_manifests_with_one_domain_and_runner_load_together(tmp_path: Path) -> None:
    first = _manifest(tmp_path)
    payload = json.loads(first.read_text(encoding="utf-8"))
    payload["manifest_id"] = "other"
    payload["entities"] = payload["entities"][:1]
    payload["entities"][0]["name"] = "audit"
    payload["entities"][0]["asset_key"] = ["sales", "audit"]
    second = tmp_path / "other.json"
    second.write_text(json.dumps(payload), encoding="utf-8")

    definitions = build_definitions_from_manifest_paths(manifest_paths=[str(first), str(second)])

    assert definitions.resolve_all_job_defs()  # conflicting op names raise here
