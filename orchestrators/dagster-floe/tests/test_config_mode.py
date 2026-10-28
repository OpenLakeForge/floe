"""#986: assets and jobs built from the Floe config (manifest built in memory) match manifest mode."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from unittest.mock import patch

import pytest
from dagster import SourceAsset, materialize

floe = pytest.importorskip("floe")
if not hasattr(floe, "build_manifest"):
    pytest.skip("needs the Floe Python core", allow_module_level=True)

from floe_dagster.assets import load_floe_assets  # noqa: E402 -- after the skip
from floe_dagster.definitions import build_definitions, build_job_run_config_from_manifest  # noqa: E402
from floe_dagster.runner import InProcessRunner, LocalRunner, RunResult  # noqa: E402

EXAMPLE = Path(__file__).parents[1] / "example" / "config.yml"
PROFILE = """\
apiVersion: floe/v1
kind: EnvironmentProfile
metadata: {name: prod}
execution:
  runner: {type: kubernetes_job, image: registry/floe:0.8, namespace: floe-prod}
  orchestration: {strategy: sequential}
"""


@pytest.fixture
def paths(tmp_path: Path) -> tuple[str, str, str]:
    config, profile, manifest = tmp_path / "config.yml", tmp_path / "prod.yml", tmp_path / "m.json"
    shutil.copy(EXAMPLE, config)
    profile.write_text(PROFILE)
    manifest.write_text(json.dumps(floe.build_manifest(str(config), str(profile))))
    return str(config), str(profile), str(manifest)


def _k8s_args(**source: str) -> tuple[list[str], list[list[str]]]:
    """Asset keys, and the command each materialized entity's Kubernetes Job runs."""
    calls: list[list[str]] = []

    def job(args: list[str], **_: object) -> RunResult:
        calls.append(args)
        return RunResult(stdout="", stderr="", exit_code=0, status="succeeded")

    defs = load_floe_assets(**source, runner=LocalRunner(), domains=["sales"])
    assets = list(defs.assets or [])
    with patch("floe_dagster.kubernetes_runner.run_kubernetes_job", job):
        materialize(assets, raise_on_error=False)
    keys = sorted(k.to_user_string() for a in assets for k in ([a.key] if isinstance(a, SourceAsset) else a.keys))
    return keys, sorted(calls)


def test_config_mode_matches_manifest_mode_but_runs_with_config_and_profile(paths) -> None:
    config, profile, manifest = paths
    manifest_keys, manifest_calls = _k8s_args(manifest_path=manifest)
    config_keys, config_calls = _k8s_args(config_path=config, profile_path=profile)
    assert config_keys == manifest_keys
    assert {k.split("/")[0] for k in manifest_keys} == {"sales"}
    tail = ["--log-format", "json", "--quiet", "--entities"]
    assert manifest_calls == [["floe", "run", "--manifest", manifest, *tail, e] for e in ("invoices", "orders")]
    assert config_calls == [["floe", "run", "-c", config, "-p", profile, *tail, e] for e in ("invoices", "orders")]

    jobs = lambda defs: sorted(j.name for j in defs.resolve_all_job_defs())  # noqa: E731
    runner = LocalRunner()
    assert jobs(build_definitions(config_path=config, profile_path=profile, runner=runner)) == jobs(
        build_definitions(manifest_path=manifest, runner=runner)
    )
    run_config = build_job_run_config_from_manifest(config_path=config, profile_path=profile)
    assert run_config == build_job_run_config_from_manifest(manifest) is not None


def test_manifest_path_and_config_path_are_exclusive(paths) -> None:
    config, _, manifest = paths
    with pytest.raises(ValueError, match="exactly one of"):
        load_floe_assets(manifest_path=manifest, config_path=config, runner=LocalRunner())


def test_in_process_runner_runs_from_the_config_and_profile(paths) -> None:
    config, profile, _ = paths
    calls: list[tuple[str, dict[str, object]]] = []

    class Outcome:
        summary = {"run": {"exit_code": 0}}

    def run(config_path: str, **kwargs: object) -> Outcome:
        calls.append((config_path, kwargs))
        return Outcome()

    # no profile: the example's default runner is local_process, which runs in process
    defs = load_floe_assets(config_path=config, runner=InProcessRunner(), domains=["sales"])
    with patch.object(floe, "run", run):
        materialize(list(defs.assets or []), raise_on_error=False)
    run_id = calls[0][1]["run_id"]
    expected = [(config, {"entities": [e], "run_id": run_id, "full_refresh": False, "profile_path": None}) for e in ("invoices", "orders")]  # fmt: skip
    assert sorted(calls, key=str) == expected


def _domain_op_names(config: str, profile: str, **kwargs: str) -> set[str]:
    defs = load_floe_assets(config_path=config, profile_path=profile, runner=LocalRunner(), **kwargs)
    return {a.op.name for a in defs.assets or [] if not isinstance(a, SourceAsset) and a.op.name.startswith("floe_")}


def test_manifest_name_keeps_domain_op_names_across_config_edits(paths) -> None:
    config, profile, _ = paths
    Path(profile).write_text(PROFILE.replace("namespace: floe-prod", "namespace: floe-prod, job_scope: domain"))
    named_before = _domain_op_names(config, profile, manifest_name="sales")
    unnamed_before = _domain_op_names(config, profile)
    assert named_before and unnamed_before

    with open(config, "a") as f:
        f.write("\n# an edit that changes the checksum\n")
    assert _domain_op_names(config, profile, manifest_name="sales") == named_before
    assert all(name.startswith("floe_sales__") for name in named_before)
    assert _domain_op_names(config, profile) != unnamed_before  # without a name, as today


def test_manifest_name_needs_config_path(paths) -> None:
    _, _, manifest = paths
    with pytest.raises(ValueError, match="manifest_name= needs config_path="):
        load_floe_assets(manifest_path=manifest, manifest_name="sales", runner=LocalRunner())
