from __future__ import annotations

from pathlib import Path

import pytest

from floe_dagster.definitions import build_runner_from_env
from floe_dagster.manifest import ManifestExecution
from floe_dagster.runner import LocalRunner, RunResult


def _execution(*, env: dict[str, str], workdir: str | None) -> ManifestExecution:
    return ManifestExecution.from_dict(
        {
            "entrypoint": "floe",
            "base_args": ["run", "-c", "{config_uri}", "--log-format", "json"],
            "per_entity_args": ["--entities", "{entity_name}"],
            "log_format": "json",
            "result_contract": {
                "run_finished_event": True,
                "summary_uri_field": "summary_uri",
                "exit_codes": {"0": "ok"},
            },
            "defaults": {
                "env": env,
                "workdir": workdir,
            },
        }
    )


def test_local_runner_applies_execution_defaults(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def fake_run(args, cwd=None, env_overrides=None, dagster_job_name=None, timeout=None):
        captured["args"] = args
        captured["cwd"] = cwd
        captured["env_overrides"] = env_overrides
        captured["timeout"] = timeout
        return RunResult(stdout="", stderr="", exit_code=0)

    monkeypatch.setattr("floe_dagster.runner._run", fake_run)

    config_path = tmp_path / "cfg" / "config.yml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text('version: "0.2"\n', encoding="utf-8")

    runner = LocalRunner("floe")
    execution = _execution(env={"FLOE_TEST_FLAG": "1"}, workdir="../work")
    runner.run_floe_entity(
        config_uri=str(config_path),
        run_id="run-123",
        entity="customers",
        execution=execution,
    )

    assert captured["env_overrides"] == {"FLOE_TEST_FLAG": "1"}
    assert captured["cwd"] == str((config_path.parent / "../work").resolve())


def test_local_runner_without_execution_keeps_default_process_context(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run(args, cwd=None, env_overrides=None, dagster_job_name=None, timeout=None):
        captured["args"] = args
        captured["cwd"] = cwd
        captured["env_overrides"] = env_overrides
        captured["timeout"] = timeout
        return RunResult(stdout="", stderr="", exit_code=0)

    monkeypatch.setattr("floe_dagster.runner._run", fake_run)

    runner = LocalRunner("floe")
    runner.run_floe_entity(
        config_uri="/tmp/config.yml",
        run_id=None,
        entity="customers",
    )

    assert captured["cwd"] is None
    assert captured["env_overrides"] is None
    assert captured["timeout"] is None


def test_local_runner_passes_timeout_to_subprocess(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run(args, cwd=None, env_overrides=None, dagster_job_name=None, timeout=None):
        captured["timeout"] = timeout
        return RunResult(stdout="", stderr="", exit_code=0)

    monkeypatch.setattr("floe_dagster.runner._run", fake_run)

    runner = LocalRunner("floe", timeout=30)
    runner.run_floe_entity(config_uri="/tmp/config.yml", run_id=None, entity="customers")

    assert captured["timeout"] == 30


def test_run_surfaces_timeout_as_failed_result(monkeypatch) -> None:
    import subprocess

    from floe_dagster import runner as runner_mod

    def fake_subprocess_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(
            cmd=kwargs.get("args") or args[0],
            timeout=kwargs["timeout"],
            output="partial-stdout",
            stderr="partial-stderr",
        )

    monkeypatch.setattr(runner_mod.subprocess, "run", fake_subprocess_run)

    result = runner_mod._run(["floe", "run"], timeout=5)

    assert result.exit_code != 0
    assert result.status == "timeout"
    assert "5s timeout" in (result.failure_reason or "")
    assert result.stdout == "partial-stdout"
    assert result.stderr == "partial-stderr"


def test_build_runner_from_env_reads_timeout(monkeypatch) -> None:
    monkeypatch.setenv("FLOE_RUN_TIMEOUT_SECONDS", "45")
    runner = build_runner_from_env()
    assert isinstance(runner, LocalRunner)
    assert runner._timeout == 45.0


def test_build_runner_from_env_no_timeout_by_default(monkeypatch) -> None:
    monkeypatch.delenv("FLOE_RUN_TIMEOUT_SECONDS", raising=False)
    runner = build_runner_from_env()
    assert isinstance(runner, LocalRunner)
    assert runner._timeout is None


def test_build_runner_from_env_rejects_invalid_timeout(monkeypatch) -> None:
    monkeypatch.setenv("FLOE_RUN_TIMEOUT_SECONDS", "nope")
    with pytest.raises(ValueError):
        build_runner_from_env()
