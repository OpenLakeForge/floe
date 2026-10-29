"""`-p env:NAME`: the profile YAML is read from an environment variable (#1068)."""

import sys
from pathlib import Path

import pytest

import floe
from floe._cli import main
from floe.errors import FloeConfigError

REPO = Path(__file__).resolve().parents[2]
CONFIG = REPO / "example/standalone/config.yml"
PROFILE = REPO / "example/profiles/dev.yml"
RUNNER_PROFILE = """\
apiVersion: floe/v1
kind: EnvironmentProfile
metadata: {name: job}
execution:
  runner:
    type: kubernetes_job
    image: example/floe:1
    namespace: floe
    service_account: floe
variables: {ZONE: bronze}
"""


def cli(monkeypatch: pytest.MonkeyPatch, *args: str) -> int:
    monkeypatch.setattr(sys, "argv", ["floe", *args])
    return main()


def test_cli_validate_reads_the_profile_from_the_variable(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("X", PROFILE.read_text())
    assert cli(monkeypatch, "validate", "-p", "env:X") == 0
    assert (
        capsys.readouterr().out
        == "Profile valid: dev\nSchema: floe/v1/EnvironmentProfile\nCatalogs: 0\n"
    )
    assert cli(monkeypatch, "validate", "-c", str(CONFIG), "-p", "env:X") == 0
    assert capsys.readouterr().out.endswith(" -p env:X\n")


@pytest.mark.parametrize("value", [None, "", "  \n"])
def test_cli_unset_or_empty_variable_names_it(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    value: str | None,
) -> None:
    monkeypatch.delenv("NOPE_PROFILE", raising=False)
    if value is not None:
        monkeypatch.setenv("NOPE_PROFILE", value)
    assert cli(monkeypatch, "validate", "-p", "env:NOPE_PROFILE") == 1
    assert "NOPE_PROFILE" in capsys.readouterr().err


def test_api_validate_reads_the_profile_from_the_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("X", PROFILE.read_text())
    floe.validate(str(CONFIG), profile_path="env:X")


def test_api_unset_variable_raises_naming_it(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NOPE_PROFILE", raising=False)
    with pytest.raises(FloeConfigError, match="NOPE_PROFILE"):
        floe.validate(str(CONFIG), profile_path="env:NOPE_PROFILE")


def test_run_with_env_profile_expands_variables_and_never_launches_the_runner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "in").mkdir()
    (tmp_path / "in/a.csv").write_text("id\n1\n")
    (tmp_path / "config.yml").write_text(
        'version: "0.3"\ndomains:\n  - name: d\nentities:\n'
        "  - name: a\n    domain: d\n"
        "    source: {format: csv, path: ./in/a.csv, options: {header: true, separator: ','}}\n"
        "    sink:\n      accepted: {format: parquet, path: './out/{{ZONE}}/a'}\n"
        "      rejected: {format: csv, path: ./out/rejected/a}\n"
        "    policy: {severity: abort}\n"
        "    schema:\n      columns:\n        - {name: id, type: int64}\n"
        "report: {path: ./report}\n"
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("FLOE_PROFILE_CONTENT", RUNNER_PROFILE)
    assert cli(monkeypatch, "run", "-c", "config.yml", "-p", "env:FLOE_PROFILE_CONTENT") == 0  # fmt: skip
    assert (tmp_path / "out/bronze/a").exists()  # ran locally, in this process
