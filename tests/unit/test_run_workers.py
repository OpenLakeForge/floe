"""`execution.orchestration` reaches `floe.run.run` as `workers` from `-c -p`, `--manifest` and the Python API (#1116)."""

import os
import sys
from pathlib import Path
from typing import Any

import pytest

import floe
from floe._cli import main
from floe.lineage import observer

CONFIG = """\
version: "0.3"
domains: [{name: d}]
report: {path: ./report}
entities:
  - name: a
    domain: d
    source: {format: csv, path: ./in/a.csv, options: {header: true, separator: ","}}
    sink:
      accepted: {format: parquet, path: ./out/accepted/a}
      rejected: {format: csv, path: ./out/rejected/a}
    policy: {severity: reject}
    schema:
      columns:
        - {name: id, type: int64}
"""
PROFILE = "apiVersion: floe/v1\nkind: EnvironmentProfile\nmetadata: {name: dev}\n"
RUNNER = "execution:\n  runner: {type: local}\n"
CASES = [
    (RUNNER + "  orchestration: {strategy: parallel, max_concurrent_entities: 3}\n", 3),
    (RUNNER + "  orchestration: {strategy: parallel}\n", min(4, os.cpu_count() or 1)),
    (RUNNER + "  orchestration: {strategy: sequential, max_concurrent_entities: 3}\n", 1),
    (RUNNER + "  orchestration: {max_concurrent_entities: 3}\n", 1),
    (RUNNER, 1),
    ("", 1),
]  # fmt: skip


@pytest.fixture
def seen(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    workers: list[int] = []
    real = observer.run

    def spy(*args: Any, **kwargs: Any) -> Any:
        workers.append(kwargs["workers"])
        return real(*args, **kwargs)

    monkeypatch.setattr(observer, "run", spy)
    return workers


def project(tmp_path: Path, profile: str) -> tuple[Path, Path]:
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "a.csv").write_text("id\n1\n")
    (tmp_path / "config.yml").write_text(CONFIG)
    (tmp_path / "p.yml").write_text(PROFILE + profile)
    return tmp_path / "config.yml", tmp_path / "p.yml"


def cli(monkeypatch: pytest.MonkeyPatch, *argv: str) -> int:
    monkeypatch.setattr(sys, "argv", ["floe", *argv])
    return main()


@pytest.mark.parametrize(("profile", "expected"), CASES)
def test_a_profile_reaches_the_run_from_the_cli(profile: str, expected: int, seen: list[int], tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    config, p = project(tmp_path, profile)
    assert cli(monkeypatch, "run", "-c", str(config), "-p", str(p)) == 0
    assert seen == [expected]


@pytest.mark.parametrize(("profile", "expected"), CASES)
def test_a_profile_reaches_the_run_from_the_python_api(profile: str, expected: int, seen: list[int], tmp_path: Path) -> None:  # fmt: skip
    config, p = project(tmp_path, profile)
    floe.run(str(config), profile_path=str(p))
    assert seen == [expected]


@pytest.mark.parametrize(("profile", "expected"), CASES)
def test_the_manifest_carries_it_to_the_run(profile: str, expected: int, seen: list[int], tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    config, p = project(tmp_path, profile)
    manifest = tmp_path / "m.json"
    assert cli(monkeypatch, "manifest", "generate", "-c", str(config), "-p", str(p), "-o", str(manifest)) == 0  # fmt: skip
    assert cli(monkeypatch, "run", "--manifest", str(manifest)) == 0
    floe.run_manifest(str(manifest))
    assert seen == [expected, expected]


def test_without_a_profile_the_run_is_sequential(seen: list[int], tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    config, _ = project(tmp_path, "")
    assert cli(monkeypatch, "run", "-c", str(config)) == 0
    assert seen == [1]
