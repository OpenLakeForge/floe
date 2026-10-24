"""`floe run`: exit code, run reports and the stop after an aborted entity."""

import json
import sys
from pathlib import Path
from typing import Any

import pytest

from floe._cli import main

ENTITY = """\
  - name: {name}
    domain: d
    source:
      format: csv
      path: ./in/{name}.csv
      options: {{header: true, separator: ","}}
    sink:
      accepted: {{format: parquet, path: ./out/accepted/{name}}}
      rejected: {{format: csv, path: ./out/rejected/{name}}}
    policy: {{severity: abort}}
    schema:
      columns:
        - {{name: id, type: int64}}
"""


def run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rows: dict[str, str]) -> int:
    (tmp_path / "in").mkdir()
    for name, text in rows.items():
        (tmp_path / "in" / f"{name}.csv").write_text(text)
    config = 'version: "0.3"\ndomains:\n  - name: d\nentities:\n'
    config += "".join(ENTITY.format(name=n) for n in rows)
    (tmp_path / "config.yml").write_text(config + "report: {path: ./report}\n")
    monkeypatch.setattr(
        sys, "argv", ["floe", "run", "-c", str(tmp_path / "config.yml")]
    )
    return main()


def summary(tmp_path: Path) -> dict[str, Any]:
    (path,) = (tmp_path / "report").glob("run_*/run.summary.json")
    return json.loads(path.read_text())


def test_clean_run_exits_zero_and_writes_both_reports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert run(tmp_path, monkeypatch, {"a": "id\n1\n"}) == 0
    assert summary(tmp_path)["run"]["status"] == "success"
    assert list((tmp_path / "report").glob("run_*/d/a/run.json"))


def test_abort_exits_two_and_skips_the_later_entities(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert run(tmp_path, monkeypatch, {"a": "id\nx\n", "b": "id\n1\n"}) == 2
    assert [e["name"] for e in summary(tmp_path)["entities"]] == ["d.a"]
    assert not (tmp_path / "out" / "accepted" / "b").exists()
