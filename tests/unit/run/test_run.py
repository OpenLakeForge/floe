"""`floe.run.run`: selection, dry run, full refresh, and events."""

from pathlib import Path
from typing import Any

import polars as pl
import pytest

from floe.config.parse import parse_config
from floe.errors import FloeConfigError
from floe.run import run

ENTITY = """\
  - name: {name}
    domain: d
    source:
      format: csv
      path: ./in/{name}.csv
      options: {{header: true, separator: ","}}
    sink:
      write_mode: {mode}
      accepted: {{format: parquet, path: ./out/accepted/{name}}}
      rejected: {{format: csv, path: ./out/rejected/{name}}}
    policy: {{severity: reject}}
    schema:
      columns:
        - {{name: id, type: int64}}
"""


def setup(tmp_path: Path, names: list[str], mode: str = "overwrite") -> Any:
    (tmp_path / "in").mkdir()
    for n in names:
        (tmp_path / "in" / f"{n}.csv").write_text("id\n1\n2\n")
    text = 'version: "0.3"\ndomains:\n  - name: d\nentities:\n'
    text += "".join(ENTITY.format(name=n, mode=mode) for n in names)
    path = tmp_path / "config.yml"
    path.write_text(text + "report: {path: ./report}\n")
    return path, parse_config(path, {})


def rows(tmp_path: Path, name: str) -> int:
    return pl.read_parquet(tmp_path / "out/accepted" / name).height


def test_selection_runs_only_the_named_entities_in_config_order(tmp_path: Path) -> None:
    path, root = setup(tmp_path, ["a", "b", "c"])
    out = run(path, root, entities=["c", "a"])
    assert [e["name"] for e in out.summary["entities"]] == ["d.a", "d.c"]
    assert not (tmp_path / "out/accepted/b").exists()


def test_unknown_entity_is_a_config_error(tmp_path: Path) -> None:
    path, root = setup(tmp_path, ["a"])
    with pytest.raises(FloeConfigError, match="entities not found: x, y"):
        run(path, root, entities=["x", "a", "y"])


def test_dry_run_lists_inputs_and_writes_nothing(tmp_path: Path) -> None:
    path, root = setup(tmp_path, ["a"])
    events: list[dict[str, Any]] = []
    out = run(path, root, dry_run=True, observer=events.append)
    assert out.dry_run is not None
    assert out.dry_run["a"][0].endswith("in/a.csv")
    assert not (tmp_path / "out").exists()
    assert not (tmp_path / "report").exists()
    assert events == []


def test_full_refresh_overwrites_an_append_sink(tmp_path: Path) -> None:
    path, root = setup(tmp_path, ["a"], mode="append")
    run(path, root)
    run(path, root)
    assert rows(tmp_path, "a") == 4
    out = run(path, root, full_refresh=True)
    assert rows(tmp_path, "a") == 2
    assert out.summary["run"]["status"] == "success"


def test_event_order(tmp_path: Path) -> None:
    path, root = setup(tmp_path, ["a", "b"])
    events: list[dict[str, Any]] = []
    out = run(path, root, observer=events.append)
    assert [(e["event"], e.get("name") or e.get("entity")) for e in events] == [
        ("run_started", None),
        ("entity_started", "a"),
        ("file_started", "a"),
        ("file_finished", "a"),
        ("entity_finished", "a"),
        ("entity_started", "b"),
        ("file_started", "b"),
        ("file_finished", "b"),
        ("entity_finished", "b"),
        ("run_finished", None),
    ]
    assert {e["run_id"] for e in events} == {out.run_id}
    assert events[-1]["rows"] == 4 and events[-1]["exit_code"] == 0
    assert events[3]["accepted"] == 2 and events[3]["status"] == "success"
