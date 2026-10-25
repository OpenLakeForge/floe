"""`floe.run.run`: selection, dry run, full refresh, and events."""

import json
import shutil
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from floe.config.parse import parse_config
from floe.errors import FloeConfigError, FloeRunError
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
    assert out.dry_run["d.a"][0].endswith("in/a.csv")
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
        ("entity_started", "d.a"),
        ("file_started", "a"),
        ("file_finished", "a"),
        ("entity_finished", "d.a"),
        ("entity_started", "d.b"),
        ("file_started", "b"),
        ("file_finished", "b"),
        ("entity_finished", "d.b"),
        ("run_finished", None),
    ]
    assert {e["run_id"] for e in events} == {out.run_id}
    assert events[-1]["rows"] == 4 and events[-1]["exit_code"] == 0
    assert events[3]["accepted"] == 2 and events[3]["status"] == "success"


def test_unresolved_inputs_fail_before_any_entity_starts(tmp_path: Path) -> None:
    """#864: Rust resolves every entity's inputs before the first `entity_started`."""
    path, root = setup(tmp_path, ["a", "b"])
    (tmp_path / "in/b.csv").unlink()
    events: list[dict[str, Any]] = []
    with pytest.raises(FloeRunError, match="no input files matched"):
        run(path, root, observer=events.append)
    assert [e["event"] for e in events] == ["run_started"]
    assert not (tmp_path / "out").exists()
    assert not (tmp_path / "report").exists()


def qualified(tmp_path: Path) -> Any:
    (tmp_path / "in").mkdir()
    text = 'version: "0.3"\ndomains:\n  - name: sales\n  - name: hr\nentities:\n'
    for d in ("sales", "hr"):
        (tmp_path / "in" / f"{d}.csv").write_text("id\n1\n")
        text += (
            ENTITY.format(name="users", mode="overwrite")
            .replace("domain: d", f"domain: {d}")
            .replace("./in/users.csv", f"./in/{d}.csv")
            .replace("/users}", f"/{d}}}")
        )
    path = tmp_path / "config.yml"
    path.write_text(text + "report: {path: ./report}\n")
    return path, parse_config(path, {})


def test_dry_run_keeps_same_named_entities_of_different_domains(tmp_path: Path) -> None:
    path, root = qualified(tmp_path)
    out = run(path, root, dry_run=True)
    assert out.dry_run is not None
    assert sorted(out.dry_run) == ["hr.users", "sales.users"]


def test_lifecycle_events_use_the_qualified_id(tmp_path: Path) -> None:
    path, root = qualified(tmp_path)
    events: list[dict[str, Any]] = []
    run(path, root, observer=events.append)
    names = [e["name"] for e in events if e["event"].startswith("entity_")]
    assert names == ["sales.users"] * 2 + ["hr.users"] * 2


def test_a_raising_observer_does_not_abort_the_run(tmp_path: Path) -> None:
    path, root = setup(tmp_path, ["a", "b"])

    def boom(_: dict[str, Any]) -> None:
        raise RuntimeError("telemetry down")

    out = run(path, root, observer=boom)
    assert [e["name"] for e in out.summary["entities"]] == ["d.a", "d.b"]


def archived_once(tmp_path: Path) -> Path:
    """An archive-mode entity whose first run archived `a.csv` (2 rows)."""
    (tmp_path / "in").mkdir()
    (tmp_path / "in/a.csv").write_text("id\n1\n2\n")
    text = ENTITY.format(name="o", mode="append").replace("./in/o.csv", "./in")
    text = text.replace("    sink:\n", "    incremental_mode: archive\n    sink:\n      archive: {path: ./arch}\n")  # fmt: skip
    path = tmp_path / "config.yml"
    path.write_text(f'version: "0.3"\ndomains:\n  - name: d\nentities:\n{text}report: {{path: ./report}}\n')  # fmt: skip
    run(path, parse_config(path, {}))
    assert not (tmp_path / "in/a.csv").exists()
    return path


def test_full_refresh_reingests_archived_files(tmp_path: Path) -> None:
    """D24: archived files first (by name, not moved again), then the current sources."""
    path = archived_once(tmp_path)
    (tmp_path / "in/b.csv").write_text("id\n3\n")
    out = run(path, parse_config(path, {}), full_refresh=True)
    assert rows(tmp_path, "o") == 3
    assert out.summary["run"]["status"] == "success"
    report = json.loads(max(tmp_path.glob("report/run_*/d/o/run.json")).read_text())
    files = report["files"]
    assert [Path(f["input_file"]).name.split("__")[0] for f in files] == ["a", "b.csv"]
    assert [f["output"]["archived_path"] is None for f in files] == [True, False]
    assert sorted(p.name.split("__")[0] for p in (tmp_path / "arch/o").iterdir()) == [
        "a",
        "b",
    ]


def test_full_refresh_runs_from_the_archive_alone(tmp_path: Path) -> None:
    """D24: no new source file still rebuilds from the archived files."""
    path = archived_once(tmp_path)
    out = run(path, parse_config(path, {}), full_refresh=True)
    assert out.summary["run"]["status"] == "success"
    assert rows(tmp_path, "o") == 2


def test_full_refresh_with_nothing_archived_and_no_source_still_fails(tmp_path: Path) -> None:  # fmt: skip
    path = archived_once(tmp_path)
    shutil.rmtree(tmp_path / "arch")
    with pytest.raises(FloeRunError, match="no input files matched"):
        run(path, parse_config(path, {}), full_refresh=True)
