"""#1410: `policy.min_rows_per_file` settles a short file whole, by its own severity."""

import json
import sys
from pathlib import Path

import polars as pl
import pytest

from floe._cli import main
from floe.config.parse import parse_config
from floe.config.validate import validate_config
from floe.errors import FloeConfigError
from floe.run.entity import run_entity

CONFIG = """\
version: "0.3"
domains:
  - name: d
entities:
  - name: orders
    domain: d
    source:
      format: csv
      path: ./in
      options: {header: true, separator: ","}
    sink:
      accepted: {format: parquet, path: ./out/accepted}
      rejected: {format: csv, path: ./out/rejected}
    policy: {severity: reject, min_rows_per_file: 1, rule_severity: {min_rows_per_file: LEVEL}}
    schema:
      columns:
        - {name: id, type: int64}
report: {path: ./report}
"""
EMPTY = "id\n"


def write(tmp_path: Path, level: str, files: dict[str, str]) -> Path:
    (tmp_path / "in").mkdir()
    for name, rows in files.items():
        (tmp_path / "in" / name).write_text(rows)
    (tmp_path / "config.yml").write_text(CONFIG.replace("LEVEL", level))
    return tmp_path / "config.yml"


def test_reject_copies_the_short_file_and_keeps_the_good_one(tmp_path: Path) -> None:
    config = write(tmp_path, "reject", {"a.csv": EMPTY, "b.csv": "id\n1\n"})
    result = run_entity(parse_config(config).entities[0], tmp_path, "0.3")
    assert not result.aborted
    short, good = result.report["files"]
    assert (short["status"], short["accepted_count"], short["rejected_count"]) == ("rejected", 0, 0)  # fmt: skip
    assert short["validation"]["rules"] == [
        {"rule": "min_rows_per_file", "severity": "reject", "violations": 1, "columns": []}
    ]  # fmt: skip
    assert short["output"]["rejected_path"] == str(tmp_path / "out" / "rejected" / "a.csv")  # fmt: skip
    assert (tmp_path / "out" / "rejected" / "a.csv").read_text() == EMPTY
    assert good["status"] == "success"
    assert pl.read_parquet(tmp_path / "out" / "accepted")["id"].to_list() == [1]


def test_reject_drops_every_row_of_a_file_under_the_threshold(tmp_path: Path) -> None:
    rows = "id\n1\nx\n"
    config = write(tmp_path, "reject", {"a.csv": rows})
    e = parse_config(config).entities[0]
    e = e.model_copy(update={"policy": e.policy.model_copy(update={"min_rows_per_file": 3})})  # fmt: skip
    result = run_entity(e, tmp_path, "0.3")
    (f,) = result.report["files"]
    assert (f["status"], f["accepted_count"], f["rejected_count"]) == ("rejected", 0, 2)
    assert [r["rule"] for r in f["validation"]["rules"]] == ["cast_error", "min_rows_per_file"]  # fmt: skip
    assert [p.name for p in (tmp_path / "out" / "rejected").iterdir()] == ["a.csv"]
    assert (tmp_path / "out" / "rejected" / "a.csv").read_text() == rows
    assert pl.read_parquet(tmp_path / "out" / "accepted").height == 0


def test_warn_keeps_the_file_with_one_warning(tmp_path: Path) -> None:
    config = write(tmp_path, "warn", {"a.csv": EMPTY})
    result = run_entity(parse_config(config).entities[0], tmp_path, "0.3")
    (f,) = result.report["files"]
    assert f["status"] == "success"
    assert (f["validation"]["errors"], f["validation"]["warnings"]) == (0, 1)
    assert f["validation"]["rules"] == [
        {"rule": "min_rows_per_file", "severity": "warn", "violations": 1, "columns": []}
    ]  # fmt: skip
    assert result.report["results"]["warnings_total"] == 1


def test_abort_aborts_the_file_and_the_entity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = write(tmp_path, "abort", {"a.csv": EMPTY, "b.csv": "id\n1\n"})
    monkeypatch.setattr(sys, "argv", ["floe", "run", "-c", str(config)])
    assert main() == 2
    (report,) = (tmp_path / "report").rglob("orders/run.json")
    files = json.loads(report.read_text())["files"]
    assert [f["status"] for f in files] == ["aborted"]
    assert files[0]["validation"]["rules"] == [
        {"rule": "min_rows_per_file", "severity": "abort", "violations": 1, "columns": []}
    ]  # fmt: skip
    assert (tmp_path / "out" / "rejected" / "a.csv").read_text() == EMPTY


def test_a_reject_level_needs_a_rejected_sink(tmp_path: Path) -> None:
    config = write(tmp_path, "reject", {"a.csv": EMPTY})
    config.write_text(
        config.read_text()
        .replace("severity: reject,", "severity: warn,")
        .replace("      rejected: {format: csv, path: ./out/rejected}\n", "")
    )
    parsed = parse_config(config)
    with pytest.raises(FloeConfigError) as err:
        validate_config(parsed)
    assert err.value.code == "config_missing_key"
    assert str(err.value) == "entity.name=orders sink.rejected is required when policy.rule_severity.min_rows_per_file=reject"  # fmt: skip


def test_a_rejected_file_leaves_no_keys_behind(tmp_path: Path) -> None:
    config = write(tmp_path, "reject", {"a.csv": "id\n1\n", "b.csv": "id\n1\n2\n"})
    config.write_text(
        config.read_text()
        .replace("min_rows_per_file: 1,", "min_rows_per_file: 2,")
        .replace("    schema:\n", "    schema:\n      primary_key: [id]\n")
    )
    result = run_entity(parse_config(config).entities[0], tmp_path, "0.3")
    assert [f["status"] for f in result.report["files"]] == ["rejected", "success"]
    assert pl.read_parquet(tmp_path / "out" / "accepted")["id"].to_list() == [1, 2]
