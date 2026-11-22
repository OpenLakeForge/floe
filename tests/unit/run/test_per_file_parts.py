"""#1424: with unique keys, `incremental_mode: file` and a Parquet append, no accepted part spans two input files."""

from pathlib import Path
from typing import Any

import polars as pl
import pytest

from floe.config.parse import parse_config
from floe.run.entity import run_entity

CONFIG = """\
version: "0.3"
domains:
  - name: d
entities:
  - name: orders
    domain: d
    incremental_mode: file
    source: {format: csv, path: ./in, options: {header: true, separator: ","}}
    sink:
      write_mode: append
      accepted: {format: parquet, path: ./out/accepted, options: {max_size_per_file: 268435456}}
      rejected: {format: csv, path: ./out/rejected}
    policy: {severity: reject}
    schema:
      primary_key: [id]
      columns:
        - {name: id, type: int64}
report: {path: ./report}
"""


def run(tmp_path: Path, files: dict[str, str]) -> dict[str, Any]:
    (tmp_path / "in").mkdir()
    for name, body in files.items():
        (tmp_path / "in" / name).write_text(body)
    (tmp_path / "config.yml").write_text(CONFIG)
    return run_entity(parse_config(tmp_path / "config.yml").entities[0], tmp_path, "0.3").report  # fmt: skip


def parts(tmp_path: Path) -> list[list[int]]:
    return sorted(pl.read_parquet(p)["id"].to_list() for p in (tmp_path / "out/accepted").glob("part-*.parquet"))  # fmt: skip


def test_each_input_file_gets_its_own_accepted_part(tmp_path: Path) -> None:
    report = run(tmp_path, {"a.csv": "id\n1\n", "b.csv": "id\n2\n"})
    assert report["accepted_output"]["files_written"] == 2
    assert parts(tmp_path) == [[1], [2]]


@pytest.mark.parametrize(
    "body", ["id\n1\n", "id\nx\n", ""], ids=["duplicate", "uncastable", "unreadable"]
)
def test_a_file_that_accepted_no_row_gets_one_empty_part(tmp_path: Path, body: str) -> None:  # fmt: skip
    report = run(tmp_path, {"a.csv": "id\n1\n", "b.csv": body})
    assert report["accepted_output"]["files_written"] == 2
    assert parts(tmp_path) == [[], [1]]


def test_a_run_that_accepted_no_row_writes_one_empty_part_per_file(tmp_path: Path) -> None:  # fmt: skip
    report = run(tmp_path, {"a.csv": "id\nx\n", "b.csv": "id\ny\n"})
    assert report["accepted_output"]["files_written"] == 2
    assert parts(tmp_path) == [[], []]
