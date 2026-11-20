"""#1254: rejected CSV and Parquet output always appends, whatever the entity's `write_mode`."""

import os
from pathlib import Path

import pytest

from floe.config.parse import parse_config
from floe.run import run

CONFIG = """\
version: "0.3"
domains:
  - name: d
entities:
  - name: e
    domain: d
    source:
      format: csv
      path: ./in/e.csv
      options: {{header: true, separator: ","}}
    sink:
      write_mode: overwrite
      accepted: {{format: parquet, path: ./out/accepted/e}}
      rejected: {{format: {fmt}, path: ./out/rejected/e}}
    policy: {{severity: reject}}
    schema:
      columns:
        - {{name: id, type: int64}}
report: {{path: ./report}}
"""


def go(tmp_path: Path, fmt: str, body: str, mtime: int) -> None:
    (tmp_path / "in").mkdir(exist_ok=True)
    source = tmp_path / "in/e.csv"
    source.write_text(body)
    os.utime(
        source, (mtime, mtime)
    )  # another input hash, as a new file in the next run
    path = tmp_path / "config.yml"
    path.write_text(CONFIG.format(fmt=fmt))
    run(path, parse_config(path, {}))


def parts(tmp_path: Path, fmt: str) -> list[Path]:
    base = tmp_path / "out/rejected/e" / ("rows" if fmt == "parquet" else "")
    return sorted(base.glob(f"part-*.{fmt}"))


@pytest.mark.parametrize("fmt", ["csv", "parquet"])
def test_overwrite_entity_keeps_the_earlier_runs_rejected_parts(tmp_path: Path, fmt: str) -> None:  # fmt: skip
    go(tmp_path, fmt, "id\n1\nx\n", 1_700_000_000)
    first = parts(tmp_path, fmt)
    go(tmp_path, fmt, "id\n2\ny\n", 1_700_000_100)
    assert len(first) == 1
    assert len(parts(tmp_path, fmt)) == 2
    assert set(first) <= set(parts(tmp_path, fmt))


@pytest.mark.parametrize("fmt", ["csv", "parquet"])
def test_a_retry_of_the_same_input_rewrites_its_rejected_part(tmp_path: Path, fmt: str) -> None:  # fmt: skip
    go(tmp_path, fmt, "id\n1\nx\n", 1_700_000_000)
    first = parts(tmp_path, fmt)
    go(tmp_path, fmt, "id\n1\nx\n", 1_700_000_000)
    assert len(first) == 1
    assert parts(tmp_path, fmt) == first
