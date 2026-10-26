"""D28: `floe run` on a `source.partitioning` layout (local; S3 is in test_run_s3.py)."""

import json
import os
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from floe.config.parse import parse_config
from floe.errors import FloeRunError
from floe.run import run

CONFIG = """\
version: "0.3"
domains:
  - name: d
entities:
  - name: o
    domain: d
    {mode}
    source:
      format: csv
      path: ./in
      options: {{header: true, separator: ","}}
      partitioning: {{pattern: "{{yyyy}}/{{MM}}/{{dd}}/{{batch}}", date_column: ingest_date, batch_column: batch_id}}
    sink:
      accepted: {{format: parquet, path: ./out/accepted}}
      rejected: {{format: csv, path: ./out/rejected}}
      {archive}
    policy: {{severity: reject}}
    schema:
      primary_key: [id, ingest_date]
      columns:
        - {{name: id, type: int64}}
report: {{path: ./report}}
"""


def put(tmp_path: Path, rel: str, body: str, mtime: int) -> None:
    path = tmp_path / "in" / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    os.utime(path, (mtime, mtime))


def go(tmp_path: Path, mode: str = "", archive: str = "", **kw: Any) -> Any:
    path = tmp_path / "config.yml"
    path.write_text(CONFIG.format(mode=mode, archive=archive))
    return run(path, parse_config(path, {}), **kw)


def report(tmp_path: Path) -> Any:
    return json.loads(max(tmp_path.glob("report/run_*/d/o/run.json")).read_text())


def test_two_days_two_batches_load_in_date_then_batch_order(tmp_path: Path) -> None:
    put(tmp_path, "2026/10/25/c3d4/y.csv", "id\n1\n", 300)
    put(
        tmp_path, "2026/10/24/ff00/x.csv", "id\n2\n", 100
    )  # earlier batch, though it sorts last by name
    put(tmp_path, "2026/10/24/a1b2/x.csv", "id\n1\n1\n", 200)
    go(tmp_path)
    got = pl.read_parquet(tmp_path / "out/accepted")
    assert got.rows() == [(2, date(2026, 10, 24), "ff00"), (1, date(2026, 10, 24), "a1b2"), (1, date(2026, 10, 25), "c3d4")]  # fmt: skip
    assert got.schema == {"id": pl.Int64, "ingest_date": pl.Date, "batch_id": pl.String}
    r = report(tmp_path)
    order = ["2026/10/24/ff00/x.csv", "2026/10/24/a1b2/x.csv", "2026/10/25/c3d4/y.csv"]
    assert [f["input_file"].split("/in/")[1] for f in r["files"]] == order
    assert [
        u.split("/in/")[1] for u in r["source"]["resolved_inputs"]["files"]
    ] == order
    # the second (1, 2026-10-24) is a primary-key duplicate; (1, 2026-10-25) is not
    assert [f["rejected_count"] for f in r["files"]] == [0, 1, 0]
    mismatch = r["files"][0]["mismatch"]
    assert (mismatch["declared_columns_count"], mismatch["input_columns_count"]) == (
        3,
        1,
    )
    assert mismatch["missing_columns"] == mismatch["extra_columns"] == []


def test_a_file_outside_the_pattern_fails_the_run_before_any_entity_starts(tmp_path: Path) -> None:  # fmt: skip
    put(tmp_path, "2026/10/24/a1b2/x.csv", "id\n1\n", 100)
    put(tmp_path, "2026/10/24/stray.csv", "id\n1\n", 100)
    events: list[str] = []

    def observe(event: dict[str, Any]) -> None:
        events.append(event["event"])

    with pytest.raises(FloeRunError) as err:
        go(tmp_path, observer=observe)
    assert str(err.value).endswith(
        "/in/2026/10/24/stray.csv does not match source.partitioning.pattern={yyyy}/{MM}/{dd}/{batch}"
    )
    assert "run_started" in events and "entity_started" not in events


def test_full_refresh_fails_on_archived_inputs_until_d29(tmp_path: Path) -> None:
    put(tmp_path, "2026/10/24/a1b2/x.csv", "id\n1\n", 100)
    archive = "archive: {path: ./arch}"
    go(tmp_path, "incremental_mode: archive", archive)
    put(tmp_path, "2026/10/25/c3d4/y.csv", "id\n2\n", 200)
    with pytest.raises(FloeRunError, match=r"archived inputs keep their partition path only with D29 \(#884\): local://.*/arch/o/x__run-"):  # fmt: skip
        go(tmp_path, "incremental_mode: archive", archive, full_refresh=True)
