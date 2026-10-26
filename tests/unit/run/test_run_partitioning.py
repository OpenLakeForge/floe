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


def test_d29_archive_keeps_the_partition_path_so_a_full_refresh_rederives_the_date(tmp_path: Path) -> None:  # fmt: skip
    put(tmp_path, "2026/10/24/a1b2/x.csv", "id\n1\n", 100)
    archive = "archive: {path: ./arch}"
    go(tmp_path, "incremental_mode: archive", archive)
    [archived] = (tmp_path / "arch/o").rglob("*.csv")
    rel = archived.relative_to(tmp_path / "arch/o").as_posix()
    assert rel.startswith("2026/10/24/a1b2/x__run-") and rel.endswith(".csv")
    assert report(tmp_path)["files"][0]["output"]["archived_path"] == str(archived)
    put(tmp_path, "2026/10/25/c3d4/y.csv", "id\n2\n", 200)
    go(tmp_path, "incremental_mode: archive", archive, full_refresh=True)
    got = pl.read_parquet(tmp_path / "out/accepted")
    assert got.rows() == [(1, date(2026, 10, 24), "a1b2"), (2, date(2026, 10, 25), "c3d4")]  # fmt: skip


def test_d29_without_partitioning_the_archive_stays_flat(tmp_path: Path) -> None:
    (tmp_path / "in").mkdir()
    (tmp_path / "in/x.csv").write_text("id\n1\n")
    path = tmp_path / "config.yml"
    text = CONFIG.format(mode="incremental_mode: archive", archive="archive: {path: ./arch}")  # fmt: skip
    path.write_text("\n".join(line for line in text.splitlines() if "partitioning:" not in line).replace("primary_key: [id, ingest_date]", "primary_key: [id]"))  # fmt: skip
    run(path, parse_config(path, {}))
    assert [p.parent for p in (tmp_path / "arch").rglob("*.csv")] == [
        tmp_path / "arch/o"
    ]


def test_derived_columns_never_name_headerless_csv_fields(tmp_path: Path) -> None:
    """An extra field of a headerless file stays an extra column; it is not taken for `ingest_date`."""
    put(tmp_path, "2026/10/24/a1b2/x.csv", "1,unexpected\n", 100)
    path = tmp_path / "config.yml"
    text = CONFIG.format(mode="", archive="").replace("header: true", "header: false")
    path.write_text(text.replace("      primary_key:", "      mismatch: {extra_columns: reject_file}\n      primary_key:"))  # fmt: skip
    run(path, parse_config(path, {}))
    (f,) = report(tmp_path)["files"]
    assert (f["status"], f["mismatch"]["extra_columns"]) == (
        "rejected",
        ["extra_column_2"],
    )


XML = """\
version: "0.3"
entities:
  - name: o
    source:
      format: xml
      path: ./in
      options: {row_tag: row}
      partitioning: {pattern: "{yyyy}/{MM}/{dd}", date_column: ingest.date}
    sink: {accepted: {format: parquet, path: ./out/accepted}, rejected: {format: csv, path: ./out/rejected}}
    policy: {severity: reject}
    schema: {columns: [{name: id, type: int64}]}
report: {path: ./report}
"""


def test_an_xml_derived_column_is_declared_whatever_its_name(tmp_path: Path) -> None:
    """A derived name is an output column, not an XML selector: `ingest.date` is not taken for a nested one."""
    put(tmp_path, "2026/10/24/x.xml", "<root><row><id>1</id></row></root>", 100)
    path = tmp_path / "config.yml"
    path.write_text(XML)
    run(path, parse_config(path, {}))
    assert pl.read_parquet(tmp_path / "out/accepted").rows() == [
        (1, date(2026, 10, 24))
    ]
