"""`run_entity`: statuses, outputs and file-level rejections for a local CSV entity."""

from pathlib import Path
from typing import Any

import polars as pl
import pytest

from floe.config.parse import parse_config
from floe.errors import FloeRunError
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
      options: {{header: true, separator: ","}}
    sink:
      accepted: {{format: parquet, path: ./out/accepted}}
      rejected: {{format: csv, path: ./out/rejected}}
    policy: {{severity: {severity}}}
    schema:
      {mismatch}
      columns:
        - {{name: id, type: int64, nullable: false}}
        - {{name: name, type: string}}
report: {{path: ./report}}
"""


def run(
    tmp_path: Path, severity: str, rows: str, mismatch: str = ""
) -> tuple[dict[str, Any], bool]:
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "orders.csv").write_text(rows)
    (tmp_path / "config.yml").write_text(
        CONFIG.format(severity=severity, mismatch=mismatch)
    )
    entity = parse_config(tmp_path / "config.yml").entities[0]
    result = run_entity(entity, tmp_path, "0.3")
    return result.report, result.aborted


def accepted(tmp_path: Path) -> pl.DataFrame:
    return pl.read_parquet(tmp_path / "out" / "accepted")


def test_clean_file_is_all_accepted(tmp_path: Path) -> None:
    report, aborted = run(tmp_path, "reject", "id,name\n1,a\n2,b\n")
    assert not aborted
    assert report["results"]["accepted_total"] == 2
    assert accepted(tmp_path)["id"].to_list() == [1, 2]
    assert report["files"][0]["status"] == "success"


def test_reject_splits_rows_and_numbers_them_by_file_position(tmp_path: Path) -> None:
    report, aborted = run(tmp_path, "reject", "id,name\n1,a\nx,b\n3,c\n,d\n")
    assert not aborted
    assert accepted(tmp_path)["id"].to_list() == [1, 3]
    (part,) = (tmp_path / "out" / "rejected").glob("*.csv")
    rejected = pl.read_csv(part)
    assert rejected["__floe_row_index"].to_list() == [1, 3]
    assert report["files"][0]["status"] == "rejected"
    assert report["files"][0]["output"]["rejected_path"] == str(part)


def test_warn_keeps_every_row_and_writes_an_error_report(tmp_path: Path) -> None:
    report, _ = run(tmp_path, "warn", "id,name\n1,a\nx,b\n")
    assert accepted(tmp_path)["id"].to_list() == [1, None]
    f = report["files"][0]
    assert f["status"] == "success" and f["validation"]["warnings"] == 2
    errors = Path(f["output"]["errors_path"])
    assert errors.name == "orders_reject_errors.json"
    assert errors.read_text().startswith('[{"row_index":1,"errors":[')


def test_abort_copies_the_raw_file_and_writes_the_accepted_schema(
    tmp_path: Path,
) -> None:
    report, aborted = run(tmp_path, "abort", "id,name\n1,a\nx,b\n")
    assert aborted
    assert report["files"][0]["status"] == "aborted"
    assert (
        tmp_path / "out" / "rejected" / "orders.csv"
    ).read_text() == "id,name\n1,a\nx,b\n"
    frame = accepted(tmp_path)
    assert frame.height == 0 and frame.columns == ["id", "name"]


def test_abort_with_a_cast_error_leaves_not_null_out_of_the_errors(
    tmp_path: Path,
) -> None:
    report, _ = run(tmp_path, "abort", "id,name\nx,a\n,b\n")
    rules = [r["rule"] for r in report["files"][0]["validation"]["rules"]]
    assert rules == ["cast_error"]


def test_mismatch_reject_file_skips_the_rows(tmp_path: Path) -> None:
    report, _ = run(
        tmp_path, "reject", "id\n1\n", "mismatch: {missing_columns: reject_file}"
    )
    f = report["files"][0]
    assert f["status"] == "rejected" and f["row_count"] == 0
    assert f["mismatch"]["error"]["rule"] == "schema_mismatch"
    assert (tmp_path / "out" / "rejected" / "orders.csv").exists()
    assert accepted(tmp_path).height == 0


def test_incremental_file_is_refused(tmp_path: Path) -> None:
    (tmp_path / "config.yml").write_text(
        CONFIG.format(severity="warn", mismatch="").replace(
            "    source:", "    incremental_mode: file\n    source:"
        )
    )
    entity = parse_config(tmp_path / "config.yml").entities[0]
    with pytest.raises(FloeRunError, match="incremental_mode file"):
        run_entity(entity, tmp_path, "0.3")


def test_coerce_mode_runs_a_multi_row_file(tmp_path: Path) -> None:
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "orders.csv").write_text("id,name\n1,a\n2,b\n")
    config = CONFIG.format(severity="reject", mismatch="").replace(
        "    source:\n", "    source:\n      cast_mode: coerce\n"
    )
    (tmp_path / "config.yml").write_text(config)
    entity = parse_config(tmp_path / "config.yml").entities[0]
    run_entity(entity, tmp_path, "0.3")
    assert accepted(tmp_path)["id"].to_list() == [1, 2]


def test_tsv_source_uses_the_csv_reader(tmp_path: Path) -> None:
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "orders.tsv").write_text("id\tname\n1\ta\n")
    config = CONFIG.format(severity="reject", mismatch="")
    config = config.replace(
        "format: csv\n      path: ./in", "format: tsv\n      path: ./in"
    )
    config = config.replace(
        'options: {header: true, separator: ","}', "options: {header: true}"
    )
    (tmp_path / "config.yml").write_text(config)
    entity = parse_config(tmp_path / "config.yml").entities[0]
    run_entity(entity, tmp_path, "0.3")
    assert accepted(tmp_path).to_dicts() == [{"id": 1, "name": "a"}]
