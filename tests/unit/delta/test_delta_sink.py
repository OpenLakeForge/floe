"""`run_entity` with a Delta accepted sink on local storage."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
# ^ deltalake and pyarrow ship no type stubs

import json
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from floe.config.parse import parse_config
from floe.config.storage import StorageDefinition
from floe.delta.sink import storage_options
from floe.errors import FloeRunError
from floe.run.entity import run_entity

CONFIG = """\
version: "0.3"
domains:
  - name: d
entities:
  - name: orders
    domain: d
    source: {{format: csv, path: ./in, options: {{header: true, separator: ","}}}}
    sink:
      write_mode: {mode}
      accepted: {{format: delta, path: ./out/accepted}}
    policy: {{severity: reject}}
    schema:
      {extra}
      columns:
{columns}
report: {{path: ./report}}
"""
COLUMNS = ("id", "int64"), ("name", "string")


def run(tmp_path: Path, rows: str, mode: str = "overwrite", columns: Any = COLUMNS, extra: str = "") -> dict[str, Any]:  # fmt: skip
    (tmp_path / "in").mkdir(exist_ok=True)
    for old in (tmp_path / "in").glob("*.csv"):
        old.unlink()
    (tmp_path / "in" / "orders.csv").write_text(rows)
    cols = "\n".join(f"        - {{name: {n}, type: {t}}}" for n, t in columns)
    (tmp_path / "config.yml").write_text(CONFIG.format(mode=mode, columns=cols, extra=extra))  # fmt: skip
    entity = parse_config(tmp_path / "config.yml").entities[0]
    return run_entity(entity, tmp_path, "0.3").report


def table(tmp_path: Path) -> pl.DataFrame:
    return pl.DataFrame(pa.table(DeltaTable(str(tmp_path / "out" / "accepted")).to_pyarrow_table()))  # fmt: skip


def test_two_overwrites_leave_only_the_second_batch(tmp_path: Path) -> None:
    run(tmp_path, "id,name\n1,a\n2,b\n")
    report = run(tmp_path, "id,name\n3,c\n")
    assert table(tmp_path)["id"].to_list() == [3]
    assert report["accepted_output"]["table_version"] == 1
    assert report["accepted_output"]["files_written"] == 1
    assert report["accepted_output"]["accepted_rows"] == 1


def test_append_adds_a_batch_and_reports_only_its_files(tmp_path: Path) -> None:
    run(tmp_path, "id,name\n1,a\n", "append")
    report = run(tmp_path, "id,name\n2,b\n", "append")
    assert sorted(table(tmp_path)["id"].to_list()) == [1, 2]
    assert report["accepted_output"]["table_version"] == 1
    assert report["accepted_output"]["files_written"] == 1


def test_uint8_value_above_the_signed_range_round_trips(tmp_path: Path) -> None:
    run(tmp_path, "id,name\n200,a\n", columns=(("id", "uint8"), ("name", "string")))
    assert table(tmp_path)["id"].to_list() == [200]


def test_uint64_is_refused(tmp_path: Path) -> None:
    with pytest.raises(FloeRunError, match="cannot store the uint64 column id") as e:
        run(tmp_path, "id,name\n1,a\n", columns=(("id", "uint64"), ("name", "string")))  # fmt: skip
    assert e.value.code == "run_not_supported"


def test_append_with_a_type_mismatch_is_refused(tmp_path: Path) -> None:
    run(tmp_path, "id,name\n1,a\n", "append")
    with pytest.raises(FloeRunError, match="column id is int64 in the table, string in the batch") as e:  # fmt: skip
        run(tmp_path, "id,name\nx,b\n", "append", columns=(("id", "string"), ("name", "string")))  # fmt: skip
    assert e.value.code == "run_schema_evolution"
    assert table(tmp_path)["id"].to_list() == [1]


def test_table_with_an_unsupported_writer_feature_is_refused_untouched(tmp_path: Path) -> None:  # fmt: skip
    run(tmp_path, "id,name\n1,a\n")
    first = tmp_path / "out" / "accepted" / "_delta_log" / "00000000000000000000.json"
    lines = [json.loads(l) for l in first.read_text().splitlines()]
    for line in lines:
        if "protocol" in line:
            line["protocol"] = {"minReaderVersion": 1, "minWriterVersion": 7, "writerFeatures": ["identityColumns"]}  # fmt: skip
    first.write_text("\n".join(json.dumps(l) for l in lines) + "\n")
    with pytest.raises(FloeRunError, match=r"table feature delta-rs cannot write .*IdentityColumns") as e:  # fmt: skip
        run(tmp_path, "id,name\n2,b\n")
    assert e.value.code == "run_not_supported"
    assert DeltaTable(str(tmp_path / "out" / "accepted")).version() == 0


def test_table_created_by_another_writer_is_appended_to(tmp_path: Path) -> None:
    write_deltalake(str(tmp_path / "out" / "accepted"), pa.table({"id": pa.array([9], pa.int64()), "name": ["z"]}))  # fmt: skip
    run(tmp_path, "id,name\n1,a\n", "append")
    assert sorted(table(tmp_path)["id"].to_list()) == [1, 9]


def test_append_with_unique_keys_is_refused_before_anything_is_read(tmp_path: Path) -> None:  # fmt: skip
    with pytest.raises(FloeRunError, match="unique keys on a delta append are not supported yet") as e:  # fmt: skip
        run(tmp_path, "id,name\n1,a\n", "append", extra="unique_keys: [[id]]")
    assert e.value.code == "run_not_supported"
    assert not (tmp_path / "out").exists()


def test_s3_storage_options_follow_the_storage_definition() -> None:
    s3 = StorageDefinition(name="s", type="s3", bucket="b", region="eu-west-1", account=None, container=None, prefix=None, endpoint="http://localhost:9000", path_style_access=False)  # fmt: skip
    assert storage_options(s3) == {"AWS_REGION": "eu-west-1", "AWS_ENDPOINT_URL": "http://localhost:9000", "AWS_ALLOW_HTTP": "true", "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "true"}  # fmt: skip
    assert storage_options(None) == {}


def test_repeated_writes_with_datetime_and_date_columns_match_the_table(tmp_path: Path) -> None:  # fmt: skip
    columns = (("id", "int64"), ("at", "datetime"), ("day", "date"))
    run(tmp_path, "id,at,day\n1,2024-01-02T03:04:00,2024-01-02\n", columns=columns)
    run(tmp_path, "id,at,day\n2,2024-01-03T03:04:00,2024-01-03\n", columns=columns)
    run(tmp_path, "id,at,day\n3,2024-01-04T03:04:00,2024-01-04\n", "append", columns=columns)  # fmt: skip
    assert sorted(table(tmp_path)["id"].to_list()) == [2, 3]
