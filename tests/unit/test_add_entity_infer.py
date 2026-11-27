"""`infer_entity` (Rust: `infer_entity_from_input` in `crates/floe-core/src/add_entity.rs`).

Expected types were recorded from the Rust 0.7 binary (`floe add-entity --dry-run`).
"""

import datetime as dt
import decimal
import sys
from pathlib import Path
from typing import cast
from zipfile import ZipFile

import polars as pl
import pytest

from floe.add_entity import infer_entity
from floe.errors import FloeConfigError, FloeIoError


def columns(entity: dict[object, object]) -> list[tuple[object, ...]]:
    schema = cast("dict[str, list[dict[str, object]]]", entity["schema"])
    return [(c["name"], c["type"], c["nullable"]) for c in schema["columns"]]


def test_csv_sniffs_the_separator_and_infers_types(tmp_path: Path) -> None:
    path = tmp_path / "cust.csv"
    path.write_text(
        "id;name;active;amt;d\n1;Alice;true;1.5;2024-01-01\n2;;false;2;2024-01-02\n"
    )
    entity, fmt, _ = infer_entity(str(path), None, None, None, None, None)
    assert fmt == "csv"
    assert entity["name"] == "cust"
    assert entity["source"] == {
        "format": "csv",
        "path": str(path),
        "options": {"separator": ";"},
    }
    assert columns(entity) == [
        ("id", "number", False),
        ("name", "string", True),
        ("active", "boolean", False),
        ("amt", "number", False),
        ("d", "string", False),  # Rust reads CSV without date parsing
    ]


def test_csv_header_only_columns_are_nullable(tmp_path: Path) -> None:
    path = tmp_path / "a.csv"
    path.write_text("x|y\n")
    entity, _, _ = infer_entity(str(path), None, None, None, None, None)
    assert columns(entity) == [("x", "string", True), ("y", "string", True)]


def test_csv_quoted_empty_value_is_null_as_in_rust(tmp_path: Path) -> None:
    path = tmp_path / "a.csv"
    path.write_text('id,name\n1,Alice\n2,""\n')
    entity, _, _ = infer_entity(str(path), None, None, None, None, None)
    assert columns(entity) == [("id", "number", False), ("name", "string", True)]


def test_csv_path_with_glob_characters_is_read_literally(tmp_path: Path) -> None:
    path = tmp_path / "orders[1].csv"
    path.write_text("id\n1\n")
    (tmp_path / "orders1.csv").write_text("other\nx\n")
    entity, _, _ = infer_entity(str(path), None, None, None, None, None)
    assert columns(entity) == [("id", "number", False)]


def write_first_sheet(path: Path, rows: str) -> None:
    book = Path(__file__).parents[2] / "conformance/xlsx/in/orders.xlsx"
    worksheet = (
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f"<sheetData>{rows}</sheetData></worksheet>"
    )
    with ZipFile(book) as source, ZipFile(path, "w") as dest:
        for name in source.namelist():
            dest.writestr(
                name,
                worksheet if name == "xl/worksheets/sheet1.xml" else source.read(name),
            )


def test_xlsx_infers_raw_string_columns_from_the_first_sheet(tmp_path: Path) -> None:
    path = tmp_path / "orders.xlsx"
    write_first_sheet(
        path,
        '<row r="1"><c r="A1" t="inlineStr"><is><t>sku</t></is></c>'
        '<c r="B1" t="inlineStr"><is><t>quantity</t></is></c></row>'
        '<row r="2"><c r="A2" t="inlineStr"><is><t>001</t></is></c><c r="B2"><v>12</v></c></row>'
        '<row r="3"><c r="A3" t="inlineStr"><is><t>002</t></is></c></row>',
    )
    entity, fmt, _ = infer_entity(str(path), None, None, None, None, None)
    assert fmt == "xlsx"
    assert entity["source"] == {"format": "xlsx", "path": str(path)}
    assert columns(entity) == [("sku", "string", False), ("quantity", "string", True)]


@pytest.mark.parametrize(
    "invalid", [False, True], ids=["empty-sheet", "invalid-workbook"]
)
def test_xlsx_unreadable_workbooks_raise_typed_errors(
    tmp_path: Path, invalid: bool
) -> None:
    path = tmp_path / "orders.xlsx"
    if invalid:
        path.write_bytes(b"not an Excel workbook")
    else:
        write_first_sheet(path, "")
    with pytest.raises(FloeIoError) as error:
        infer_entity(str(path), None, None, None, None, None)
    assert (error.value.code, error.value.rule) == ("io_read_error", "xlsx_read_error")


def test_xlsx_missing_extra_reports_install_hint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "orders.xlsx"
    path.touch()
    monkeypatch.setitem(sys.modules, "fastexcel", None)
    monkeypatch.delitem(sys.modules, "floe.read.xlsx", raising=False)
    with pytest.raises(FloeConfigError) as error:
        infer_entity(str(path), None, None, None, None, None)
    assert error.value.code == "config_not_supported"
    assert "pip install 'floe-python[xlsx]'" in str(error.value)


def test_parquet_path_with_glob_characters_is_read_literally(tmp_path: Path) -> None:
    path = tmp_path / "p[1].parquet"
    pl.DataFrame({"a": [1]}).write_parquet(path)
    pl.DataFrame({"other": ["x"]}).write_parquet(tmp_path / "p1.parquet")
    entity, _, _ = infer_entity(str(path), None, None, None, None, None)
    assert columns(entity) == [("a", "number", True)]


def test_parquet_maps_types_as_rust_does(tmp_path: Path) -> None:
    path = tmp_path / "emp.parquet"
    pl.DataFrame(
        {
            "i8": pl.Series([1], dtype=pl.Int8),
            "u32": pl.Series([1], dtype=pl.UInt32),
            "f": [1.5],
            "dec": pl.Series([decimal.Decimal("1.25")], dtype=pl.Decimal(10, 2)),
            "b": [True],
            "s": ["x"],
            "d": [dt.date(2024, 1, 1)],
            "ts": [dt.datetime(2024, 1, 1)],
            "t": [dt.time(1, 2)],
            "l": [[1]],
            "st": [{"a": 1}],
            "dur": [dt.timedelta(1)],
            "cat": pl.Series(["a"], dtype=pl.Categorical),
            "nul": pl.Series([None], dtype=pl.Null),
        }
    ).write_parquet(path)
    entity, fmt, _ = infer_entity(str(path), None, None, None, None, None)
    assert fmt == "parquet"
    assert entity["source"] == {"format": "parquet", "path": str(path)}
    types = ["number"] * 4 + [
        "boolean",
        "string",
        "date",
        "datetime",
        "string",
    ]  # D27b: Rust writes `time`
    assert columns(entity) == [
        (n, t, True)
        for n, t in zip(
            (
                "i8",
                "u32",
                "f",
                "dec",
                "b",
                "s",
                "d",
                "ts",
                "t",
                "l",
                "st",
                "dur",
                "cat",
                "nul",
            ),
            types + ["string"] * 5,
            strict=True,
        )
    ]


def test_entity_defaults_and_overrides(tmp_path: Path) -> None:
    path = tmp_path / "strange_name.data"
    path.write_text("id,name\n1,Alice\n")
    entity, fmt, _ = infer_entity(str(path), "csv", "customers", "sales", None, None)
    assert fmt == "csv"
    assert {k: v for k, v in entity.items() if k not in ("source", "schema")} == {
        "name": "customers",
        "domain": "sales",
        "sink": {
            "accepted": {"format": "parquet", "path": "out/accepted/customers/"},
            "rejected": {"format": "csv", "path": "out/rejected/customers/"},
        },
        "policy": {"severity": "reject"},
    }
    schema = entity["schema"]
    assert isinstance(schema, dict)
    assert schema["mismatch"] == {
        "missing_columns": "reject_file",
        "extra_columns": "ignore",
    }


def test_source_reference_replaces_format_and_path(tmp_path: Path) -> None:
    path = tmp_path / "orders.csv"
    path.write_text("id\n1\n")
    entity, _, _ = infer_entity(str(path), None, None, None, "crm", "crm_orders")
    assert entity["source"] == {"ref": "crm", "resource": "crm_orders"}


def test_file_uri_names_the_entity_and_persists_the_local_path(tmp_path: Path) -> None:
    path = tmp_path / "Order Events-2024.csv"
    path.write_text("id\n1\n")
    entity, _, _ = infer_entity(path.as_uri(), None, None, None, None, None)
    assert entity["name"] == "order_events_2024"
    source = entity["source"]
    assert isinstance(source, dict)
    assert source["path"] == str(path)


@pytest.mark.parametrize(
    ("input", "fmt", "name", "error", "message"),
    [
        (
            "s3://b/k/orders.csv?x=1",
            None,
            None,
            FloeConfigError,
            "remote URI inference is not supported yet for add-entity input: s3://b/k/orders.csv?x=1",
        ),
        (
            "___.csv",
            None,
            None,
            FloeConfigError,
            "failed to derive entity name from input: ___.csv",
        ),
        ("a.csv", None, "  ", FloeConfigError, "entity name must not be empty"),
        (
            "payload.unknown",
            None,
            None,
            FloeConfigError,
            (
                "could not infer add-entity format from input payload.unknown "
                "(supported extensions: .csv, .json, .parquet, .xlsx); use --format"
            ),
        ),
        (
            "missing.csv",
            None,
            None,
            FloeIoError,
            "failed to access input at {cwd}/missing.csv: No such file or directory (os error 2)",
        ),
        (
            "dir",
            None,
            None,
            FloeConfigError,
            "add-entity expects a file input for schema inference, got directory: {cwd}/dir",
        ),
    ],
)
def test_input_errors_match_rust(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    input: str,
    fmt: str | None,
    name: str | None,
    error: type[Exception],
    message: str,
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "a.csv").write_text("x\n1\n")
    (tmp_path / "payload.unknown").write_text("x\n1\n")
    (tmp_path / "___.csv").write_text("x\n1\n")
    (tmp_path / "dir").mkdir()
    with pytest.raises(error) as e:
        infer_entity(input, fmt, name, None, None, None)
    assert str(e.value) == message.format(cwd=tmp_path)


def test_errors_carry_codes(tmp_path: Path) -> None:
    empty = tmp_path / "empty.json"
    empty.write_text("")
    with pytest.raises(FloeConfigError) as e:
        infer_entity(str(empty), None, None, None, None, None)
    assert e.value.code == "config_invalid_value"
    csv = tmp_path / "x.csv"
    csv.write_text("a\n1\n")
    with pytest.raises(FloeConfigError) as e:
        infer_entity(str(csv), "xml", None, None, None, None)
    assert e.value.code == "config_invalid_value"
    with pytest.raises(FloeIoError) as io:
        infer_entity(str(tmp_path / "gone.csv"), None, None, None, None, None)
    assert io.value.code == "io_read_error"


@pytest.mark.parametrize(
    "text",
    [
        "0o10",
        "+5",
        "0o7",
        "0x1F",
        "1e3",
        "007",
        "true",
        "null",
        "~",
        ".inf",
        "12",
        "a: b",
    ],
)
def test_render_round_trips_strings_through_floe_yaml(text: str) -> None:
    from floe.add_entity import _render  # pyright: ignore[reportPrivateUsage] -- the emitter under test
    from floe.config.yaml import parse_yaml

    assert parse_yaml(_render({"values": [text]})) == [{"values": [text]}]
