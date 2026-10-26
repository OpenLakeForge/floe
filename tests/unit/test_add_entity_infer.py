"""`infer_entity` (Rust: `infer_entity_from_input` in `crates/floe-core/src/add_entity.rs`).

Expected types were recorded from the Rust 0.7 binary (`floe add-entity --dry-run`).
"""

import datetime as dt
import decimal
from pathlib import Path
from typing import cast

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
                "(supported extensions: .csv, .json, .parquet); use --format"
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
