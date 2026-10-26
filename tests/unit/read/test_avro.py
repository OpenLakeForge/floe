"""Avro reader: native types from the conformance input, projection, chunking and read errors."""

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from floe.errors import FloeIoError
from floe.read.avro import read_avro

INPUT = Path(__file__).parents[3] / "conformance/avro_read/in/avro_read.avro"


def test_conformance_input_keeps_native_types() -> None:
    (chunk,) = read_avro(INPUT)
    assert chunk.offset == 0
    assert chunk.df.schema == {
        "id": pl.String,
        "qty": pl.Int64,
        "price": pl.Float64,
        "active": pl.Boolean,
        "note": pl.String,
    }
    assert chunk.df.rows() == [
        ("r0", 1, 1.5, True, "plain"),
        ("r1", 2, 2.0, False, None),
        ("r2", None, None, None, "x"),
    ]


def test_date_and_nested_columns_stay_native(tmp_path: Path) -> None:
    path = tmp_path / "typed.avro"
    pl.DataFrame({"day": [date(2024, 2, 29)], "tags": [["a", "b"]]}).write_avro(path)
    (chunk,) = read_avro(path)
    assert chunk.df.rows() == [(date(2024, 2, 29), ["a", "b"])]


def test_projection_keeps_only_declared_columns() -> None:
    (chunk,) = read_avro(INPUT, columns={"qty", "id", "absent"})
    assert chunk.df.columns == ["id", "qty"]


def test_no_declared_column_present_keeps_everything() -> None:
    (chunk,) = read_avro(INPUT, columns={"absent"})
    assert chunk.df.width == 5


def test_chunks_carry_whole_file_offsets() -> None:
    chunks = list(read_avro(INPUT, chunk_rows=2))
    assert [(c.offset, c.df.height) for c in chunks] == [(0, 2), (2, 1)]


def test_not_avro_is_an_io_error(tmp_path: Path) -> None:
    bad = tmp_path / "bad.avro"
    bad.write_bytes(b"not avro")
    with pytest.raises(FloeIoError, match="avro read failed"):
        list(read_avro(bad))
