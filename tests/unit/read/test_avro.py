"""Avro reader: the conformance input as raw strings, chunking and read errors."""

from pathlib import Path

import polars as pl
import pytest

from floe.errors import FloeIoError
from floe.read.avro import read_avro

INPUT = Path(__file__).parents[3] / "conformance/avro_read/in/avro_read.avro"


def test_conformance_input_is_raw_strings() -> None:
    (chunk,) = read_avro(INPUT)
    assert chunk.offset == 0
    assert chunk.df.rows() == [
        ("r0", "1", "1.5", "true", "plain"),
        ("r1", "2", "2.0", "false", None),
        ("r2", None, None, None, "x"),
    ]
    assert set(chunk.df.dtypes) == {pl.String}


def test_chunks_carry_whole_file_offsets() -> None:
    chunks = list(read_avro(INPUT, chunk_rows=2))
    assert [(c.offset, c.df.height) for c in chunks] == [(0, 2), (2, 1)]


def test_not_avro_is_an_io_error(tmp_path: Path) -> None:
    bad = tmp_path / "bad.avro"
    bad.write_bytes(b"not avro")
    with pytest.raises(FloeIoError, match="avro read failed"):
        list(read_avro(bad))
