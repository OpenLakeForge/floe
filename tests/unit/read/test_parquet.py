"""Parquet reader: native types, projection, offsets across chunks and read errors."""

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from floe.errors import FloeIoError
from floe.read.parquet import read_parquet


@pytest.fixture
def typed(tmp_path: Path) -> Path:
    path = tmp_path / "typed.parquet"
    pl.DataFrame(
        {
            "id": ["r0", "r1", "r2", "r3", "r4"],
            "qty": [1, 2, None, 4, 5],
            "day": [date(2024, 1, 1)] * 5,
        }
    ).write_parquet(path)
    return path


def test_keeps_native_types(typed: Path) -> None:
    (chunk,) = read_parquet(typed)
    assert chunk.offset == 0
    assert chunk.df.schema == {"id": pl.String, "qty": pl.Int64, "day": pl.Date}
    assert chunk.df["qty"].to_list() == [1, 2, None, 4, 5]


def test_projection_reads_only_declared_columns(typed: Path) -> None:
    (chunk,) = read_parquet(typed, columns={"qty", "id", "absent"})
    assert chunk.df.columns == ["id", "qty"]


def test_no_declared_column_present_reads_everything(typed: Path) -> None:
    (chunk,) = read_parquet(typed, columns={"absent"})
    assert chunk.df.width == 3


def test_chunks_carry_whole_file_offsets(tmp_path: Path) -> None:
    path = tmp_path / "groups.parquet"
    pl.DataFrame({"n": range(6)}).write_parquet(path, row_group_size=2)
    chunks = list(read_parquet(path, chunk_rows=2))
    assert len(chunks) > 1
    assert sum(c.df.height for c in chunks) == 6
    assert [c.offset for c in chunks] == [
        sum(p.df.height for p in chunks[:i]) for i in range(len(chunks))
    ]


def test_empty_file_yields_one_empty_chunk(tmp_path: Path) -> None:
    path = tmp_path / "empty.parquet"
    pl.DataFrame(schema={"id": pl.String}).write_parquet(path)
    (chunk,) = read_parquet(path)
    assert chunk.df.columns == ["id"]
    assert chunk.df.height == 0


def test_corrupt_file_is_an_io_error(tmp_path: Path) -> None:
    bad = tmp_path / "bad.parquet"
    bad.write_bytes(b"not parquet")
    with pytest.raises(FloeIoError, match="parquet"):
        list(read_parquet(bad))


def test_missing_file_is_an_io_error(tmp_path: Path) -> None:
    with pytest.raises(FloeIoError, match="parquet"):
        list(read_parquet(tmp_path / "gone.parquet"))
