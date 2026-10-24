"""ORC reader: the conformance input as raw strings, stripes and read errors."""
# pyarrow ships no type stubs, so strict pyright cannot see its types.
# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false, reportUnknownArgumentType=false

from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.orc as paorc
import pytest

from floe.errors import FloeIoError
from floe.read.orc import read_orc

INPUT = Path(__file__).parents[3] / "conformance/orc_read/in/orc_read.orc"


def test_conformance_input_is_raw_strings() -> None:
    (chunk,) = read_orc(INPUT)
    assert chunk.offset == 0
    assert chunk.df.rows() == [
        ("r0", "1", "1.5", "true", "plain"),
        ("r1", "2", "2.0", "false", None),
        ("r2", None, None, None, "x"),
    ]
    assert set(chunk.df.dtypes) == {pl.String}


def test_one_chunk_per_stripe_with_whole_file_offsets(tmp_path: Path) -> None:
    path = tmp_path / "stripes.orc"
    table = pa.table({"id": [str(i) for i in range(6)]})
    paorc.write_table(table, path, stripe_size=1, batch_size=2)
    chunks = list(read_orc(path))
    assert len(chunks) > 1
    assert sum(c.df.height for c in chunks) == 6
    offsets = [0]
    for c in chunks[:-1]:
        offsets.append(offsets[-1] + c.df.height)
    assert [c.offset for c in chunks] == offsets


def test_empty_file_yields_one_empty_chunk(tmp_path: Path) -> None:
    path = tmp_path / "empty.orc"
    paorc.write_table(pa.table({"id": pa.array([], pa.string())}), path)
    (chunk,) = read_orc(path)
    assert chunk.df.columns == ["id"]
    assert chunk.df.height == 0


def test_not_orc_is_an_io_error(tmp_path: Path) -> None:
    bad = tmp_path / "bad.orc"
    bad.write_bytes(b"not orc")
    with pytest.raises(FloeIoError, match="orc read failed"):
        list(read_orc(bad))
