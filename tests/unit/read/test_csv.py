"""CSV reader: the conformance inputs, source options and the D3 ragged-row rule (#603)."""

import csv
from pathlib import Path

import polars as pl
import pytest

from floe.config.types import SourceOptions
from floe.errors import FloeConfigError, FloeIoError
from floe.read.csv import RAGGED_MESSAGE, read_csv

CONFORMANCE = Path(__file__).parents[3] / "conformance"
# (input, separator) of every CSV `in/` file in the recorded cases.
CASES = [
    ("casts/in/casts.csv", ","),
    ("casts_coerce/in/casts_coerce.csv", ","),
    ("casts_date/in/casts_date.csv", ","),
    ("casts_datetime/in/casts_datetime.csv", ","),
    ("mismatch/in/customers.csv", ";"),
    ("unique_pii/in/a.csv", ","),
    ("unique_pii/in/b.csv", ","),
    ("example/standalone/in/orders.csv", ","),
]


def opts(**kwargs: object) -> SourceOptions:
    return SourceOptions.model_validate(kwargs)


def write(tmp_path: Path, content: bytes) -> Path:
    path = tmp_path / "in.csv"
    path.write_bytes(content)
    return path


def read(
    path: Path, options: SourceOptions, chunk_rows: int = 1_000_000
) -> list[pl.DataFrame]:
    return [c.df for c in read_csv(path, options, chunk_rows=chunk_rows)]


@pytest.mark.parametrize(("name", "separator"), CASES)
def test_conformance_inputs_are_raw_strings(name: str, separator: str) -> None:
    path = CONFORMANCE / name
    with path.open(newline="", encoding="utf-8") as handle:
        header, *rows = csv.reader(handle, delimiter=separator)
    expected = [
        [v or None for v in row] for row in rows
    ]  # Rust default null_values: [""]
    [df] = read(path, opts(separator=separator))
    assert df.columns == header
    assert set(df.dtypes) == {pl.String}
    assert df.rows() == [tuple(r) for r in expected]


def test_quoted_separator_and_escaped_quote(tmp_path: Path) -> None:
    path = write(tmp_path, b'a;b\n"t3;semi";"+33 ""q"""\n')
    [df] = read(path, opts())
    assert df.rows() == [("t3;semi", '+33 "q"')]


def test_bom_crlf_and_untrimmed_values(tmp_path: Path) -> None:
    path = write(tmp_path, b"\xef\xbb\xbfa,b\r\n 1 ,x \r\n")
    [df] = read(path, opts(separator=","))
    assert df.columns == ["a", "b"]
    assert df.rows() == [(" 1 ", "x ")]


def test_null_values(tmp_path: Path) -> None:
    path = write(tmp_path, b"a,b\nNA,\n-,x\n")
    [df] = read(path, opts(separator=",", null_values=["NA", "-"]))
    assert df.rows() == [(None, None), (None, "x")]
    [df] = read(path, opts(separator=",", null_values=[]))
    assert df.rows() == [("NA", None), ("-", "x")]


def test_encoding(tmp_path: Path) -> None:
    path = write(tmp_path, b"a\n\xff\n")
    with pytest.raises(FloeIoError, match="csv read failed: .*utf-8"):
        read(path, opts(separator=","))
    [df] = read(path, opts(separator=",", encoding="lossy_utf8"))
    assert df.rows() == [("�",)]


def test_headless_uses_declared_names_then_extra(tmp_path: Path) -> None:
    path = write(tmp_path, b"1,2,3\n4,5,6\n")
    [chunk] = read_csv(path, opts(separator=",", header=False), declared=["x", "y"])
    assert chunk.df.columns == ["x", "y", "extra_column_3"]
    assert chunk.df.height == 2


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"separator": "ab"}, 'separator must be a single byte, got "ab"'),
        ({"separator": "é"}, 'separator must be a single byte, got "é"'),
        ({"encoding": "latin1"}, "unsupported encoding: latin1"),
    ],
)
def test_invalid_options(tmp_path: Path, kwargs: dict[str, str], message: str) -> None:
    with pytest.raises(FloeConfigError) as err:
        read(write(tmp_path, b"a\n1\n"), opts(**kwargs))
    assert str(err.value) == message


def test_empty_file_and_header_only(tmp_path: Path) -> None:
    with pytest.raises(FloeIoError, match="csv header read failed"):
        read(write(tmp_path, b""), opts())
    [df] = read(write(tmp_path, b"a;b\n"), opts())
    assert df.columns == ["a", "b"]
    assert df.height == 0


def test_chunks_carry_whole_file_offsets(tmp_path: Path) -> None:
    path = write(tmp_path, b"a\n" + b"".join(b"%d\n" % i for i in range(25)))
    chunks = list(read_csv(path, opts(), chunk_rows=10))
    assert [(c.offset, c.df.height) for c in chunks] == [(0, 10), (10, 10), (20, 5)]
    assert not any(c.ragged.any() for c in chunks)


def test_d3_ragged_row_rejects_only_that_row(tmp_path: Path) -> None:
    # Rust 0.7 fails the whole file with csv_read_error (#603); only row 12 is flagged here.
    body = b"".join(b"%d,x\n" % i for i in range(12)) + b'12,x,"y,z"\n13,x\n'
    path = write(tmp_path, b"a,b\n" + body)
    chunks = list(read_csv(path, opts(separator=","), chunk_rows=5))
    assert [(c.offset, c.df.height) for c in chunks] == [(0, 5), (5, 5), (10, 4)]
    flagged = [c.offset + i for c in chunks for i, r in enumerate(c.ragged) if r]
    assert flagged == [12]
    assert chunks[2].df.rows()[2:] == [("12", "x"), ("13", "x")]
    assert RAGGED_MESSAGE == "found more fields than defined in 'Schema'"
