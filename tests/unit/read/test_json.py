"""JSON reader: array and ndjson modes, selectors, non-scalar handling, Rust's errors."""

from pathlib import Path

import polars as pl
import pytest

from floe.config.types import ColumnConfig, SourceOptions
from floe.errors import FloeIoError
from floe.read.json import JsonChunk, parse_selector, read_json


def column(name: str, source: str | None = None) -> ColumnConfig:
    return ColumnConfig.model_validate(
        {
            **dict.fromkeys(("nullable", "unique", "width", "trim")),
            "name": name,
            "source": source,
            "column_type": "string",
        }
    )


def read(
    tmp_path: Path,
    content: str,
    *columns: ColumnConfig,
    mode: str | None = None,
    cast_mode: str = "strict",
    chunk_rows: int = 1000,
) -> list[JsonChunk]:
    path = tmp_path / "in.json"
    path.write_text(content)
    options = SourceOptions.model_validate({"json_mode": mode})
    return list(read_json(path, options, columns, cast_mode, chunk_rows))


ARRAY = '[{"id":1,"a":{"b":[10,{"c":"x"}]},"ok":true,"n":null,"f":1.5},{"id":"2"}]'


def test_array_selectors(tmp_path: Path) -> None:
    columns = [
        column("id"),
        column("c", "a.b[1].c"),
        column("b0", "a.b[0]"),
        column("ok"),
        column("n"),
        column("f"),
        column("gone", "a.b[5]"),
    ]
    (chunk,) = read(tmp_path, ARRAY, *columns)
    assert chunk.df.columns == ["id", "a.b[1].c", "a.b[0]", "ok", "n", "f", "a.b[5]"]
    assert chunk.df.rows() == [
        ("1", "x", "10", "true", None, "1.5", None),
        ("2", None, None, None, None, None, None),
    ]
    assert set(chunk.df.schema.values()) == {pl.String}


def test_ndjson_skips_blank_lines(tmp_path: Path) -> None:
    content = '{"a":1}\n\n  \n{"a":2,"b":3}\n'
    (chunk,) = read(tmp_path, content, column("a"), column("b"), mode="ndjson")
    assert chunk.df.rows() == [("1", None), ("2", "3")]


def test_non_scalar_strict_errors_coerce_stringifies(tmp_path: Path) -> None:
    content = '[{"a":{"x":[1,"é"]}},{"a":[]}]'
    with pytest.raises(FloeIoError, match="non-scalar value for selector a at index 0"):
        read(tmp_path, content, column("a"))
    (chunk,) = read(tmp_path, content, column("a"), cast_mode="coerce")
    assert chunk.df.rows() == [('{"x":[1,"é"]}',), ("[]",)]


@pytest.mark.parametrize(
    ("content", "mode", "message"),
    [
        ("{}", None, "expected json array at root"),
        ("[1]", None, "expected json object at index 0"),
        ("[", None, "json parse error: "),
        ('{"a":1}\n[2]', "ndjson", "expected json object at line 2"),
        ('{"a":1}\n{', "ndjson", "json parse error at line 2: "),
        ('[{"a":NaN}]', None, "json parse error: "),
        ('[{"a":1e400}]', None, "json parse error: number out of range"),
        ('[{"a":1' + "0" * 400 + "}]", None, "json parse error: number out of range"),
        ("[]", "yaml", "unsupported source.options.json_mode=yaml"),
    ],
)
def test_file_errors(
    tmp_path: Path, content: str, mode: str | None, message: str
) -> None:
    with pytest.raises(FloeIoError, match=message):
        read(tmp_path, content, column("a"), mode=mode)


def test_selector_errors(tmp_path: Path) -> None:
    with pytest.raises(FloeIoError, match="duplicate json selector source: a"):
        read(tmp_path, "[]", column("a"), column("b", "a"))
    with pytest.raises(
        FloeIoError, match=r"invalid selector a\.\.b: selector contains"
    ):
        read(tmp_path, "[]", column("a..b"))


def test_ndjson_invalid_utf8_names_the_line(tmp_path: Path) -> None:
    path = tmp_path / "in.json"
    path.write_bytes(b'{"a":1}\n{"a":"\xff"}\n')
    options = SourceOptions.model_validate({"json_mode": "ndjson"})
    with pytest.raises(FloeIoError, match="json parse error at line 2: "):
        list(read_json(path, options, [column("a")]))


@pytest.mark.parametrize(
    ("content", "mode", "message"),
    [
        ('[{"a":"\\ud800"}]', None, "json parse error: "),
        ('[{"a":["\\udc00"]}]', None, "json parse error: "),
        ('{"a":1}\n{"\\ud800":1}', "ndjson", "json parse error at line 2: "),
    ],
)
def test_unpaired_surrogates_are_parse_errors(
    tmp_path: Path, content: str, mode: str | None, message: str
) -> None:
    with pytest.raises(FloeIoError, match=message):
        read(tmp_path, content, column("a"), mode=mode)
    (chunk,) = read(tmp_path, '[{"a":"\\ud83d\\ude00"}]', column("a"))
    assert chunk.df.rows() == [("\U0001f600",)]


def test_coerce_sorts_object_keys(tmp_path: Path) -> None:
    (chunk,) = read(
        tmp_path, '[{"a":{"z":1,"a":[{"y":1,"b":2}]}}]', column("a"), cast_mode="coerce"
    )
    assert chunk.df.rows() == [('{"a":[{"b":2,"y":1}],"z":1}',)]


def test_empty_array_yields_empty_frame(tmp_path: Path) -> None:
    (chunk,) = read(tmp_path, "[]", column("a"))
    assert (chunk.offset, chunk.df.columns, chunk.df.height) == (0, ["a"], 0)


def test_chunks_have_whole_file_offsets(tmp_path: Path) -> None:
    content = "".join(f'{{"n":{i}}}\n' for i in range(5))
    chunks = read(tmp_path, content, column("n"), mode="ndjson", chunk_rows=2)
    assert [(c.offset, c.df.height) for c in chunks] == [(0, 2), (2, 2), (4, 1)]


@pytest.mark.parametrize(
    ("selector", "tokens"),
    [
        (" a.b[0][1].c ", ["a", "b", 0, 1, "c"]),
        ("a[0].b", ["a", 0, "b"]),
        ("$.a", ["$", "a"]),
    ],
)
def test_parse_selector(selector: str, tokens: list[str | int]) -> None:
    assert parse_selector(selector) == tokens


@pytest.mark.parametrize(
    ("selector", "message"),
    [
        ("  ", "selector is empty"),
        (".a", "selector contains empty field"),
        ("a..b", "selector contains empty field"),
        ("a[]", "selector index is empty"),
        ("a[", "selector index is empty"),
        ("a[-1]", "selector index is not numeric: -1"),
        ("a[x]", "selector index is not numeric: x"),
        ("a]", "selector contains unexpected ']'"),
        (
            "a[99999999999999999999999]",
            "selector index is invalid: 99999999999999999999999",
        ),
    ],
)
def test_parse_selector_errors(selector: str, message: str) -> None:
    with pytest.raises(
        ValueError, match=message.replace("[", r"\[").replace("]", r"\]")
    ):
        parse_selector(selector)
