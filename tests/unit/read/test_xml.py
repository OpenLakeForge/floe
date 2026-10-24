"""XML reader: the conformance run case, selectors, chunking and DTD refusal."""

from pathlib import Path

import polars as pl
import pytest

from floe.config.types import ColumnConfig, SourceOptions
from floe.errors import FloeIoError
from floe.read.xml import XmlChunk, parse_selector, read_xml, xml_columns

CASE = Path(__file__).parents[3] / "conformance" / "xml"


def column(name: str, source: str | None = None) -> ColumnConfig:
    return ColumnConfig.model_validate(
        {
            **dict.fromkeys(("nullable", "unique", "width", "trim")),
            "name": name,
            "source": source,
            "column_type": "string",
        }
    )


def opts(**kwargs: object) -> SourceOptions:
    return SourceOptions.model_validate({"row_tag": "item", **kwargs})


def read(
    path: Path, options: SourceOptions, *columns: ColumnConfig, chunk_rows: int = 1000
) -> list[XmlChunk]:
    return list(read_xml(path, options, columns, chunk_rows))


def write(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "in.xml"
    path.write_text(content)
    return path


def test_conformance_case() -> None:
    options = opts(namespace="urn:x", value_tag="value")
    columns = [
        column("id", "@id"),
        column("name"),
        column("city", "addr.city"),
        column("price"),
    ]
    (chunk,) = read(CASE / "in" / "items.xml", options, *columns)
    assert chunk.offset == 0
    assert chunk.df.columns == ["@id", "name", "addr.city", "price"]
    assert chunk.df.rows() == [
        ("1", "a", "Paris", "9.5"),
        ("2", "b", None, "7"),
        ("3", None, "Rome", None),
    ]
    assert set(chunk.df.schema.values()) == {pl.String}


def test_namespace_and_prefix(tmp_path: Path) -> None:
    xml = '<r xmlns:a="urn:a" xmlns:b="urn:b"><a:item><a:v>1</a:v><b:v>2</b:v></a:item><b:item/></r>'
    path = write(tmp_path, xml)
    assert read(path, opts(namespace="urn:a"), column("v"))[0].df.rows() == [("1",)]
    assert read(path, opts(row_tag="b:item"), column("v"))[0].df.rows() == [
        ("1",),
        (None,),
    ]
    with pytest.raises(FloeIoError, match="row_tag=item produced no rows"):
        read(path, opts(namespace="urn:c"), column("v"))


def test_chunks_have_whole_file_offsets(tmp_path: Path) -> None:
    path = write(
        tmp_path, "<r>" + "".join(f"<item><n>{i}</n></item>" for i in range(5)) + "</r>"
    )
    chunks = read(path, opts(), column("n"), chunk_rows=2)
    assert [(c.offset, c.df.height) for c in chunks] == [(0, 2), (2, 2), (4, 1)]


def test_errors(tmp_path: Path) -> None:
    path = write(tmp_path, "<r><item>")
    with pytest.raises(FloeIoError, match="xml parse error"):
        read(path, opts(), column("n"))
    with pytest.raises(FloeIoError, match="duplicate xml selector source: n"):
        read(path, opts(), column("n"), column("x", "n"))


@pytest.mark.parametrize(
    ("selector", "message"),
    [
        ("", "selector is empty"),
        ("a//b", "selector contains empty token"),
        ("@a.b", "attribute selector must be final token"),
        ("@", "attribute selector is empty"),
        ("a@b", "invalid selector token: a@b"),
    ],
)
def test_selector_errors(selector: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        parse_selector(selector)


def test_dtd_and_entities_are_refused(tmp_path: Path) -> None:
    bomb = '<!DOCTYPE r [<!ENTITY a "aaaa"><!ENTITY b "&a;&a;&a;&a;">]><r><item><n>&b;</n></item></r>'
    with pytest.raises(
        FloeIoError, match="DTDs and external entities are not supported"
    ):
        read(write(tmp_path, bomb), opts(), column("n"))
    external = (
        '<!DOCTYPE r [<!ENTITY x SYSTEM "file:///etc/passwd">]><r><item>&x;</item></r>'
    )
    with pytest.raises(
        FloeIoError, match="DTDs and external entities are not supported"
    ):
        read(write(tmp_path, external), opts(), column("n"))


def test_probe_lists_the_first_rows_child_elements() -> None:
    path = CASE / "in" / "items.xml"
    assert xml_columns(path, opts(namespace="urn:x")) == ["addr", "name", "price"]


def test_probe_fails_without_the_row_tag() -> None:
    with pytest.raises(FloeIoError, match="row_tag=nope not found in xml"):
        xml_columns(CASE / "in" / "items.xml", opts(row_tag="nope"))
