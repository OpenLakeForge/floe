"""XML reader: every selected column as a raw string, streamed in bounded chunks (Rust: `io/read/xml.rs`)."""

import re
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from xml.etree import ElementTree as ET

import polars as pl

from floe.config.types import ColumnConfig, SourceOptions
from floe.errors import FloeConfigError, FloeIoError

_READ_SIZE = 1 << 16
Selector = list[tuple[bool, str]]  # (is_attribute, name)


@dataclass(frozen=True)
class XmlChunk:
    """`offset` is the whole-file index of the chunk's first row (add it to the row index)."""

    offset: int
    df: pl.DataFrame


def parse_selector(value: str) -> Selector:
    """A `.` or `/` separated path of element names, optionally ending in `@attribute`."""
    trimmed = value.strip()
    if not trimmed:
        raise ValueError("selector is empty")
    parts = [p.strip() for p in re.split(r"[./]", trimmed)]
    if "" in parts:
        raise ValueError("selector contains empty token")
    tokens: Selector = []
    for index, part in enumerate(parts):
        if part.startswith("@"):
            if index + 1 != len(parts):
                raise ValueError("attribute selector must be final token")
            attribute = part.lstrip("@").strip()
            if not attribute:
                raise ValueError("attribute selector is empty")
            tokens.append((True, attribute))
        elif "@" in part:
            raise ValueError(f"invalid selector token: {part}")
        else:
            tokens.append((False, part))
    return tokens


def _matches(element: ET.Element, tag: str, namespace: str | None) -> bool:
    prefix, _, local = tag.rpartition(":")
    ns, _, name = (
        element.tag[1:].partition("}")
        if element.tag.startswith("{")
        else ("", "", element.tag)
    )
    if name != local:
        return False
    if prefix and namespace is None:
        return True
    return namespace is None or ns == namespace


def _text(element: ET.Element) -> str | None:
    return "".join(element.itertext()).strip() or None


def _evaluate(
    row: ET.Element, tokens: Selector, namespace: str | None, value_tag: str | None
) -> str | None:
    node = row
    for is_attribute, name in tokens:
        if is_attribute:
            return node.get(name)
        child = next((c for c in node if _matches(c, name, namespace)), None)
        if child is None:
            return None
        node = child
    if value_tag:
        node = next(
            (
                d
                for d in node.iter()
                if d is not node and _matches(d, value_tag, namespace)
            ),
            node,
        )
    return _text(node)


class _Rows:
    """Parser target: evaluates each finished row element, then clears it (bounded memory)."""

    def __init__(
        self,
        evaluate: Callable[[ET.Element], list[str | None]],
        row_tag: str,
        namespace: str | None,
    ) -> None:
        self.builder = ET.TreeBuilder()
        self.evaluate, self.row_tag, self.namespace = evaluate, row_tag, namespace
        self.rows: list[list[str | None]] = []
        self.depth = 0  # ponytail: row_tag elements nested in a row are content, not rows (Rust yields both)

    def start(self, tag: str, attrib: dict[str, str]) -> ET.Element:
        self.depth += self.depth > 0
        element = self.builder.start(tag, attrib)
        if self.depth == 0 and _matches(element, self.row_tag, self.namespace):
            self.depth = 1
        return element

    def end(self, tag: str) -> ET.Element:
        element = self.builder.end(tag)
        if self.depth == 1 and _matches(element, self.row_tag, self.namespace):
            self.rows.append(self.evaluate(element))
            element.clear()
            # ponytail: the emptied row stays in its parent; drop it there if huge files need it
        self.depth -= self.depth > 0
        return element

    def data(self, data: str) -> None:
        self.builder.data(data)

    def close(self) -> ET.Element:
        return self.builder.close()

    def doctype(self, name: str, pubid: str | None, system: str | None) -> None:
        raise ET.ParseError("DTDs and external entities are not supported")


def _options(options: SourceOptions) -> tuple[str, str | None, str | None]:
    row_tag = (options.row_tag or "").strip()
    if not row_tag:
        raise FloeConfigError("source.options.row_tag is required for xml input")
    return (
        row_tag,
        (options.namespace or "").strip() or None,
        (options.value_tag or "").strip() or None,
    )


def read_xml(
    path: Path,
    options: SourceOptions,
    columns: Sequence[ColumnConfig],
    chunk_rows: int = 1_000_000,
) -> Iterator[XmlChunk]:
    """Yield the rows under `row_tag` in chunks of at most `chunk_rows`, columns named by selector.

    Missing elements and attributes, and empty text, are null. A file without rows is an error.
    """
    row_tag, namespace, value_tag = _options(options)
    names = [c.source if c.source is not None else c.name for c in columns]
    if len(set(names)) != len(names):
        dup = next(n for n in names if names.count(n) > 1)
        raise FloeIoError(f"duplicate xml selector source: {dup}")
    plans: list[Selector] = []
    for name in names:
        try:
            plans.append(parse_selector(name))
        except ValueError as err:
            raise FloeIoError(f"invalid selector {name}: {err}") from err

    target = _Rows(
        lambda row: [_evaluate(row, p, namespace, value_tag) for p in plans],
        row_tag,
        namespace,
    )
    parser = ET.XMLParser(target=target)
    done = 0

    def frame() -> pl.DataFrame:
        cols = zip(*target.rows, strict=True) if target.rows else [() for _ in names]
        return pl.DataFrame(
            {n: pl.Series(n, list(c), dtype=pl.String) for n, c in zip(names, cols)}
        )

    try:
        with path.open("rb") as handle:
            while block := handle.read(_READ_SIZE):
                parser.feed(block)
                while len(target.rows) >= chunk_rows:
                    rest = target.rows[chunk_rows:]
                    target.rows = target.rows[:chunk_rows]
                    yield XmlChunk(done, frame())
                    done += chunk_rows
                    target.rows = rest
        parser.close()
    except ET.ParseError as err:
        raise FloeIoError(f"xml parse error: {err}") from err
    except OSError as err:
        raise FloeIoError(f"failed to read xml at {path}: {err}") from err
    if target.rows:
        yield XmlChunk(done, frame())
    elif done == 0:
        raise FloeIoError(f"row_tag={row_tag} produced no rows")


def _in_namespace(tag: str, namespace: str | None) -> bool:
    return namespace is None or tag[1:].partition("}")[0] == namespace


def xml_columns(path: Path, options: SourceOptions) -> list[str]:
    """Sorted child element names of the first row: the probe Rust's `read_input_columns` runs."""
    row_tag, namespace, _ = _options(options)
    try:
        for _, element in ET.iterparse(path):
            if _matches(element, row_tag, namespace):
                return sorted(
                    {
                        c.tag.rpartition("}")[2]
                        for c in element
                        if _in_namespace(c.tag, namespace)
                    }
                )
    except ET.ParseError as err:
        raise FloeIoError(f"xml parse error: {err}") from err
    except OSError as err:
        raise FloeIoError(f"failed to read xml at {path}: {err}") from err
    raise FloeIoError(f"row_tag={row_tag} not found in xml")
