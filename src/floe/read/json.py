"""JSON reader: every selected column as a raw string (Rust: `io/read/json.rs`, `json_selector.rs`)."""

import json
import math
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NoReturn

import polars as pl

from floe.config.entity import WHITE_SPACE
from floe.config.types import ColumnConfig, SourceOptions
from floe.errors import FloeIoError

Selector = list[str | int]  # a field name or an array index


@dataclass(frozen=True)
class JsonChunk:
    """`offset` is the whole-file index of the chunk's first row (add it to the row index)."""

    offset: int
    df: pl.DataFrame


def parse_selector(value: str) -> Selector:
    """A path of fields and array indexes, e.g. `a.b[0].c` (a `.` right after `]` is optional)."""
    trimmed = value.strip()
    if not trimmed:
        raise ValueError("selector is empty")
    tokens: Selector = []
    buf = ""
    after_index = False
    chars = iter(trimmed)
    for ch in chars:
        if ch == ".":
            if buf:
                tokens.append(buf)
                buf = ""
            elif not after_index:
                raise ValueError("selector contains empty field")
            after_index = False
        elif ch == "[":
            if buf:
                tokens.append(buf)
                buf = ""
            digits = ""
            for nxt in chars:
                if nxt == "]":
                    break
                digits += nxt
            if not digits:
                raise ValueError("selector index is empty")
            if not (digits.isascii() and digits.isdigit()):
                raise ValueError(f"selector index is not numeric: {digits}")
            if int(digits) >= 1 << 64:
                raise ValueError(f"selector index is invalid: {digits}")
            tokens.append(int(digits))
            after_index = True
        elif ch == "]":
            raise ValueError("selector contains unexpected ']'")
        else:
            buf += ch
            after_index = False
    if buf:
        tokens.append(buf)
    return tokens


def _is(value: Any, kind: type) -> bool:
    """isinstance without narrowing: pyright strict rejects the Unknown containers it infers."""
    return isinstance(value, kind)


def _compact(value: Any) -> str:
    return json.dumps(
        value, separators=(",", ":"), ensure_ascii=False, sort_keys=True
    )  # serde_json sorts keys


def _cell(
    value: Any, tokens: Selector, source: str, cast_mode: str, where: str
) -> str | None:
    """Missing paths and JSON null are null; objects and arrays are an error unless `coerce`."""
    row: Any = value
    for token in tokens:
        if isinstance(token, str):
            if not _is(row, dict) or token not in row:
                return None
        elif not _is(row, list) or token >= len(row):
            return None
        row = row[token]
    if row is None:
        return None
    if _is(row, dict) or _is(row, list):
        if cast_mode != "coerce":
            raise FloeIoError(f"non-scalar value for selector {source} at {where}")
        return _compact(row)
    if isinstance(row, bool):
        return "true" if row else "false"
    return str(row)


def _reject_constant(name: str) -> NoReturn:
    raise ValueError(f"invalid number: {name}")


def _float(text: str) -> float:
    value = float(text)
    if math.isinf(value):
        raise ValueError("number out of range")
    return value


def _int(text: str) -> float:
    """serde reads integers beyond i64/u64 as f64."""
    value = int(text)
    return value if -(1 << 63) <= value < 1 << 64 else _float(text)


def _loads(text: str) -> Any:
    value = json.loads(
        text, parse_constant=_reject_constant, parse_float=_float, parse_int=_int
    )
    # serde rejects unpaired surrogate escapes; Python keeps them and only fails later
    # ponytail: a second pass over each value; a parse_string hook would avoid it
    json.dumps(value, ensure_ascii=False).encode("utf-8")
    return value


def _rows(path: Path, mode: str) -> Iterator[tuple[str, Any]]:
    """Yield (location, object) per row; `where` is `line N` for ndjson, `index N` for arrays."""
    try:
        if mode == "ndjson":
            with path.open("rb") as handle:
                for number, raw in enumerate(handle, 1):
                    try:
                        line = raw.decode("utf-8").strip(WHITE_SPACE)  # Rust str::trim
                        if not line:
                            continue
                        value = _loads(line)
                    except ValueError as err:  # includes UnicodeDecodeError
                        raise FloeIoError(
                            f"json parse error at line {number}: {err}"
                        ) from err
                    if not _is(value, dict):
                        raise FloeIoError(f"expected json object at line {number}")
                    yield f"line {number}", value
            return
        # ponytail: array mode parses the whole file in memory, as Rust; stream if arrays outgrow it
        try:
            array: Any = _loads(path.read_text(encoding="utf-8"))
        except ValueError as err:
            raise FloeIoError(f"json parse error: {err}") from err
    except OSError as err:
        raise FloeIoError(f"failed to read json at {path}: {err}") from err
    if not _is(array, list):
        raise FloeIoError("expected json array at root")
    for index, value in enumerate(array):
        if not _is(value, dict):
            raise FloeIoError(f"expected json object at index {index}")
        yield f"index {index}", value


def read_json(
    path: Path,
    options: SourceOptions,
    columns: Sequence[ColumnConfig],
    cast_mode: str = "strict",
    chunk_rows: int = 1_000_000,
) -> Iterator[JsonChunk]:
    """Yield the rows in chunks of at most `chunk_rows`, columns named by selector; always at least one.

    `options.json_mode` is `array` (default) or `ndjson`. A selector that finds nothing is null.
    Objects and arrays are an error unless `cast_mode` is `coerce`, which keeps their compact JSON.
    Scalars are stringified with Python's `str`; Polars would render `1.0` as `1`, so it is not used.
    """
    mode = options.json_mode or "array"
    if mode not in ("array", "ndjson"):
        raise FloeIoError(
            f"unsupported source.options.json_mode={mode} (allowed: array, ndjson)"
        )
    names = [c.source if c.source is not None else c.name for c in columns]
    if len(set(names)) != len(names):
        dup = next(n for n in names if names.count(n) > 1)
        raise FloeIoError(f"duplicate json selector source: {dup}")
    plans: list[Selector] = []
    for name in names:
        try:
            plans.append(parse_selector(name))
        except ValueError as err:
            raise FloeIoError(f"invalid selector {name}: {err}") from err

    def frame(rows: list[list[str | None]]) -> pl.DataFrame:
        cols = zip(*rows, strict=True) if rows else [() for _ in names]
        return pl.DataFrame(
            {n: pl.Series(n, list(c), dtype=pl.String) for n, c in zip(names, cols)}
        )

    rows: list[list[str | None]] = []
    done = 0
    for where, value in _rows(path, mode):
        rows.append(
            [_cell(value, p, n, cast_mode, where) for p, n in zip(plans, names)]
        )
        if len(rows) == chunk_rows:
            yield JsonChunk(done, frame(rows))
            done += len(rows)
            rows = []
    if rows or done == 0:
        yield JsonChunk(done, frame(rows))
