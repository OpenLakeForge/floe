"""CSV reader: every column as a raw string, in bounded chunks (Rust: `io/read/csv.rs`)."""

import csv
import json
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from typing import Literal

import polars as pl

from floe.config.types import SourceOptions
from floe.errors import FloeConfigError, FloeIoError

RAGGED_MESSAGE = "found more fields than defined in 'Schema'"
_ENCODINGS: dict[str, Literal["utf8", "utf8-lossy"]] = {
    "utf8": "utf8",
    "lossyutf8": "utf8-lossy",
}


@dataclass(frozen=True)
class CsvChunk:
    """`offset` is the whole-file index of the chunk's first data row (add it to the row index).

    `ragged` marks rows with more fields than the header (decision D3, #603): their values are
    truncated and the caller rejects just those rows with `RAGGED_MESSAGE`.
    """

    offset: int
    df: pl.DataFrame
    ragged: pl.Series


@dataclass(frozen=True)
class _Args:
    separator: str
    encoding: Literal["utf8", "utf8-lossy"]
    null_values: list[str] | None
    has_header: bool

    def scan(
        self,
        path: Path,
        schema: dict[str, type[pl.String]] | None = None,
        n_rows: int | None = None,
        ragged_from: int | None = None,
    ) -> pl.LazyFrame:
        """`ragged_from`: truncate over-long rows and skip that many data rows."""
        return pl.scan_csv(
            path,
            has_header=self.has_header,
            separator=self.separator,
            encoding=self.encoding,
            null_values=self.null_values,
            infer_schema=False,
            schema=schema,
            n_rows=n_rows,
            truncate_ragged_lines=ragged_from is not None,
            skip_rows_after_header=ragged_from or 0,
        )


def _scan_args(options: SourceOptions) -> _Args:
    separator = ";" if options.separator is None else options.separator
    if len(separator.encode()) != 1:
        raise FloeConfigError(
            f"separator must be a single byte, got {json.dumps(separator, ensure_ascii=False)}"
        )
    raw = "utf8" if options.encoding is None else options.encoding
    encoding = _ENCODINGS.get(raw.lower().replace("-", "").replace("_", ""))
    if encoding is None:
        raise FloeConfigError(f"unsupported encoding: {raw}")
    return _Args(
        separator, encoding, options.null_values or None, options.header is not False
    )


def _columns(path: Path, args: _Args, declared: Sequence[str]) -> list[str]:
    try:
        probe = args.scan(path, n_rows=0 if args.has_header else 1).collect()
    except (pl.exceptions.PolarsError, OSError) as err:
        # Rust's polars prints the error kind: `no data: empty CSV`
        kind = "no data: " if isinstance(err, pl.exceptions.NoDataError) else ""
        raise FloeIoError(f"csv header read failed: {kind}{err}") from err
    if args.has_header:
        return probe.columns
    names = list(declared[: probe.width])
    return names + [f"extra_column_{i + 1}" for i in range(len(declared), probe.width)]


def _field_counts(path: Path, args: _Args, skip: int) -> Iterator[int]:
    errors = "replace" if args.encoding == "utf8-lossy" else "strict"
    with path.open(newline="", encoding="utf-8-sig", errors=errors) as handle:
        rows = csv.reader(handle, delimiter=args.separator)
        for _ in islice(rows, skip):
            pass
        for row in rows:
            yield len(row)


def read_csv(
    path: Path,
    options: SourceOptions,
    declared: Sequence[str] = (),
    chunk_rows: int = 1_000_000,
) -> Iterator[CsvChunk]:
    """Yield the file in chunks of at most `chunk_rows` rows; always at least one (maybe empty).

    `declared` names the headless columns, as Rust. Nulls follow `null_values` and Polars: an
    unquoted empty field is always null.
    """
    args = _scan_args(options)
    names = _columns(path, args, declared)
    schema = dict.fromkeys(names, pl.String)
    done = 0
    try:
        for df in args.scan(path, schema).collect_batches(chunk_size=chunk_rows):
            yield CsvChunk(done, df, pl.Series([False] * df.height, dtype=pl.Boolean))
            done += df.height
    except pl.exceptions.ComputeError as err:
        if RAGGED_MESSAGE not in str(err):
            raise FloeIoError(f"csv read failed: {err}") from err
        # ponytail: the ragged-row pass reads the file again in Python's csv module, only for files
        # that have such rows; a Polars option to flag them would replace it.
        counts = _field_counts(path, args, done + int(args.has_header))
        for df in args.scan(path, schema, ragged_from=done).collect_batches(
            chunk_size=chunk_rows
        ):
            mask = [n > len(names) for n in islice(counts, df.height)]
            yield CsvChunk(done, df, pl.Series(mask, dtype=pl.Boolean))
            done += df.height
    except pl.exceptions.PolarsError as err:
        raise FloeIoError(f"csv read failed: {err}") from err
    if done == 0:
        empty = pl.DataFrame(schema=dict.fromkeys(names, pl.String))
        yield CsvChunk(0, empty, pl.Series([], dtype=pl.Boolean))
