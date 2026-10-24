"""Avro reader: every column as a raw string, in chunks (Rust: `io/read/avro.rs`)."""

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from floe.errors import FloeIoError


@dataclass(frozen=True)
class AvroChunk:
    """`offset` is the whole-file index of the chunk's first row."""

    offset: int
    df: pl.DataFrame


def read_avro(path: Path, chunk_rows: int = 1_000_000) -> Iterator[AvroChunk]:
    """Yield the file in chunks of at most `chunk_rows` rows; always at least one (maybe empty).

    Nested values (record, array, map) are rejected, as Rust's strict cast mode does.
    """
    try:
        # ponytail: Polars reads the whole file (no streaming Avro scan); chunks bound only what
        # the checks hold at once. Switch to a streaming reader if Avro files outgrow memory.
        df = pl.read_avro(path).cast(pl.String)
    except (pl.exceptions.PolarsError, OSError) as err:
        raise FloeIoError(f"avro read failed: {err}") from err
    for offset in range(0, df.height, chunk_rows):
        yield AvroChunk(offset, df.slice(offset, chunk_rows))
    if df.height == 0:
        yield AvroChunk(0, df)
