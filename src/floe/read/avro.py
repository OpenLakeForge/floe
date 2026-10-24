"""Avro reader: native column types, in chunks (Rust: `io/read/avro.rs`)."""

from collections.abc import Collection, Iterator
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from floe.errors import FloeIoError


@dataclass(frozen=True)
class AvroChunk:
    """`offset` is the whole-file index of the chunk's first row."""

    offset: int
    df: pl.DataFrame


def read_avro(
    path: Path, columns: Collection[str] = (), chunk_rows: int = 1_000_000
) -> Iterator[AvroChunk]:
    """Yield the file in chunks of at most `chunk_rows` rows; always at least one (maybe empty).

    Columns keep their native types (casting them is #758, D15). `columns` projects to the
    declared columns that exist in the file; none of them present keeps everything.
    """
    try:
        # ponytail: Polars reads the whole file (no streaming Avro scan); chunks bound only what
        # the checks hold at once. Switch to a streaming reader if Avro files outgrow memory.
        df = pl.read_avro(path)
    except (pl.exceptions.PolarsError, OSError) as err:
        raise FloeIoError(f"avro read failed: {err}") from err
    picked = [name for name in df.columns if name in columns]
    if picked:
        df = df.select(picked)
    for offset in range(0, df.height, chunk_rows):
        yield AvroChunk(offset, df.slice(offset, chunk_rows))
    if df.height == 0:
        yield AvroChunk(0, df)
