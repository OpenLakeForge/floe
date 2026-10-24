"""Parquet reader: native column types, in bounded chunks (Rust: `io/read/parquet.rs`)."""

from collections.abc import Collection, Iterator
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from floe.errors import FloeIoError


@dataclass(frozen=True)
class ParquetChunk:
    """`offset` is the whole-file index of the chunk's first row."""

    offset: int
    df: pl.DataFrame


def read_parquet(
    path: Path, columns: Collection[str] = (), chunk_rows: int = 1_000_000
) -> Iterator[ParquetChunk]:
    """Yield the file in chunks of at most `chunk_rows` rows; always at least one (maybe empty).

    Columns keep their native types (casting them is #758, D15). `columns` projects to the
    declared columns that exist in the file; none of them present reads everything, as Rust.
    """
    try:
        lf = pl.scan_parquet(path)
        picked = [name for name in lf.collect_schema().names() if name in columns]
    except (pl.exceptions.PolarsError, OSError) as err:
        raise FloeIoError(f"failed to scan parquet at {path}: {err}") from err
    if picked:
        lf = lf.select(picked)
    try:
        done = 0
        for df in lf.collect_batches(chunk_size=chunk_rows):
            yield ParquetChunk(done, df)
            done += df.height
        if done == 0:
            yield ParquetChunk(0, lf.head(0).collect())
    except (pl.exceptions.PolarsError, OSError) as err:
        raise FloeIoError(f"parquet read failed: {err}") from err
