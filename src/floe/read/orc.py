"""ORC reader: every column as a raw string, one chunk per stripe (Rust: `io/read/orc.rs`)."""
# pyarrow ships no type stubs, so strict pyright cannot see its types.
# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false, reportUnknownArgumentType=false

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.orc as paorc

from floe.errors import FloeIoError


@dataclass(frozen=True)
class OrcChunk:
    """`offset` is the whole-file index of the chunk's first row."""

    offset: int
    df: pl.DataFrame


def read_orc(path: Path) -> Iterator[OrcChunk]:
    """Yield one chunk per stripe; always at least one (maybe empty).

    Nested values (struct, list, map) are rejected, as Rust's strict cast does.
    """
    try:
        orc = paorc.ORCFile(path)
        done = 0
        for i in range(orc.nstripes):
            df = pl.DataFrame(orc.read_stripe(i)).cast(pl.String)
            yield OrcChunk(done, df)
            done += df.height
        if done == 0:
            yield OrcChunk(0, pl.DataFrame(orc.read()).cast(pl.String))
    except (pl.exceptions.PolarsError, pa.ArrowException, OSError) as err:
        raise FloeIoError(f"orc read failed: {err}") from err
