"""ORC reader: native column types, one chunk per stripe (Rust: `io/read/orc.rs`)."""
# pyarrow ships no type stubs, so strict pyright cannot see its types.
# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false, reportUnknownArgumentType=false

from collections.abc import Collection, Iterator
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


def read_orc(path: Path, columns: Collection[str] = ()) -> Iterator[OrcChunk]:
    """Yield one chunk per stripe; always at least one (maybe empty).

    Columns keep their native types (casting them is #758, D15). `columns` projects to the
    declared columns that exist in the file; none of them present reads everything, as Rust.
    """
    try:
        orc = paorc.ORCFile(path)
        picked = [name for name in orc.schema.names if name in columns] or None
        done = 0
        for i in range(orc.nstripes):
            df = pl.DataFrame(orc.read_stripe(i, columns=picked))
            yield OrcChunk(done, df)
            done += df.height
        if done == 0:
            yield OrcChunk(0, pl.DataFrame(orc.read(columns=picked)))
    except (pl.exceptions.PolarsError, pa.ArrowException, OSError) as err:
        raise FloeIoError(f"orc read failed: {err}") from err
