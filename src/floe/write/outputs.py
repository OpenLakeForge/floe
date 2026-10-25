"""Accepted Parquet and rejected CSV writers (Rust: `io/write/{parquet,csv,parts}.rs`, local targets)."""

import math
import re
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from itertools import count
from pathlib import Path

import polars as pl

from floe.config.types import SinkOptions, WriteMode
from floe.errors import FloeError

DEFAULT_MAX_SIZE_PER_FILE = 256 * 1024 * 1024
MAX_REPORTED_PARTS = 50
_MIB = 1024 * 1024
_PART = re.compile(r"part-.+")


@contextmanager
def _os_errors() -> Iterator[None]:
    """An `OSError` while writing becomes a bare `FloeError`: Rust surfaces a raw `io::Error` here, which logs with code `error`."""
    try:
        yield
    except OSError as e:
        raise FloeError(f"{e.strerror} (os error {e.errno})" if e.errno is not None else str(e)) from e  # fmt: skip


@dataclass(frozen=True)
class AcceptedWrite:
    """Rust's accepted write output; the byte metrics are recorded, not compared (G5)."""

    files_written: int
    part_files: list[str]
    total_bytes_written: int
    avg_file_size_mb: float | None
    small_files_count: int


def _part_names(base: Path, mode: WriteMode, ext: str) -> Iterator[str]:
    """Overwrite clears the old parts and numbers from 0 (`part-00000`); other modes use `part-<uuid>`."""
    base.mkdir(parents=True, exist_ok=True)
    if mode != "overwrite":
        while True:
            yield f"part-{uuid.uuid4()}.{ext}"
    for p in base.glob(f"*.{ext}"):
        if p.is_file() and _PART.fullmatch(p.stem):
            p.unlink()
    for i in count():
        yield f"part-{i:05}.{ext}"


def write_accepted(
    df: pl.DataFrame,
    base: Path,
    mode: WriteMode = "overwrite",
    options: SinkOptions | None = None,
) -> AcceptedWrite:
    """Write `df` as `part-*.parquet` files under `base`, split so each holds about `max_size_per_file`."""
    names = _part_names(base, mode, "parquet")
    max_size = (options and options.max_size_per_file) or DEFAULT_MAX_SIZE_PER_FILE
    avg_row = max(1, math.ceil(df.estimated_size() / df.height)) if df.height else 1
    rows = max(1, max_size // avg_row)
    kwargs: dict[str, str | int] = {}
    if options and options.compression:
        kwargs["compression"] = options.compression
    if options and options.row_group_size:
        kwargs["row_group_size"] = options.row_group_size
    files: list[str] = []
    sizes: list[int] = []
    with _os_errors():
        for chunk in [df.slice(i, rows) for i in range(0, df.height, rows)] or [df]:
            name = next(names)
            chunk.write_parquet(base / name, **kwargs)  # pyright: ignore[reportArgumentType]  # ponytail: kwargs loses the compression Literal; validated by the config
            files.append(name)
            sizes.append((base / name).stat().st_size)
    total = sum(sizes)
    return AcceptedWrite(
        len(files),
        files[:MAX_REPORTED_PARTS],
        total,
        total / len(files) / _MIB,
        sum(s < max(1, max_size // 2) for s in sizes),
    )


def write_rejected(df: pl.DataFrame, base: Path, mode: WriteMode = "overwrite") -> Path:
    """Write the rejected frame from `split_rows` as one CSV part under `base`."""
    with _os_errors():
        path = base / next(_part_names(base, mode, "csv"))
        df.write_csv(path)
    return path
