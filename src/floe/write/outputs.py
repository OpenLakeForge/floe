"""Accepted Parquet and rejected CSV writers (Rust: `io/write/{parquet,csv,parts}.rs`); `base` is a `local://` or `s3://` URI."""

import io
import math
import re
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from itertools import count

import polars as pl

from floe.config.types import SinkOptions, WriteMode
from floe.storage.paths import parse_s3_uri
from floe.storage.types import StorageClient

DEFAULT_MAX_SIZE_PER_FILE = 256 * 1024 * 1024
MAX_REPORTED_PARTS = 50
_MIB = 1024 * 1024
_PART = re.compile(r"part-.+")


@dataclass(frozen=True)
class AcceptedWrite:
    """Rust's accepted write output; the byte metrics are recorded, not compared (G5)."""

    files_written: int
    part_files: list[str]
    total_bytes_written: int
    avg_file_size_mb: float | None
    small_files_count: int


def part_uris(client: StorageClient, base: str, ext: str) -> list[str]:
    """The `part-*.<ext>` files directly under `base`, sorted."""
    prefix = parse_s3_uri(base)[1] + "/" if base.startswith("s3://") else base.removeprefix("local://")  # fmt: skip
    uris = (r.uri for r in client.list(prefix))
    return sorted(u for u in uris if u.rpartition("/")[0] == base and u.endswith(f".{ext}") and _PART.fullmatch(u.rpartition("/")[2].removesuffix(f".{ext}")))  # fmt: skip


def _part_names(client: StorageClient, base: str, mode: WriteMode, ext: str) -> Iterator[str]:  # fmt: skip
    """Overwrite clears the old parts and numbers from 0 (`part-00000`); other modes use `part-<uuid>`."""
    if mode != "overwrite":
        while True:
            yield f"part-{uuid.uuid4()}.{ext}"
    for uri in part_uris(client, base, ext):
        client.delete(uri)
    for i in count():
        yield f"part-{i:05}.{ext}"


def write_accepted(
    df: pl.DataFrame,
    client: StorageClient,
    base: str,
    mode: WriteMode = "overwrite",
    options: SinkOptions | None = None,
) -> AcceptedWrite:
    """Write `df` as `part-*.parquet` files under `base`, split so each holds about `max_size_per_file`."""
    base = base.rstrip("/")
    names = _part_names(client, base, mode, "parquet")
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
    for chunk in [df.slice(i, rows) for i in range(0, df.height, rows)] or [df]:
        name = next(names)
        # ponytail: a part is held in memory (≤ max_size_per_file) before the put; stream multipart uploads if that is too much
        body = io.BytesIO()
        chunk.write_parquet(body, **kwargs)  # pyright: ignore[reportArgumentType]  # ponytail: kwargs loses the compression Literal; validated by the config
        client.write(f"{base}/{name}", body.getvalue())
        files.append(name)
        sizes.append(body.tell())
    total = sum(sizes)
    return AcceptedWrite(
        len(files),
        files[:MAX_REPORTED_PARTS],
        total,
        total / len(files) / _MIB,
        sum(s < max(1, max_size // 2) for s in sizes),
    )


def write_rejected(df: pl.DataFrame, client: StorageClient, base: str, mode: WriteMode = "overwrite") -> str:  # fmt: skip
    """Write the rejected frame from `split_rows` as one CSV part under `base`; returns its URI."""
    base = base.rstrip("/")
    uri = f"{base}/{next(_part_names(client, base, mode, 'csv'))}"
    client.write(uri, df.write_csv().encode())
    return uri
