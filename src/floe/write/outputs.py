"""Accepted Parquet and rejected CSV writers (Rust: `io/write/{parquet,csv,parts}.rs`); `base` is a `local://` or `s3://` URI."""

import io
import math
import re
import uuid
from collections.abc import Callable, Generator, Iterable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from itertools import groupby
from operator import itemgetter

import polars as pl

from floe.config.types import SinkOptions, WriteMode
from floe.errors import FloeError, FloeStorageError
from floe.storage.paths import parse_s3_uri
from floe.storage.types import StorageClient

DEFAULT_MAX_SIZE_PER_FILE = 256 * 1024 * 1024
MAX_REPORTED_PARTS = 50
_MIB = 1024 * 1024
_PART = re.compile(r"part-.+")


@contextmanager
def _os_errors() -> Generator[None]:
    """An `OSError` while writing becomes a bare `FloeError`: Rust surfaces a raw `io::Error` here, which logs with code `error`."""
    try:
        yield
    except (
        OSError,
        FloeStorageError,
    ) as e:  # the storage client wraps the OSError; Rust prints the bare io::Error
        os = e if isinstance(e, OSError) else e.__cause__
        if not isinstance(os, OSError):
            raise
        raise FloeError(f"{os.strerror} (os error {os.errno})" if os.errno is not None else str(os)) from os  # fmt: skip


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
    key = parse_s3_uri(base)[1] if base.startswith("s3://") else None
    prefix = (
        base.removeprefix("local://") if key is None else key and f"{key}/"
    )  # a bucket root lists every key
    uris = (r.uri for r in client.list(prefix))
    return sorted(u for u in uris if u.rpartition("/")[0] == base and u.endswith(f".{ext}") and _PART.fullmatch(u.rpartition("/")[2].removesuffix(f".{ext}")))  # fmt: skip


def keyed_part(key: str, index: int, ext: str) -> str:
    """The name of a keyed part: the same `key` and index always give the same name (#919)."""
    return f"part-{uuid.uuid5(uuid.NAMESPACE_URL, f'{key}:{index}')}.{ext}"


@contextmanager
def _parts(client: StorageClient, base: str, mode: WriteMode, ext: str, key: str | None = None) -> Generator[Callable[..., str]]:  # fmt: skip
    """Yield `put(body) -> name`. Every part is staged as `_staging-<id>-<name>` (invisible to `part_uris`).

    Only when all were written are they copied over `<name>` and the staged copies deleted (#1054).
    Overwrite names the parts `part-NNNNN` and deletes the old parts that no new part replaced, so a
    failed write leaves them untouched. Other modes name them `part-<uuid>` and keep the old parts;
    staging keeps a killed run from leaving visible parts for a retry to duplicate (#1056: parts are
    written while the input is still read). A failed write deletes the staged parts it wrote.
    With a `key`, a non-overwrite part is named `part-<uuid5(key:index)>` so a retry replaces the same parts (#919);
    `put(body, key)` names it from that key instead, its index counting the parts of that key (#1203).
    """
    overwrite = mode == "overwrite"
    stage = f"_staging-{uuid.uuid4()}-"
    names: list[str] = []
    counts: dict[str, int] = {}

    def put(body: bytes, part_key: str | None = None) -> str:
        part_key = part_key or key or ""
        index = counts[part_key] = counts.get(part_key, -1) + 1
        name = f"part-{len(names):05}.{ext}" if overwrite else keyed_part(part_key, index, ext) if part_key else f"part-{uuid.uuid4()}.{ext}"  # fmt: skip
        names.append(name)  # before the write: a partial file needs cleanup too
        client.write(f"{base}/{stage}{name}", body)
        return name

    try:
        yield put
    except BaseException:
        for name in names:
            with suppress(FloeError, OSError):  # best effort: the original error wins
                client.delete(f"{base}/{stage}{name}")
        raise
    old = part_uris(client, base, ext) if overwrite else []
    for name in names:
        client.copy(f"{base}/{stage}{name}", f"{base}/{name}")
    for uri in set(old) - {f"{base}/{n}" for n in names}:
        client.delete(uri)
    for name in names:
        client.delete(f"{base}/{stage}{name}")


def _part_frames(
    frames: Iterable[pl.DataFrame], max_size: int
) -> Iterator[pl.DataFrame]:
    """`frames` regrouped into parts of about `max_size` bytes in memory; at most one part and a frame are pooled."""
    pool: list[pl.DataFrame] = []
    size = height = 0
    for df in frames:
        pool.append(df)
        size += df.estimated_size()
        height += df.height
        rows = max(1, max_size // max(1, math.ceil(size / height))) if height else 1
        if height >= rows:
            whole = pl.concat(pool)
            full = height - height % rows
            yield from (whole.slice(i, rows) for i in range(0, full, rows))
            rest = whole.slice(full)
            pool = [rest] if rest.height else []
            size, height = rest.estimated_size(), rest.height
    if pool:  # also the one empty part of a run that accepted no row
        yield pl.concat(pool)


def write_accepted(
    frames: Iterable[pl.DataFrame],
    client: StorageClient,
    base: str,
    mode: WriteMode = "overwrite",
    options: SinkOptions | None = None,
) -> AcceptedWrite:
    """Write `frames` (at least one) as `part-*.parquet` files under `base`, each holding about `max_size_per_file`."""
    max_size = (options and options.max_size_per_file) or DEFAULT_MAX_SIZE_PER_FILE
    return _write_parts(((None, c) for c in _part_frames(frames, max_size)), client, base, mode, options)  # fmt: skip


def write_accepted_per_file(
    frames: Iterable[tuple[str, pl.DataFrame]],
    client: StorageClient,
    base: str,
    mode: WriteMode,
    options: SinkOptions | None = None,
) -> AcceptedWrite:
    """`write_accepted` for `(input key, frame)` pairs (at least one), the frames of a key together: a part holds the rows of one input.

    An append part is named from its input key and index, so a retry that reads the same input replaces exactly its parts and
    never touches another's (#1203).
    # ponytail: if `max_size_per_file` changed since the dead run, its higher-index parts of a file survive next to the retry's; compact or clean by hand
    """
    max_size = (options and options.max_size_per_file) or DEFAULT_MAX_SIZE_PER_FILE

    # an input whose rows were all rejected still gets its (empty) part: a dead run's part for it is replaced, not left to duplicate rows another input took
    parts = ((key, c) for key, group in groupby(frames, itemgetter(0)) for c in _part_frames((df for _, df in group), max_size))  # fmt: skip
    return _write_parts(parts, client, base, mode, options)


def _write_parts(
    parts: Iterable[tuple[str | None, pl.DataFrame]],
    client: StorageClient,
    base: str,
    mode: WriteMode,
    options: SinkOptions | None,
) -> AcceptedWrite:
    base = base.rstrip("/")
    max_size = (options and options.max_size_per_file) or DEFAULT_MAX_SIZE_PER_FILE
    kwargs: dict[str, str | int] = {}
    if options and options.compression:
        kwargs["compression"] = options.compression
    if options and options.row_group_size:
        kwargs["row_group_size"] = options.row_group_size
    files: list[str] = []
    sizes: list[int] = []
    with _os_errors(), _parts(client, base, mode, "parquet") as put:
        for key, chunk in parts:
            # ponytail: a part is held in memory (≤ max_size_per_file) before the put; stream multipart uploads if that is too much
            body = io.BytesIO()
            chunk.write_parquet(body, **kwargs)  # pyright: ignore[reportArgumentType]  # ponytail: kwargs loses the compression Literal; validated by the config
            files.append(put(body.getvalue(), key))
            sizes.append(body.tell())
    total = sum(sizes)
    return AcceptedWrite(
        len(files),
        files[:MAX_REPORTED_PARTS],
        total,
        total / len(files) / _MIB,
        sum(s < max(1, max_size // 2) for s in sizes),
    )


def write_rejected(df: pl.DataFrame, client: StorageClient, base: str, mode: WriteMode = "overwrite", key: str | None = None) -> str:  # fmt: skip
    """Write the rejected frame from `split_rows` as one CSV part under `base`; returns its URI.

    With a `key` (the run's input hash and the file) a non-overwrite part is named from it, so a retry rewrites the same part (S6, #919); Rust has no retry guard.
    """
    with _os_errors():
        base = base.rstrip("/")
        if key and mode != "overwrite":
            name = f"part-{uuid.uuid5(uuid.NAMESPACE_URL, key)}.csv"
            client.write(f"{base}/{name}", df.write_csv().encode())
        else:
            with _parts(client, base, mode, "csv") as put:
                name = put(df.write_csv().encode())
    return f"{base}/{name}"


def write_rejected_parquet(frames: Iterable[pl.DataFrame], client: StorageClient, base: str, mode: WriteMode, key: str) -> list[str]:  # fmt: skip
    """Write the rejected `frames` (at least one) as bounded `part-*.parquet` files; returns the first `MAX_REPORTED_PARTS` URIs.

    Staged and published like the accepted parts; the `key` names a non-overwrite part so a retry rewrites it (S6, #919).
    """
    base = base.rstrip("/")
    names: list[str] = []
    with _os_errors(), _parts(client, base, mode, "parquet", key) as put:
        for chunk in _part_frames(frames, DEFAULT_MAX_SIZE_PER_FILE):
            body = io.BytesIO()
            chunk.write_parquet(body)
            names.append(put(body.getvalue()))
    return [f"{base}/{n}" for n in names[:MAX_REPORTED_PARTS]]
