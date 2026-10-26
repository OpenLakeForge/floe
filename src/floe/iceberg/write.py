"""Overwrite and append of an Arrow stream in one catalog commit, one snapshot (#597's `single` path)."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs

import itertools
import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import PurePosixPath
from typing import Literal

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.io.pyarrow import (
    _check_pyarrow_schema_compatible,  # pyright: ignore[reportPrivateUsage] -- same check `Transaction.append` makes
    _dataframe_to_data_files,  # pyright: ignore[reportPrivateUsage] -- feeds one snapshot producer from many chunks
)
from pyiceberg.manifest import DataFile, DataFileContent, ManifestEntryStatus
from pyiceberg.table import Table, TableProperties
from pyiceberg.table.snapshots import ancestors_of

from floe.errors import FloeRunError
from floe.iceberg.catalog import run_error
from floe.state import (
    _backoff,  # pyright: ignore[reportPrivateUsage] -- one jittered backoff for every CAS retry
)

WriteMode = Literal["overwrite", "append"]

RUN_ID = "floe.run_id"
INPUT_HASH = "floe.input_hash"

# `metrics::DEFAULT_SMALL_FILE_THRESHOLD_BYTES`
SMALL_FILE_THRESHOLD_BYTES = 16 * 1024 * 1024
_BYTES_PER_MIB = 1024 * 1024
_REPORTED_FILE_PATHS = 50
CHUNK_ROWS = 1_000_000
COMMIT_ATTEMPTS = 5


@dataclass(frozen=True)
class IcebergWriteResult:
    files_written: int
    snapshot_id: int | None
    metadata_version: int | None
    file_paths: list[str]
    total_bytes_written: int
    avg_file_size_mb: float | None
    small_files_count: int
    already_committed: bool = (
        False  # a conflict showed another run committed this batch (#545)
    )


def _chunks(reader: pa.RecordBatchReader, chunk_rows: int) -> Iterator[pa.Table]:
    pending: list[pa.RecordBatch] = []
    rows = 0
    for batch in reader:
        offset = 0
        while offset < batch.num_rows:
            part = batch.slice(offset, chunk_rows - rows)
            offset += part.num_rows
            pending.append(part)
            rows += part.num_rows
            if rows >= chunk_rows:
                yield pa.Table.from_batches(pending, reader.schema)
                pending, rows = [], 0
    if rows:
        yield pa.Table.from_batches(pending, reader.schema)


def _metadata_version(location: str | None) -> int | None:
    prefix = PurePosixPath(location or "").name.split("-", 1)[0]
    return int(prefix) if prefix.isdigit() else None


def write_table(
    table: Table,
    reader: pa.RecordBatchReader,
    mode: WriteMode,
    run_id: str,
    input_hash: str,
    small_file_threshold_bytes: int = SMALL_FILE_THRESHOLD_BYTES,
    chunk_rows: int = CHUNK_ROWS,
    unique_keys: Sequence[Sequence[str]] = (),
) -> IcebergWriteResult:
    """Data files are written chunk by chunk (partitioned tables split each chunk), then committed once.

    Overwrite replaces every existing file in the same snapshot. An append of no rows commits nothing.
    On a conflict an append checks `unique_keys` (table column names) against the files added since the base
    snapshot and fails closed, committing nothing, when a rival already holds a key (#549).
    """
    with run_error("iceberg data write failed"):
        _check_pyarrow_schema_compatible(table.schema(), reader.schema)
        counter = itertools.count()
        write_uuid = uuid.uuid4()
        data_files: list[DataFile] = [
            data_file
            for chunk in _chunks(reader, chunk_rows)
            for data_file in _dataframe_to_data_files(
                table.metadata, chunk, table.io, write_uuid, counter
            )
        ]
    base = (
        table.metadata.current_snapshot_id
    )  # only commits after it can be a rival's copy of this batch
    for attempt in range(COMMIT_ATTEMPTS):
        if attempt:  # a conflict: the data files stay, only the transaction is rebuilt on the fresh table
            _backoff(attempt - 1)
            with run_error("rest iceberg table refresh failed"):
                table.refresh()
                seen = mode == "append" and find_committed(
                    table, input_hash, after=base
                )
            if seen:
                return replace(write_result(table, [], small_file_threshold_bytes), already_committed=True)  # fmt: skip
            if mode == "append":
                _revalidate_unique(table, base, data_files, unique_keys)
        replaced: list[DataFile] = []
        if mode == "overwrite":
            with run_error("iceberg overwrite scan failed"):
                replaced = [task.file for task in table.scan().plan_files()]
        if not (data_files or replaced):
            break
        try:
            with run_error("rest iceberg commit failed"):
                # PyIceberg retries a 409 itself (refresh, rebuild) and would skip the guard; in memory only, never sent
                table.metadata = table.metadata.model_copy(update={"properties": {**table.properties, TableProperties.COMMIT_NUM_RETRIES: "0"}})  # fmt: skip
                tx = table.transaction()
                update = tx.update_snapshot(
                    snapshot_properties={RUN_ID: run_id, INPUT_HASH: input_hash}
                )
                with (
                    update.overwrite()
                    if mode == "overwrite"
                    else update.fast_append() as snap
                ):
                    for old in replaced:
                        snap.delete_data_file(old)
                    for data_file in data_files:
                        snap.append_data_file(data_file)
                table = tx.commit_transaction()
        except FloeRunError as e:
            if not isinstance(e.__cause__, CommitFailedException):
                raise
            continue
        break
    else:
        raise FloeRunError(f"rest iceberg commit failed: conflicted {COMMIT_ATTEMPTS} times in a row")  # fmt: skip
    return write_result(table, data_files, small_file_threshold_bytes)


def added_data_files(table: Table, since: int | None) -> list[DataFile]:
    """Data files added by the snapshots after `since` (a DuckDB MERGE commits two)."""
    files: list[DataFile] = []
    snapshot = table.current_snapshot()
    while snapshot is not None and snapshot.snapshot_id != since:
        files += [
            entry.data_file
            for manifest in snapshot.manifests(table.io)
            if manifest.added_snapshot_id == snapshot.snapshot_id
            for entry in manifest.fetch_manifest_entry(table.io)
            if entry.status == ManifestEntryStatus.ADDED
            and entry.data_file.content == DataFileContent.DATA
        ]
        parent = snapshot.parent_snapshot_id
        snapshot = None if parent is None else table.snapshot_by_id(parent)
    return files


def _keys(
    table: Table, file: DataFile, key: Sequence[str]
) -> Iterator[tuple[object, ...]]:
    """The non-NULL key tuples of a data file; a file without a key column (evolved later) holds none."""
    with table.io.new_input(file.file_path).open() as f:
        schema = pq.read_schema(f)
        if not set(key) <= set(schema.names):
            return
        f.seek(0)
        rows = pq.read_table(f, columns=list(key))
    yield from (k for k in zip(*(rows[c].to_pylist() for c in key)) if None not in k)


def _revalidate_unique(
    table: Table,
    base: int | None,
    data_files: list[DataFile],
    unique_keys: Sequence[Sequence[str]],
) -> None:
    """Raise when a key of `data_files` is in a file a rival added since `base`.

    ponytail: the rivals' keys sit in memory (cost follows the concurrent writes, not the history); a
    float compares by Python equality, so NaN never collides and -0.0 equals 0.0, unlike the tracker's bits.
    """
    with run_error("iceberg unique revalidation failed"):
        rival = added_data_files(table, base)
        clash = next(
            (
                key
                for key in unique_keys
                if (held := {k for file in rival for k in _keys(table, file, key)})
                and any(
                    k in held for file in data_files for k in _keys(table, file, key)
                )
            ),
            None,
        )
    if clash:
        raise FloeRunError(f"unique key ({', '.join(clash)}) already committed by a concurrent run; nothing was committed, rerun to revalidate")  # fmt: skip


def write_result(
    table: Table,
    data_files: list[DataFile],
    small_file_threshold_bytes: int = SMALL_FILE_THRESHOLD_BYTES,
) -> IcebergWriteResult:
    """Rust's report fields for the files a commit added."""
    sizes = [f.file_size_in_bytes for f in data_files]
    snapshot = table.current_snapshot()
    return IcebergWriteResult(
        files_written=len(data_files),
        snapshot_id=None if snapshot is None else snapshot.snapshot_id,
        metadata_version=_metadata_version(table.metadata_location),
        file_paths=[PurePosixPath(f.file_path).name for f in data_files][
            :_REPORTED_FILE_PATHS
        ],
        total_bytes_written=sum(sizes),
        avg_file_size_mb=sum(sizes) / len(sizes) / _BYTES_PER_MIB if sizes else None,
        small_files_count=sum(s < small_file_threshold_bytes for s in sizes),
    )


def find_committed(
    table: Table,
    input_hash: str,
    since: datetime | None = None,
    after: int | None = None,
) -> bool | None:
    """Whether a snapshot ancestor carries `floe.input_hash` (#545); the metadata is in memory, no manifest I/O.

    The walk stops at snapshots older than `since`, the claim's `acquired_at`: a batch can't commit before its
    claim. None when the history back to `since` was expired, so the answer is unknown. `after` is a snapshot id
    that ends the walk: only the commits made since it count.
    """
    stop = None if since is None else since.timestamp() * 1000
    oldest = None
    for snapshot in ancestors_of(table.current_snapshot(), table.metadata):
        if snapshot.snapshot_id == after or (
            stop is not None and snapshot.timestamp_ms < stop
        ):
            return False
        if (
            snapshot.summary is not None
            and snapshot.summary.get(INPUT_HASH) == input_hash
        ):
            return True
        oldest = snapshot
    expired = oldest is not None and oldest.parent_snapshot_id is not None
    return None if stop is not None and expired else False
