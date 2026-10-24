"""Overwrite and append of an Arrow stream in one catalog commit, one snapshot (#597's `single` path)."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs

import itertools
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Literal

import pyarrow as pa
from pyiceberg.io.pyarrow import (
    _check_pyarrow_schema_compatible,  # pyright: ignore[reportPrivateUsage] -- same check `Transaction.append` makes
    _dataframe_to_data_files,  # pyright: ignore[reportPrivateUsage] -- feeds one snapshot producer from many chunks
)
from pyiceberg.manifest import DataFile
from pyiceberg.table import Table

from floe.iceberg.catalog import run_error

WriteMode = Literal["overwrite", "append"]

RUN_ID = "floe.run_id"
INPUT_HASH = "floe.input_hash"

# `metrics::DEFAULT_SMALL_FILE_THRESHOLD_BYTES`
SMALL_FILE_THRESHOLD_BYTES = 16 * 1024 * 1024
_BYTES_PER_MIB = 1024 * 1024
_REPORTED_FILE_PATHS = 50
CHUNK_ROWS = 1_000_000


@dataclass(frozen=True)
class IcebergWriteResult:
    files_written: int
    snapshot_id: int | None
    metadata_version: int | None
    file_paths: list[str]
    total_bytes_written: int
    avg_file_size_mb: float | None
    small_files_count: int


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
) -> IcebergWriteResult:
    """Data files are written chunk by chunk (partitioned tables split each chunk), then committed once.

    Overwrite replaces every existing file in the same snapshot. An append of no rows commits nothing.
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
    replaced: list[DataFile] = []
    if mode == "overwrite":
        with run_error("iceberg overwrite scan failed"):
            replaced = [task.file for task in table.scan().plan_files()]
    if data_files or replaced:
        with run_error("rest iceberg commit failed"):
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
    return write_result(table, data_files, small_file_threshold_bytes)


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
