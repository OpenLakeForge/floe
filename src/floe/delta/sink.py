"""The run's Delta accepted sink: table location, storage options and the Arrow stream of one commit."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs

from collections.abc import Iterator
from itertools import chain
from typing import Any

import polars as pl
import pyarrow as pa
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from floe.checks.unique import resolve_unique_keys
from floe.config.storage import StorageDefinition
from floe.config.types import EntityConfig, WriteMode
from floe.delta.write import write_table
from floe.errors import FloeRunError
from floe.write.outputs import AcceptedWrite

# Delta has no unsigned integers: delta-rs narrows them to the signed type of the same width and a value above its range
# corrupts the table, so they widen as the Iceberg sink does (`iceberg/sink.py:_WIDER`; that module imports PyIceberg, an
# optional extra a Delta-only install lacks, so the map is repeated here).
_WIDER: dict[pl.DataType, pl.DataType] = {pl.UInt8(): pl.Int32(), pl.UInt16(): pl.Int32(), pl.UInt32(): pl.Int64()}  # fmt: skip


def check_supported(entity: EntityConfig) -> None:
    """Refuse, before anything is claimed, what the writer would otherwise skip silently."""
    who, accepted = f"entity.name={entity.name}", entity.sink.accepted
    if entity.sink.write_mode not in ("overwrite", "append"):
        raise FloeRunError(f"{who} sink.write_mode={entity.sink.write_mode} is not supported with sink.accepted.format=delta yet", code="run_not_supported")  # fmt: skip
    if accepted.partition_by:
        raise FloeRunError(f"{who} sink.accepted.partition_by is not supported with sink.accepted.format=delta yet", code="run_not_supported")  # fmt: skip
    # the existing table's keys are not read yet, so an append could not tell a duplicate from a first write
    if entity.sink.write_mode == "append" and resolve_unique_keys(entity.schema_.unique_keys, entity.schema_.primary_key):  # fmt: skip
        raise FloeRunError(f"{who} unique keys on a delta append are not supported yet", code="run_not_supported")  # fmt: skip


def storage_options(storage: StorageDefinition | None) -> dict[str, str]:
    """delta-rs options for an S3 storage; credentials come from the `AWS_*` environment, as for Floe's own S3 client."""
    if storage is None or storage.type == "local":
        return {}
    options = {k: v for k, v in (("AWS_REGION", storage.region), ("AWS_ENDPOINT_URL", storage.endpoint)) if v}  # fmt: skip
    if storage.endpoint and storage.endpoint.startswith("http://"):
        options["AWS_ALLOW_HTTP"] = "true"
    if storage.path_style_access is False:
        options["AWS_VIRTUAL_HOSTED_STYLE_REQUEST"] = "true"
    return options


def _reader(entity: EntityConfig, frames: Iterator[pl.DataFrame], names: list[str]) -> pa.RecordBatchReader:  # fmt: skip
    """Output columns in schema order, from at least one frame; `nullable: false` columns are required (delta-rs rejects a null)."""
    first = next(frames)

    def arrow(df: pl.DataFrame) -> pa.Table:
        return df.select(pl.col(n).cast(_WIDER.get(df.schema[n], df.schema[n])) for n in names).to_arrow()  # fmt: skip

    for n in names:  # before the table is created
        if first.schema[n] == pl.UInt64:
            raise FloeRunError(f"entity.name={entity.name} sink.accepted.format=delta cannot store the uint64 column {n}: Delta has no unsigned integers", code="run_not_supported")  # fmt: skip
    schema = arrow(first.head(0)).schema
    for i, c in enumerate(entity.schema_.columns):
        schema = schema.set(i, schema.field(i).with_nullable(c.nullable is not False))
    frames = chain([first], frames)  # `batches` must not keep `first` alive
    del first

    def batches() -> Iterator[pa.RecordBatch]:
        for df in frames:
            yield from arrow(df).cast(schema).to_batches()

    return pa.RecordBatchReader.from_batches(schema, batches())


def existing_table(uri: str, storage: StorageDefinition | None) -> DeltaTable | None:
    """The table at `uri`, None before the first write; the retry guard reads its `txn` actions."""
    try:
        return DeltaTable(uri.removeprefix("local://"), storage_options=storage_options(storage))  # delta-rs takes a plain path for local storage  # fmt: skip
    except TableNotFoundError:
        return None


def write_delta(entity: EntityConfig, uri: str, storage: StorageDefinition | None, frames: Iterator[pl.DataFrame], names: list[str], mode: WriteMode, run_id: str, input_hash: str | None) -> tuple[AcceptedWrite, dict[str, Any], bool]:  # fmt: skip
    """`frames` (at least one) hold the output columns `names`; returns the commit's files, the `accepted_output` table fields and whether a racing run had committed this batch."""
    reader = _reader(entity, frames, names)
    path = uri.removeprefix("local://")  # delta-rs takes a plain path for local storage
    written, version, raced = write_table(f"entity.name={entity.name}", path, storage_options(storage), reader, mode, run_id, input_hash)  # fmt: skip
    return written, {"table_version": version}, raced
