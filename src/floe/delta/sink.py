"""The run's Delta accepted sink: table location, storage options and the Arrow stream of one commit."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs

from collections.abc import Iterator
from itertools import chain
from typing import Any

import polars as pl
import pyarrow as pa
from deltalake import DeltaTable
from deltalake.exceptions import DeltaError, TableNotFoundError

from floe.config.storage import CatalogsConfig, StorageDefinition
from floe.config.types import EntityConfig, WriteMode, max_level
from floe.delta.unity import register
from floe.delta.write import named, write_table
from floe.errors import FloeRunError
from floe.write.outputs import AcceptedWrite

# Delta has no unsigned integers: delta-rs narrows them to the signed type of the same width and a value above its range
# corrupts the table, so they widen as the Iceberg sink does (`iceberg/sink.py:_WIDER`; that module imports PyIceberg, an
# optional extra a Delta-only install lacks, so the map is repeated here).
_WIDER: dict[pl.DataType, pl.DataType] = {pl.UInt8(): pl.Int32(), pl.UInt16(): pl.Int32(), pl.UInt32(): pl.Int64()}  # fmt: skip


def check_supported(entity: EntityConfig) -> None:
    """Refuse, before anything is claimed, what the writer would otherwise skip silently."""
    who = f"entity.name={entity.name}"
    if entity.sink.write_mode not in ("overwrite", "append"):
        raise FloeRunError(f"{who} sink.write_mode={entity.sink.write_mode} is not supported with sink.accepted.format=delta yet", code="run_not_supported")  # fmt: skip
    # an aborted file leaves a smaller batch committed than the hash the retry guard stamps names
    if entity.sink.write_mode == "append" and max_level(entity) == "abort":
        cause = (
            "policy.severity=abort"
            if entity.policy.severity == "abort"
            else "a rule_severity of abort"
        )
        raise FloeRunError(f"{who} {cause} on a delta append is not supported yet: the retry guard cannot cover it", code="run_not_supported")  # fmt: skip


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


def existing_table(entity: EntityConfig, uri: str, storage: StorageDefinition | None) -> DeltaTable | None:  # fmt: skip
    """The table at `uri`, None before the first write; the retry guard reads its `txn` actions."""
    try:
        return DeltaTable(uri.removeprefix("local://"), storage_options=storage_options(storage))  # delta-rs takes a plain path for local storage  # fmt: skip
    except TableNotFoundError:
        return None
    except (DeltaError, OSError) as err:
        raise named(f"entity.name={entity.name}", uri, err) from err


def write_delta(entity: EntityConfig, uri: str, storage: StorageDefinition | None, frames: Iterator[pl.DataFrame], names: list[str], mode: WriteMode, run_id: str, input_hash: str | None, table: DeltaTable | None, claimed_ms: int | None = None) -> tuple[AcceptedWrite, dict[str, Any], bool]:  # fmt: skip
    """`frames` (at least one) hold the output columns `names`, `table` is the snapshot the retry guard checked; returns the commit's files, the `accepted_output` table fields and whether a rival had committed this batch."""
    reader = _reader(entity, frames, names)
    path = uri.removeprefix("local://")  # delta-rs takes a plain path for local storage
    out = dict(zip((c.name for c in entity.schema_.columns), names, strict=True))  # `partition_by` names config columns; the table has the output names  # fmt: skip
    by = [out[c] for c in entity.sink.accepted.partition_by] if entity.sink.accepted.partition_by else None  # fmt: skip
    written, version, raced = write_table(f"entity.name={entity.name}", path, storage_options(storage), reader, mode, run_id, input_hash, table, by, claimed_ms)  # fmt: skip
    return written, {"table_version": version}, raced


def register_unity(entity: EntityConfig, catalogs: CatalogsConfig | None, uri: str, storage: StorageDefinition | None) -> dict[str, str]:  # fmt: skip
    """After the commit, on every run: registers the table in Unity Catalog; returns the report fields (none without a `delta:` block or a table)."""
    delta = entity.sink.accepted.delta
    table = existing_table(entity, uri, storage) if delta else None
    if delta is None or table is None:  # no table when no run ever had a row to write
        return {}
    name = delta.catalog or (catalogs.default if catalogs else None)
    definition = next(c for c in catalogs.definitions if c.name == name) if catalogs else None  # validation checked it  # fmt: skip
    if definition is None or delta.schema_ is None or delta.table is None:
        raise FloeRunError(f"entity.name={entity.name} sink.accepted.delta does not name a unity catalog, schema and table", code="run_invalid_setup")  # fmt: skip
    register(definition, delta.schema_, delta.table, uri, table)
    return {
        "delta_catalog_name": definition.name,
        "delta_schema": delta.schema_,
        "delta_table": delta.table,
    }
