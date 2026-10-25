"""The run's Iceberg accepted sink: catalog target, table and one commit (Rust `io/write/iceberg.rs` `write`)."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs

import hashlib
import json
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema, assign_fresh_schema_ids
from pyiceberg.table import Table

from floe.config.storage import (
    CatalogDefinition,
    CatalogsConfig,
    StorageDefinition,
    StoragesConfig,
)
from floe.config.types import EntityConfig
from floe.errors import FloeConfigError, FloeRunError
from floe.iceberg.catalog import (
    PartitionFields,
    ensure_table,
    open_catalog,
    run_error,
)
from floe.iceberg.write import (
    SMALL_FILE_THRESHOLD_BYTES,
    IcebergWriteResult,
    WriteMode,
    write_table,
)
from floe.storage.inputs import InputFile
from floe.storage.paths import normalize_local_path, parse_s3_uri


@dataclass(frozen=True)
class IcebergTarget:
    catalog: CatalogDefinition
    storage: StorageDefinition | None  # the table location's storage
    namespace: str
    table: str
    location: str
    accepted_uri: str  # `sink.accepted.path` resolved, as Rust reports it


def catalog_ident(value: str) -> str:
    """Rust `normalize_catalog_ident`."""
    out = "".join(c.lower() if c.isascii() and (c.isalnum() or c in "_-") else "_" for c in value)  # fmt: skip
    return out.strip("_") or "default"


def _resolve(storages: StoragesConfig | None, config_dir: Path, entity: EntityConfig, storage: str | None, raw: str) -> tuple[str, StorageDefinition | None]:  # fmt: skip
    """Rust `StorageResolver::resolve_path` for local and S3 (validated references)."""
    name = storage or (storages and storages.default) or "local"
    definition = next((d for d in storages.definitions if d.name == name), None) if storages else None  # fmt: skip
    if definition is None or definition.type == "local":
        return f"local://{normalize_local_path(config_dir / raw)}", definition
    if definition.type != "s3" or definition.bucket is None:
        raise FloeConfigError(f"entity.name={entity.name} storage {name} ({definition.type}) is not supported for iceberg")  # fmt: skip
    if raw.startswith("s3://"):
        bucket, key = parse_s3_uri(raw)
        if bucket != definition.bucket:
            raise FloeConfigError(f"storage {name} bucket mismatch: {bucket}")
    else:
        key = "/".join(p for p in ((definition.prefix or "").strip("/"), raw.lstrip("/")) if p)  # fmt: skip
    return f"s3://{definition.bucket}/{key}" if key else f"s3://{definition.bucket}", definition  # fmt: skip


def resolve_target(storages: StoragesConfig | None, catalogs: CatalogsConfig | None, config_dir: Path, entity: EntityConfig) -> IcebergTarget:  # fmt: skip
    """Rust `CatalogResolver::resolve_iceberg_target`; the catalog reference is validated."""
    sink = entity.sink.accepted
    if sink.iceberg is None:
        # ponytail: Rust 0.7 also wrote catalog-less filesystem tables; PyIceberg has no such catalog without sqlalchemy
        raise FloeRunError(f"entity.name={entity.name} sink.accepted.format=iceberg requires sink.accepted.iceberg with a catalog in 0.8")  # fmt: skip
    name = sink.iceberg.catalog or (catalogs and catalogs.default)
    catalog = next((c for c in (catalogs.definitions if catalogs else []) if c.name == name), None)  # fmt: skip
    if catalog is None:  # validated; kept for callers that skip validation
        raise FloeConfigError(f"entity.name={entity.name} sink.accepted.iceberg.catalog references unknown catalog {name}")  # fmt: skip
    namespace = catalog_ident(sink.iceberg.namespace or entity.domain or "default")
    table = catalog_ident(sink.iceberg.table or entity.name)
    accepted_uri, storage = _resolve(
        storages, config_dir, entity, sink.storage, sink.path
    )
    where = catalog.warehouse_storage or sink.storage
    if sink.iceberg.location is not None:
        location, storage = _resolve(storages, config_dir, entity, where, sink.iceberg.location)  # fmt: skip
    elif catalog.warehouse_storage is not None or catalog.warehouse_prefix is not None:
        parts = ((catalog.warehouse_prefix or "").strip("/"), namespace, table)
        location, storage = _resolve(storages, config_dir, entity, where, "/".join(p for p in parts if p))  # fmt: skip
    else:
        location = accepted_uri
    return IcebergTarget(catalog, storage, namespace, table, location, accepted_uri)  # fmt: skip


def existing_table(target: IcebergTarget) -> Table | None:
    catalog = open_catalog(target.catalog, target.storage)
    identifier = (target.namespace, target.table)
    with run_error("rest catalog table_exists check failed"):
        if not catalog.table_exists(identifier):
            return None
    with run_error("rest catalog load_table failed"):
        return catalog.load_table(identifier)


def input_hash(entity: EntityConfig, files: Sequence[InputFile]) -> str:
    """`floe.input_hash`: the entity and its claimed files with size and mtime (ADR "Retry guard"; Rust has none)."""
    claimed = sorted([f.source_uri, f.source_size, f.source_mtime] for f in files)
    return hashlib.sha256(json.dumps([entity.id, claimed]).encode()).hexdigest()


_WIDER: dict[pl.DataType, pl.DataType] = {pl.UInt8(): pl.Int32(), pl.UInt16(): pl.Int32(), pl.UInt32(): pl.Int64()}  # fmt: skip


def _reader(entity: EntityConfig, frames: list[pl.DataFrame], names: list[str]) -> pa.RecordBatchReader:  # fmt: skip
    """Output columns in schema order; `nullable: false` columns are required, as in Rust."""
    columns = entity.schema_.columns
    for name, c in zip(names, columns, strict=True):
        if c.nullable is False and any(f[name].null_count() for f in frames):
            raise FloeRunError(f"iceberg write rejected nulls for non-nullable column {name}")  # fmt: skip

    def arrow(df: pl.DataFrame) -> pa.Table:  # Iceberg has no unsigned types
        return df.select(pl.col(n).cast(_WIDER.get(df.schema[n], df.schema[n])) for n in names).to_arrow()  # fmt: skip

    schema = arrow(frames[0].head(0)).schema
    for i, c in enumerate(columns):
        schema = schema.set(i, schema.field(i).with_nullable(c.nullable is not False))

    def batches() -> Iterator[pa.RecordBatch]:
        for df in frames:
            yield from arrow(df).cast(schema).to_batches()

    return pa.RecordBatchReader.from_batches(schema, batches())


def partition_fields(entity: EntityConfig) -> PartitionFields | None:
    """`partition_spec` as `ensure_table` takes it; a bucket is PyIceberg's `bucket[N]` (D12)."""
    spec = entity.sink.accepted.partition_spec
    if spec is None:
        return None
    return [(f.column, f.transform if f.num_buckets is None else f"bucket[{f.num_buckets}]") for f in spec]  # fmt: skip


def write_iceberg(
    entity: EntityConfig,
    target: IcebergTarget,
    frames: list[pl.DataFrame],
    names: list[str],
    mode: WriteMode,
    run_id: str,
    digest: str,
) -> tuple[IcebergWriteResult, dict[str, Any]]:
    """`frames` hold the output columns `names`; returns the result and its `accepted_output` fields."""
    reader = _reader(entity, frames, names)
    arrow_schema = Catalog._convert_schema_if_needed(reader.schema)  # pyright: ignore[reportPrivateUsage] -- PyIceberg's own pyarrow -> Iceberg conversion
    schema: Schema = assign_fresh_schema_ids(arrow_schema)
    evolution = entity.schema_.schema_evolution
    catalog = open_catalog(target.catalog, target.storage)
    table = ensure_table(
        catalog, target.namespace, target.table, schema, entity.name, target.location.removeprefix("local://"),
        partition_fields(entity), evolution.mode if evolution else "strict",
    )  # fmt: skip
    options = entity.sink.accepted.options
    size = options.max_size_per_file if options else None
    # Rust `default_small_file_threshold_bytes`
    threshold = SMALL_FILE_THRESHOLD_BYTES if size is None else max(1, size // 2)
    result = write_table(table, reader, mode, run_id, digest, threshold)
    ids = {"table_version": result.metadata_version, "snapshot_id": result.snapshot_id}
    return result, {k: v for k, v in ids.items() if v is not None} | {
        "iceberg_catalog_name": target.catalog.name,
        "iceberg_namespace": target.namespace,
        "iceberg_table": target.table,
    }
