"""Entity validation, ported from `config/validate.rs` (`validate_sink`, `validate_schema` and their helpers)."""
# `_get` is unused since #733; removed in #735
# pyright: reportUnusedFunction=false

from typing import cast

from floe.config.project import RootConfig, config_version
from floe.config.types import EntityConfig, WriteMode
from floe.errors import FloeConfigError

MAX_JSON_COLUMNS = 1024
NORMALIZE_STRATEGIES = ("snake_case", "lower", "camel_case", "none")
MISMATCH_MISSING = ("reject_file", "fill_nulls")
MISMATCH_EXTRA = ("reject_file", "ignore")
# D10 (#619): Rust 0.7 also lists `time`; it is not supported in 0.8.
COLUMN_TYPES = ("string", "number", "boolean", "datetime", "date")
SINK_FORMATS = ("parquet", "iceberg")
PARQUET_COMPRESSIONS = ("snappy", "gzip", "zstd", "uncompressed")
PARTITION_TRANSFORMS = ("identity", "year", "month", "day", "hour")
# D2 (#619): Iceberg accepts the merge modes; Rust 0.7 allows them for Delta (D1) and DuckDB (D1) only.
SINK_WRITE_MODES: dict[str, tuple[WriteMode, ...]] = {
    "parquet": ("overwrite", "append"),
    "iceberg": ("overwrite", "append", "merge_scd1", "merge_scd2"),
}
SCD2_DEFAULTS = {
    "current_flag_column": "__floe_is_current",
    "valid_from_column": "__floe_valid_from",
    "valid_to_column": "__floe_valid_to",
}
# Same normalisation as `checks/cast.py`, so every accepted type casts.
_TYPE_NAMES = {
    *("string", "str", "text", "boolean", "bool", "datetime", "timestamp", "date"),
    *("number", "float", "float32", "float64", "double", "decimal", "int"),
    *("int8", "int16", "int32", "int64", "integer", "long"),
    *("uint8", "uint16", "uint32", "uint64"),
}  # fmt: skip


def _canonical(value: str) -> str:
    # D7 (#584 S1, S2): no trim, unlike Rust's `normalize_value`; the run-time paths never trimmed.
    return value.lower().replace("-", "").replace("_", "")


def _get(value: object, key: str) -> object:
    return (
        cast(dict[object, object], value).get(key) if isinstance(value, dict) else None
    )


def _fail(entity: EntityConfig, message: str) -> FloeConfigError:
    return FloeConfigError(f"entity.name={entity.name} {message}")


def _check_primary_key(entity: EntityConfig) -> None:
    primary_key = entity.schema_.primary_key
    if primary_key is None:
        return
    if not primary_key:
        raise _fail(entity, "schema.primary_key must not be empty")
    columns = {c.name: c for c in reversed(entity.schema_.columns)}
    seen: set[str] = set()
    for index, name in enumerate(primary_key):
        value = name.strip()
        if not value:
            raise _fail(entity, f"schema.primary_key[{index}] must not be empty")
        if value in seen:
            raise _fail(entity, f"schema.primary_key has duplicate column {value}")
        seen.add(value)
        if value not in columns:
            raise _fail(
                entity,
                f"schema.primary_key[{index}]={value} references unknown schema column",
            )
        if columns[value].nullable is True:
            raise _fail(
                entity, f"schema.primary_key column {value} cannot set nullable=true"
            )


def _check_unique_keys(entity: EntityConfig) -> None:
    # ponytail: Rust's override and dedup warnings are not ported; they are not errors.
    unique_keys = entity.schema_.unique_keys
    if unique_keys is None:
        return
    if not unique_keys:
        raise _fail(entity, "schema.unique_keys must not be empty")
    names = {c.name for c in entity.schema_.columns}
    for index, key in enumerate(unique_keys):
        where = f"schema.unique_keys[{index}]"
        if not key:
            raise _fail(entity, f"{where} must not be empty")
        seen: set[str] = set()
        for column_index, name in enumerate(key):
            value = name.strip()
            if not value:
                raise _fail(entity, f"{where}[{column_index}] must not be empty")
            if value not in names:
                raise _fail(
                    entity,
                    f"{where}[{column_index}]={value} references unknown schema column",
                )
            if value in seen:
                raise _fail(entity, f"{where} has duplicate column {value}")
            seen.add(value)


def _check_evolution(entity: EntityConfig, version: str) -> None:
    evolution = entity.schema_.schema_evolution
    if evolution is None:
        return
    if config_version(version) < (0, 2):
        raise _fail(entity, 'schema.schema_evolution requires root.version >= "0.2"')
    # D13 (#619): add_columns is accepted on Iceberg; Rust allows only Delta, which D1 drops.
    if evolution.mode == "add_columns" and entity.sink.accepted.format != "iceberg":
        raise _fail(
            entity,
            f"schema.schema_evolution.mode={evolution.mode} requires "
            "sink.accepted.format=delta (Delta-only additive schema evolution in this phase)",
        )


def _merge_list(entity: EntityConfig, field: str, values: list[str]) -> None:
    names = {c.name for c in entity.schema_.columns}
    primary_key = {c.strip() for c in entity.schema_.primary_key or []}
    where = f"sink.accepted.merge.{field}"
    seen: set[str] = set()
    for index, raw in enumerate(values):
        value = raw.strip()
        if not value:
            raise _fail(entity, f"{where}[{index}] must not be empty")
        if value not in names:
            raise _fail(
                entity, f"{where}[{index}]={value} references unknown schema column"
            )
        if value in seen:
            raise _fail(entity, f"{where} has duplicate column {value}")
        seen.add(value)
    for index, raw in enumerate(values):
        if raw.strip() in primary_key:
            raise _fail(
                entity,
                f"{where}[{index}]={raw.strip()} cannot reference schema.primary_key column",
            )


def _check_merge(entity: EntityConfig) -> None:
    merge = entity.sink.accepted.merge
    if merge is None:
        return
    mode = entity.sink.write_mode
    if not any(
        m.startswith("merge") for m in SINK_WRITE_MODES[entity.sink.accepted.format]
    ):
        raise _fail(
            entity,
            "sink.accepted.merge is only supported when sink.accepted.format supports merge (e.g. delta)",
        )
    if mode not in ("merge_scd1", "merge_scd2"):
        raise _fail(
            entity,
            "sink.accepted.merge is only supported with sink.write_mode=merge_scd1 or merge_scd2",
        )
    for field in ("ignore_columns", "compare_columns"):
        values = getattr(merge, field)
        if values is not None:
            _merge_list(entity, field, values)
    if merge.scd2 is None:
        return
    if mode != "merge_scd2":
        raise _fail(
            entity,
            "sink.accepted.merge.scd2 is only supported with sink.write_mode=merge_scd2",
        )
    # ponytail: schema names are not normalised (`normalize_columns` lands with #640), so a
    # collision with a normalised output name is missed.
    names = {c.name for c in entity.schema_.columns}
    resolved = {
        field: (default if (v := getattr(merge.scd2, field)) is None else v).strip()
        for field, default in SCD2_DEFAULTS.items()
    }
    for field, value in resolved.items():
        if not value:
            raise _fail(entity, f"sink.accepted.merge.scd2.{field} must not be empty")
        if value in names:
            raise _fail(
                entity,
                f"sink.accepted.merge.scd2.{field}={value} collides with schema column name",
            )
    if len(set(resolved.values())) != len(resolved):
        raise _fail(entity, "sink.accepted.merge.scd2 column names must be unique")


def _check_write_mode(entity: EntityConfig) -> None:
    mode, fmt = entity.sink.write_mode, entity.sink.accepted.format
    if mode not in SINK_WRITE_MODES[fmt]:
        raise _fail(
            entity,
            f"sink.write_mode={mode} is not supported by sink.accepted.format={fmt}",
        )
    if mode in ("merge_scd1", "merge_scd2"):
        primary_key = entity.schema_.primary_key
        if primary_key is None:
            raise _fail(entity, f"sink.write_mode={mode} requires schema.primary_key")
        if not primary_key:
            raise _fail(
                entity, f"sink.write_mode={mode} requires non-empty schema.primary_key"
            )
    _check_merge(entity)


def _check_partitioning(entity: EntityConfig) -> None:
    accepted = entity.sink.accepted
    if accepted.partition_by is not None and accepted.partition_spec is not None:
        raise _fail(
            entity,
            "sink.accepted.partition_by and sink.accepted.partition_spec are mutually exclusive",
        )
    # ponytail: `partition_by` is Delta-only (D1), so its per-column checks are unreachable.
    if accepted.partition_by is not None:
        raise _fail(
            entity,
            "sink.accepted.partition_by is only supported for sink.accepted.format=delta",
        )
    spec = accepted.partition_spec
    if spec is None:
        return
    where = "sink.accepted.partition_spec"
    if accepted.format != "iceberg":
        raise _fail(
            entity, f"{where} is only supported for sink.accepted.format=iceberg"
        )
    if not spec:
        raise _fail(entity, f"{where} must not be empty")
    names = {c.name for c in entity.schema_.columns}
    seen: set[tuple[str, str]] = set()
    for index, field in enumerate(spec):
        column = field.column.strip()
        if not column:
            raise _fail(entity, f"{where}[{index}].column must not be empty")
        if column not in names:
            raise _fail(
                entity,
                f"{where}[{index}].column={column} references unknown schema column",
            )
        transform = field.transform.strip().lower()
        if transform not in PARTITION_TRANSFORMS:
            raise _fail(
                entity,
                f"{where}[{index}].transform={field.transform} is unsupported "
                f"(allowed: {', '.join(PARTITION_TRANSFORMS)})",
            )
        if (column, transform) in seen:
            raise _fail(entity, f"{where} has duplicate entry ({column}, {transform})")
        seen.add((column, transform))


def _storage_name(config: RootConfig, name: str | None) -> str:
    # ponytail: no unknown-storage errors here; the storage registry lands with its own task.
    if name is not None:
        return name
    return (config.storages and config.storages.default) or "local"


def _storage_type(config: RootConfig, name: str) -> str:
    definitions = config.storages.definitions if config.storages else []
    return next((d.type for d in definitions if d.name == name), "local")


def _check_iceberg(entity: EntityConfig, config: RootConfig) -> None:
    accepted = entity.sink.accepted
    iceberg = accepted.iceberg
    if iceberg is None:
        return
    if accepted.format != "iceberg":
        raise _fail(
            entity,
            "sink.accepted.iceberg is only supported for sink.accepted.format=iceberg",
        )
    catalogs = config.catalogs
    name = (
        iceberg.catalog
        if iceberg.catalog is not None
        else catalogs and catalogs.default
    )
    if name is None:
        raise _fail(
            entity,
            "sink.accepted.iceberg.catalog is required (or set catalogs.default)",
        )
    definitions = catalogs.definitions if catalogs else []
    definition = next((c for c in definitions if c.name == name), None)
    if definition is None:
        raise _fail(
            entity, f"sink.accepted.iceberg.catalog references unknown catalog {name}"
        )
    # Unity and Glue are rejected at parse (D1), so only REST catalogs get here.
    if definition.warehouse_storage is None:
        kind = _storage_type(config, _storage_name(config, accepted.storage))
        if kind not in ("s3", "gcs"):
            raise _fail(
                entity,
                f"sink.accepted.iceberg.catalog requires sink.accepted storage type s3 or gcs (got {kind})",
            )
    else:
        kind = _storage_type(config, definition.warehouse_storage)
        if kind not in ("s3", "gcs"):
            raise FloeConfigError(
                f"catalogs.definitions name={definition.name} warehouse_storage must "
                f"reference s3 or gcs storage for rest catalog (got {kind})"
            )


def _check_sink(entity: EntityConfig, config: RootConfig, profiled: bool) -> None:
    sink, accepted = entity.sink, entity.sink.accepted
    if accepted.format in ("delta", "duckdb"):  # D1 (#619): reported first
        raise _fail(
            entity, f"sink.accepted.format={accepted.format} is not supported in 0.8"
        )
    if accepted.format not in SINK_FORMATS:
        raise _fail(entity, f"sink.accepted.format={accepted.format} is unsupported")
    options = accepted.options
    if options is not None and accepted.format == "parquet":
        if options.compression not in (None, *PARQUET_COMPRESSIONS):
            raise _fail(
                entity,
                f"sink.accepted.options.compression={options.compression} is unsupported "
                f"(allowed: {', '.join(PARQUET_COMPRESSIONS)})",
            )
        for field in ("row_group_size", "max_size_per_file"):
            if getattr(options, field) == 0:
                raise _fail(
                    entity, f"sink.accepted.options.{field} must be greater than 0"
                )
    if entity.policy.severity == "reject" and sink.rejected is None:
        raise _fail(entity, "sink.rejected is required when policy.severity=reject")
    if sink.rejected is not None and sink.rejected.format != "csv":
        raise _fail(
            entity, f"sink.rejected.format={sink.rejected.format} is unsupported"
        )
    # ponytail: a profile can add catalogs and storages, which land with #635; skip the binding.
    if not profiled:
        _check_iceberg(entity, config)
    _check_partitioning(entity)
    _check_write_mode(entity)
    archive = sink.archive
    source = _storage_name(config, entity.source.storage)
    if archive is not None and archive.storage not in (None, source):
        raise _fail(
            entity, f"sink.archive.storage must match source.storage ({source})"
        )


def validate_schema(entity: EntityConfig, version: str) -> None:
    schema = entity.schema_
    if entity.source.format == "json" and len(schema.columns) > MAX_JSON_COLUMNS:
        raise _fail(
            entity,
            f"schema.columns has {len(schema.columns)} entries which exceeds "
            f"the JSON selector limit of {MAX_JSON_COLUMNS}",
        )
    strategy = schema.normalize_columns and schema.normalize_columns.strategy
    if strategy is not None and _canonical(strategy) not in {
        _canonical(s) for s in NORMALIZE_STRATEGIES
    }:
        raise _fail(
            entity,
            f"schema.normalize_columns.strategy={strategy} is unsupported "
            f"(allowed: {', '.join(NORMALIZE_STRATEGIES)})",
        )
    mismatch = schema.mismatch
    if mismatch is not None:
        for field, policy, allowed in (
            ("missing_columns", mismatch.missing_columns, MISMATCH_MISSING),
            ("extra_columns", mismatch.extra_columns, MISMATCH_EXTRA),
        ):
            if policy is not None and policy not in allowed:
                raise _fail(
                    entity,
                    f"schema.mismatch.{field}={policy} is unsupported "
                    f"(allowed: {', '.join(allowed)})",
                )
    for index, column in enumerate(schema.columns):
        if _canonical(column.column_type) not in _TYPE_NAMES:
            raise _fail(
                entity,
                f"schema.columns[{index}].type={column.column_type} is unsupported "
                f"for column {column.name} (allowed: {', '.join(COLUMN_TYPES)})",
            )
    _check_primary_key(entity)
    _check_unique_keys(entity)
    _check_evolution(entity, version)


def validate_entities(config: RootConfig, profiled: bool = False) -> None:
    for entity in config.entities:
        _check_sink(entity, config, profiled)
        validate_schema(entity, config.version)
