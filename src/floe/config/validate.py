"""Schema validation, ported from `config/validate.rs` (`validate_schema` and its helpers)."""
# `_get` is unused since #733; removed in #735
# pyright: reportUnusedFunction=false

from typing import cast

from floe.config.project import RootConfig, config_version
from floe.config.types import EntityConfig
from floe.errors import FloeConfigError

MAX_JSON_COLUMNS = 1024
NORMALIZE_STRATEGIES = ("snake_case", "lower", "camel_case", "none")
MISMATCH_MISSING = ("reject_file", "fill_nulls")
MISMATCH_EXTRA = ("reject_file", "ignore")
# D10 (#619): Rust 0.7 also lists `time`; it is not supported in 0.8.
COLUMN_TYPES = ("string", "number", "boolean", "datetime", "date")
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


def validate_schemas(config: RootConfig) -> None:
    for entity in config.entities:
        validate_schema(entity, config.version)
