"""Entity parsing, ported from `config/parse.rs` (`parse_entity` and the entity-level blocks)."""

import string
from collections.abc import Callable
from typing import TypeVar, get_args

from pydantic import BaseModel

from floe.config.types import (
    ArchiveTarget,
    ColumnConfig,
    EntityConfig,
    EntityMetadata,
    EntityStateConfig,
    IcebergPartitionField,
    IcebergSinkOptions,
    IncrementalMode,
    MergeOptions,
    MergeScd2Options,
    NormalizeColumnsConfig,
    PiiConfig,
    PolicyConfig,
    PolicySeverity,
    SchemaConfig,
    SchemaEvolutionConfig,
    SchemaEvolutionIncompatibleAction,
    SchemaEvolutionMode,
    SchemaMismatchConfig,
    SinkConfig,
    SinkOptions,
    SinkTarget,
    SourceConfig,
    SourceOptions,
    WriteMode,
)
from floe.config.yaml import (
    get_array,
    get_string,
    get_value,
    opt_bool,
    opt_string,
    opt_u64,
    opt_vec_string,
    opt_vec_vec_string,
    rust_debug,
    validate_known_keys,
    yaml_array,
    yaml_hash,
)
from floe.errors import FloeConfigError, FloeError
from floe.pii import PiiColumn, Strategy

_T = TypeVar("_T", bound=str)
_R = TypeVar("_R")
_M = TypeVar("_M", bound=BaseModel)
_ASCII_LOWER = str.maketrans(string.ascii_uppercase, string.ascii_lowercase)
# Rust's `trim()` strips Unicode White_Space only; `str.strip()` also strips U+001C to U+001F
WHITE_SPACE = "\t\n\v\f\r \x85\xa0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000"


def _or(value: _R | None, default: _R) -> _R:
    return default if value is None else value


def _pick(value: str, allowed: tuple[_T, ...]) -> _T | None:
    return next((option for option in allowed if option == value), None)


def _norm(value: str) -> str:
    return value.strip(WHITE_SPACE).translate(_ASCII_LOWER)


def _choice(value: str, allowed: tuple[_T, ...], ctx: str) -> _T:
    picked = _pick(_norm(value), allowed)
    if picked is None:
        raise FloeConfigError(
            f"unsupported value at {ctx}: {value} (allowed: {', '.join(allowed)})"
        )
    return picked


def _each(items: list[object], prefix: str, parse: Callable[[object], _R]) -> list[_R]:
    out: list[_R] = []
    for index, item in enumerate(items):
        try:
            out.append(parse(item))
        except FloeError as e:
            raise FloeConfigError(f"{prefix}[{index}]: {e}") from e
    return out


def _metadata(value: object) -> EntityMetadata:
    h = yaml_hash(value, "entity.metadata")
    return EntityMetadata(
        data_product=opt_string(h, "data_product", "entity.metadata"),
        domain=opt_string(h, "domain", "entity.metadata"),
        owner=opt_string(h, "owner", "entity.metadata"),
        description=opt_string(h, "description", "entity.metadata"),
        tags=opt_vec_string(h, "tags", "entity.metadata"),
    )


def _state(value: object) -> EntityStateConfig:
    h = yaml_hash(value, "entity.state")
    validate_known_keys(h, "entity.state", ("path",))
    return EntityStateConfig(path=opt_string(h, "path", "entity.state"))


def _policy(value: object) -> PolicyConfig:
    h = yaml_hash(value, "policy")
    validate_known_keys(h, "policy", ("severity",))
    severity = get_string(h, "severity", "policy")
    picked = _pick(severity, get_args(PolicySeverity))
    if picked is None:
        raise FloeConfigError(
            f"policy.severity={severity} is unsupported (allowed: warn, reject, abort)"
        )
    return PolicyConfig(severity=picked)


def _normalize_columns(value: object) -> NormalizeColumnsConfig:
    ctx = "schema.normalize_columns"
    h = yaml_hash(value, ctx)
    validate_known_keys(h, ctx, ("enabled", "strategy"))
    return NormalizeColumnsConfig(
        enabled=opt_bool(h, "enabled", ctx), strategy=opt_string(h, "strategy", ctx)
    )


def _mismatch(value: object) -> SchemaMismatchConfig:
    h = yaml_hash(value, "schema.mismatch")
    validate_known_keys(h, "schema.mismatch", ("missing_columns", "extra_columns"))
    return SchemaMismatchConfig(
        missing_columns=opt_string(h, "missing_columns", "schema.mismatch"),
        extra_columns=opt_string(h, "extra_columns", "schema.mismatch"),
    )


def _schema_evolution(value: object) -> SchemaEvolutionConfig:
    ctx = "schema.schema_evolution"
    h = yaml_hash(value, ctx)
    validate_known_keys(h, ctx, ("mode", "on_incompatible"))
    mode = opt_string(h, "mode", ctx)
    mode_value: SchemaEvolutionMode = (
        "strict"
        if mode is None
        else _choice(mode, get_args(SchemaEvolutionMode), f"{ctx}.mode")
    )
    action = opt_string(h, "on_incompatible", ctx)
    return SchemaEvolutionConfig(
        mode=mode_value,
        on_incompatible="fail"
        if action is None
        else _choice(
            action,
            get_args(SchemaEvolutionIncompatibleAction),
            f"{ctx}.on_incompatible",
        ),
    )


def _column(value: object) -> ColumnConfig:
    ctx = "schema.columns"
    h = yaml_hash(value, ctx)
    validate_known_keys(
        h, ctx, ("name", "source", "type", "nullable", "unique", "width", "trim")
    )
    return ColumnConfig(
        name=get_string(h, "name", ctx),
        source=opt_string(h, "source", ctx),
        column_type=get_string(h, "type", ctx),
        nullable=opt_bool(h, "nullable", ctx),
        unique=opt_bool(h, "unique", ctx),
        width=opt_u64(h, "width", ctx),
        trim=opt_bool(h, "trim", ctx),
    )


def _schema(value: object) -> SchemaConfig:
    h = yaml_hash(value, "schema")
    validate_known_keys(
        h,
        "schema",
        (
            "normalize_columns",
            "mismatch",
            "schema_evolution",
            "primary_key",
            "unique_keys",
            "columns",
        ),
    )
    normalize = (
        _normalize_columns(h["normalize_columns"]) if "normalize_columns" in h else None
    )
    mismatch = _mismatch(h["mismatch"]) if "mismatch" in h else None
    evolution = (
        _schema_evolution(h["schema_evolution"]) if "schema_evolution" in h else None
    )
    primary_key = opt_vec_string(h, "primary_key", "schema")
    unique_keys = opt_vec_vec_string(h, "unique_keys", "schema")
    columns = _each(get_array(h, "columns", "schema"), "schema.columns", _column)
    return SchemaConfig(
        normalize_columns=normalize,
        mismatch=mismatch,
        schema_evolution=evolution,
        primary_key=primary_key,
        unique_keys=unique_keys,
        columns=columns,
    )


def _pii_column(value: object) -> PiiColumn:
    ctx = "pii.columns"
    h = yaml_hash(value, ctx)
    validate_known_keys(
        h, ctx, ("name", "strategy", "mask_pattern", "redact_value", "key")
    )
    name = get_string(h, "name", ctx)
    key = _norm(get_string(h, "strategy", ctx))
    strategy = _pick(key, get_args(Strategy))
    if strategy is None:
        raise FloeConfigError(
            f"pii.columns[name={name}].strategy={key} is unsupported "
            "(allowed: hash, drop, nullify, redact, mask)"
        )
    return PiiColumn(
        name=name,
        strategy=strategy,
        mask_pattern=opt_string(h, "mask_pattern", ctx),
        redact_value=opt_string(h, "redact_value", ctx),
        key=opt_string(h, "key", ctx),
    )


def _pii(value: object) -> PiiConfig:
    h = yaml_hash(value, "pii")
    validate_known_keys(h, "pii", ("columns",))
    return PiiConfig(
        columns=_each(get_array(h, "columns", "pii"), "pii.columns", _pii_column)
    )


def _source_options(value: object, defaults: SourceOptions) -> SourceOptions:
    ctx = "source.options"
    h = yaml_hash(value, ctx)
    validate_known_keys(h, ctx, tuple(SourceOptions.model_fields))
    readers: dict[str, Callable[[dict[object, object], str, str], object]] = {
        **dict.fromkeys(("header", "recursive"), opt_bool),
        **dict.fromkeys(("header_row", "data_row"), opt_u64),
        "null_values": opt_vec_string,
    }
    read = {
        k: readers.get(k, opt_string)(h, k, ctx) for k in SourceOptions.model_fields
    }
    # a null or absent key keeps the format default
    return defaults.model_copy(update={k: v for k, v in read.items() if v is not None})


def _storage(h: dict[object, object], ctx: str) -> str | None:
    storage = opt_string(h, "storage", ctx)
    filesystem = opt_string(h, "filesystem", ctx)
    if storage is not None and filesystem is not None:
        raise FloeConfigError(f"{ctx}.storage and {ctx}.storage are mutually exclusive")
    return _or(storage, filesystem)


def _source(value: object, ref: str | None, resource: str | None) -> SourceConfig:
    h = yaml_hash(value, "source")
    validate_known_keys(
        h,
        "source",
        (
            "format",
            "path",
            "storage",
            "filesystem",
            "options",
            "cast_mode",
        ),
    )
    fmt = get_string(h, "format", "source")
    defaults = SourceOptions(separator="\t") if fmt == "tsv" else SourceOptions()
    options = _source_options(h["options"], defaults) if "options" in h else defaults
    return SourceConfig(
        format=fmt,
        storage=_storage(h, "source"),
        path=get_string(h, "path", "source"),
        options=options,
        cast_mode=_or(opt_string(h, "cast_mode", "source"), "strict"),
        source_ref=ref,
        resource=resource,
    )


def _sink_options(value: object, ctx: str) -> SinkOptions:
    h = yaml_hash(value, ctx)
    validate_known_keys(h, ctx, tuple(SinkOptions.model_fields))
    return SinkOptions(
        compression=opt_string(h, "compression", ctx),
        row_group_size=opt_u64(h, "row_group_size", ctx),
        max_size_per_file=opt_u64(h, "max_size_per_file", ctx),
    )


def _strings_only(
    model: type[_M], parent: dict[object, object], key: str, ctx: str
) -> _M | None:
    if key not in parent:
        return None
    ctx = f"{ctx}.{key}"
    h = yaml_hash(parent[key], ctx)
    validate_known_keys(h, ctx, tuple(model.model_fields))
    return model(**{key: opt_string(h, key, ctx) for key in model.model_fields})


def _merge(value: object, ctx: str) -> MergeOptions:
    h = yaml_hash(value, ctx)
    validate_known_keys(h, ctx, tuple(MergeOptions.model_fields))
    return MergeOptions(
        ignore_columns=opt_vec_string(h, "ignore_columns", ctx),
        compare_columns=opt_vec_string(h, "compare_columns", ctx),
        scd2=_strings_only(MergeScd2Options, h, "scd2", ctx),
    )


def _partition_spec(
    parent: dict[object, object], ctx: str
) -> list[IcebergPartitionField] | None:
    if "partition_spec" not in parent:
        return None
    ctx = f"{ctx}.partition_spec"
    out: list[IcebergPartitionField] = []
    for index, item in enumerate(yaml_array(parent["partition_spec"], ctx)):
        item_ctx = f"{ctx}[{index}]"
        h = yaml_hash(item, item_ctx)
        validate_known_keys(h, item_ctx, ("column", "transform"))
        out.append(
            IcebergPartitionField(
                column=get_string(h, "column", item_ctx),
                transform=_or(opt_string(h, "transform", item_ctx), "identity"),
            )
        )
    return out


def _sink_target(
    value: object, ctx: str, allow_options: bool, write_mode: WriteMode
) -> SinkTarget:
    h = yaml_hash(value, ctx)
    allowed = ("format", "path", "storage", "filesystem")
    if allow_options:
        allowed += ("options", "merge", "iceberg", "delta", "duckdb")
        allowed += ("partition_by", "partition_spec")
    validate_known_keys(h, ctx, allowed)
    storage = _storage(h, ctx)
    options = _sink_options(h["options"], f"{ctx}.options") if "options" in h else None
    merge = _merge(h["merge"], f"{ctx}.merge") if "merge" in h else None
    partition_by = opt_vec_string(h, "partition_by", ctx)
    iceberg = _strings_only(IcebergSinkOptions, h, "iceberg", ctx)
    fmt = get_string(h, "format", ctx)
    if "duckdb" in h and fmt != "duckdb":
        raise FloeConfigError(
            f'{ctx}.duckdb is only valid when format is "duckdb" (found format={rust_debug(fmt)})'
        )
    spec = _partition_spec(h, ctx)
    return SinkTarget(
        format=fmt,
        # a MotherDuck sink has no path; D1 rejects duckdb in validation (#633)
        path=opt_string(h, "path", ctx) or ""
        if fmt == "duckdb"
        else get_string(h, "path", ctx),
        storage=storage,
        options=options,
        merge=merge,
        iceberg=iceberg,
        delta=h.get("delta"),  # D1: dropped, left raw for validation (#633)
        duckdb=h.get("duckdb"),
        partition_by=partition_by,
        partition_spec=spec,
        write_mode=write_mode,
    )


def _archive(value: object) -> ArchiveTarget:
    h = yaml_hash(value, "sink.archive")
    validate_known_keys(h, "sink.archive", ("path", "storage"))
    return ArchiveTarget(
        path=_or(opt_string(h, "path", "sink.archive"), "archive"),
        storage=opt_string(h, "storage", "sink.archive"),
    )


def _sink(value: object) -> SinkConfig:
    h = yaml_hash(value, "sink")
    validate_known_keys(h, "sink", ("write_mode", "accepted", "rejected", "archive"))
    mode = _or(opt_string(h, "write_mode", "sink"), "overwrite")
    write_mode = _choice(mode, get_args(WriteMode), "sink.write_mode")
    rejected = (
        _sink_target(h["rejected"], "sink.rejected", False, write_mode)
        if "rejected" in h
        else None
    )
    archive = _archive(h["archive"]) if "archive" in h else None
    accepted = _sink_target(
        get_value(h, "accepted", "sink"), "sink.accepted", True, write_mode
    )
    return SinkConfig(
        write_mode=write_mode, accepted=accepted, rejected=rejected, archive=archive
    )


def parse_entity(
    value: object, source_ref: str | None = None, resource: str | None = None
) -> EntityConfig:
    h = yaml_hash(value, "entity")
    validate_known_keys(
        h,
        "entity",
        (
            "name",
            "metadata",
            "domain",
            "incremental_mode",
            "state",
            "source",
            "sink",
            "policy",
            "schema",
            "pii",
        ),
    )
    name = get_string(h, "name", "entity")
    metadata = _metadata(h["metadata"]) if "metadata" in h else None
    domain = opt_string(h, "domain", "entity")
    mode = opt_string(h, "incremental_mode", "entity")
    incremental_mode: IncrementalMode = (
        "none"
        if mode is None
        else _choice(mode, get_args(IncrementalMode), "entity.incremental_mode")
    )
    state = _state(h["state"]) if "state" in h else None
    source = _source(get_value(h, "source", "entity"), source_ref, resource)
    sink = _sink(get_value(h, "sink", "entity"))
    policy = _policy(get_value(h, "policy", "entity"))
    schema = _schema(get_value(h, "schema", "entity"))
    pii = _pii(h["pii"]) if "pii" in h else None
    return EntityConfig(
        name=name,
        id=name,
        metadata=metadata,
        domain=domain,
        incremental_mode=incremental_mode,
        state=state,
        source=source,
        sink=sink,
        policy=policy,
        schema=schema,
        pii=pii,
    )
