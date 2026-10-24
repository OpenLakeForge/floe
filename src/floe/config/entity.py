"""Entity parsing, ported from `config/parse.rs` (`parse_entity` and the entity-level blocks)."""

import string
from collections.abc import Callable
from typing import TypeVar, get_args

from floe.config.types import (
    ColumnConfig,
    EntityConfig,
    EntityMetadata,
    EntityStateConfig,
    IncrementalMode,
    NormalizeColumnsConfig,
    PiiConfig,
    PolicyConfig,
    PolicySeverity,
    SchemaConfig,
    SchemaEvolutionConfig,
    SchemaEvolutionIncompatibleAction,
    SchemaEvolutionMode,
    SchemaMismatchConfig,
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
    validate_known_keys,
    yaml_hash,
)
from floe.errors import FloeConfigError, FloeError
from floe.pii import PiiColumn, Strategy

_T = TypeVar("_T", bound=str)
_R = TypeVar("_R")
_ASCII_LOWER = str.maketrans(string.ascii_uppercase, string.ascii_lowercase)
# Rust's `trim()` strips Unicode White_Space only; `str.strip()` also strips U+001C to U+001F
_WHITE_SPACE = "\t\n\v\f\r \x85\xa0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000"


def _pick(value: str, allowed: tuple[_T, ...]) -> _T | None:
    return next((option for option in allowed if option == value), None)


def _norm(value: str) -> str:
    return value.strip(_WHITE_SPACE).translate(_ASCII_LOWER)


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


def parse_entity(value: object) -> EntityConfig:
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
    source = get_value(h, "source", "entity")
    sink = get_value(h, "sink", "entity")
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
