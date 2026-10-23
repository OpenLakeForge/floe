"""Entity parsing, ported from `config/parse.rs` (`parse_entity` and the entity-level blocks)."""

import string
from typing import TypeVar, get_args

from floe.config.types import (
    EntityConfig,
    EntityMetadata,
    EntityStateConfig,
    IncrementalMode,
    PolicyConfig,
    PolicySeverity,
)
from floe.config.yaml import (
    get_string,
    get_value,
    opt_string,
    opt_vec_string,
    validate_known_keys,
    yaml_hash,
)
from floe.errors import FloeConfigError

_T = TypeVar("_T", bound=str)
_ASCII_LOWER = str.maketrans(string.ascii_uppercase, string.ascii_lowercase)
# Rust's `trim()` strips Unicode White_Space only; `str.strip()` also strips U+001C to U+001F
_WHITE_SPACE = "\t\n\v\f\r \x85\xa0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000"


def _pick(value: str, allowed: tuple[_T, ...]) -> _T | None:
    return next((option for option in allowed if option == value), None)


def _choice(value: str, allowed: tuple[_T, ...], ctx: str) -> _T:
    picked = _pick(value.strip(_WHITE_SPACE).translate(_ASCII_LOWER), allowed)
    if picked is None:
        raise FloeConfigError(
            f"unsupported value at {ctx}: {value} (allowed: {', '.join(allowed)})"
        )
    return picked


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
    schema = get_value(h, "schema", "entity")
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
        pii=h.get("pii"),
    )
