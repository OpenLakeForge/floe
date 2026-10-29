"""Environment profiles, ported from `profile/parse.rs` and `profile/validate.rs`."""

import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from floe.config.project import LineageConfig, RootConfig, parse_lineage
from floe.config.storage import (
    CatalogsConfig,
    StoragesConfig,
    parse_catalogs,
    parse_storages,
)
from floe.config.validate import check_lineage
from floe.config.vars import string_map
from floe.config.yaml import (
    load_yaml,
    parse_yaml,
    rust_debug,
    validate_known_keys,
    yaml_array,
    yaml_hash,
    yaml_string,
)
from floe.errors import FloeConfigError, FloeIoError
from floe.storage.s3 import read_s3

Hash = dict[object, object]
Reader = Callable[[Hash, str, str], object]

API_VERSION = "floe/v1"
KIND = "EnvironmentProfile"
RUNNERS = ("local", "kubernetes_job", "databricks_job")
CONTRACT = {
    "kubernetes_job": ("image", "namespace"),
    "databricks_job": (
        *("workspace_url", "existing_cluster_id", "config_uri", "python_file_uri"),
        "auth.service_principal_oauth_ref",
    ),
}


@dataclass(frozen=True)
class ProfileConfig:
    name: str
    runner: dict[str, object]  # `type` and every field, None when unset
    variables: dict[str, str]
    catalogs: CatalogsConfig | None
    storages: StoragesConfig | None
    lineage: LineageConfig | None
    orchestration: dict[str, object] | None = None  # its set fields only, as Rust


def _string(hash: Hash, key: str, ctx: str, required: bool = False) -> str | None:
    if key not in hash:
        if required:
            raise FloeConfigError(f"{ctx}.{key} is required")
        return None
    return yaml_string(hash[key], f"{ctx}.{key}")


def _strings(hash: Hash, key: str, ctx: str) -> list[str] | None:
    if key not in hash:
        return None
    items = yaml_array(hash[key], f"{ctx}.{key}")
    return [yaml_string(item, f"{ctx}.{key}[]") for item in items]


def _command(hash: Hash, key: str, ctx: str) -> str | list[str] | None:
    """A string, or a list as the manifest and the connectors take it."""
    if isinstance(hash.get(key), list):
        return _strings(hash, key, ctx)
    return _string(hash, key, ctx)


def _u64(hash: Hash, key: str, ctx: str) -> int | None:
    if key not in hash:
        return None
    value = hash[key]
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise FloeConfigError(f"{ctx}.{key} must be a non-negative integer")
    return value


def _map(hash: Hash, key: str, ctx: str) -> dict[str, str] | None:
    return string_map(hash[key], f"{ctx}.{key}") if key in hash else None


def _secrets(hash: Hash, key: str, ctx: str) -> list[dict[str, str | None]] | None:
    if key not in hash:
        return None
    where = f"{ctx}.{key}[]"
    secrets: list[dict[str, str | None]] = []
    for item in yaml_array(hash[key], f"{ctx}.{key}"):
        secret = yaml_hash(item, where)
        validate_known_keys(secret, where, ("name", "secret_name", "key"))
        fields = ("name", "secret_name", "key")
        secrets.append({f: _string(secret, f, where, True) for f in fields})
    return secrets


def _resources(
    hash: Hash, key: str, ctx: str, per_domain: bool = False
) -> dict[str, object] | None:
    if key not in hash:
        return None
    where = f"{ctx}.{key}"
    resources = yaml_hash(hash[key], where)
    validate_known_keys(
        resources, where, ("cpu", "memory_mb", "cpu_limit", "memory_limit_mb")
    )
    cpu = _string(resources, "cpu", where)
    read: dict[str, object] = {
        "cpu": cpu,
        "memory_mb": _u64(resources, "memory_mb", where),
    }
    if per_domain and read["memory_mb"] == 0:  # the manifest schema requires >= 1
        raise FloeConfigError(f"{where}.memory_mb: must be >= 1")
    memory_limit = _u64(resources, "memory_limit_mb", where)
    if memory_limit == 0:  # the schemas require >= 1
        raise FloeConfigError(f"{where}.memory_limit_mb: must be >= 1")
    limits = {  # the limits are Python-only: carried only when set, so Rust manifests stay identical
        "cpu_limit": _string(resources, "cpu_limit", where),
        "memory_limit_mb": memory_limit,
    }
    return read | {k: v for k, v in limits.items() if v is not None}


def _domain_resources(hash: Hash, key: str, ctx: str) -> dict[str, object] | None:
    if key not in hash:
        return None
    where = f"{ctx}.{key}"
    by_domain = yaml_hash(hash[key], where)
    validate_known_keys(
        by_domain, where, by_domain.keys()
    )  # every key must be a string
    return {
        str(domain): _resources(by_domain, str(domain), where, per_domain=True)
        for domain in by_domain
    }


def _job_scope(hash: Hash, key: str, ctx: str) -> str | None:
    scope = _string(hash, key, ctx)
    if scope not in (None, "entity", "domain"):
        raise FloeConfigError(
            f'{ctx}.{key}: expected "entity" or "domain", got "{scope}"'
        )
    return scope


def _str_keyed(value: object, where: str) -> dict[str, object]:
    hash = yaml_hash(value, where)
    validate_known_keys(hash, where, hash.keys())  # every key must be a string
    return cast(dict[str, object], hash)


def _object(hash: Hash, key: str, ctx: str) -> dict[str, object] | None:
    return _str_keyed(hash[key], f"{ctx}.{key}") if key in hash else None


def _objects(hash: Hash, key: str, ctx: str) -> list[dict[str, object]] | None:
    if key not in hash:
        return None
    where = f"{ctx}.{key}"
    return [_str_keyed(item, f"{where}[]") for item in yaml_array(hash[key], where)]


def _auth(hash: Hash, key: str, ctx: str) -> dict[str, str | None] | None:
    if key not in hash:
        return None
    where = f"{ctx}.{key}"
    auth = yaml_hash(hash[key], where)
    validate_known_keys(auth, where, ("service_principal_oauth_ref",))
    ref = _string(auth, "service_principal_oauth_ref", where)
    return {"service_principal_oauth_ref": ref}


# Rust reads the runner fields in this order, so the first malformed one is the one reported.
_RUNNER_FIELDS: tuple[tuple[str, Reader], ...] = (
    ("command", _command),
    ("args", _strings),
    ("timeout_seconds", _u64),
    ("ttl_seconds_after_finished", _u64),
    ("poll_interval_seconds", _u64),
    ("secrets", _secrets),
    ("image", _string),
    ("namespace", _string),
    ("service_account", _string),
    ("resources", _resources),
    ("env", _map),
    ("workspace_url", _string),
    ("existing_cluster_id", _string),
    ("config_uri", _string),
    ("python_file_uri", _string),
    ("job_name", _string),
    ("auth", _auth),
    ("env_parameters", _map),
    ("job_scope", _job_scope),
    ("domain_resources", _domain_resources),
    ("labels", _map),
    ("annotations", _map),
    ("image_pull_policy", _string),
    ("security_context", _object),
    ("node_selector", _map),
    ("tolerations", _objects),
    ("backoff_limit", _u64),
    ("env_from", _objects),
)


def _runner(value: object) -> dict[str, object]:
    ctx = "profile.execution.runner"
    hash = yaml_hash(value, ctx)
    validate_known_keys(hash, ctx, ("type", *(name for name, _ in _RUNNER_FIELDS)))
    read: dict[str, object] = {"type": _string(hash, "type", ctx, True)}
    for name, reader in _RUNNER_FIELDS:
        read[name] = reader(hash, name, ctx)
    return read


def _orchestration(value: object) -> dict[str, object]:
    ctx = "profile.execution.orchestration"
    hash = yaml_hash(value, ctx)
    validate_known_keys(hash, ctx, ("max_concurrent_entities", "strategy"))
    max_concurrent = _u64(hash, "max_concurrent_entities", ctx)
    if max_concurrent == 0:
        raise FloeConfigError(f"{ctx}.max_concurrent_entities: must be >= 1")
    strategy = _string(hash, "strategy", ctx)
    if strategy not in (None, "sequential", "parallel"):
        raise FloeConfigError(
            f'{ctx}.strategy: expected "sequential" or "parallel", got "{strategy}"'
        )
    read = {"max_concurrent_entities": max_concurrent, "strategy": strategy}
    return {k: v for k, v in read.items() if v is not None}


def _metadata(value: object) -> str:
    ctx = "profile.metadata"
    hash = yaml_hash(value, ctx)
    validate_known_keys(hash, ctx, ("name", "description", "env", "tags"))
    name = _string(hash, "name", ctx, True)
    _string(hash, "description", ctx)
    _string(hash, "env", ctx)
    _strings(hash, "tags", ctx)
    assert name is not None  # `required` raised otherwise
    return name


def _validation(value: object) -> None:
    ctx = "profile.validation"
    hash = yaml_hash(value, ctx)
    validate_known_keys(hash, ctx, ("strict",))
    if "strict" in hash and not isinstance(hash["strict"], bool):
        raise FloeConfigError(f"{ctx}.strict must be a boolean")


def _parse_doc(doc: object) -> ProfileConfig:
    root = yaml_hash(doc, "profile")
    validate_known_keys(
        root,
        "profile",
        (
            *("apiVersion", "kind", "metadata", "execution", "variables"),
            *("catalogs", "storages", "lineage", "validation"),
        ),
    )
    for key, expected in (("apiVersion", API_VERSION), ("kind", KIND)):
        got = _string(root, key, "profile", True)
        if got != expected:
            raise FloeConfigError(f'profile.{key}: expected "{expected}", got "{got}"')
    if "metadata" not in root:
        raise FloeConfigError("profile.metadata is required")
    name = _metadata(root["metadata"])
    runner: dict[str, object] = {}
    orchestration = None
    if "execution" in root:
        ctx = "profile.execution"
        execution = yaml_hash(root["execution"], ctx)
        validate_known_keys(execution, ctx, ("runner", "orchestration"))
        if "runner" not in execution:
            raise FloeConfigError(f"{ctx}.runner is required")
        runner = _runner(execution["runner"])
        if "orchestration" in execution:
            orchestration = _orchestration(execution["orchestration"])
    variables = (
        string_map(root["variables"], "profile.variables")
        if "variables" in root
        else {}
    )
    catalogs = (
        parse_catalogs(root["catalogs"], "profile.catalogs")
        if "catalogs" in root
        else None
    )
    storages = parse_storages(root["storages"]) if "storages" in root else None
    lineage = parse_lineage(root["lineage"]) if "lineage" in root else None
    if "validation" in root:
        _validation(root["validation"])
    return ProfileConfig(
        name, runner, variables, catalogs, storages, lineage, orchestration
    )


def _malformed(value: str) -> str | None:
    """The first `${}` or unclosed `${` in `value`; well-formed references are left to `resolve_vars`."""
    rest = value
    while (start := rest.find("${")) != -1:
        rest = rest[start + 2 :]
        end = rest.find("}")
        if end == -1:
            return f"${{... (unclosed placeholder in: {rust_debug(value)})}}"[:120]
        if not rest[:end].strip():
            return "${} (empty placeholder)"
        rest = rest[end + 1 :]
    return None


def _check_catalogs(catalogs: CatalogsConfig) -> None:
    ctx = "profile.catalogs"
    if not catalogs.definitions:
        raise FloeConfigError(f"{ctx}.definitions must not be empty")
    seen: set[str] = set()
    for d in catalogs.definitions:
        if not d.name.strip():
            raise FloeConfigError(f"{ctx}.definitions.name must not be empty")
        if d.name in seen:
            raise FloeConfigError(f"{ctx}.definitions name={d.name} is duplicated")
        seen.add(d.name)
    if catalogs.default is not None and catalogs.default not in seen:
        raise FloeConfigError(
            f"{ctx}.default={catalogs.default} does not match any definition"
        )


def _check_storages(storages: StoragesConfig) -> None:
    ctx = "profile.storages"
    if not storages.definitions:
        raise FloeConfigError(f"{ctx}.definitions must not be empty")
    seen: set[str] = set()
    for d in storages.definitions:
        # ponytail: Rust's adls/gcs requirements are unreachable here; both types are rejected at parse (D1)
        if not d.name.strip():
            raise FloeConfigError(f"{ctx}.definitions.name must not be empty")
        if d.name in seen:
            raise FloeConfigError(f"{ctx}.definitions name={d.name} is duplicated")
        seen.add(d.name)
        if d.type not in ("local", "s3", "adls", "gcs"):
            raise FloeConfigError(
                f"{ctx}.definitions name={d.name} type={d.type} is unsupported "
                "(allowed: local, s3, adls, gcs)"
            )
        for field in ("bucket", "region") if d.type == "s3" else ():
            if getattr(d, field) is None:
                raise FloeConfigError(
                    f"{ctx}.definitions name={d.name} requires {field} for type s3"
                )
    if storages.default is None:
        raise FloeConfigError(f"{ctx}.default is required when storages is set")
    if storages.default not in seen:
        raise FloeConfigError(
            f"{ctx}.default={storages.default} does not match any definition"
        )


def validate_profile(profile: ProfileConfig) -> None:
    ctx = "profile.execution.runner"
    if not profile.name.strip():
        raise FloeConfigError("profile.metadata.name must not be empty")
    if profile.runner:
        kind = profile.runner["type"]
        if kind not in RUNNERS:
            raise FloeConfigError(
                f'{ctx}.type: unknown runner "{kind}"; known runners: {", ".join(RUNNERS)}'
            )
        for field in CONTRACT.get(kind, ()):
            key, _, sub = field.partition(".")
            value = profile.runner.get(key)
            if sub and isinstance(value, dict):
                value = cast(dict[str, object], value).get(sub)
            if not isinstance(value, str) or not value.strip():
                raise FloeConfigError(f"{ctx}.{field} is required for {kind}")
    for key, value in profile.variables.items():
        if (placeholder := _malformed(value)) is not None:
            raise FloeConfigError(
                f'profile variable "{key}" contains malformed placeholder: {placeholder}'
            )
    if profile.catalogs is not None:
        _check_catalogs(profile.catalogs)
    if profile.storages is not None:
        _check_storages(profile.storages)
    if profile.lineage is not None:
        check_lineage(profile.lineage, "profile.lineage")


def apply_profile(config: RootConfig, profile: ProfileConfig) -> RootConfig:
    """The profile's blocks replace the config's, as `apply_profile_catalogs`, `_storages` and `_lineage` do."""
    blocks = {
        "catalogs": profile.catalogs,
        "storages": profile.storages,
        "lineage": profile.lineage,
    }
    return config.model_copy(update={k: v for k, v in blocks.items() if v is not None})


ENV_PREFIX = "env:"


def read_env_profile(location: str) -> str:
    """`env:NAME`: the profile YAML held in environment variable `NAME`."""
    name = location.removeprefix(ENV_PREFIX)
    if not (text := os.environ.get(name, "")).strip():
        raise FloeConfigError(f"profile environment variable {name} is unset or empty")
    return text


def _docs(location: str | Path) -> list[object]:
    if isinstance(location, str) and location.startswith(ENV_PREFIX):
        return parse_yaml(read_env_profile(location))
    if isinstance(location, Path) or not location.startswith("s3://"):
        return load_yaml(Path(location))
    try:
        return parse_yaml(read_s3(location, "profile").decode())
    except UnicodeDecodeError as e:
        raise FloeIoError("stream did not contain valid UTF-8") from e


def parse_profile(location: str | Path) -> ProfileConfig:
    docs = _docs(location)
    if not docs:
        raise FloeConfigError("profile YAML is empty")
    if len(docs) > 1:
        raise FloeConfigError("profile YAML contains multiple documents; expected one")
    profile = _parse_doc(docs[0])
    validate_profile(profile)
    return profile
