"""Storage and catalog definitions, ported from `config/parse.rs` (`parse_storages`, `parse_catalogs_with_context`)."""

from collections.abc import Callable
from typing import TypeVar, cast

from pydantic import BaseModel, ConfigDict, SecretStr

from floe.config.yaml import yaml_hash, yaml_string
from floe.errors import FloeConfigError

T = TypeVar("T")
Hash = dict[object, object]


class _Model(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class StorageDefinition(_Model):
    name: str
    type: str
    bucket: str | None
    region: str | None
    account: str | None
    container: str | None
    prefix: str | None
    endpoint: str | None
    path_style_access: bool | None


class StoragesConfig(_Model):
    default: str | None
    definitions: list[StorageDefinition]


class CatalogDefinition(_Model):
    """An Iceberg REST catalog; Glue and Unity are rejected (D1)."""

    name: str
    uri: str
    credential: SecretStr | None
    warehouse: str | None
    oauth2_server_uri: str | None
    scope: str | None
    warehouse_storage: str | None
    warehouse_prefix: str | None


class CatalogsConfig(_Model):
    default: str | None
    definitions: list[CatalogDefinition]


class _Unsupported(FloeConfigError):
    """A D1 rejection: raised as is, without the `definitions[i]:` prefix."""


def _known_keys(hash: Hash, ctx: str, allowed: tuple[str, ...]) -> None:
    for key in hash:
        if not isinstance(key, str):
            raise FloeConfigError(f"expected string key at {ctx}")
        if key not in allowed:
            raise FloeConfigError(f"unknown field {ctx}.{key}")


def _get_string(hash: Hash, key: str, ctx: str) -> str:
    if key not in hash:
        raise FloeConfigError(f"missing required field {ctx}.{key}")
    return yaml_string(hash[key], f"{ctx}.{key}")


def _opt_string(hash: Hash, key: str, ctx: str) -> str | None:
    value = hash.get(key)
    return None if value is None else yaml_string(value, f"{ctx}.{key}")


def _opt_bool(hash: Hash, key: str, ctx: str) -> bool | None:
    value = hash.get(key)
    if value is None:
        return None
    if not isinstance(value, bool):
        raise FloeConfigError(f"expected boolean at {ctx}.{key}")
    return value


def _block(
    value: object, context: str, parse_item: Callable[[object, str], T]
) -> tuple[str | None, list[T]]:
    """The `default` + `definitions` shape shared by `storages` and `catalogs`."""
    hash = yaml_hash(value, context)
    _known_keys(hash, context, ("default", "definitions"))
    definitions_context = f"{context}.definitions"
    if "definitions" not in hash:
        raise FloeConfigError(f"missing required field {definitions_context}")
    raw = hash["definitions"]
    if not isinstance(raw, list):
        raise FloeConfigError(f"expected array at {definitions_context}")
    definitions: list[T] = []
    for index, item in enumerate(cast("list[object]", raw)):
        try:
            definitions.append(parse_item(item, definitions_context))
        except _Unsupported:
            raise
        except FloeConfigError as e:
            raise FloeConfigError(f"{definitions_context}[{index}]: {e}") from e
    return _opt_string(hash, "default", context), definitions


_STORAGE_KEYS = (
    "name",
    "type",
    "bucket",
    "region",
    "account",
    "container",
    "prefix",
) + ("endpoint", "path_style_access")
_CATALOG_KEYS = (
    ("name", "type", "uri", "credential", "warehouse", "oauth2_server_uri", "scope")
    + ("warehouse_storage", "warehouse_prefix")
    # Glue and Unity keys stay allowed, as in Rust, so a typo reports the key first
    + ("region", "database", "create_database_if_missing", "allow_takeover")
    + ("host", "catalog", "schema", "token", "create_schema_if_missing")
)


def _storage(value: object, ctx: str) -> StorageDefinition:
    hash = yaml_hash(value, ctx)
    _known_keys(hash, ctx, _STORAGE_KEYS)
    definition = StorageDefinition(
        name=_get_string(hash, "name", ctx),
        type=_get_string(hash, "type", ctx),
        bucket=_opt_string(hash, "bucket", ctx),
        region=_opt_string(hash, "region", ctx),
        account=_opt_string(hash, "account", ctx),
        container=_opt_string(hash, "container", ctx),
        prefix=_opt_string(hash, "prefix", ctx),
        endpoint=_opt_string(hash, "endpoint", ctx),
        path_style_access=_opt_bool(hash, "path_style_access", ctx),
    )
    if definition.type in ("adls", "gcs"):
        raise _Unsupported(
            f"{ctx} name={definition.name} type={definition.type} is not supported in 0.8"
        )
    return definition


def _catalog(value: object, ctx: str) -> CatalogDefinition:
    hash = yaml_hash(value, ctx)
    _known_keys(hash, ctx, _CATALOG_KEYS)
    name = _get_string(hash, "name", ctx)
    catalog_type = _get_string(hash, "type", ctx)
    if catalog_type in ("glue", "unity"):
        raise _Unsupported(
            f"{ctx} name={name} type={catalog_type} is not supported in 0.8"
        )
    if catalog_type != "rest":
        raise FloeConfigError(
            f"{ctx} name={name} has unsupported type={catalog_type} (supported: glue, rest, unity)"
        )
    uri = _get_string(hash, "uri", ctx)
    credential = _opt_string(hash, "credential", ctx)
    return CatalogDefinition(
        name=name,
        uri=uri,
        credential=None if credential is None else SecretStr(credential),
        warehouse=_opt_string(hash, "warehouse", ctx),
        oauth2_server_uri=_opt_string(hash, "oauth2_server_uri", ctx),
        scope=_opt_string(hash, "scope", ctx),
        warehouse_storage=_opt_string(hash, "warehouse_storage", ctx),
        warehouse_prefix=_opt_string(hash, "warehouse_prefix", ctx),
    )


def parse_storages(value: object) -> StoragesConfig:
    default, definitions = _block(value, "storages", _storage)
    return StoragesConfig(default=default, definitions=definitions)


def parse_catalogs(value: object, context: str = "catalogs") -> CatalogsConfig:
    default, definitions = _block(value, context, _catalog)
    return CatalogsConfig(default=default, definitions=definitions)
