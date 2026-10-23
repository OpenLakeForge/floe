"""Storage and catalog definitions, ported from `config/parse.rs` (`parse_storages`, `parse_catalogs_with_context`)."""

from pydantic import BaseModel, ConfigDict, SecretStr

from floe.config.yaml import (
    get_string,
    opt_bool,
    opt_string,
    validate_known_keys,
    yaml_array,
    yaml_hash,
)
from floe.errors import FloeConfigError

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


def _items(value: object, context: str) -> tuple[Hash, list[object]]:
    """The `default` + `definitions` shape shared by `storages` and `catalogs`."""
    hash = yaml_hash(value, context)
    validate_known_keys(hash, context, ("default", "definitions"))
    if "definitions" not in hash:
        raise FloeConfigError(f"missing required field {context}.definitions")
    return hash, yaml_array(hash["definitions"], f"{context}.definitions")


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
    validate_known_keys(hash, ctx, _STORAGE_KEYS)
    definition = StorageDefinition(
        name=get_string(hash, "name", ctx),
        type=get_string(hash, "type", ctx),
        bucket=opt_string(hash, "bucket", ctx),
        region=opt_string(hash, "region", ctx),
        account=opt_string(hash, "account", ctx),
        container=opt_string(hash, "container", ctx),
        prefix=opt_string(hash, "prefix", ctx),
        endpoint=opt_string(hash, "endpoint", ctx),
        path_style_access=opt_bool(hash, "path_style_access", ctx),
    )
    if definition.type in ("adls", "gcs"):
        raise _Unsupported(
            f"{ctx} name={definition.name} type={definition.type} is not supported in 0.8"
        )
    return definition


def _catalog(value: object, ctx: str) -> CatalogDefinition:
    hash = yaml_hash(value, ctx)
    validate_known_keys(hash, ctx, _CATALOG_KEYS)
    name = get_string(hash, "name", ctx)
    catalog_type = get_string(hash, "type", ctx)
    if catalog_type in ("glue", "unity"):
        raise _Unsupported(
            f"{ctx} name={name} type={catalog_type} is not supported in 0.8"
        )
    if catalog_type != "rest":
        raise FloeConfigError(
            f"{ctx} name={name} has unsupported type={catalog_type} (supported: glue, rest, unity)"
        )
    uri = get_string(hash, "uri", ctx)
    credential = opt_string(hash, "credential", ctx)
    return CatalogDefinition(
        name=name,
        uri=uri,
        credential=None if credential is None else SecretStr(credential),
        warehouse=opt_string(hash, "warehouse", ctx),
        oauth2_server_uri=opt_string(hash, "oauth2_server_uri", ctx),
        scope=opt_string(hash, "scope", ctx),
        warehouse_storage=opt_string(hash, "warehouse_storage", ctx),
        warehouse_prefix=opt_string(hash, "warehouse_prefix", ctx),
    )


def parse_storages(value: object) -> StoragesConfig:
    hash, items = _items(value, "storages")
    definitions: list[StorageDefinition] = []
    for index, item in enumerate(items):
        try:
            definitions.append(_storage(item, "storages.definitions"))
        except _Unsupported:
            raise
        except FloeConfigError as e:
            raise FloeConfigError(f"storages.definitions[{index}]: {e}") from e
    return StoragesConfig(
        default=opt_string(hash, "default", "storages"), definitions=definitions
    )


def parse_catalogs(value: object, context: str = "catalogs") -> CatalogsConfig:
    hash, items = _items(value, context)
    definitions: list[CatalogDefinition] = []
    for index, item in enumerate(items):
        try:
            definitions.append(_catalog(item, f"{context}.definitions"))
        except _Unsupported:
            raise
        except FloeConfigError as e:
            raise FloeConfigError(f"{context}.definitions[{index}]: {e}") from e
    return CatalogsConfig(
        default=opt_string(hash, "default", context), definitions=definitions
    )
