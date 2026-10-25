"""Storage reference resolution: name, else `storages.default`, else local (Rust `StorageResolver::resolve_path`)."""

from pathlib import Path

from floe.config.storage import StorageDefinition, StoragesConfig
from floe.errors import FloeConfigError, FloeRunError
from floe.storage.local import LocalClient
from floe.storage.paths import normalize_local_path, parse_s3_uri
from floe.storage.s3 import S3Client


def _definition(
    storages: StoragesConfig | None, name: str | None, field: str
) -> StorageDefinition | None:
    """`None` is the implicit local storage; unknown names raise, never fall back to local."""
    where = name or (storages and storages.default) or "local"
    if storages is None and where == "local":
        return None
    found = storages and next((d for d in storages.definitions if d.name == where), None)  # fmt: skip
    if not found:
        raise FloeRunError(f"{field}={where} references unknown storage")
    return found


def resolve_path(
    storages: StoragesConfig | None,
    config_dir: Path,
    name: str | None,
    raw: str,
    field: str,
) -> tuple[str, StorageDefinition | None]:
    """`raw` as a `local://` or `s3://` URI, with its storage definition."""
    definition = _definition(storages, name, field)
    if definition is None or definition.type == "local":
        return f"local://{normalize_local_path(config_dir / raw)}", definition
    if definition.type != "s3":
        raise FloeRunError(f"{field}={definition.name} (type {definition.type}) is not supported")  # fmt: skip
    if definition.bucket is None:
        raise FloeConfigError(f"storage {definition.name} requires bucket for type s3")  # fmt: skip
    if raw.startswith("s3://"):
        bucket, key = parse_s3_uri(raw)
        if bucket != definition.bucket:
            raise FloeConfigError(f"storage {definition.name} bucket mismatch: {bucket}")  # fmt: skip
    else:
        key = "/".join(p for p in ((definition.prefix or "").strip("/"), raw.lstrip("/")) if p)  # fmt: skip
    return f"s3://{definition.bucket}/{key}" if key else f"s3://{definition.bucket}", definition  # fmt: skip


def storage_client(definition: StorageDefinition | None) -> LocalClient | S3Client:
    """S3 credentials come from the environment, as in Rust."""
    if definition is None or definition.type == "local":
        return LocalClient()
    return S3Client(definition.bucket or "", definition.region, definition.endpoint, definition.path_style_access)  # fmt: skip


def require_local(
    storages: StoragesConfig | None, name: str | None, field: str, feature: str
) -> None:
    """Refuse a remote storage for a feature that is still local-only."""
    definition = _definition(storages, name, field)
    if definition is not None and definition.type != "local":
        raise FloeRunError(f"{field}={definition.name} (type {definition.type}) is not supported yet: {feature} is local only")  # fmt: skip
