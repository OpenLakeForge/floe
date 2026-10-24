"""Storage reference resolution: name, else `storages.default`, else local (Rust `resolve_path`)."""

from floe.config.storage import StoragesConfig
from floe.errors import FloeRunError


def require_local(
    storages: StoragesConfig | None, name: str | None, field: str
) -> None:
    """Refuse a reference whose definition is not `local`, never fall back to local.

    ponytail: only local is routed until the S3 client lands (#722); route by type then.
    """
    where = name or (storages and storages.default) or "local"
    kind = next((d.type for d in storages.definitions if d.name == where), "local") if storages else "local"  # fmt: skip
    if kind != "local":
        raise FloeRunError(f"{field}={where} (type {kind}) is not supported yet: only local storage is routed")  # fmt: skip
