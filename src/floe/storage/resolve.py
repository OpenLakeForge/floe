"""Storage reference resolution: name, else `storages.default`, else local."""

from pathlib import Path
from threading import Lock

from floe.config.storage import StorageDefinition, StoragesConfig
from floe.errors import FloeConfigError, FloeIoError, FloeRunError
from floe.storage.local import LocalClient
from floe.storage.object_store import ObjectStoreClient, azure_store, gcs_store
from floe.storage.paths import normalize_local_path, parse_s3_uri, split_uri
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
        raise FloeRunError(
            f"{field}={where} references unknown storage", code="run_invalid_setup"
        )
    return found


def refuse_unsupported_storages(storages: StoragesConfig | None, command: str) -> None:
    """These commands handle only local and s3 storages; an adls or gcs one must not fall through to local."""
    for d in storages.definitions if storages else []:
        if d.type in ("adls", "gcs"):
            raise FloeConfigError(f"{command}: storage {d.name} (type {d.type}) is not supported yet", code="config_not_supported")  # fmt: skip


def resolve_path(
    storages: StoragesConfig | None,
    config_dir: Path,
    name: str | None,
    raw: str,
    field: str,
) -> tuple[str, StorageDefinition | None]:
    """`raw` as a `local://`, `s3://`, `abfss://` or `gs://` URI, with its storage definition."""
    definition = _definition(storages, name, field)
    if definition is None or definition.type == "local":
        if "://" in raw:
            raise FloeConfigError(
                f"{field} must be a local path (got {raw})", code="config_invalid_value"
            )
        return f"local://{normalize_local_path(config_dir / raw)}", definition
    if definition.type == "adls":
        if not (definition.account and definition.container):
            raise FloeConfigError(f"storage {definition.name} requires account and container for type adls", code="config_missing_key")  # fmt: skip
        authority = f"{definition.container}@{definition.account}.dfs.core.windows.net"
        if "://" not in raw:
            key = "/".join(p for p in ((definition.prefix or "").strip("/"), raw.lstrip("/")) if p)  # fmt: skip
        elif not raw.startswith("abfss://"):
            raise FloeConfigError(f"{field} must use an abfss:// uri for adls storage {definition.name} (got {raw})", code="config_invalid_value")  # fmt: skip
        else:
            found, key = split_uri(raw)
            if found != authority:
                raise FloeConfigError(f"storage {definition.name} container or account mismatch: {found}", code="config_invalid_value")  # fmt: skip
        return f"abfss://{authority}/{key}" if key else f"abfss://{authority}", definition  # fmt: skip
    if definition.type == "gcs":
        if not definition.bucket:
            raise FloeConfigError(f"storage {definition.name} requires bucket for type gcs", code="config_missing_key")  # fmt: skip
        if "://" not in raw:
            key = "/".join(p for p in ((definition.prefix or "").strip("/"), raw.lstrip("/")) if p)  # fmt: skip
        elif not raw.startswith("gs://"):
            raise FloeConfigError(f"{field} must use a gs:// uri for gcs storage {definition.name} (got {raw})", code="config_invalid_value")  # fmt: skip
        else:
            found, key = split_uri(raw)
            if found != definition.bucket:
                raise FloeConfigError(f"storage {definition.name} bucket mismatch: {found}", code="config_invalid_value")  # fmt: skip
        return f"gs://{definition.bucket}/{key}" if key else f"gs://{definition.bucket}", definition  # fmt: skip
    if definition.type != "s3":
        raise FloeRunError(f"{field}={definition.name} (type {definition.type}) is not supported", code="run_not_supported")  # fmt: skip
    if definition.bucket is None:
        raise FloeConfigError(f"storage {definition.name} requires bucket for type s3", code="config_missing_key")  # fmt: skip
    if raw.startswith("s3://"):
        bucket, key = parse_s3_uri(raw)
        if bucket != definition.bucket:
            raise FloeConfigError(f"storage {definition.name} bucket mismatch: {bucket}", code="config_invalid_value")  # fmt: skip
    else:
        key = "/".join(p for p in ((definition.prefix or "").strip("/"), raw.lstrip("/")) if p)  # fmt: skip
    return f"s3://{definition.bucket}/{key}" if key else f"s3://{definition.bucket}", definition  # fmt: skip


_clients: dict[StorageDefinition, S3Client | ObjectStoreClient] = {}
_clients_lock = Lock()


def clear_storage_clients() -> None:
    """Start of a run: the clients below read credentials and `AWS_*` env when built."""
    with _clients_lock:
        _clients.clear()


def storage_client(
    definition: StorageDefinition | None,
) -> LocalClient | S3Client | ObjectStoreClient:
    """S3, Azure and GCS credentials come from the environment. One shared client per definition between `clear_storage_clients` calls."""
    if definition is None or definition.type == "local":
        return LocalClient()
    with (
        _clients_lock
    ):  # boto3 clients are thread-safe; the lock only stops a duplicate build
        if definition not in _clients:
            _clients[definition] = {"adls": azure_store, "gcs": gcs_store}[definition.type](definition) if definition.type in ("adls", "gcs") else S3Client(definition.bucket or "", definition.region, definition.endpoint, definition.path_style_access)  # fmt: skip
        return _clients[definition]


def _object_client(uri: str) -> S3Client | ObjectStoreClient:
    """A bare `s3://`, `abfss://` or `gs://` URI has no storage definition: the endpoint and credentials come from the environment (`AWS_*`, `AZURE_*`, `GOOGLE_*`)."""
    if uri.startswith("s3://"):
        return S3Client(parse_s3_uri(uri)[0])
    root, _ = split_uri(uri)
    none = dict.fromkeys(("region", "prefix", "endpoint", "path_style_access"))
    if uri.startswith("gs://"):
        return gcs_store(StorageDefinition(name="gs", type="gcs", bucket=root, account=None, container=None, **none))  # fmt: skip
    container, _, host = root.partition("@")
    account, _, suffix = host.partition(".")
    if not (uri.startswith("abfss://") and container and account and suffix == "dfs.core.windows.net"):  # fmt: skip
        raise FloeConfigError(f"remote location must be s3://, gs:// or abfss://<container>@<account>.dfs.core.windows.net/ (got {uri})", code="config_not_supported")  # fmt: skip
    adls = StorageDefinition(name="abfss", type="adls", bucket=None, account=account, container=container, **none)  # fmt: skip
    return azure_store(adls, env_endpoint=True)


def read_object(uri: str, what: str) -> bytes:
    found = _object_client(uri).read(uri)
    if found is None:
        raise FloeIoError(f"{what} not found: {uri}", code="io_not_found")
    return found.body


def write_object(uri: str, body: bytes) -> None:
    """One put: readers see the old or the new object, never a partial one."""
    _object_client(uri).write(uri, body)
