"""obstore-backed storage client (Azure now; GCS plugs in through `token_field`)."""

from datetime import UTC
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

from floe.config.storage import StorageDefinition
from floe.errors import FloeConfigError, FloeStorageError
from floe.extras import missing
from floe.storage.s3 import temp_path_for_key
from floe.storage.types import ObjectRef, StoredObject

if TYPE_CHECKING:
    import obstore as obs
    from obstore import PutMode, UpdateVersion
    from obstore import exceptions as oe
    from obstore.store import ObjectStore
else:
    try:
        import obstore as obs
        from obstore import exceptions as oe
    except ImportError:
        # no adls/gcs extra: `azure_store()` raises, so nothing below is ever called
        obs = oe = None


def _fail(what: str, e: Exception) -> FloeStorageError:
    msg = f"{what} failed: {e}"
    if isinstance(e, FileNotFoundError):
        return FloeStorageError(msg, code="storage_not_found")
    if isinstance(e, (oe.PermissionDeniedError, oe.UnauthenticatedError)):
        return FloeStorageError(msg, code="storage_access_denied")
    if isinstance(e, (oe.AlreadyExistsError, oe.PreconditionError)):
        return FloeStorageError(msg, code="storage_conflict")
    return FloeStorageError(msg, code="storage_error")


def _check(key: str, *, prefix: bool = False) -> str:
    """obstore strips a leading `/` and rejects `a//b`: refuse both, so a key never names another object."""
    if key.startswith("/") or "//" in key or (not prefix and (not key or key.endswith("/"))):  # fmt: skip
        raise FloeStorageError(f"invalid object key {key!r}: a key has no leading or empty path segment", code="storage_error")  # fmt: skip
    return key


class ObjectStoreClient:
    def __init__(
        self,
        store: "ObjectStore",
        root_uri: str,
        endpoint: str | None,
        token_field: Literal["e_tag", "version"],
    ) -> None:
        self._store, self._root = store, root_uri.rstrip("/")
        self.endpoint, self._token = endpoint, token_field

    def _key(self, uri: str) -> str:
        if not uri.startswith(f"{self._root}/"):
            raise FloeStorageError(f"{uri} is not under {self._root}", code="storage_error")  # fmt: skip
        return _check(uri.removeprefix(f"{self._root}/"))

    def resolve_uri(self, path: str) -> str:
        return f"{self._root}/{_check(path)}" if path else self._root

    def list(self, prefix: str, start_after: str = "") -> list[ObjectRef]:
        """Keys starting with the raw `prefix` after `start_after`, in key order, as S3 lists them."""
        _check(prefix, prefix=True)
        # obstore lists whole directories only: list the parent, then filter on the raw prefix
        directory = prefix.rpartition("/")[0]
        try:
            metas = [m for chunk in obs.list(self._store, directory or None) for m in chunk]  # fmt: skip
        except (oe.BaseError, OSError, ValueError) as e:
            raise _fail(f"list objects under {self._root}/{prefix}", e) from e
        return [
            ObjectRef(
                f"{self._root}/{m['path']}",
                m["path"],
                m["last_modified"].astimezone(UTC).isoformat().replace("+00:00", "Z"),
                m["size"],
            )
            for m in sorted(metas, key=lambda m: m["path"])
            if m["path"].startswith(prefix) and m["path"] > start_after
        ]

    def download(self, uri: str, temp_dir: Path) -> Path:
        key = self._key(uri)
        dest = temp_path_for_key(temp_dir, key)
        try:
            result = obs.get(self._store, key)
            dest.parent.mkdir(parents=True, exist_ok=True)
            with dest.open("wb") as f:
                for chunk in result:
                    f.write(chunk)
        except (oe.BaseError, OSError, ValueError) as e:
            raise _fail(f"get object {uri}", e) from e
        return dest

    def write(self, uri: str, body: bytes) -> None:
        try:
            obs.put(self._store, self._key(uri), body)
        except (oe.BaseError, OSError, ValueError) as e:
            raise _fail(f"put object {uri}", e) from e

    def copy(self, src_uri: str, dst_uri: str) -> None:
        try:
            obs.copy(self._store, self._key(src_uri), self._key(dst_uri))
        except (oe.BaseError, OSError, ValueError) as e:
            raise _fail(f"copy object {src_uri} to {dst_uri}", e) from e

    def source_mtime(self, uri: str) -> str | None:
        return None

    def delete(self, uri: str) -> None:
        try:
            obs.delete(self._store, self._key(uri))
        except FileNotFoundError:
            pass  # a missing key is a no-op, as on S3
        except (oe.BaseError, OSError, ValueError) as e:
            raise _fail(f"delete object {uri}", e) from e

    def read(self, uri: str) -> StoredObject | None:
        try:
            result = obs.get(self._store, self._key(uri))
            return StoredObject(bytes(result.bytes()), result.meta[self._token] or "")
        except FileNotFoundError:
            return None
        except (oe.BaseError, OSError, ValueError) as e:
            raise _fail(f"get object {uri}", e) from e

    def write_conditional(
        self, uri: str, expected_version: str | None, body: bytes
    ) -> str | None:
        """The new version, or None when the object is not at `expected_version` (None: not there yet)."""
        mode: PutMode = "create" if expected_version is None else cast("UpdateVersion", {self._token: expected_version})  # fmt: skip
        try:
            put = obs.put(self._store, self._key(uri), body, mode=mode)
            return (put["e_tag"] if self._token == "e_tag" else put["version"]) or ""
        except (oe.AlreadyExistsError, oe.PreconditionError, FileNotFoundError):
            return None
        except (oe.BaseError, OSError, ValueError) as e:
            raise _fail(f"put object {uri}", e) from e


def azure_store(definition: StorageDefinition) -> ObjectStoreClient:
    """Credentials come from the environment (`AZURE_STORAGE_*`, `AZURE_CLIENT_*`)."""
    try:
        from obstore.store import AzureStore
    except ImportError as e:
        raise missing("adls", "an adls storage") from e
    if not (definition.account and definition.container):
        raise FloeConfigError(f"storage {definition.name} requires account and container for type adls", code="config_missing_key")  # fmt: skip
    account, container = definition.account, definition.container
    endpoint = (definition.endpoint or f"https://{account}.blob.core.windows.net").rstrip("/")  # fmt: skip
    try:
        store = AzureStore(
            container,
            account_name=account,
            endpoint=endpoint,
            client_options={"allow_http": endpoint.startswith("http://")},
        )
    except (oe.BaseError, ValueError) as e:
        raise _fail(f"azure store for {account}/{container}", e) from e
    return ObjectStoreClient(store, f"abfss://{container}@{account}.dfs.core.windows.net", endpoint, "e_tag")  # fmt: skip
