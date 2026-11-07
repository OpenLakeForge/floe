"""Storage client types, ported from `io/storage/mod.rs` and `planner.rs`."""

from pathlib import Path
from typing import NamedTuple, Protocol, runtime_checkable


class ObjectRef(NamedTuple):
    uri: str
    key: str
    last_modified: str | None
    size: int | None


class StoredObject(NamedTuple):
    body: bytes
    version: str


class StorageClient(Protocol):
    # identifies the store behind a bucket name; None for local
    @property
    def endpoint(self) -> str | None: ...
    def resolve_uri(self, path: str) -> str: ...
    def list(self, prefix: str, start_after: str = "") -> list[ObjectRef]: ...
    def write(self, uri: str, body: bytes) -> None: ...
    def copy(
        self, src_uri: str, dst_uri: str, source_mtime: str | None = None
    ) -> None: ...
    def source_mtime(self, uri: str) -> str | None: ...
    def delete(self, uri: str) -> None: ...
    def read(self, uri: str) -> StoredObject | None: ...
    def write_conditional(
        self, uri: str, expected_version: str | None, body: bytes
    ) -> str | None: ...
    def delete_conditional(self, uri: str, expected_version: str | None) -> bool: ...


@runtime_checkable
class Downloader(Protocol):
    def download(self, uri: str, temp_dir: Path) -> Path: ...
