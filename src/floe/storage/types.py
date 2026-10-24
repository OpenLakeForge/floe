"""Storage client types, ported from `io/storage/mod.rs` and `planner.rs`."""

from pathlib import Path
from typing import NamedTuple, Protocol


class ObjectRef(NamedTuple):
    uri: str
    key: str
    last_modified: str | None
    size: int | None


class StoredObject(NamedTuple):
    body: bytes
    version: str


class StorageClient(Protocol):
    def resolve_uri(self, path: str) -> str: ...
    def list(self, prefix: str) -> list[ObjectRef]: ...
    def copy(self, src_uri: str, dst_uri: str) -> None: ...
    def delete(self, uri: str) -> None: ...
    def read(self, uri: str) -> StoredObject | None: ...
    def write_conditional(
        self, uri: str, expected_version: str | None, body: bytes
    ) -> str | None: ...


class Downloader(Protocol):
    def download(self, uri: str, temp_dir: Path) -> Path: ...
