"""Storage client types, ported from `io/storage/mod.rs` and `planner.rs`."""

from typing import NamedTuple, Protocol


class ObjectRef(NamedTuple):
    uri: str
    key: str
    last_modified: str | None
    size: int | None


class StorageClient(Protocol):
    def resolve_uri(self, path: str) -> str: ...
    def list(self, prefix: str) -> list[ObjectRef]: ...
