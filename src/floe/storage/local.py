"""Local storage client, ported from `io/storage/providers/local.rs` (`LocalClient`)."""

import os
import shutil
import time
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

from floe.errors import FloeStorageError
from floe.storage.paths import normalize_local_path
from floe.storage.types import ObjectRef, StoredObject

_LOCK_STALE_TTL_S = 5 * 60
_LOCK_TIMEOUT_S = 10


def _path(uri: str) -> Path:
    return Path(uri.removeprefix("local://"))


def _version(path: Path) -> str:
    st = path.stat()
    return f"{st.st_size}:{st.st_mtime_ns}"


def _lock_is_stale(lock: Path) -> bool:
    try:
        acquired_ms = int(lock.read_text().split(":")[0].strip())
        age = time.time() - acquired_ms / 1000
    except (ValueError, OSError):
        try:
            age = time.time() - lock.stat().st_mtime
        except OSError:
            return False
    return age > _LOCK_STALE_TTL_S


@contextmanager
def _file_lock(base: Path) -> Generator[None]:
    lock = Path(f"{base}.lock")
    deadline = time.monotonic() + _LOCK_TIMEOUT_S
    while True:
        try:
            fd = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
        except FileExistsError:
            if _lock_is_stale(lock):
                lock.unlink(missing_ok=True)
                continue
            if time.monotonic() >= deadline:
                raise FloeStorageError(
                    f"timed out acquiring local state lock {lock}"
                ) from None
            time.sleep(0.01)
            continue
        with os.fdopen(fd, "w") as f:
            f.write(f"{time.time_ns() // 1_000_000}:{os.getpid()}")
        break
    try:
        yield
    finally:
        lock.unlink(missing_ok=True)


class LocalClient:
    def resolve_uri(self, path: str) -> str:
        return f"local://{normalize_local_path(Path.cwd() / path)}"

    def list(self, prefix: str) -> list[ObjectRef]:
        p = Path(prefix)
        if p.is_file():
            return [ObjectRef(self.resolve_uri(prefix), prefix, None, None)]
        if not p.exists():
            return []
        keys = [str(c) for c in p.iterdir() if c.is_file()]
        return sorted(
            (ObjectRef(self.resolve_uri(k), k, None, None) for k in keys),
            key=lambda r: r.uri,
        )

    def copy(self, src_uri: str, dst_uri: str) -> None:
        src, dst = _path(src_uri), _path(dst_uri)
        try:
            dst.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(
                dst, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, src.stat().st_mode & 0o777
            )
            with src.open("rb") as fin, os.fdopen(fd, "wb") as fout:
                shutil.copyfileobj(fin, fout)
        except OSError as e:
            raise FloeStorageError(f"local copy failed from {src} to {dst}: {e}") from e

    def delete(self, uri: str) -> None:
        path = _path(uri)
        try:
            path.unlink(missing_ok=True)
        except OSError as e:
            raise FloeStorageError(f"local delete failed for {path}: {e}") from e

    def read(self, uri: str) -> StoredObject | None:
        path = _path(uri)
        try:
            if not path.exists():
                return None
            with _file_lock(path):
                if not path.exists():
                    return None
                return StoredObject(path.read_bytes(), _version(path))
        except OSError as e:
            raise FloeStorageError(f"local read failed for {path}: {e}") from e

    def write_conditional(
        self, uri: str, expected_version: str | None, body: bytes
    ) -> str | None:
        path = _path(uri)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with _file_lock(path):
                current = _version(path) if path.exists() else None
                if current != expected_version:
                    return None
                path.write_bytes(body)
                return _version(path)
        except OSError as e:
            raise FloeStorageError(f"local write failed for {path}: {e}") from e
