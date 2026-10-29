"""Local storage client, ported from `io/storage/providers/local.rs` (`LocalClient`)."""

import hashlib
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
    return hashlib.sha256(path.read_bytes()).hexdigest()


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


def _break_stale_lock(lock: Path) -> bool:
    """Single-winner break: a hard link to a per-inode claim name succeeds once."""
    try:
        ino = lock.stat().st_ino
        claim = Path(f"{lock}.{ino}.742.claim")
        os.link(lock, claim)
    except OSError:  # lock vanished, or another waiter already holds the claim
        return False
    try:
        # the lock may have been released and retaken between the check and the link
        if claim.stat().st_ino == ino and _lock_is_stale(claim):
            lock.unlink(missing_ok=True)
            return True
        return False
    finally:
        # ponytail: a crash before this unlink leaves the claim; the lock then times out
        claim.unlink(missing_ok=True)


@contextmanager
def _file_lock(base: Path) -> Generator[None]:
    lock = Path(f"{base}.lock")
    deadline = time.monotonic() + _LOCK_TIMEOUT_S
    while True:
        try:
            fd = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
        except FileExistsError:
            if _lock_is_stale(lock) and _break_stale_lock(lock):
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

    def list(self, prefix: str, start_after: str = "") -> list[ObjectRef]:
        p = Path(prefix)
        if p.is_file():
            return [ObjectRef(self.resolve_uri(prefix), prefix, None, None)]
        if not p.exists():
            return []
        keys = [str(c) for c in p.iterdir() if c.is_file() and str(c) > start_after]
        return sorted(
            (ObjectRef(self.resolve_uri(k), k, None, None) for k in keys),
            key=lambda r: r.uri,
        )

    def copy(self, src_uri: str, dst_uri: str) -> None:
        src, dst = _path(src_uri), _path(dst_uri)
        try:
            dst.parent.mkdir(parents=True, exist_ok=True)
            mode = src.stat().st_mode & 0o777
            dst.unlink(missing_ok=True)  # not through a symlink or shared inode
            fd = os.open(dst, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
            os.fchmod(fd, mode)  # #779: the open mode applies on create only
            with src.open("rb") as fin, os.fdopen(fd, "wb") as fout:
                shutil.copyfileobj(fin, fout)
        except OSError as e:
            raise FloeStorageError(f"local copy failed from {src} to {dst}: {e}") from e

    def write(self, uri: str, body: bytes) -> None:
        path = _path(uri)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
        except OSError as e:
            raise FloeStorageError(f"local write failed for {path}: {e}") from e

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
                tmp = path.with_name(f"{path.name}.{os.getpid()}.742.tmp")
                try:
                    with tmp.open("wb") as f:
                        if current is not None:
                            shutil.copymode(path, tmp)
                        f.write(body)
                        f.flush()
                        os.fsync(f.fileno())
                    os.replace(tmp, path)
                finally:
                    tmp.unlink(missing_ok=True)
                return _version(path)
        except OSError as e:
            raise FloeStorageError(f"local write failed for {path}: {e}") from e

    def delete_conditional(self, uri: str, expected_version: str | None) -> bool:
        """False on a version conflict; `None` expects no file."""
        path = _path(uri)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with _file_lock(path):
                current = _version(path) if path.exists() else None
                if current != expected_version:
                    return False
                path.unlink(missing_ok=True)
                return True
        except OSError as e:
            raise FloeStorageError(f"local delete failed for {path}: {e}") from e
