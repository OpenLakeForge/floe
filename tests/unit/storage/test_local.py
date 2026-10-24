"""Ported from `crates/floe-core/tests/unit/io/storage/local.rs` (client cases)."""

import os
import threading
import time
from pathlib import Path

import pytest

from floe.errors import FloeRunError, FloeStorageError
from floe.storage import local
from floe.storage.archive import archive_input_file
from floe.storage.inputs import InputFile
from floe.storage.local import LocalClient


def test_conditional_create_update_and_stale(tmp_path: Path) -> None:
    uri, client = str(tmp_path / "state.json"), LocalClient()
    created = client.write_conditional(uri, None, b'{"a":1}')
    assert created is not None
    assert (tmp_path / "state.json").read_bytes() == b'{"a":1}'
    assert client.write_conditional(uri, None, b'{"a":2}') is None
    updated = client.write_conditional(uri, created, b'{"aa":22}')
    assert updated is not None
    assert updated != created
    assert client.write_conditional(uri, created, b'{"a":3}') is None
    assert client.read(uri) == (b'{"aa":22}', updated)
    assert client.read(str(tmp_path / "missing")) is None


def test_concurrent_creates_have_one_winner(tmp_path: Path) -> None:
    uri, client = str(tmp_path / "state.json"), LocalClient()
    results: list[str | None] = []
    start = threading.Barrier(8)

    def writer(i: int) -> None:
        start.wait()
        results.append(client.write_conditional(uri, None, str(i).encode()))

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(r is not None for r in results) == 1


def test_stale_lock_is_broken(tmp_path: Path) -> None:
    lock = tmp_path / "state.json.lock"
    lock.write_text("0:12345")
    assert LocalClient().write_conditional(str(tmp_path / "state.json"), None, b"x")
    assert not lock.exists()


def test_same_size_writes_in_one_tick_get_distinct_versions(tmp_path: Path) -> None:
    path, client = tmp_path / "state.json", LocalClient()
    first = client.write_conditional(str(path), None, b"aa")
    assert first is not None
    st = path.stat()
    second = client.write_conditional(str(path), first, b"bb")
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns))  # same size, same mtime
    assert second is not None
    assert client.write_conditional(str(path), first, b"cc") is None
    assert path.read_bytes() == b"bb"


def test_concurrent_stale_lock_breakers_have_one_winner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = tmp_path / "state.json"
    Path(f"{base}.lock").write_text("0:12345")
    seen = threading.local()
    both_saw_stale = threading.Barrier(2)
    real = local._lock_is_stale  # pyright: ignore[reportPrivateUsage]

    def stale(lock: Path) -> bool:
        result = real(lock)
        if not getattr(seen, "done", False):
            seen.done = True
            both_saw_stale.wait(5)  # both decide to break before either does
        return result

    monkeypatch.setattr(local, "_lock_is_stale", stale)
    inside = 0
    peak = 0
    guard = threading.Lock()

    def worker() -> None:
        nonlocal inside, peak
        with local._file_lock(base):  # pyright: ignore[reportPrivateUsage]
            with guard:
                inside += 1
                peak = max(peak, inside)
            time.sleep(0.3)
            with guard:
                inside -= 1

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert peak == 1


def test_crash_during_replace_keeps_old_content(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path, client = tmp_path / "state.json", LocalClient()
    version = client.write_conditional(str(path), None, b"old")

    def boom(*_: object) -> None:
        raise OSError("crash")

    monkeypatch.setattr(os, "replace", boom)
    with pytest.raises(FloeStorageError):
        client.write_conditional(str(path), version, b"new")
    assert path.read_bytes() == b"old"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["state.json"]


def test_fresh_lock_blocks_until_released(tmp_path: Path) -> None:
    lock = tmp_path / "state.json.lock"
    lock.write_text(f"{time.time_ns() // 1_000_000}:99999")
    done = threading.Event()
    out: list[str | None] = []

    def writer() -> None:
        out.append(
            LocalClient().write_conditional(str(tmp_path / "state.json"), None, b"x")
        )
        done.set()

    threading.Thread(target=writer).start()
    assert not done.wait(0.2)
    lock.unlink()
    assert done.wait(5)
    assert out[0] is not None


def test_list_sorted_and_missing(tmp_path: Path) -> None:
    (tmp_path / "b.csv").write_text("1")
    (tmp_path / "a.csv").write_text("2")
    (tmp_path / "sub").mkdir()
    client = LocalClient()
    assert [r.key for r in client.list(str(tmp_path))] == [
        str(tmp_path / "a.csv"),
        str(tmp_path / "b.csv"),
    ]
    assert client.list(str(tmp_path / "nope")) == []


def _input(path: Path) -> InputFile:
    return InputFile(f"local://{path}", path.name, path.stem, None, None)


def test_archive_copies_then_deletes(tmp_path: Path) -> None:
    src = tmp_path / "in" / "input.csv"
    src.parent.mkdir()
    src.write_text("id\n1\n")
    dest = archive_input_file(
        LocalClient(),
        "run-1",
        "orders",
        lambda rel: str(tmp_path / "arch" / rel),
        _input(src),
    )
    assert Path(dest).read_text() == "id\n1\n"
    assert not src.exists()
    assert Path(dest).parent == tmp_path / "arch" / "orders"


def test_archive_delete_failure_is_run_error(tmp_path: Path) -> None:
    src = tmp_path / "input.csv"
    src.write_text("x")

    class Failing(LocalClient):
        def delete(self, uri: str) -> None:
            raise FloeStorageError("boom")

    with pytest.raises(
        FloeRunError, match=r"entity\.name=orders archive delete failed for .*boom"
    ):
        archive_input_file(
            Failing(), "r", "orders", lambda rel: str(tmp_path / rel), _input(src)
        )


def test_read_and_write_failures_are_storage_errors(tmp_path: Path) -> None:
    client = LocalClient()
    with pytest.raises(FloeStorageError):
        client.read(str(tmp_path))
    (tmp_path / "f").write_text("x")
    with pytest.raises(FloeStorageError):
        client.write_conditional(str(tmp_path / "f" / "state.json"), None, b"x")


def test_copy_keeps_restrictive_mode(tmp_path: Path) -> None:
    src = tmp_path / "s"
    src.write_text("x")
    src.chmod(0o600)
    LocalClient().copy(str(src), str(tmp_path / "d"))
    assert (tmp_path / "d").stat().st_mode & 0o777 == 0o600
