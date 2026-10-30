import threading
import time
from pathlib import Path

import pytest

from floe.errors import FloeStorageError
from floe.storage import prefetch
from floe.storage.inputs import InputFile
from floe.storage.prefetch import PREFETCH_BYTES, PREFETCH_WINDOW, localized_inputs


class SleepingDownloader:
    """A `Downloader` that takes `delay` seconds per file and writes the file for real."""

    def __init__(self, delay: float = 0.05, fail: str | None = None) -> None:
        self.delay, self.fail = delay, fail
        self.started: list[str] = []
        self.peak = self.active = 0
        self._lock = threading.Lock()

    def download(self, uri: str, temp_dir: Path) -> Path:
        with self._lock:
            self.started.append(uri)
            self.active += 1
            self.peak = max(self.peak, self.active)
        try:
            time.sleep(self.delay)
            if uri == self.fail:
                raise FloeStorageError(f"s3 get object failed: {uri}")
            dest = temp_dir / uri.rsplit("/", 1)[-1]
            dest.write_text(uri)
            return dest
        finally:
            with self._lock:
                self.active -= 1


def inputs(n: int) -> list[InputFile]:
    return [InputFile(f"s3://b/in/{i:02}.csv", f"{i:02}.csv", f"{i:02}", None, None) for i in range(n)]  # fmt: skip


def test_files_come_back_in_the_given_order_with_a_bounded_window(tmp_path: Path) -> None:  # fmt: skip
    client = SleepingDownloader()
    files = inputs(30)[::-1]
    got = [x.file.source_uri for x in localized_inputs(files, client, tmp_path)]
    assert got == [f.source_uri for f in files]
    assert 1 < client.peak <= PREFETCH_WINDOW


def test_downloads_stay_within_the_window_of_the_file_being_read(tmp_path: Path) -> None:  # fmt: skip
    client = SleepingDownloader()
    stream = localized_inputs(inputs(30), client, tmp_path)
    next(stream)
    time.sleep(0.2)
    assert len(client.started) == PREFETCH_WINDOW + 1
    stream.close()


def test_large_files_are_prefetched_one_at_a_time_and_small_ones_fill_the_window(tmp_path: Path) -> None:  # fmt: skip
    big = SleepingDownloader(delay=0.02)
    files = [InputFile(f.source_uri, f.source_name, f.source_stem, PREFETCH_BYTES, None) for f in inputs(6)]  # fmt: skip
    assert len(list(localized_inputs(files, big, tmp_path))) == 6
    assert big.peak == 1
    small = SleepingDownloader(delay=0.02)
    files = [InputFile(f.source_uri, f.source_name, f.source_stem, 1, None) for f in inputs(30)]  # fmt: skip
    assert len(list(localized_inputs(files, small, tmp_path))) == 30
    assert 1 < small.peak <= PREFETCH_WINDOW


def test_a_failed_download_raises_at_its_turn_with_its_own_error(tmp_path: Path) -> None:  # fmt: skip
    client = SleepingDownloader(delay=0.01, fail="s3://b/in/02.csv")
    stream = localized_inputs(inputs(5), client, tmp_path)
    assert [next(stream).file.source_uri for _ in range(2)] == ["s3://b/in/00.csv", "s3://b/in/01.csv"]  # fmt: skip
    with pytest.raises(FloeStorageError) as err:
        next(stream)
    assert str(err.value) == "s3 get object failed: s3://b/in/02.csv"


def test_closing_early_cancels_the_queued_downloads_and_awaits_the_running_ones(tmp_path: Path) -> None:  # fmt: skip
    client = SleepingDownloader(delay=0.2)
    stream = localized_inputs(inputs(40), client, tmp_path)
    next(stream)
    stream.close()
    assert client.active == 0  # the downloads were awaited, not abandoned
    seen = len(client.started)
    time.sleep(0.4)
    assert len(client.started) == seen <= PREFETCH_WINDOW + 1
    assert not [t for t in threading.enumerate() if t.name.startswith("floe-prefetch")]


def test_a_local_source_never_starts_a_pool(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    def no_pool(*_: object, **__: object) -> None:
        raise AssertionError("pool")

    monkeypatch.setattr(prefetch, "ThreadPoolExecutor", no_pool)
    files = [InputFile(f"local://{tmp_path}/{n}.csv", f"{n}.csv", str(n), None, None) for n in "ab"]  # fmt: skip
    got = [x for x in localized_inputs(files, None, tmp_path)]
    assert [(x.local_path, x.is_ephemeral) for x in got] == [(tmp_path / "a.csv", False), (tmp_path / "b.csv", False)]  # fmt: skip
