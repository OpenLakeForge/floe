"""Download the inputs of one entity a few files ahead of the one being read (#1102)."""

from collections import deque
from collections.abc import Generator, Iterable
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path

from floe.storage.inputs import InputFile, LocalInputFile, localize_input
from floe.storage.types import Downloader

# ponytail: a fixed window of 8 files and 512 MiB (unknown size counts as 0; the next file is always allowed) and one pool per entity; a config knob or a run-wide pool once a bucket needs more
PREFETCH_WINDOW = 8
PREFETCH_BYTES = 512 * 1024 * 1024


def localized_inputs(
    files: Iterable[InputFile], client: Downloader | None, temp_dir: Path
) -> Generator[LocalInputFile]:
    """`localize_input` for each file, in order. Close it to stop: pending downloads are cancelled and the running ones awaited."""
    if client is None:  # local inputs are used in place; no pool
        for f in files:
            yield localize_input(f, None, temp_dir)
        return
    todo = deque(files)
    ahead: deque[tuple[Future[LocalInputFile], int]] = deque()
    pool = ThreadPoolExecutor(PREFETCH_WINDOW, thread_name_prefix="floe-prefetch")

    def refill() -> None:
        held = sum(size for _, size in ahead)
        while todo and len(ahead) < PREFETCH_WINDOW:
            size = todo[0].source_size or 0
            if ahead and held + size > PREFETCH_BYTES:
                break
            held += size
            ahead.append(
                (pool.submit(localize_input, todo.popleft(), client, temp_dir), size)
            )

    try:
        refill()
        while ahead:
            local = ahead.popleft()[0].result()
            refill()
            yield local
    finally:
        pool.shutdown(cancel_futures=True)
