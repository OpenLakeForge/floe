"""Download the inputs of one entity a few files ahead of the one being read (#1102)."""

from collections import deque
from collections.abc import Generator, Iterable
from concurrent.futures import Future, ThreadPoolExecutor
from itertools import islice
from pathlib import Path

from floe.storage.inputs import InputFile, LocalInputFile, localize_input
from floe.storage.types import Downloader

# ponytail: a fixed window and one pool per entity; a config knob or a run-wide pool once a bucket needs more than 8 GETs in flight
PREFETCH_WINDOW = 8


def localized_inputs(
    files: Iterable[InputFile], client: Downloader | None, temp_dir: Path
) -> Generator[LocalInputFile]:
    """`localize_input` for each file, in order. Close it to stop: pending downloads are cancelled and the running ones awaited."""
    if client is None:  # local inputs are used in place; no pool
        for f in files:
            yield localize_input(f, None, temp_dir)
        return
    todo = iter(files)
    ahead: deque[Future[LocalInputFile]] = deque()
    pool = ThreadPoolExecutor(PREFETCH_WINDOW, thread_name_prefix="floe-prefetch")
    try:
        for f in islice(todo, PREFETCH_WINDOW):
            ahead.append(pool.submit(localize_input, f, client, temp_dir))
        while ahead:
            local = ahead.popleft().result()
            for f in islice(todo, 1):
                ahead.append(pool.submit(localize_input, f, client, temp_dir))
            yield local
    finally:
        pool.shutdown(cancel_futures=True)
