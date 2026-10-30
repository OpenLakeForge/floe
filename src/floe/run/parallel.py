"""Entities in parallel threads (#1105): the schedule keeps entities that share a resource apart and the failure rule of D36."""

from collections.abc import Callable, Sequence
from concurrent.futures import Future, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Any

from floe.config.storage import CatalogsConfig, StoragesConfig
from floe.config.types import EntityConfig
from floe.errors import FloeError
from floe.iceberg.sink import resolve_target
from floe.run.entity import state_uri
from floe.storage.resolve import resolve_path

MERGE = "merge"  # every merge entity shares this key: DuckDB memory, and Polaris conflicts on one table


def exclusive_keys(e: EntityConfig, config_dir: Path, storages: StoragesConfig | None, catalogs: CatalogsConfig | None) -> frozenset[str]:  # fmt: skip
    """What `e` writes that no other running entity may touch: its accepted path, Iceberg table, state file, and the merge slot."""
    sink = e.sink
    try:
        keys = {resolve_path(storages, config_dir, sink.accepted.storage, sink.accepted.path, f"entity.name={e.name} sink.accepted.storage")[0]}  # fmt: skip
        if sink.accepted.format == "iceberg":
            t = resolve_target(storages, catalogs, config_dir, e)
            keys.add(f"iceberg://{t.catalog.name}/{t.namespace}/{t.table}")
        if e.incremental_mode == "file":
            keys.add(state_uri(e, config_dir, storages)[0])
    except FloeError:
        return frozenset()  # `run_entity` raises the same error when it runs
    if sink.write_mode.startswith("merge"):
        keys.add(MERGE)
    return frozenset(keys)


def run_parallel(entities: Sequence[EntityConfig], keys: Sequence[frozenset[str]], workers: int, task: Callable[[EntityConfig], dict[str, Any]]) -> list[dict[str, Any]]:  # fmt: skip
    """Run `task` over `entities` on `workers` threads, never two with a key in common; the reports in `entities` order.

    After a failure no entity starts, the running ones finish (and release their claims), and the first error in `entities` order is raised.
    """
    reports: dict[int, dict[str, Any]] = {}
    errors: dict[int, BaseException] = {}
    pending = list(range(len(entities)))
    running: dict[Future[dict[str, Any]], int] = {}
    busy: set[str] = set()
    with ThreadPoolExecutor(workers, thread_name_prefix="floe-entity") as pool:
        while pending or running:
            for i in list(pending):
                if len(running) == workers:
                    break
                if not keys[i] & busy:
                    pending.remove(i)
                    busy |= keys[i]
                    running[pool.submit(task, entities[i])] = i
            for future in wait(running, return_when="FIRST_COMPLETED").done:
                i = running.pop(future)
                busy -= keys[i]
                if error := future.exception():
                    errors[i] = error
                    pending.clear()
                else:
                    reports[i] = future.result()
    if errors:
        raise errors[min(errors)]
    return [reports[i] for i in range(len(entities))]
