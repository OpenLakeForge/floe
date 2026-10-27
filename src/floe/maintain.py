"""`floe maintain`: Iceberg table maintenance for the entities' accepted sinks (#618, Python only)."""

import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from pyiceberg.table import Table

from floe.config.project import RootConfig
from floe.config.validate import validate_config
from floe.errors import FloeConfigError
from floe.iceberg.catalog import run_error
from floe.iceberg.sink import existing_table, resolve_target
from floe.run import select_entities
from floe.storage.paths import normalize_local_path

# the retention floor (#545 decision 4): the retry guard reads snapshot summaries this recent
FLOOR_DAYS, FLOOR_SNAPSHOTS = 7, 10


def expire_snapshots(table: Table, older_than_days: int, retain_last: int) -> tuple[int, int]:  # fmt: skip
    """Expire snapshots older than the window and outside the newest `retain_last`; returns (expired, kept).

    Metadata only: no file is deleted. Branch and tag heads are never expired.
    """
    cutoff = (time.time() - older_than_days * 86_400) * 1000
    newest = sorted(table.metadata.snapshots, key=lambda s: s.timestamp_ms, reverse=True)  # fmt: skip
    protected = {ref.snapshot_id for ref in table.metadata.refs.values()}
    expired = [
        s.snapshot_id for s in newest[retain_last:]
        if s.timestamp_ms < cutoff and s.snapshot_id not in protected
    ]  # fmt: skip
    if expired:
        with run_error("iceberg snapshot expiry failed"):
            table.maintenance.expire_snapshots().by_ids(expired).commit()
    return len(expired), len(newest) - len(expired)


def maintain(path: Path, root: RootConfig, entities: Sequence[str], older_than_days: int = FLOOR_DAYS, retain_last: int = FLOOR_SNAPSHOTS) -> list[dict[str, Any]]:  # fmt: skip
    """Expire snapshots of every selected entity's Iceberg table; a request below the floor fails closed."""
    if older_than_days < FLOOR_DAYS or retain_last < FLOOR_SNAPSHOTS:
        raise FloeConfigError(
            f"snapshot expiry below the retention floor: --older-than-days must be at least {FLOOR_DAYS} "
            f"and --retain-last at least {FLOOR_SNAPSHOTS} (got {older_than_days} and {retain_last})"
        )
    validate_config(root)
    config_dir = normalize_local_path(path.resolve().parent)
    results: list[dict[str, Any]] = []
    for entity in select_entities(root, entities):
        if entity.sink.accepted.format != "iceberg":
            continue
        target = resolve_target(root.storages, root.catalogs, config_dir, entity)
        table = existing_table(target)
        expired, kept = expire_snapshots(table, older_than_days, retain_last) if table else (0, 0)  # fmt: skip
        results.append({
            "entity": entity.name, "table": f"{target.namespace}.{target.table}",
            "expired_snapshots": expired, "kept_snapshots": kept,
        })  # fmt: skip
    return results
