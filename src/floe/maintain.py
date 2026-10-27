"""`floe maintain`: Iceberg table maintenance for the entities' accepted sinks (#618, Python only)."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
# ^ pyarrow ships no type stubs

import time
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from pyiceberg.table import Table

from floe.config.project import RootConfig
from floe.config.storage import StorageDefinition
from floe.config.validate import validate_config
from floe.errors import FloeConfigError, FloeRunError
from floe.iceberg.catalog import run_error
from floe.iceberg.sink import existing_table, resolve_target
from floe.run import select_entities
from floe.storage.paths import normalize_local_path, parse_s3_uri
from floe.storage.s3 import S3Client

# the retention floor (#545 decision 4): the retry guard reads snapshot summaries this recent
FLOOR_DAYS, FLOOR_SNAPSHOTS = 7, 10
# an orphan younger than the delay may be a file an in-flight `floe run` has not committed yet
ORPHAN_DELAY_DAYS, ORPHAN_FLOOR_DAYS = 3, 1
DAY = 86_400


def expire_snapshots(table: Table, older_than_days: int, retain_last: int, dry_run: bool) -> tuple[int, int]:  # fmt: skip
    """Expire snapshots older than the window and outside the newest `retain_last`; returns (expired, kept).

    Metadata only: no file is deleted. Branch and tag heads are never expired.
    """
    cutoff = (time.time() - older_than_days * DAY) * 1000
    newest = sorted(table.metadata.snapshots, key=lambda s: s.timestamp_ms, reverse=True)  # fmt: skip
    protected = {ref.snapshot_id for ref in table.metadata.refs.values()}
    expired = [
        s.snapshot_id for s in newest[retain_last:]
        if s.timestamp_ms < cutoff and s.snapshot_id not in protected
    ]  # fmt: skip
    if expired and not dry_run:
        with run_error("iceberg snapshot expiry failed"):
            table.maintenance.expire_snapshots().by_ids(expired).commit()
    return len(expired), len(newest) - len(expired)


def _key(uri: str) -> str:
    """`s3://b/k` -> `b/k`; `file:///p` and `/p` -> `/p`: metadata and listings spell paths differently."""
    parts = urlsplit(uri)
    return parts.netloc + parts.path


def _list(location: str, storage: StorageDefinition | None) -> list[tuple[str, float | None, int]]:  # fmt: skip
    """(uri, mtime, size) of every object under the table location."""
    if location.startswith("s3://"):
        bucket, key = parse_s3_uri(location)
        s3 = storage if storage and storage.type == "s3" else None
        client = S3Client(bucket, s3 and s3.region, s3 and s3.endpoint, s3 and s3.path_style_access)  # fmt: skip
        return [
            (r.uri, datetime.fromisoformat(r.last_modified).timestamp() if r.last_modified else None,r.size or 0)
            for r in client.list(f"{key.strip('/')}/" if key.strip("/") else "")  # a bucket root lists every key
        ]  # fmt: skip
    # ponytail: LocalClient.list is flat and has no mtime (a Rust port), so a local warehouse is walked here
    files = (p for p in Path(_key(location)).rglob("*") if p.is_file())
    return [(f"file://{p}", (st := p.stat()).st_mtime, st.st_size) for p in files]


def _reachable(table: Table) -> set[str]:
    """Every file the table's retained metadata references."""
    m, inspect = table.metadata, table.inspect
    paths = [table.metadata_location, *(e.metadata_file for e in m.metadata_log), *(s.manifest_list for s in m.snapshots)]  # fmt: skip
    paths += [s.statistics_path for s in m.statistics] + [s.statistics_path for s in m.partition_statistics]  # fmt: skip
    with run_error("iceberg metadata scan failed"):
        paths += inspect.all_manifests()["path"].to_pylist() + inspect.all_files()["file_path"].to_pylist()  # fmt: skip
    return {_key(p) for p in paths}


def remove_orphans(table: Table, storage: StorageDefinition | None, delay_days: int, dry_run: bool) -> list[tuple[str, int]]:  # fmt: skip
    """Delete the objects under the table location that no retained metadata references and older than the delay."""
    listed = _list(table.location(), storage)
    if _key(table.metadata_location) not in {_key(uri) for uri, _, _ in listed}:
        # a listing that misses the current metadata would make every file look orphaned
        raise FloeRunError(f"orphan scan of {table.location()} did not list the current metadata {table.metadata_location}; nothing was deleted")  # fmt: skip
    keep, cutoff = _reachable(table), time.time() - delay_days * DAY
    orphans = [(uri, size) for uri, mtime, size in listed if _key(uri) not in keep and mtime is not None and mtime < cutoff]  # fmt: skip
    if not dry_run:
        with run_error("iceberg orphan file delete failed"):
            for uri, _ in orphans:
                table.io.delete(uri)
    return orphans


def maintain(path: Path, root: RootConfig, entities: Sequence[str], older_than_days: int, retain_last: int, orphan_delay_days: int, dry_run: bool) -> list[dict[str, Any]]:  # fmt: skip
    """Expire snapshots, then remove orphan files, of every selected entity's Iceberg table.

    A request below the retention floor or the orphan delay floor fails closed.
    """
    if older_than_days < FLOOR_DAYS or retain_last < FLOOR_SNAPSHOTS:
        raise FloeConfigError(
            f"snapshot expiry below the retention floor: --older-than-days must be at least {FLOOR_DAYS} "
            f"and --retain-last at least {FLOOR_SNAPSHOTS} (got {older_than_days} and {retain_last})"
        )
    if orphan_delay_days < ORPHAN_FLOOR_DAYS:
        raise FloeConfigError(f"orphan delay below the floor: --orphan-delay-days must be at least {ORPHAN_FLOOR_DAYS} (got {orphan_delay_days})")  # fmt: skip
    validate_config(root)
    config_dir = normalize_local_path(path.resolve().parent)
    results: list[dict[str, Any]] = []
    for entity in select_entities(root, entities):
        if entity.sink.accepted.format != "iceberg":
            continue
        target = resolve_target(root.storages, root.catalogs, config_dir, entity)
        table = existing_table(target)
        expired, kept = expire_snapshots(table, older_than_days, retain_last, dry_run) if table else (0, 0)  # fmt: skip
        orphans = remove_orphans(table, target.storage, orphan_delay_days, dry_run) if table else []  # fmt: skip
        results.append({
            "entity": entity.name, "table": f"{target.namespace}.{target.table}",
            "expired_snapshots": expired, "kept_snapshots": kept,
            "orphan_files": [uri for uri, _ in orphans], "orphan_bytes": sum(size for _, size in orphans),
        })  # fmt: skip
    return results
