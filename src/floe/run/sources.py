"""Source side of an entity run: resolving and listing the inputs (and the archive a full refresh reads back)."""

from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

from floe.config.storage import StorageDefinition, StoragesConfig
from floe.config.types import EntityConfig
from floe.run.incremental import state_uri
from floe.state import read_state
from floe.storage.inputs import NoInputsError, ResolvedInputs, resolve_inputs
from floe.storage.local import LocalClient
from floe.storage.partitioning import (
    in_load_order,
    listing_start,
    source_root,
    window,
)
from floe.storage.resolve import resolve_path, storage_client
from floe.storage.s3 import S3Client
from floe.storage.types import StorageClient


def resolve_source(entity: EntityConfig, config_dir: Path, storages: StoragesConfig | None) -> tuple[str, LocalClient | S3Client]:  # fmt: skip
    """`source.path` as a URI, and the client of its storage."""
    src = entity.source
    uri, definition = resolve_path(storages, config_dir, src.storage, src.path, f"entity.name={entity.name} source.storage")  # fmt: skip
    return uri, storage_client(definition)


def archive_location(entity: EntityConfig, config_dir: Path, storages: StoragesConfig | None) -> tuple[str, StorageDefinition | None] | None:  # fmt: skip
    """`sink.archive` as a URI (no trailing `/`) and its storage definition."""
    archive, src = entity.sink.archive, entity.source
    if archive is None:
        return None
    uri, definition = resolve_path(storages, config_dir, archive.storage or src.storage, archive.path, f"entity.name={entity.name} sink.archive.storage")  # fmt: skip
    return uri.rstrip("/"), definition


def list_source(entity: EntityConfig, config_dir: Path, storages: StoragesConfig | None, uri: str, client: StorageClient, full_refresh: bool = False) -> ResolvedInputs:  # fmt: skip
    """The source inputs; a local path keeps its globs relative to the config.

    D31: the archive is never listed as source input; only a full refresh reads it, explicitly (D24).
    D30: `file` mode with partitioning lists only from the state's window.
    """
    src, archive = entity.source, archive_location(entity, config_dir, storages)
    p, root, state = src.partitioning, source_root(uri), None
    if p and entity.incremental_mode == "file" and not full_refresh:
        at, state_client = state_uri(entity, config_dir, storages)
        state = read_state(state_client, at, entity.name)
    start = window(p, entity.name, root, state) if p else None
    after = listing_start(p, start) if p and start else None

    def source() -> ResolvedInputs:
        try:
            inputs = resolve_inputs(
                config_dir, src.path if uri.startswith("local://") else uri, src.format, entity.name,
                src.storage or "local", client, glob_override=src.options.glob, exclude=archive and archive[0],
                recursive=bool(src.options.recursive) or p is not None,  # D28: partitioning lists recursively
                after=f"{root}/{after}" if after else "",
            )  # fmt: skip
        except NoInputsError:
            # #1139: an empty incremental source has nothing pending; a full refresh still fails (a broken extraction)
            if entity.incremental_mode != "file" or full_refresh:
                raise
            return ResolvedInputs([], [], "directory")
        if not p:
            return inputs
        inputs = in_load_order(p, entity.name, root, inputs, start=start)
        if start is None or state is None:
            return inputs
        late = f"entity.name={entity.name} incremental_mode=file lists source.partitioning dates from {start} (watermark {state.watermark}, lookback_days {p.lookback_days}); a late file in an older day is not ingested until --full-refresh"  # fmt: skip
        return replace(inputs, warning=late)

    if full_refresh and archive and entity.incremental_mode in ("none", "archive"):
        return _with_archived(entity, (archive[0], storage_client(archive[1])), source)
    return source()


def _with_archived(e: EntityConfig, archive: tuple[str, StorageClient], source: Callable[[], ResolvedInputs]) -> ResolvedInputs:  # fmt: skip
    """D24: a full refresh reads `<archive>/<entity>/` first (by name; D29: in load order), then the source inputs; either may be empty, not both.

    Only "no inputs matched" counts as empty: any other listing error fails the refresh rather than rebuild from part of the inputs.
    """
    base, client = archive
    p = e.source.partitioning
    path = e.name if base.startswith("local://") else f"{base}/{e.name}/"
    try:
        old = resolve_inputs(Path(base.removeprefix("local://")), path, e.source.format, e.name, e.source.storage or "local", client, recursive=p is not None)  # fmt: skip
    except NoInputsError:
        return (
            source()
        )  # nothing archived: the source's "no input files matched" stands
    if p is not None:  # D29: the archive keeps the partition sub-paths
        # #939: D28 orders a day's batches by the source's modification time, which the copy's own does not keep
        kept = [replace(f, source_mtime=client.source_mtime(f.source_uri) or f.source_mtime) for f in old.files]  # fmt: skip
        old = in_load_order(
            p, e.name, f"{base}/{e.name}", replace(old, files=kept), archived=True
        )
    try:
        new = source()
    except NoInputsError:  # no new source file: rebuild from the archive alone
        return old
    seen = set(old.listed)  # an S3 source prefix also lists an archive nested under it
    files = [f for f in new.files if f.source_uri not in seen]
    return ResolvedInputs(old.files + files, old.listed + [u for u in new.listed if u not in seen], new.mode)  # fmt: skip
