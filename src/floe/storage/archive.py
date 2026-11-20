"""Input archiving, ported from `io/storage/ops/archive.rs`."""

import json
from collections.abc import Callable

from floe.errors import FloeError, FloeRunError, FloeStorageError
from floe.storage.inputs import InputFile
from floe.storage.local import LocalClient
from floe.storage.paths import (
    archive_relative_path_for_run,
    parse_s3_uri,
    run_component,
)
from floe.storage.types import StorageClient

_INDEX = ".floe/source_mtime"


def archive_relative(
    entity_name: str, subdir: str, input_file: InputFile, run_id: str
) -> str:
    """`<entity>/[<subdir>/]<stem>__run-…__src-….<ext>`; `subdir` (D29): the partition sub-path kept after the entity folder."""
    return archive_relative_path_for_run(
        f"{entity_name}/{subdir}" if subdir else entity_name, input_file.source_name, run_id, input_file.source_uri
    )  # fmt: skip


def write_source_mtime_index(client: StorageClient, base: str, entity_name: str, run_id: str, mtimes: dict[str, str]) -> None:  # fmt: skip
    """#1271: before a run's archive copies, one `<base>/<entity>/.floe/source_mtime/<run_id>.json` maps each file's archived
    path under `<entity>/` to its source mtime (the copy keeps only its own, to the second on S3), for the D28 order.

    One object per run, not a sidecar per file: its key is base, entity and the same bounded run component as the archived
    names (`run_component`), so it is never longer than the archived keys it describes; a per-file key could pass S3's 1024 bytes. Not on local storage: a
    local copy's nanosecond mtime already follows the load order. A failed copy leaves an extra entry, harmless; a retry with the same run id adds to the entries."""
    if mtimes and not isinstance(client, LocalClient):
        uri = f"{base}/{entity_name}/{_INDEX}/{run_component(run_id)}.json"
        stored = client.read(
            uri
        )  # a retry with the same run id (Dagster) adds to the first attempt's entries
        try:
            kept: dict[str, str] = json.loads(stored.body) if stored else {}
        except ValueError as e:
            raise FloeStorageError(f"unreadable source mtime index {uri}: {e}") from e
        client.write(uri, json.dumps(kept | mtimes).encode())


def read_source_mtime_index(client: StorageClient, entity_uri: str) -> dict[str, str]:
    """The merged run indexes of `<entity_uri>/` (archived path under the entity to source mtime); empty on local storage."""
    if isinstance(client, LocalClient):
        return {}
    prefix = f"{parse_s3_uri(entity_uri)[1]}/{_INDEX}/"
    merged: dict[str, str] = {}
    for ref in client.list(prefix):
        stored = client.read(ref.uri) if ref.key.endswith(".json") else None
        if stored:
            merged.update(json.loads(stored.body))
    return merged


def archive_input_file(
    client: StorageClient,
    run_id: str,
    entity_name: str,
    join_relative: Callable[[str], str],
    input_file: InputFile,
    subdir: str = "",
) -> str:
    """`subdir` (D29): the partition sub-path kept between the entity folder and the file name."""
    relative = archive_relative(entity_name, subdir, input_file, run_id)
    dest_uri = join_relative(relative)
    client.copy(input_file.source_uri, dest_uri)
    try:
        client.delete(input_file.source_uri)
    except FloeError as e:
        raise FloeRunError(
            f"entity.name={entity_name} archive delete failed for {input_file.source_uri}: {e}"
        ) from e
    return dest_uri
