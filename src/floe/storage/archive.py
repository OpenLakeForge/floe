"""Input archiving, ported from `io/storage/ops/archive.rs`."""

from collections.abc import Callable

from floe.errors import FloeError, FloeRunError
from floe.storage.inputs import InputFile
from floe.storage.local import LocalClient
from floe.storage.paths import archive_relative_path_for_run
from floe.storage.types import StorageClient

_SIDECAR = ".floe/source_mtime"


def archived_source_mtime(
    client: StorageClient, entity_uri: str, uri: str
) -> str | None:
    """#1271: the source mtime of an archived file, for D28 order: its `<entity_uri>/.floe/source_mtime/<path>` sidecar,
    else the object metadata of archives written before the sidecar (#939); None when neither exists.

    Not on local storage: a local copy's nanosecond mtime already follows the load order, and a lock file for
    a sidecar named like a ~255-byte archived file would exceed the file name limit."""
    if isinstance(client, LocalClient):
        return None
    stored = client.read(
        f"{entity_uri}/{_SIDECAR}/{uri.removeprefix(f'{entity_uri}/')}"
    )
    return stored.body.decode() if stored else client.source_mtime(uri)


def archive_input_file(
    client: StorageClient,
    run_id: str,
    entity_name: str,
    join_relative: Callable[[str], str],
    input_file: InputFile,
    subdir: str = "",
) -> str:
    """`subdir` (D29): the partition sub-path kept between the entity folder and the file name."""
    relative = archive_relative_path_for_run(
        f"{entity_name}/{subdir}" if subdir else entity_name, input_file.source_name, run_id, input_file.source_uri
    )  # fmt: skip
    dest_uri = join_relative(relative)
    if (
        input_file.source_mtime and not isinstance(client, LocalClient)
    ):  # first: an archived file then always has its sidecar; a retry overwrites a stray one
        client.write(join_relative(f"{entity_name}/{_SIDECAR}/{relative.removeprefix(f'{entity_name}/')}"), input_file.source_mtime.encode())  # fmt: skip
    client.copy(input_file.source_uri, dest_uri)
    try:
        client.delete(input_file.source_uri)
    except FloeError as e:
        raise FloeRunError(
            f"entity.name={entity_name} archive delete failed for {input_file.source_uri}: {e}"
        ) from e
    return dest_uri
