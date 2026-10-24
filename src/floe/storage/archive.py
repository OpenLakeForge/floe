"""Input archiving, ported from `io/storage/ops/archive.rs`."""

from collections.abc import Callable

from floe.errors import FloeError, FloeRunError
from floe.storage.inputs import InputFile
from floe.storage.paths import archive_relative_path_for_run
from floe.storage.types import StorageClient


def archive_input_file(
    client: StorageClient,
    run_id: str,
    entity_name: str,
    join_relative: Callable[[str], str],
    input_file: InputFile,
) -> str:
    relative = archive_relative_path_for_run(
        entity_name, input_file.source_name, run_id, input_file.source_uri
    )
    dest_uri = join_relative(relative)
    client.copy(input_file.source_uri, dest_uri)
    try:
        client.delete(input_file.source_uri)
    except FloeError as e:
        raise FloeRunError(
            f"entity.name={entity_name} archive delete failed for {input_file.source_uri}: {e}"
        ) from e
    return dest_uri
