"""`require_local`: unknown storage names raise instead of falling back to local."""

import pytest

from floe.config.storage import StorageDefinition, StoragesConfig
from floe.errors import FloeRunError
from floe.storage.resolve import require_local


def definition(name: str, kind: str) -> StorageDefinition:
    return StorageDefinition(
        name=name, type=kind, bucket=None, region=None, account=None,
        container=None, prefix=None, endpoint=None, path_style_access=None,
    )  # fmt: skip


def test_unknown_names_raise() -> None:
    config = StoragesConfig(default="a", definitions=[definition("a", "local")])
    require_local(None, None, "f")
    require_local(config, None, "f")
    with pytest.raises(FloeRunError, match="f=ghost references unknown storage"):
        require_local(config, "ghost", "f")
    with pytest.raises(FloeRunError, match="f=ghost references unknown storage"):
        require_local(None, "ghost", "f")
    with pytest.raises(FloeRunError, match="f=a references unknown storage"):
        require_local(StoragesConfig(default="a", definitions=[]), None, "f")
