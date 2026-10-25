"""`resolve_path` and `require_local`: unknown storage names raise instead of falling back to local."""

from pathlib import Path

import pytest

from floe.config.storage import StorageDefinition, StoragesConfig
from floe.errors import FloeConfigError, FloeRunError
from floe.storage.resolve import require_local, resolve_path


def definition(name: str, kind: str, bucket: str | None = None, prefix: str | None = None) -> StorageDefinition:  # fmt: skip
    return StorageDefinition(
        name=name, type=kind, bucket=bucket, region=None, account=None,
        container=None, prefix=prefix, endpoint=None, path_style_access=None,
    )  # fmt: skip


def test_unknown_names_raise() -> None:
    config = StoragesConfig(default="a", definitions=[definition("a", "local")])
    resolve_path(None, Path("/c"), None, "x", "f")
    resolve_path(config, Path("/c"), None, "x", "f")
    with pytest.raises(FloeRunError, match="f=ghost references unknown storage"):
        resolve_path(config, Path("/c"), "ghost", "x", "f")
    with pytest.raises(FloeRunError, match="f=ghost references unknown storage"):
        resolve_path(None, Path("/c"), "ghost", "x", "f")
    with pytest.raises(FloeRunError, match="f=a references unknown storage"):
        resolve_path(StoragesConfig(default="a", definitions=[]), Path("/c"), None, "x", "f")  # fmt: skip


def test_local_and_s3_uris() -> None:
    config = StoragesConfig(
        default="local",
        definitions=[definition("local", "local"), definition("lake", "s3", "b", "/p/")],
    )  # fmt: skip
    assert resolve_path(config, Path("/c"), None, "./in/../a.csv", "f")[0] == "local:///c/a.csv"  # fmt: skip
    assert resolve_path(config, Path("/c"), "lake", "in/*.csv", "f")[0] == "s3://b/p/in/*.csv"  # fmt: skip
    assert resolve_path(config, Path("/c"), "lake", "s3://b/k", "f")[0] == "s3://b/k"
    with pytest.raises(FloeConfigError, match="storage lake bucket mismatch: x"):
        resolve_path(config, Path("/c"), "lake", "s3://x/k", "f")


def test_require_local_names_the_feature() -> None:
    config = StoragesConfig(default=None, definitions=[definition("lake", "s3", "b")])
    with pytest.raises(FloeRunError, match=r"f=lake \(type s3\) is not supported yet: sink.archive is local only"):  # fmt: skip
        require_local(config, "lake", "f", "sink.archive")
