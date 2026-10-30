"""`resolve_path`: unknown storage names raise instead of falling back to local."""

from pathlib import Path
from threading import Thread

import pytest

import floe.storage.resolve as resolve_module
from floe.config.storage import StorageDefinition, StoragesConfig
from floe.errors import FloeConfigError, FloeRunError
from floe.storage.local import LocalClient
from floe.storage.resolve import clear_storage_clients, resolve_path, storage_client
from floe.storage.s3 import S3Client


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


@pytest.fixture
def built(monkeypatch: pytest.MonkeyPatch) -> list[S3Client]:
    """The real `S3Client`, recording each construction."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "k")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "s")
    clients: list[S3Client] = []

    def build(*args: object) -> S3Client:
        clients.append(S3Client(*args))  # pyright: ignore[reportArgumentType]  # forwards the definition's fields
        return clients[-1]

    monkeypatch.setattr(resolve_module, "S3Client", build)
    clear_storage_clients()
    return clients


def s3_client(d: StorageDefinition) -> S3Client:
    client = storage_client(d)
    assert isinstance(client, S3Client)
    return client


def test_1100_one_s3_client_per_distinct_definition(built: list[S3Client]) -> None:
    lake, other = definition("lake", "s3", "b"), definition("other", "s3", "c")
    assert storage_client(lake) is storage_client(definition("lake", "s3", "b"))
    assert storage_client(other) is not storage_client(lake)
    assert len(built) == 2
    assert isinstance(storage_client(definition("l", "local")), LocalClient)


def test_1100_a_cleared_cache_rebuilds_with_the_new_env(built: list[S3Client], monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    lake = definition("lake", "s3", "b")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://one:9000")
    assert s3_client(lake).endpoint == "http://one:9000"
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://two:9000")
    assert s3_client(lake).endpoint == "http://one:9000"
    clear_storage_clients()
    assert s3_client(lake).endpoint == "http://two:9000"


def test_1100_threads_share_one_client(built: list[S3Client]) -> None:
    lake = definition("lake", "s3", "b")
    threads = [Thread(target=storage_client, args=(lake,)) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(built) == 1
