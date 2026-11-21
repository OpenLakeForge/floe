"""`ObjectStoreClient` over obstore's real in-memory store; no network."""

from pathlib import Path

import pytest
from obstore.store import MemoryStore

from floe.config.storage import StorageDefinition
from floe.errors import FloeConfigError, FloeStorageError
from floe.storage.object_store import ObjectStoreClient, azure_store, gcs_store
from floe.storage.types import Downloader

ROOT = "abfss://c@a.dfs.core.windows.net"


def _client(*keys: str) -> ObjectStoreClient:
    client = ObjectStoreClient(MemoryStore(), ROOT, "https://a.blob", "e_tag")
    for key in keys:
        client.write(f"{ROOT}/{key}", b"x")
    return client


def test_list_filters_by_raw_key_prefix() -> None:
    client = _client(
        "d/sales_1.csv", "d/sales_2024.csv", "d/other.csv", "e/sales_2.csv"
    )
    assert [r.key for r in client.list("d/sales_2")] == ["d/sales_2024.csv"]
    refs = client.list("d/")
    assert [r.key for r in refs] == ["d/other.csv", "d/sales_1.csv", "d/sales_2024.csv"]
    assert refs[0].uri == f"{ROOT}/d/other.csv" and refs[0].size == 1
    assert refs[0].last_modified and refs[0].last_modified.endswith("Z")
    assert len(client.list("")) == 4


def test_list_exact_key_and_start_after() -> None:
    client = _client("d/a.csv", "d/b.csv", "d/c.csv")
    assert [r.key for r in client.list("d/b.csv")] == ["d/b.csv"]
    assert [r.key for r in client.list("d/", "d/a.csv")] == ["d/b.csv", "d/c.csv"]
    assert client.list("d/", "d/c.csv") == []


def test_read_write_copy_delete_and_download(tmp_path: Path) -> None:
    client = _client()
    uri, dst = f"{ROOT}/d/a.csv", f"{ROOT}/d/b.csv"
    assert client.read(uri) is None
    client.write(uri, b"hello")
    stored = client.read(uri)
    assert stored and stored.body == b"hello" and stored.version
    client.copy(uri, dst)
    assert isinstance(client, Downloader)
    assert client.download(dst, tmp_path).read_bytes() == b"hello"
    client.delete(uri)
    assert client.read(uri) is None
    assert client.source_mtime(dst) is None
    assert client.resolve_uri("d/a.csv") == uri and client.resolve_uri("") == ROOT


def test_delete_missing_key_is_a_no_op() -> None:
    _client().delete(f"{ROOT}/nope.csv")


def test_write_conditional_conflicts_return_none() -> None:
    client = _client()
    uri = f"{ROOT}/state.json"
    v1 = client.write_conditional(uri, None, b"1")
    assert v1
    assert client.write_conditional(uri, None, b"2") is None  # create conflict
    v2 = client.write_conditional(uri, v1, b"2")
    assert v2 and v2 != v1
    assert client.write_conditional(uri, v1, b"3") is None  # stale token
    assert client.write_conditional(f"{ROOT}/gone.json", "x", b"3") is None
    stored = client.read(uri)
    assert stored and stored.body == b"2" and stored.version == v2


def test_leading_slash_and_empty_segments_are_refused() -> None:
    client = _client("a.csv")
    for bad in (f"{ROOT}//a.csv", f"{ROOT}/d//a.csv", f"{ROOT}/d/"):
        with pytest.raises(FloeStorageError) as e:
            client.read(bad)
        assert e.value.code == "storage_error"
    with pytest.raises(FloeStorageError):
        client.list("/a")
    with pytest.raises(FloeStorageError):
        client.resolve_uri("/a.csv")
    with pytest.raises(FloeStorageError, match="not under"):
        client.read("abfss://other@a.dfs.core.windows.net/a.csv")


def test_errors_carry_codes(tmp_path: Path) -> None:
    client = _client()
    with pytest.raises(FloeStorageError) as e:
        client.download(f"{ROOT}/missing.csv", tmp_path)
    assert e.value.code == "storage_not_found"
    with pytest.raises(FloeStorageError) as e:
        client.copy(f"{ROOT}/missing.csv", f"{ROOT}/b.csv")
    assert e.value.code == "storage_not_found"


def test_azure_store_builds_a_client() -> None:
    d = StorageDefinition(name="az", type="adls", bucket=None, region=None, account="acct", container="box", prefix=None, endpoint=None, path_style_access=None)  # fmt: skip
    client = azure_store(d)
    assert client.endpoint == "https://acct.blob.core.windows.net"
    assert client.resolve_uri("k") == "abfss://box@acct.dfs.core.windows.net/k"
    with pytest.raises(FloeConfigError) as e:
        azure_store(d.model_copy(update={"container": None}))
    assert e.value.code == "config_missing_key"


def test_gcs_conditional_put_uses_version(monkeypatch: pytest.MonkeyPatch) -> None:
    modes: list[object] = []

    class Obs:
        @staticmethod
        def put(_store: object, _key: str, _body: bytes, mode: object) -> dict[str, str]:  # fmt: skip
            modes.append(mode)
            return {"e_tag": "etag", "version": "gen2"}

    monkeypatch.setattr("floe.storage.object_store.obs", Obs)
    client = ObjectStoreClient(MemoryStore(), "gs://b", None, "version")
    assert client.write_conditional("gs://b/s.json", "gen1", b"x") == "gen2"
    assert client.write_conditional("gs://b/s.json", None, b"x") == "gen2"
    assert modes == [{"version": "gen1"}, "create"]


def test_gcs_store_builds_a_client() -> None:
    d = StorageDefinition(name="g", type="gcs", bucket="box", region=None, account=None, container=None, prefix=None, endpoint="http://127.0.0.1:4443/", path_style_access=None)  # fmt: skip
    client = gcs_store(d)
    assert client.endpoint == "http://127.0.0.1:4443"
    assert client.resolve_uri("k") == "gs://box/k"
    with pytest.raises(FloeConfigError) as e:
        gcs_store(d.model_copy(update={"bucket": None}))
    assert e.value.code == "config_missing_key"
