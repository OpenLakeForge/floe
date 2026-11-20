"""Ported from `crates/floe-core/tests/unit/io/storage/` (s3 option cases); no network.

The S3 client runs over obstore's in-memory store, so the conditional-write, list and copy paths are the production ones.
"""

import threading
from datetime import datetime
from pathlib import Path

import obstore as obs
import pytest
from obstore.exceptions import (
    AlreadyExistsError,
    GenericError,
    PermissionDeniedError,
    PreconditionError,
)
from obstore.store import MemoryStore

from floe.errors import FloeRunError, FloeStorageError
from floe.storage.inputs import InputFile, localize_input
from floe.storage.s3 import S3Client, _fail, resolve_path_style_access  # pyright: ignore[reportPrivateUsage]  # the error mapping is tested on its own


def memory_client(monkeypatch: pytest.MonkeyPatch) -> S3Client:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "k")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "s")
    client = S3Client("b", "us-east-1")
    client._store = MemoryStore()  # pyright: ignore[reportPrivateUsage]
    return client


def test_path_style_config_beats_env_and_env_accepts_1_or_true(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AWS_S3_FORCE_PATH_STYLE", raising=False)
    assert resolve_path_style_access(None) is False
    for value, want in [("1", True), (" TRUE ", True), ("yes", False), ("", False)]:
        monkeypatch.setenv("AWS_S3_FORCE_PATH_STYLE", value)
        assert resolve_path_style_access(None) is want
    assert resolve_path_style_access(False) is False
    monkeypatch.setenv("AWS_S3_FORCE_PATH_STYLE", "false")
    assert resolve_path_style_access(True) is True


def test_resolve_uri_and_client_options(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "k")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "s")
    client = S3Client("b", "eu-west-1", "http://localhost:9000", True)
    assert client.resolve_uri("/a/b.csv") == "s3://b/a/b.csv"
    assert client.resolve_uri("") == "s3://b"


def test_localize_requires_client_and_temp_dir(tmp_path: Path) -> None:
    file = InputFile("s3://b/k.csv", "k.csv", "k", None, None)
    with pytest.raises(FloeRunError, match="storage client required to download"):
        localize_input(file, None, tmp_path)


def test_localize_local_uri_is_not_copied(tmp_path: Path) -> None:
    file = InputFile("local:///data/k.csv", "k.csv", "k", None, None)
    local = localize_input(file, None, None)
    assert local.local_path == Path("/data/k.csv")
    assert not local.is_ephemeral


def test_1271_write_conditional_returns_none_on_a_stale_etag_or_an_existing_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = memory_client(monkeypatch)
    uri = "s3://b/s/state.json"
    created = client.write_conditional(uri, None, b"1")
    assert created
    assert client.write_conditional(uri, None, b"2") is None  # create-only: it exists
    replaced = client.write_conditional(uri, created, b"22")
    assert replaced
    assert replaced != created
    assert (
        client.write_conditional(uri, created, b"3") is None
    )  # update-only: the ETag moved on
    assert (
        client.write_conditional("s3://b/s/none.json", "x", b"3") is None
    )  # nothing to replace
    assert client.read(uri) == (b"22", replaced)
    assert client.read("s3://b/s/none.json") is None


def test_1271_concurrent_creates_have_one_winner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One client is shared by the entity threads."""
    client = memory_client(monkeypatch)
    start = threading.Barrier(8)
    results: list[str | None] = []

    def create(i: int) -> None:
        start.wait()
        results.append(client.write_conditional("s3://b/state.json", None, b"%d" % i))

    threads = [threading.Thread(target=create, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(r is not None for r in results) == 1


def test_list_is_ordered_and_matches_a_partial_last_segment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = memory_client(monkeypatch)
    for key in ("in/b.csv", "in/data_2.csv", "in/data_1.csv", "other/data_9.csv"):
        client.write(f"s3://b/{key}", b"x")
    keys = [r.key for r in client.list("in/")]
    assert keys == ["in/b.csv", "in/data_1.csv", "in/data_2.csv"]
    assert [r.key for r in client.list("in/data_")] == ["in/data_1.csv", "in/data_2.csv"]  # fmt: skip
    assert len(client.list("")) == 4
    ref = client.list("in/b")[0]
    assert (ref.uri, ref.size) == ("s3://b/in/b.csv", 1)
    assert ref.last_modified is not None
    assert ref.last_modified.endswith("Z")
    assert datetime.fromisoformat(ref.last_modified).tzinfo is not None


def test_d30_list_passes_start_after(monkeypatch: pytest.MonkeyPatch) -> None:
    client = memory_client(monkeypatch)
    for day in ("22", "23", "24"):
        client.write(f"s3://b/in/2026/10/{day}/x.csv", b"x")
    after_day = [r.key for r in client.list("in/", "in/2026/10/23")]
    assert after_day == ["in/2026/10/23/x.csv", "in/2026/10/24/x.csv"]
    after_key = [r.key for r in client.list("in/", "in/2026/10/23/x.csv")]
    assert after_key == ["in/2026/10/24/x.csv"]


def test_copy_download_and_delete(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    client = memory_client(monkeypatch)
    client.write("s3://b/in/a.csv", b"id\n1\n")
    client.copy("s3://b/in/a.csv", "s3://b/arch/a.csv")
    assert client.download("s3://b/arch/a.csv", tmp_path).read_bytes() == b"id\n1\n"
    client.delete("s3://b/in/a.csv")
    assert client.read("s3://b/in/a.csv") is None
    with pytest.raises(FloeStorageError, match="^s3 get object failed: ") as err:
        client.download("s3://b/in/a.csv", tmp_path)
    assert err.value.code == "storage_not_found"


def test_endpoint_ignores_a_trailing_slash() -> None:
    """D34 (#931) compares endpoints to tell two stores apart: `http://minio:9000/` is the same store."""
    assert S3Client("b", "us-east-1", "http://minio:9000/").endpoint == S3Client("b", "us-east-1", "http://minio:9000").endpoint  # fmt: skip


def test_939_source_mtime_reads_the_legacy_copy_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#1271: an archive copy carries no metadata, but one written before still has it."""
    client = memory_client(monkeypatch)
    meta = {"owner": "x", "floe-source-mtime": "2026-10-24T10:00:00Z"}
    obs.put(client._store, "arch/a.csv", b"x", attributes=meta)  # pyright: ignore[reportPrivateUsage, reportArgumentType]  # `Attributes` names its keys as a union with str
    client.write("s3://b/arch/old.csv", b"x")
    assert client.source_mtime("s3://b/arch/a.csv") == "2026-10-24T10:00:00Z"
    assert client.source_mtime("s3://b/arch/old.csv") is None


def test_1260_errors_carry_a_code_by_obstore_class() -> None:
    assert _fail("get object", PermissionDeniedError("no")).code == "storage_access_denied"  # fmt: skip
    assert _fail("get object", FileNotFoundError("gone")).code == "storage_not_found"
    assert _fail("put object", PreconditionError("412")).code == "storage_conflict"
    assert _fail("put object", AlreadyExistsError("409")).code == "storage_conflict"
    assert str(_fail("list objects", GenericError("x"), " for bucket b")) == "s3 list objects failed for bucket b: x"  # fmt: skip
    err = _fail("put object", GenericError("boom\n\nDebug source:\nGeneric {}"))
    assert (err.code, str(err)) == ("storage_error", "s3 put object failed: boom")
