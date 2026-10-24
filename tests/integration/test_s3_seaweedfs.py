"""S3 client against the S3-compatible service of the #602 harness (SeaweedFS).

Skipped unless `FLOE_S3_ENDPOINT` is set, e.g. `FLOE_S3_ENDPOINT=http://localhost:8333`.
The bucket is `FLOE_S3_BUCKET` (default `warehouse`).
"""

import os
import threading
import uuid
from pathlib import Path

import pytest

from floe.errors import FloeStorageError
from floe.storage.inputs import InputFile, localize_input, resolve_inputs
from floe.storage.s3 import S3Client

pytestmark = [
    pytest.mark.s3,
    pytest.mark.skipif(
        not os.environ.get("FLOE_S3_ENDPOINT"), reason="FLOE_S3_ENDPOINT not set"
    ),
]


@pytest.fixture
def s3(monkeypatch: pytest.MonkeyPatch) -> S3Client:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "spike")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "spike-s3-secret")
    return S3Client(
        os.environ.get("FLOE_S3_BUCKET", "warehouse"),
        "us-east-1",
        os.environ["FLOE_S3_ENDPOINT"],
        True,
    )


@pytest.fixture
def base(s3: S3Client) -> str:
    return s3.resolve_uri(f"it-{uuid.uuid4().hex}")


def test_conditional_create_replace_and_stale(s3: S3Client, base: str) -> None:
    uri = f"{base}/state.json"
    created = s3.write_conditional(uri, None, b"1")
    assert created
    assert s3.write_conditional(uri, None, b"2") is None
    replaced = s3.write_conditional(uri, created, b"22")
    assert replaced
    assert replaced != created
    assert s3.write_conditional(uri, created, b"3") is None
    assert s3.read(uri) == (b"22", replaced)
    assert s3.read(f"{base}/missing") is None


def test_concurrent_creates_have_one_winner(s3: S3Client, base: str) -> None:
    uri = f"{base}/state.json"
    results: list[str | None] = []
    start = threading.Barrier(2)

    def writer(i: int) -> None:
        start.wait()
        results.append(s3.write_conditional(uri, None, str(i).encode()))

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(r is None for r in results) == [False, True]


def test_list_glob_copy_delete(s3: S3Client, base: str, tmp_path: Path) -> None:
    for name in ("a/x.csv", "a/y.CSV", "a/z.txt", "a/b/w.csv"):
        s3.write(f"{base}/{name}", name.encode())
    prefix = base.split("/", 3)[3]
    refs = s3.list(f"{prefix}/a/")
    assert len(refs) == 4
    assert all(r.size and r.last_modified for r in refs)
    found = resolve_inputs(
        tmp_path, f"{base}/a/*.csv", "csv", "e", "s3", s3, list_only=True
    )
    assert found.listed == [f"{base}/a/x.csv"]

    s3.copy(f"{base}/a/x.csv", f"{base}/c.csv")
    assert s3.read(f"{base}/c.csv") is not None
    s3.delete(f"{base}/c.csv")
    s3.delete(f"{base}/c.csv")
    assert s3.read(f"{base}/c.csv") is None


def test_localize_downloads_to_temp(s3: S3Client, base: str, tmp_path: Path) -> None:
    uri = f"{base}/in/data file.csv"
    s3.write(uri, b"id\n1\n")
    local = localize_input(InputFile(uri, "x", "x", None, None), s3, tmp_path)
    assert local.is_ephemeral
    assert local.local_path.parent == tmp_path
    assert local.local_path.name.endswith("_data_file.csv")
    assert local.local_path.read_bytes() == b"id\n1\n"


def test_errors_use_rust_messages(
    s3: S3Client, base: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "wrong")
    bad = S3Client(s3.bucket, "us-east-1", os.environ["FLOE_S3_ENDPOINT"], True)
    with pytest.raises(FloeStorageError, match="^s3 get object failed: "):
        bad.read(f"{base}/k")
    with pytest.raises(FloeStorageError, match="^s3 put object failed: "):
        bad.write(f"{base}/k", b"")
    with pytest.raises(FloeStorageError, match="^s3 list objects failed for bucket "):
        bad.list("")
