"""Verification of an S3-compatible provider (#1269 Scaleway, #1270 OVHcloud): one command per provider.

Opt-in: skipped unless `FLOE_S3COMPAT_ENDPOINT` and `FLOE_S3COMPAT_BUCKET` are set. Credentials come from
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, as Floe's own S3 client reads them. Optional:
`FLOE_S3COMPAT_REGION` (default `us-east-1`) and `FLOE_S3COMPAT_PATH_STYLE` (`true` or `false`; unset keeps the
client default). Every test writes under a unique `floe-s3compat-<id>/` prefix and deletes it afterwards.

    FLOE_S3COMPAT_ENDPOINT=https://s3.fr-par.scw.cloud FLOE_S3COMPAT_BUCKET=my-bucket FLOE_S3COMPAT_REGION=fr-par \\
        uv run pytest -m s3compat tests/integration/test_s3_compatible.py

A failure is a recorded result, not necessarily a Floe bug: see docs/storages/s3.md ("S3-compatible providers").
"""

import hashlib
import io
import json
import os
import threading
import uuid
from collections.abc import Iterator
from pathlib import Path

import polars as pl
import pytest
from botocore.exceptions import ClientError

from floe.config.parse import parse_config
from floe.run import run
from floe.storage.s3 import S3Client

ENDPOINT = os.environ.get("FLOE_S3COMPAT_ENDPOINT", "")
BUCKET = os.environ.get("FLOE_S3COMPAT_BUCKET", "")
REGION = os.environ.get("FLOE_S3COMPAT_REGION") or "us-east-1"
_PATH_STYLE = os.environ.get("FLOE_S3COMPAT_PATH_STYLE", "").strip().lower()
PATH_STYLE: bool | None = None if not _PATH_STYLE else _PATH_STYLE == "true"

pytestmark = [
    pytest.mark.s3compat,
    pytest.mark.skipif(
        not (ENDPOINT and BUCKET),
        reason="FLOE_S3COMPAT_ENDPOINT and FLOE_S3COMPAT_BUCKET not set",
    ),
]

# the #1271 source mtime index is written for a partitioned archive only
PARTITIONING = 'partitioning: {pattern: "{yyyy}/{MM}/{dd}/{batch}", date_column: day}, '

CONFIG = """\
version: "0.3"
storages:
  default: local
  definitions:
    - {{name: local, type: local}}
    - {{name: lake, type: s3, bucket: {bucket}, region: {region}, endpoint: "{endpoint}", {style}prefix: {prefix}}}
domains:
  - name: d
entities:
  - name: orders
    domain: d
    incremental_mode: {mode}
    source: {{format: csv, path: in/, storage: lake, {partitioning}options: {{header: true, separator: ","}}}}
    sink:
      {archive}accepted: {{format: parquet, path: out/accepted, storage: lake}}
    policy: {{severity: reject}}
    schema:
      columns:
        - {{name: id, type: int64}}
report: {{path: runs, storage: lake}}
"""


def _client(path_style: bool | None = PATH_STYLE) -> S3Client:
    return S3Client(BUCKET, REGION, ENDPOINT, path_style)


@pytest.fixture
def s3() -> S3Client:
    return _client()


@pytest.fixture
def prefix(s3: S3Client) -> Iterator[str]:
    """A unique key prefix per test; everything under it is deleted afterwards."""
    key = f"floe-s3compat-{uuid.uuid4().hex}"
    yield key
    for ref in s3.list(f"{key}/"):
        s3.delete(ref.uri)


@pytest.fixture
def base(s3: S3Client, prefix: str) -> str:
    return s3.resolve_uri(prefix)


def _config(
    tmp_path: Path, prefix: str, mode: str = "file", archive: bool = False
) -> Path:
    style = (
        "" if PATH_STYLE is None else f"path_style_access: {str(PATH_STYLE).lower()}, "
    )
    path = tmp_path / "config.yml"
    path.write_text(
        CONFIG.format(
            bucket=BUCKET,
            region=REGION,
            endpoint=ENDPOINT,
            style=style,
            prefix=prefix,
            mode=mode,
            partitioning=PARTITIONING if archive else "",
            archive="archive: {path: archive/, storage: lake}\n      "
            if archive
            else "",
        )
    )
    return path


def test_list_read_write_copy_delete(s3: S3Client, base: str, prefix: str) -> None:
    for name in ("a/x.csv", "a/y.csv", "a/b/z.csv"):
        s3.write(f"{base}/{name}", name.encode())
    refs = s3.list(f"{prefix}/a/")
    assert sorted(r.key for r in refs) == sorted(
        f"{prefix}/{n}" for n in ("a/x.csv", "a/y.csv", "a/b/z.csv")
    )
    assert all(r.size and r.last_modified for r in refs)
    assert [
        r.key for r in s3.list(f"{prefix}/a/", start_after=f"{prefix}/a/x.csv")
    ] == [f"{prefix}/a/y.csv"]
    stored = s3.read(f"{base}/a/x.csv")
    assert stored is not None
    assert stored.body == b"a/x.csv"
    s3.copy(f"{base}/a/x.csv", f"{base}/c.csv")
    copied = s3.read(f"{base}/c.csv")
    assert copied is not None
    assert copied.body == b"a/x.csv"
    s3.delete(f"{base}/c.csv")
    assert s3.read(f"{base}/c.csv") is None
    s3.delete(f"{base}/c.csv")  # deleting a missing key is not an error


def test_download_of_a_large_parquet_source(
    s3: S3Client, base: str, tmp_path: Path
) -> None:
    source = tmp_path / "large.parquet"
    # random bytes do not compress: ~32 MiB on the wire, enough for a multi-chunk download
    pl.DataFrame({"b": [os.urandom(1 << 20) for _ in range(32)]}).write_parquet(
        source, compression="uncompressed"
    )
    body = source.read_bytes()
    s3.write(f"{base}/in/large.parquet", body)
    local = s3.download(f"{base}/in/large.parquet", tmp_path / "dl")
    assert hashlib.sha256(local.read_bytes()).digest() == hashlib.sha256(body).digest()
    assert pl.read_parquet(local).height == 32


def test_conditional_put_if_none_match_on_an_existing_key_is_refused(
    s3: S3Client, base: str
) -> None:
    uri = f"{base}/state.json"
    assert s3.write_conditional(uri, None, b"1")
    # a provider that ignores `If-None-Match: *` answers 200 here and overwrites the key
    assert s3.write_conditional(uri, None, b"2") is None, (
        "If-None-Match: * was applied, not refused"
    )
    stored = s3.read(uri)
    assert stored is not None
    assert stored.body == b"1"


def test_conditional_put_with_a_stale_if_match_is_refused(
    s3: S3Client, base: str
) -> None:
    uri = f"{base}/state.json"
    created = s3.write_conditional(uri, None, b"1")
    assert created
    replaced = s3.write_conditional(uri, created, b"22")
    assert replaced
    assert replaced != created
    assert s3.write_conditional(uri, created, b"3") is None, (
        "a stale If-Match was applied, not refused"
    )
    assert s3.read(uri) == (b"22", replaced)


def test_conditional_delete_with_a_stale_if_match_is_refused(
    s3: S3Client, base: str, prefix: str
) -> None:
    """Floe's CAS needs only conditional put (no `S3Client` method deletes conditionally), so this probes the raw call."""
    uri = f"{base}/state.json"
    stale = s3.write_conditional(uri, None, b"1")
    assert stale
    assert s3.write_conditional(uri, stale, b"2")
    raw = s3._client  # pyright: ignore[reportPrivateUsage]  # Floe has no conditional delete to call
    with pytest.raises(ClientError) as refused:
        raw.delete_object(Bucket=BUCKET, Key=f"{prefix}/state.json", IfMatch=stale)
    assert refused.value.response.get("ResponseMetadata", {}).get("HTTPStatusCode") in (409, 412)  # fmt: skip
    assert s3.read(uri) is not None


def _race(target: str, expected: str | None) -> list[str | None]:
    """Four writers, one shared expectation, released together."""
    results: list[str | None] = []
    start = threading.Barrier(4)

    def writer(i: int) -> None:
        start.wait()
        results.append(_client().write_conditional(target, expected, str(i).encode()))

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results


def test_concurrent_conditional_puts_have_one_winner(s3: S3Client, base: str) -> None:
    """The CAS the incremental state relies on: of N writers with the same expectation, exactly one wins."""
    version = s3.write_conditional(f"{base}/state.json", None, b"0")
    assert version
    for target, expected in ((f"{base}/fresh.json", None), (f"{base}/state.json", version)):  # create, then replace  # fmt: skip
        results = _race(target, expected)
        assert [r is not None for r in results].count(True) == 1, results


@pytest.mark.parametrize(
    "path_style", [True, False], ids=["path-style", "virtual-hosted"]
)
def test_addressing_style(path_style: bool, prefix: str) -> None:
    """Both addressing styles reach the bucket; a style the provider does not support fails here (a recorded result)."""
    client = _client(path_style)
    uri = client.resolve_uri(f"{prefix}/addressing.txt")
    client.write(uri, b"ok")
    stored = client.read(uri)
    assert stored is not None
    assert stored.body == b"ok"
    assert len(client.list(f"{prefix}/")) == 1


def test_run_with_incremental_mode_file(
    s3: S3Client, prefix: str, tmp_path: Path
) -> None:
    s3.write(s3.resolve_uri(f"{prefix}/in/a.csv"), b"id\n1\n2\n")
    config = _config(tmp_path, prefix)
    out = run(config, parse_config(config), run_id="r1")
    assert out.summary["run"]["status"] == "success"
    part = s3.read(s3.resolve_uri(f"{prefix}/out/accepted/part-00000.parquet"))
    assert part is not None
    assert pl.read_parquet(io.BytesIO(part.body)).height == 2
    again = run(config, parse_config(config), run_id="r2")
    assert again.summary["results"]["files_skipped"] == 1
    state = s3.read(s3.resolve_uri(f"{prefix}/in/.floe/state/d/orders/state.json"))
    assert state is not None
    assert "a.csv" in state.body.decode()


def test_run_with_archive_copies_the_source_and_writes_the_mtime_index(
    s3: S3Client, prefix: str, tmp_path: Path
) -> None:
    source = s3.resolve_uri(f"{prefix}/in/2026/10/24/a1b2/a.csv")
    s3.write(source, b"id\n1\n")
    config = _config(tmp_path, prefix, mode="archive", archive=True)
    assert (
        run(config, parse_config(config), run_id="r1").summary["run"]["status"]
        == "success"
    )
    archived = [r for r in s3.list(f"{prefix}/archive/") if "/a1b2/a__run-" in r.key]
    assert len(archived) == 1
    assert s3.read(source) is None  # archiving moves the source
    index = s3.read(
        s3.resolve_uri(f"{prefix}/archive/orders/.floe/source_mtime/r1.json")
    )
    assert index is not None
    assert [k.rpartition("/")[0] for k in json.loads(index.body)] == ["2026/10/24/a1b2"]
