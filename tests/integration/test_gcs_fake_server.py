"""GCS storage against fake-gcs-server, behind a shim for obstore's XML-API uploads.

Skipped unless `FLOE_GCS_EMULATOR` is set to fake-gcs-server's URL, e.g. `http://localhost:4443`,
started with `-scheme http -public-host localhost:4443` and a bucket `FLOE_GCS_BUCKET` (default `warehouse`).
obstore signs its token with a throwaway service-account key made here: no Google endpoint is called.
"""

import http.server
import io
import json
import os
import subprocess
import threading
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from floe.config.parse import parse_config
from floe.config.storage import StorageDefinition
from floe.run import run
from floe.storage.object_store import ObjectStoreClient, gcs_store

pytestmark = [
    pytest.mark.gcs,
    pytest.mark.skipif(
        not os.environ.get("FLOE_GCS_EMULATOR"), reason="FLOE_GCS_EMULATOR not set"
    ),
]
BUCKET = os.environ.get("FLOE_GCS_BUCKET", "warehouse")

CONFIG = """\
version: "0.3"
storages:
  default: local
  definitions:
    - {{name: local, type: local}}
    - {{name: lake, type: gcs, bucket: {bucket}, endpoint: "{endpoint}", prefix: {prefix}}}
domains:
  - name: d
entities:
  - name: orders
    domain: d
    incremental_mode: file
    source: {{format: csv, path: in/, storage: lake, options: {{header: true, separator: ","}}}}
    sink:
      accepted: {{format: parquet, path: out/accepted, storage: lake}}
    policy: {{severity: reject}}
    schema:
      columns:
        - {{name: id, type: int64}}
report: {{path: runs, storage: lake}}
"""


class _XmlUploadShim(http.server.BaseHTTPRequestHandler):
    """Passes every request to fake-gcs-server, rewriting obstore's XML `PUT /<bucket>/<key>` into the JSON API.

    A PUT is an upload, or a copy when it carries `x-goog-copy-source: <bucket>/<key>` (the staged-part rename).

    ponytail: fake-gcs-server 1.56.1 sends that PUT to its JSON insert, which wants `uploadType` and reads the
    precondition only from `ifGenerationMatch`, never from `x-goog-if-generation-match`. Upgrade path: delete this
    class and point obstore at the emulator once a fake-gcs-server release routes XML PUT, header included.
    """

    upstream = ""

    def _forward(self) -> None:
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        method, path = self.command, self.path
        copy_source = self.headers.get("x-goog-copy-source")
        if method == "PUT":
            bucket, _, key = urllib.parse.urlsplit(path).path.lstrip("/").partition("/")
            query: dict[str, str] = {}
            if (match := self.headers.get("x-goog-if-generation-match")) is not None:
                query["ifGenerationMatch"] = match
            if copy_source:
                # obstore percent-encodes both keys, as the JSON API's paths want them
                source_bucket, _, source_key = copy_source.partition("/")
                path = f"/storage/v1/b/{source_bucket}/o/{source_key}/copyTo/b/{bucket}/o/{key}"  # fmt: skip
            else:
                query |= {"uploadType": "media", "name": urllib.parse.unquote(key)}
                path = f"/upload/storage/v1/b/{bucket}/o"
            method, path = "POST", f"{path}?{urllib.parse.urlencode(query)}"
        headers = {k: v for k, v in self.headers.items() if k.lower() not in ("host", "connection", "content-length")}  # fmt: skip
        request = urllib.request.Request(self.upstream + path, body if method == "POST" else None, headers, method=method)  # fmt: skip
        try:
            response = urllib.request.urlopen(request)
            status = response.status
        except urllib.error.HTTPError as e:
            # 412 on a failed precondition, as the XML API answers
            response, status = e, e.code
        data = response.read()
        out = [(k, v) for k, v in response.headers.items() if k.lower() not in ("content-length", "connection", "transfer-encoding")]  # fmt: skip
        if self.command == "PUT" and not copy_source and status == 200:
            # the XML API answers an upload with headers only
            created: dict[str, Any] = json.loads(data)
            out += [("ETag", f'"{created["etag"]}"'), ("x-goog-generation", str(created["generation"]))]  # fmt: skip
            data = b""
        self.send_response(status)
        for k, v in out:
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = _forward

    def log_message(self, format: str, *args: Any) -> None:
        pass


@pytest.fixture(scope="module")
def endpoint(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    """The shim's URL, with `GOOGLE_SERVICE_ACCOUNT` pointing at a throwaway key."""
    key = tmp_path_factory.mktemp("gcs") / "key.pem"
    subprocess.run(["openssl", "genpkey", "-algorithm", "RSA", "-out", str(key)], check=True, capture_output=True)  # fmt: skip
    account = key.with_name("service-account.json")
    account.write_text(json.dumps({"type": "service_account", "client_email": "floe-it@example.iam.gserviceaccount.com", "private_key_id": "it", "private_key": key.read_text()}))  # fmt: skip
    _XmlUploadShim.upstream = os.environ["FLOE_GCS_EMULATOR"].rstrip("/")
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _XmlUploadShim)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    with pytest.MonkeyPatch.context() as env:
        env.setenv("GOOGLE_SERVICE_ACCOUNT", str(account))
        yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()


@pytest.fixture
def gcs(endpoint: str) -> ObjectStoreClient:
    return gcs_store(
        StorageDefinition(
            name="lake",
            type="gcs",
            bucket=BUCKET,
            region=None,
            account=None,
            container=None,
            prefix=None,
            endpoint=endpoint,
            path_style_access=None,
        )
    )


def test_conditional_create_replace_and_stale(gcs: ObjectStoreClient) -> None:
    uri = gcs.resolve_uri(f"it-{uuid.uuid4().hex}/state.json")
    created = gcs.write_conditional(uri, None, b"1")
    assert created
    assert gcs.write_conditional(uri, None, b"2") is None
    replaced = gcs.write_conditional(uri, created, b"22")
    assert replaced
    assert replaced != created
    assert gcs.write_conditional(uri, created, b"3") is None
    assert gcs.read(uri) == (b"22", replaced)


def test_list_matches_the_raw_prefix_not_a_directory(gcs: ObjectStoreClient) -> None:
    """The #1302 bug class: listing matches `in/data_` and `in` by key prefix, as S3 does."""
    p = f"it-{uuid.uuid4().hex}"
    for name in ("in/data_1.csv", "in/data_2.csv", "in/other.csv", "in/sub/data_3.csv", "inbox/x.csv"):  # fmt: skip
        gcs.write(gcs.resolve_uri(f"{p}/{name}"), b"id\n1\n")
    assert [r.key for r in gcs.list(f"{p}/in/data_")] == [
        f"{p}/in/data_1.csv",
        f"{p}/in/data_2.csv",
    ]
    assert [r.key for r in gcs.list(f"{p}/in")] == [
        f"{p}/in/data_1.csv",
        f"{p}/in/data_2.csv",
        f"{p}/in/other.csv",
        f"{p}/in/sub/data_3.csv",
        f"{p}/inbox/x.csv",
    ]
    after = gcs.list(f"{p}/in/", start_after=f"{p}/in/data_2.csv")
    assert [r.key for r in after] == [f"{p}/in/other.csv", f"{p}/in/sub/data_3.csv"]
    assert all(r.size and r.last_modified for r in after)


def test_csv_on_gcs_to_parquet_on_gcs_then_an_incremental_skip(
    gcs: ObjectStoreClient, endpoint: str, tmp_path: Path
) -> None:
    prefix = f"it-{uuid.uuid4().hex}"
    gcs.write(gcs.resolve_uri(f"{prefix}/in/a.csv"), b"id\n1\n2\n")
    config = tmp_path / "config.yml"
    config.write_text(CONFIG.format(bucket=BUCKET, endpoint=endpoint, prefix=prefix))
    out = run(config, parse_config(config), run_id="r1")
    assert out.summary["run"]["status"] == "success"
    part = gcs.read(gcs.resolve_uri(f"{prefix}/out/accepted/part-00000.parquet"))
    assert part is not None
    assert pl.read_parquet(io.BytesIO(part.body)).height == 2
    stored = gcs.read(gcs.resolve_uri(f"{prefix}/runs/d/run_r1/d/orders/run.json"))
    assert stored is not None
    assert [f["input_file"] for f in json.loads(stored.body)["files"]] == [
        f"gs://{BUCKET}/{prefix}/in/a.csv"
    ]
    again = run(config, parse_config(config), run_id="r2")
    assert again.summary["results"]["files_skipped"] == 1
    assert gcs.read(gcs.resolve_uri(f"{prefix}/in/.floe/state/d/orders/state.json"))
