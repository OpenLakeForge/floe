"""GCS storage against fake-gcs-server (reached through obstore's `base_url`, the storage `endpoint`).

Skipped unless `FLOE_FAKEGCS_ENDPOINT` is set, e.g. `http://localhost:4443`. The bucket is `FLOE_FAKEGCS_BUCKET`
(default `warehouse`). obstore signs with a service account, so the workflow points `GOOGLE_SERVICE_ACCOUNT` at a
throwaway key: the self-signed token needs no Google endpoint, and fake-gcs-server does not check it.
"""

import io
import json
import os
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

import polars as pl
import pytest

from floe.config.parse import parse_config
from floe.config.storage import StorageDefinition
from floe.errors import FloeStorageError
from floe.run import run
from floe.storage.object_store import ObjectStoreClient, gcs_store

pytestmark = [
    pytest.mark.gcs,
    pytest.mark.skipif(
        not os.environ.get("FLOE_FAKEGCS_ENDPOINT"),
        reason="FLOE_FAKEGCS_ENDPOINT not set",
    ),
]
BUCKET = os.environ.get("FLOE_FAKEGCS_BUCKET", "warehouse")
# obstore writes through the XML API (`PUT /<bucket>/<object>` with `x-goog-if-generation-match`). fake-gcs-server
# v1.56.1 routes that request to its JSON `insertObject`, which answers 400 `invalid uploadType`, and it reads
# generation preconditions only from the `ifGenerationMatch` query parameter. So every obstore write fails here;
# reads and lists go through the XML API it does serve. The real-bucket check is recorded on #1268.
XML_PUT_UNSUPPORTED = pytest.mark.xfail(
    strict=True,
    raises=FloeStorageError,
    reason="fake-gcs-server 1.56.1 answers obstore's XML PUT with 400 `invalid uploadType`",
)

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


def _seed(key: str, body: bytes) -> None:
    """Put an object through fake-gcs-server's JSON upload API, the one write it serves."""
    name = urllib.parse.quote(key, safe="")
    url = f"{os.environ['FLOE_FAKEGCS_ENDPOINT']}/upload/storage/v1/b/{BUCKET}/o?uploadType=media&name={name}"
    urllib.request.urlopen(urllib.request.Request(url, body, method="POST")).close()


@pytest.fixture
def gcs() -> ObjectStoreClient:
    return gcs_store(
        StorageDefinition(
            name="lake",
            type="gcs",
            bucket=BUCKET,
            region=None,
            account=None,
            container=None,
            prefix=None,
            endpoint=os.environ["FLOE_FAKEGCS_ENDPOINT"],
            path_style_access=None,
        )
    )


@XML_PUT_UNSUPPORTED
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
    """The #1302 bug class: S3-style listing matches `in/data_` and `in` by key prefix, as S3 does."""
    p = f"it-{uuid.uuid4().hex}"
    for name in ("in/data_1.csv", "in/data_2.csv", "in/other.csv", "in/sub/data_3.csv", "inbox/x.csv"):  # fmt: skip
        _seed(f"{p}/{name}", b"id\n1\n")
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


@XML_PUT_UNSUPPORTED
def test_csv_on_gcs_to_parquet_on_gcs_then_an_incremental_skip(
    gcs: ObjectStoreClient, tmp_path: Path
) -> None:
    prefix = f"it-{uuid.uuid4().hex}"
    gcs.write(gcs.resolve_uri(f"{prefix}/in/a.csv"), b"id\n1\n2\n")
    config = tmp_path / "config.yml"
    config.write_text(
        CONFIG.format(
            bucket=BUCKET, endpoint=os.environ["FLOE_FAKEGCS_ENDPOINT"], prefix=prefix
        )
    )
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


@XML_PUT_UNSUPPORTED
def test_archive_mode_moves_the_source_into_the_archive(
    gcs: ObjectStoreClient, tmp_path: Path
) -> None:
    """The archive copies, then deletes, on the emulator: the input leaves `in/` and a run-stamped copy lands in `archive/`."""
    prefix = f"it-{uuid.uuid4().hex}"
    gcs.write(gcs.resolve_uri(f"{prefix}/in/a.csv"), b"id\n1\n2\n")
    config = tmp_path / "config.yml"
    config.write_text(
        CONFIG.replace("incremental_mode: file", "incremental_mode: archive")
        .replace("      accepted:", "      archive: {{path: archive/, storage: lake}}\n      accepted:")
        .format(bucket=BUCKET, endpoint=os.environ["FLOE_FAKEGCS_ENDPOINT"], prefix=prefix)
    )  # fmt: skip
    out = run(config, parse_config(config), run_id="r1")
    assert out.summary["run"]["status"] == "success"
    assert gcs.read(gcs.resolve_uri(f"{prefix}/in/a.csv")) is None
    archived = [r.key for r in gcs.list(f"{prefix}/archive/orders/")]
    assert len(archived) == 1
    assert archived[0].rsplit("/", 1)[-1].startswith("a__run-r1")
    copy = gcs.read(gcs.resolve_uri(archived[0]))
    assert copy is not None
    assert copy.body == b"id\n1\n2\n"
