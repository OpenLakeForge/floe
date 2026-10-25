"""`floe run` end to end on the S3-compatible service of the #602 harness (SeaweedFS).

Skipped unless `FLOE_S3_ENDPOINT` is set, e.g. `FLOE_S3_ENDPOINT=http://localhost:8333`.
"""

import json
import os
import uuid
from pathlib import Path

import polars as pl
import pytest

from floe.config.parse import parse_config
from floe.run import run
from floe.storage.s3 import S3Client

pytestmark = [
    pytest.mark.s3,
    pytest.mark.skipif(
        not os.environ.get("FLOE_S3_ENDPOINT"), reason="FLOE_S3_ENDPOINT not set"
    ),
]
BUCKET = os.environ.get("FLOE_S3_BUCKET", "warehouse")

CONFIG = """\
version: "0.3"
storages:
  default: local
  definitions:
    - {{name: local, type: local}}
    - {{name: lake, type: s3, bucket: {bucket}, region: us-east-1, endpoint: "{endpoint}", path_style_access: true, prefix: {prefix}}}
domains:
  - name: d
entities:
  - name: orders
    domain: d
    source: {{format: csv, path: in/, storage: lake, options: {{header: true, separator: ","}}}}
    sink:
      accepted: {{format: parquet, path: ./out/accepted}}
    policy: {{severity: reject}}
    schema:
      columns:
        - {{name: id, type: int64}}
report: {{path: runs, storage: lake}}
"""


@pytest.fixture
def s3(monkeypatch: pytest.MonkeyPatch) -> S3Client:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "spike")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "spike-s3-secret")
    return S3Client(BUCKET, "us-east-1", os.environ["FLOE_S3_ENDPOINT"], True)


def test_csv_on_s3_runs_with_the_report_on_s3(s3: S3Client, tmp_path: Path) -> None:
    prefix = f"it-{uuid.uuid4().hex}"
    s3.write(s3.resolve_uri(f"{prefix}/in/a.csv"), b"id\n1\n2\n")
    config = tmp_path / "config.yml"
    config.write_text(
        CONFIG.format(
            bucket=BUCKET, endpoint=os.environ["FLOE_S3_ENDPOINT"], prefix=prefix
        )
    )
    out = run(config, parse_config(config), run_id="r1")
    assert out.summary["run"]["status"] == "success"
    assert pl.read_parquet(tmp_path / "out/accepted").height == 2
    stored = s3.read(s3.resolve_uri(f"{prefix}/runs/run_r1/d/orders/run.json"))
    assert stored is not None
    report = json.loads(stored.body)
    assert [f["input_file"] for f in report["files"]] == [
        f"s3://{BUCKET}/{prefix}/in/a.csv"
    ]
