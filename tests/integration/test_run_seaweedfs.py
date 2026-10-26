"""`floe run` end to end on the S3-compatible service of the #602 harness (SeaweedFS).

Skipped unless `FLOE_S3_ENDPOINT` is set, e.g. `FLOE_S3_ENDPOINT=http://localhost:8333`.
"""

import io
import json
import os
import uuid
from datetime import date
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


@pytest.fixture
def s3(monkeypatch: pytest.MonkeyPatch) -> S3Client:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "spike")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "spike-s3-secret")
    return S3Client(BUCKET, "us-east-1", os.environ["FLOE_S3_ENDPOINT"], True)


def test_csv_on_s3_to_parquet_on_s3_then_an_incremental_skip(
    s3: S3Client, tmp_path: Path
) -> None:
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
    part = s3.read(s3.resolve_uri(f"{prefix}/out/accepted/part-00000.parquet"))
    assert part is not None
    assert pl.read_parquet(io.BytesIO(part.body)).height == 2
    stored = s3.read(s3.resolve_uri(f"{prefix}/runs/run_r1/d/orders/run.json"))
    assert stored is not None
    report = json.loads(stored.body)
    assert [f["input_file"] for f in report["files"]] == [
        f"s3://{BUCKET}/{prefix}/in/a.csv"
    ]
    again = run(config, parse_config(config), run_id="r2")
    assert again.summary["results"]["files_skipped"] == 1
    assert s3.read(s3.resolve_uri(f"{prefix}/in/.floe/state/d/orders/state.json"))


def test_d28_a_partitioned_s3_source_loads_by_date_with_derived_columns(
    s3: S3Client, tmp_path: Path
) -> None:
    prefix = f"it-{uuid.uuid4().hex}"
    s3.write(s3.resolve_uri(f"{prefix}/in/2026/10/25/c3d4/y.csv"), b"id\n1\n")
    s3.write(s3.resolve_uri(f"{prefix}/in/2026/10/24/a1b2/x.csv"), b"id\n2\n")
    partitioning = 'partitioning: {{pattern: "{{yyyy}}/{{MM}}/{{dd}}/{{batch}}", date_column: ingest_date, batch_column: batch_id}}, '  # fmt: skip
    config = tmp_path / "config.yml"
    config.write_text(
        CONFIG.replace("incremental_mode: file", "incremental_mode: none")
        .replace("storage: lake, options:", f"storage: lake, {partitioning}options:")
        .format(bucket=BUCKET, endpoint=os.environ["FLOE_S3_ENDPOINT"], prefix=prefix)
    )
    out = run(config, parse_config(config), run_id="r1")
    assert out.summary["run"]["status"] == "success"
    part = s3.read(s3.resolve_uri(f"{prefix}/out/accepted/part-00000.parquet"))
    assert part is not None
    assert pl.read_parquet(io.BytesIO(part.body)).rows() == [
        (2, date(2026, 10, 24), "a1b2"),
        (1, date(2026, 10, 25), "c3d4"),
    ]
    stored = s3.read(s3.resolve_uri(f"{prefix}/runs/run_r1/d/orders/run.json"))
    assert stored is not None
    assert json.loads(stored.body)["source"]["resolved_inputs"]["files"] == [
        f"s3://{BUCKET}/{prefix}/in/2026/10/24/a1b2/x.csv",
        f"s3://{BUCKET}/{prefix}/in/2026/10/25/c3d4/y.csv",
    ]
