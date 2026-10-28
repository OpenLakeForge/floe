"""s3:// reports and manifests read with boto3 from a real S3-compatible service (#1021).

Skipped unless `FLOE_S3_ENDPOINT` is set, e.g. `FLOE_S3_ENDPOINT=http://localhost:8333` (SeaweedFS).
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from pathlib import Path

import pytest

boto3 = pytest.importorskip("boto3")

from floe_dagster.assets import _read_json_text  # noqa: E402 -- after the importorskip
from floe_dagster.manifest import load_manifest  # noqa: E402 -- after the importorskip

ENDPOINT = os.environ.get("FLOE_S3_ENDPOINT")
BUCKET = os.environ.get("FLOE_S3_BUCKET", "warehouse")

pytestmark = pytest.mark.skipif(not ENDPOINT, reason="FLOE_S3_ENDPOINT not set")


def test_report_and_manifest_are_read_through_a_custom_path_style_endpoint(monkeypatch) -> None:
    monkeypatch.setitem(sys.modules, "boto3", boto3)  # undo conftest's block: this test uses the real client
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", os.environ.get("AWS_ACCESS_KEY_ID", "spike"))
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", os.environ.get("AWS_SECRET_ACCESS_KEY", "spike-s3-secret"))
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", ENDPOINT or "")
    monkeypatch.setenv("AWS_S3_FORCE_PATH_STYLE", "true")
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    s3 = boto3.client("s3", endpoint_url=ENDPOINT, region_name="us-east-1")
    try:
        s3.create_bucket(Bucket=BUCKET)
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass
    prefix = f"dagster-floe-it/{uuid.uuid4().hex}"
    report = {"run": {"status": "success"}, "entities": []}
    s3.put_object(Bucket=BUCKET, Key=f"{prefix}/run.summary.json", Body=json.dumps(report).encode())
    manifest = (Path(__file__).parent / "fixtures" / "manifest.json").read_bytes()
    s3.put_object(Bucket=BUCKET, Key=f"{prefix}/manifest.json", Body=manifest)

    text = _read_json_text(f"s3://{BUCKET}/{prefix}/run.summary.json", f"s3://{BUCKET}/{prefix}/config.yml")
    assert json.loads(text) == report
    assert load_manifest(f"s3://{BUCKET}/{prefix}/manifest.json").manifest_id
