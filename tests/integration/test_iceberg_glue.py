"""`floe run` overwrite then append through a Glue catalog: database placement, location and rows.

Skipped unless `FLOE_GLUE_ENDPOINT` and `FLOE_S3_ENDPOINT` are set, e.g. both to a moto server
(`moto_server -p 5055`) or LocalStack: `FLOE_GLUE_ENDPOINT=http://localhost:5055`.
"""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs

import os
import uuid
from pathlib import Path

import boto3
import pytest
from pyiceberg.catalog import load_catalog

from floe.config.parse import parse_config
from floe.run import run

GLUE = os.environ.get("FLOE_GLUE_ENDPOINT")
S3 = os.environ.get("FLOE_S3_ENDPOINT")
BUCKET = os.environ.get("FLOE_S3_BUCKET", "warehouse")

pytestmark = [
    pytest.mark.glue,
    pytest.mark.skipif(
        not (GLUE and S3), reason="FLOE_GLUE_ENDPOINT or FLOE_S3_ENDPOINT not set"
    ),
]

CONFIG = """\
version: "0.2"
storages:
  default: local
  definitions:
    - {{name: local, type: local}}
    - {{name: lake, type: s3, bucket: {bucket}, region: us-east-1, endpoint: "{s3}", path_style_access: true}}
catalogs:
  default: glue
  definitions:
    - {{name: glue, type: glue, region: us-east-1, database: {database}, warehouse_storage: lake, warehouse_prefix: "{prefix}"}}
entities:
  - name: orders
    source: {{format: csv, path: {source}, options: {{header: true}}}}
    sink:
      write_mode: {mode}
      accepted: {{format: iceberg, path: orders, storage: lake, iceberg: {{namespace: sales, table: orders}}}}
    policy: {{severity: warn}}
    schema:
      columns:
        - {{name: id, type: int64}}
"""


def test_overwrite_then_append_lands_in_the_glue_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", os.environ.get("AWS_ACCESS_KEY_ID", "test"))
    monkeypatch.setenv(
        "AWS_SECRET_ACCESS_KEY", os.environ.get("AWS_SECRET_ACCESS_KEY", "test")
    )
    monkeypatch.setenv("AWS_ENDPOINT_URL_GLUE", GLUE or "")
    database, prefix = f"floe_{uuid.uuid4().hex[:8]}", f"glue-it/{uuid.uuid4().hex[:8]}"
    s3 = boto3.client("s3", endpoint_url=S3, region_name="us-east-1")
    try:
        s3.create_bucket(Bucket=BUCKET)
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass
    config = tmp_path / "floe.yml"
    for mode, rows in (("overwrite", "1\n2\n"), ("append", "3\n4\n5\n")):
        source = (
            tmp_path / f"{mode}.csv"
        )  # another file: the D5 retry guard skips a re-appended batch
        source.write_text(f"id\n{rows}")
        config.write_text(CONFIG.format(bucket=BUCKET, s3=S3, database=database, prefix=prefix, source=source, mode=mode))  # fmt: skip
        run(config, parse_config(config), run_id=f"r-{mode}")

    catalog = load_catalog("glue", type="glue", **{"glue.region": "us-east-1", "s3.endpoint": S3 or "", "s3.region": "us-east-1"})  # fmt: skip
    table = catalog.load_table((database, "orders"))
    assert table.location() == f"s3://{BUCKET}/{prefix}/sales/orders"
    assert sorted(table.scan().to_arrow()["id"].to_pylist()) == [1, 2, 3, 4, 5]
