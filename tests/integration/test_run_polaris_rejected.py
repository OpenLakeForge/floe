"""A rejected Iceberg sink against a real Polaris: raw string values, one commit, no duplicate on a rerun.

Skipped unless `FLOE_POLARIS_URI` and `FLOE_S3_ENDPOINT` are set (harness: see `test_iceberg_catalog.py`).
"""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
# ^ pyiceberg and pyarrow ship no complete type stubs

import os
import uuid
from pathlib import Path

import pytest

from floe.config.parse import parse_config
from floe.iceberg.catalog import open_catalog
from floe.run import run

URI = os.environ.get("FLOE_POLARIS_URI")
S3 = os.environ.get("FLOE_S3_ENDPOINT")

pytestmark = [
    pytest.mark.polaris,
    pytest.mark.skipif(
        URI is None or S3 is None,
        reason="FLOE_POLARIS_URI or FLOE_S3_ENDPOINT is not set",
    ),
]

CONFIG = """\
version: "0.3"
storages:
  default: local
  definitions:
    - {{name: local, type: local}}
    - {{name: lake, type: s3, bucket: warehouse, region: us-east-1, endpoint: "{s3}", path_style_access: true, prefix: spike/{tag}}}
catalogs:
  default: polaris
  definitions:
    - {{name: polaris, type: rest, uri: "{uri}", warehouse: spike, credential: "spike-client:spike-secret", scope: "PRINCIPAL_ROLE:ALL"}}
domains:
  - name: {tag}
entities:
  - name: orders
    domain: {tag}
    source: {{format: csv, path: ./in, options: {{header: true, separator: ","}}}}
    sink:
      write_mode: append
      accepted: {{format: iceberg, path: tables/orders, storage: lake, iceberg: {{catalog: polaris}}}}
      rejected: {{format: iceberg, path: tables/orders_rejected, storage: lake}}
    policy: {{severity: reject}}
    schema:
      columns:
        - {{name: id, type: int64}}
        - {{name: amount, type: "decimal(10,2)"}}
report: {{path: ./report}}
"""


def test_rejected_rows_land_as_strings_and_a_rerun_adds_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "spike")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "spike-s3-secret")
    tag = f"rej{uuid.uuid4().hex[:8]}"
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "a.csv").write_text("id,amount\n1,1.50\n2,abc\n")
    path = tmp_path / "config.yml"
    path.write_text(CONFIG.format(s3=S3, uri=URI, tag=tag))
    root = parse_config(path, {})
    catalog = open_catalog(root.catalogs.definitions[0], None)  # pyright: ignore[reportOptionalMemberAccess] -- the config declares a catalog
    try:
        run(path, root)
        rejected = catalog.load_table((tag, "orders_rejected"))
        (row,) = rejected.scan().to_arrow().to_pylist()
        assert (row["id"], row["amount"], row["__floe_row_index"]) == ("2", "abc", 1)
        assert catalog.load_table((tag, "orders")).scan().to_arrow().num_rows == 1
        run(
            path, root
        )  # the same input again: the rejected table already holds the batch
        assert catalog.load_table((tag, "orders_rejected")).scan().to_arrow().num_rows == 1  # fmt: skip
    finally:
        for table in ("orders", "orders_rejected"):
            if catalog.table_exists((tag, table)):
                catalog.drop_table((tag, table))
        if catalog.namespace_exists((tag,)):
            catalog.drop_namespace((tag,))
