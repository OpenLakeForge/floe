"""Entities in parallel against a real Polaris: different tables commit together, one table's writers take turns.

Skipped unless `FLOE_POLARIS_URI` and `FLOE_S3_ENDPOINT` are set (harness: see `test_iceberg_catalog.py`).
"""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
# ^ pyiceberg and pyarrow ship no complete type stubs

import os
import uuid
from pathlib import Path

import pytest

from floe.config.parse import parse_config
from floe.config.project import RootConfig
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
ROWS = 500

HEAD = """\
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
"""
ENTITY = """\
  - name: {name}
    domain: {tag}
    source: {{format: csv, path: ./in/{name}, options: {{header: true, separator: ","}}}}
    sink:
      write_mode: append
      accepted: {{format: iceberg, path: tables/{table}, storage: lake, iceberg: {{catalog: polaris, table: {table}}}}}
    policy: {{severity: reject}}
    schema:
      columns:
        - {{name: id, type: int64}}
"""


def project(tmp_path: Path, tag: str, plan: dict[str, str]) -> tuple[Path, RootConfig]:
    """`plan` maps an entity name to its table; every entity loads `ROWS` rows with ids of its own.

    The domain, hence the Iceberg namespace, is `tag`: new to the catalog, so the entities race to create it.
    """
    text = HEAD.format(s3=S3, uri=URI, tag=tag)
    for k, (name, table) in enumerate(plan.items()):
        (tmp_path / "in" / name).mkdir(parents=True)
        ids = range(k * ROWS, (k + 1) * ROWS)
        (tmp_path / "in" / name / "a.csv").write_text(
            "id\n" + "".join(f"{i}\n" for i in ids)
        )
        text += ENTITY.format(name=name, table=table, tag=tag)
    path = tmp_path / "config.yml"
    path.write_text(text + "report: {path: ./report}\n")
    return path, parse_config(path, {})


def test_entities_on_different_tables_and_on_one_table_all_commit_exact_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "spike")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "spike-s3-secret")
    tag = f"par{uuid.uuid4().hex[:8]}"
    plan = {f"e{k}": f"{tag}_t{k}" for k in range(6)} | {"s1": f"{tag}_shared", "s2": f"{tag}_shared", "s3": f"{tag}_shared"}  # fmt: skip
    path, root = project(tmp_path, tag, plan)
    try:
        out = run(path, root, workers=4)
        assert [r["entity"]["name"] for r in out.entity_reports] == [f"{tag}.{n}" for n in plan]  # fmt: skip
        assert {r["files"][0]["status"] for r in out.entity_reports} == {"success"}
        assert out.summary["run"]["status"] == "success"
        catalog = open_catalog(root.catalogs.definitions[0], None)  # pyright: ignore[reportOptionalMemberAccess] -- the config declares a catalog
        for table in set(plan.values()):
            want = ROWS * list(plan.values()).count(table)
            assert catalog.load_table((tag, table)).scan().to_arrow().num_rows == want  # fmt: skip
            assert len(catalog.load_table((tag, table)).scan().to_arrow().column("id").unique()) == want  # fmt: skip
    finally:
        catalog = open_catalog(root.catalogs.definitions[0], None)  # pyright: ignore[reportOptionalMemberAccess] -- as above
        for table in set(plan.values()):
            if catalog.table_exists((tag, table)):
                catalog.drop_table((tag, table))
        if catalog.namespace_exists((tag,)):
            catalog.drop_namespace((tag,))
