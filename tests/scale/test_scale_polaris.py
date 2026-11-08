"""#558: ten generated entities of the scale project load into Iceberg tables on a real Polaris.

Skipped unless `FLOE_POLARIS_URI` and `FLOE_S3_ENDPOINT` are set (harness: see `tests/integration/test_iceberg_catalog.py`).
Run with `pytest -m "polaris and scale" tests/scale`: the nightly Polaris job does, a pull request does not.
"""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
# ^ pyiceberg and pyarrow ship no complete type stubs

import os
import uuid
from pathlib import Path

import pytest
from scale_project import ACCEPTED, FILES, REJECTED, ROWS, generate, write_input

from floe.config.parse import parse_config
from floe.iceberg.catalog import open_catalog
from floe.run import run

URI = os.environ.get("FLOE_POLARIS_URI")
S3 = os.environ.get("FLOE_S3_ENDPOINT")

pytestmark = [
    pytest.mark.polaris,
    pytest.mark.scale,
    pytest.mark.skipif(
        URI is None or S3 is None,
        reason="FLOE_POLARIS_URI or FLOE_S3_ENDPOINT is not set",
    ),
]
ENTITIES = 10

CLUSTER = """\
storages:
  default: local
  definitions:
    - {{name: local, type: local}}
    - {{name: lake, type: s3, bucket: warehouse, region: us-east-1, endpoint: "{s3}", path_style_access: true, prefix: spike/{tag}}}
catalogs:
  default: polaris
  definitions:
    - {{name: polaris, type: rest, uri: "{uri}", warehouse: spike, credential: "spike-client:spike-secret", scope: "PRINCIPAL_ROLE:ALL"}}
"""


def test_a_subset_lands_in_iceberg_with_one_snapshot_per_run_and_the_rejects_counted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "spike")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "spike-s3-secret")
    tag = f"sc{uuid.uuid4().hex[:8]}"
    ids = generate(
        tmp_path,
        ENTITIES,
        domains=2,
        tag=tag,
        iceberg=CLUSTER.format(s3=S3, uri=URI, tag=tag),
    )
    path = tmp_path / "config.yml"
    root = parse_config(path, {})
    catalog = open_catalog(root.catalogs.definitions[0], None)  # pyright: ignore[reportOptionalMemberAccess] -- the config declares a catalog
    tables = [tuple(i.split(".")) for i in ids]

    def snapshots(table: tuple[str, ...]) -> int:
        return len(catalog.load_table(table).snapshots())

    try:
        first = run(path, root, run_id="first", workers=4)
        assert [r["entity"]["name"] for r in first.entity_reports] == ids
        assert (
            first.summary["run"]["status"] == "rejected"
        )  # every entity has rejects, none fails
        results = first.summary["results"]
        assert (results["accepted_total"], results["rejected_total"]) == (
            ENTITIES * ACCEPTED,
            ENTITIES * REJECTED,
        )
        for report in first.entity_reports:
            assert report["accepted_output"]["snapshot_id"] is not None
            assert report["results"]["rejected_total"] == REJECTED
        for table in tables:
            assert catalog.load_table(table).scan().to_arrow().num_rows == ACCEPTED
            assert snapshots(table) == 1  # both input files in one append commit

        # a restart reads nothing and commits nothing
        again = run(path, root, run_id="restart", workers=4)
        assert again.summary["results"]["files_skipped"] == ENTITIES * FILES
        assert again.summary["results"]["accepted_total"] == 0
        assert {snapshots(t) for t in tables} == {1}

        # a retry with one new drop in the first entity commits one more snapshot there only
        fmt = tables[0][1].split("_")[0]
        write_input(
            tmp_path / "in" / fmt / tables[0][1] / f"part-{FILES}.{fmt}",
            fmt,
            list(range(9000, 9000 + ROWS)),
        )
        run(path, root, run_id="retry", workers=4)
        assert [snapshots(t) for t in tables] == [2] + [1] * (ENTITIES - 1)
        assert (
            catalog.load_table(tables[0]).scan().to_arrow().num_rows == ACCEPTED + ROWS
        )
    finally:
        for table in tables:
            if catalog.table_exists(table):
                catalog.drop_table(table)
        for namespace in {(t[0],) for t in tables}:
            if catalog.namespace_exists(namespace):
                catalog.drop_namespace(namespace)
