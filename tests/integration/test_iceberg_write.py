"""Overwrite then append against a real Polaris: each is one commit and exactly one new snapshot.

Skipped unless `FLOE_POLARIS_URI` is set (harness: see `test_iceberg_catalog.py`).
"""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs

import os
import uuid
from collections.abc import Iterator

import pyarrow as pa
import pytest
from pydantic import SecretStr
from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import LongType, NestedField, StringType, TimestampType

from floe.config.storage import CatalogDefinition, StorageDefinition
from floe.errors import FloeRunError
from floe.iceberg.catalog import ensure_table, open_catalog
from floe.iceberg.write import INPUT_HASH, RUN_ID, WriteMode, write_table

URI = os.environ.get("FLOE_POLARIS_URI")

pytestmark = [
    pytest.mark.polaris,
    pytest.mark.skipif(URI is None, reason="FLOE_POLARIS_URI is not set"),
]

CATALOG = CatalogDefinition(
    name="polaris",
    uri=URI or "",
    credential=SecretStr("spike-client:spike-secret"),
    warehouse="spike",
    oauth2_server_uri=None,
    scope="PRINCIPAL_ROLE:ALL",
    warehouse_storage=None,
    warehouse_prefix=None,
)
STORAGE = StorageDefinition(
    name="s3",
    type="s3",
    bucket="warehouse",
    region="us-east-1",
    account=None,
    container=None,
    prefix=None,
    endpoint="http://localhost:8333",
    path_style_access=True,
)
SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "ts", TimestampType(), required=False),
)
ARROW = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("name", pa.large_string()),
        pa.field("ts", pa.timestamp("us")),
    ]
)


def rows(start: int, count: int) -> pa.RecordBatchReader:  # pyright: ignore[reportUnknownParameterType] -- pyarrow ships no stubs
    days = [f"2026-01-0{1 + i % 3}T10:00:00" for i in range(count)]
    table = pa.table(
        {
            "id": pa.array(range(start, start + count), pa.int64()),
            "name": pa.array([f"n{i}" for i in range(start, start + count)]),
            "ts": pa.array(days).cast(pa.timestamp("us")),
        },
        schema=ARROW,
    )
    return pa.RecordBatchReader.from_batches(ARROW, table.to_batches(max_chunksize=7))


@pytest.fixture(
    params=[None, [("ts", "day")], [("id", "bucket[4]")]],
    ids=["unpartitioned", "day", "bucket"],
)
def table(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Table]:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "spike")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "spike-s3-secret")
    catalog: Catalog = open_catalog(CATALOG, STORAGE)
    name = f"w{uuid.uuid4().hex[:8]}"
    created, _ = ensure_table(
        catalog, "write_tests", name, SCHEMA, "e", partition_fields=request.param
    )
    yield created
    catalog.drop_table(("write_tests", name))


def run(table: Table, mode: WriteMode, start: int, count: int) -> list[int]:
    ids: list[int]
    before = {s.snapshot_id for s in table.snapshots()}
    result = write_table(
        table, rows(start, count), mode, f"run-{start}", "hash", chunk_rows=10
    )
    table.refresh()
    added = [s for s in table.snapshots() if s.snapshot_id not in before]
    assert len(added) == 1
    assert result.snapshot_id == added[0].snapshot_id
    summary = added[0].summary
    assert summary is not None
    assert summary.additional_properties[RUN_ID] == f"run-{start}"
    assert summary.additional_properties[INPUT_HASH] == "hash"
    assert result.files_written >= 1
    assert result.total_bytes_written > 0
    ids = table.scan().to_arrow().column("id").to_pylist()
    return sorted(ids)


def test_overwrite_then_append_are_one_snapshot_each(table: Table) -> None:
    assert run(table, "append", 0, 5) == [0, 1, 2, 3, 4]
    assert run(table, "overwrite", 100, 30) == list(range(100, 130))
    assert run(table, "append", 200, 4) == [*range(100, 130), *range(200, 204)]


def test_empty_append_commits_nothing(table: Table) -> None:
    result = write_table(table, rows(0, 0), "append", "r", "h")
    assert result.files_written == 0
    assert result.snapshot_id is None
    assert result.avg_file_size_mb is None
    assert not list(table.snapshots())


def test_overwrite_scan_failure_is_a_floe_error(table: Table) -> None:
    run(table, "append", 0, 5)
    snapshot = table.current_snapshot()
    assert snapshot is not None
    table.io.delete(snapshot.manifest_list)
    with pytest.raises(FloeRunError, match="iceberg overwrite scan failed"):
        write_table(table, rows(10, 3), "overwrite", "r", "h")
