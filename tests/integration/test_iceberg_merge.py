"""#598's SCD1 golden fixture against a real Polaris, on every partition layout it lists.

Skipped unless `FLOE_POLARIS_URI` is set (harness: see `test_iceberg_catalog.py`).
"""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs

import datetime as dt
import os
import uuid
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pydantic import SecretStr
from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    IdentityTransform,
    Transform,
)
from pyiceberg.types import (
    DateType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
)

from floe.config.storage import CatalogDefinition, StorageDefinition
from floe.iceberg.catalog import ensure_table, open_catalog, open_duckdb
from floe.iceberg.merge import merge_scd1

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
NAMESPACE = "merge_tests"
D = dt.date

SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "amount", DoubleType(), required=False),
    NestedField(4, "event_date", DateType(), required=False),
    NestedField(5, "region", StringType(), required=False),
)
# the batch carries `tier`, which the target lacks: additive evolution before the merge
WIDER = Schema(*SCHEMA.fields, NestedField(6, "tier", StringType(), required=False))
ARROW = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("name", pa.large_string()),
        pa.field("amount", pa.float64()),
        pa.field("event_date", pa.date32()),
        pa.field("region", pa.large_string()),
    ]
)
BATCH_ARROW = ARROW.append(pa.field("tier", pa.large_string()))

TARGET = [
    (1, "a", 10.0, D(2026, 1, 1), "eu"),
    (2, "b", 20.0, D(2026, 1, 2), "eu"),
    (3, "c", 30.0, D(2026, 1, 3), "us"),
    (4, "d", 40.0, D(2026, 1, 4), "us"),
    (6, "f", None, D(2026, 1, 6), "eu"),
    (7, "g", None, D(2026, 1, 7), "us"),
]
BATCH = [
    (2, "b2", 20.0, D(2026, 1, 2), "us", None),  # changed, moves identity partition
    (3, "c", 30.0, D(2026, 1, 3), "us", None),  # identical
    (4, "d", 40.0, D(2026, 1, 4), "us", "gold"),  # only the added column is set
    (5, "e", 50.0, D(2026, 1, 5), "eu", "silver"),  # new key
    (6, "f", None, D(2026, 1, 6), "eu", None),  # null in both: unchanged
    (7, "g", 70.0, D(2026, 1, 7), "us", None),  # null -> 70: changed
]
EXPECTED = [
    (1, "a", 10.0, D(2026, 1, 1), "eu", None),
    (2, "b2", 20.0, D(2026, 1, 2), "us", None),
    (3, "c", 30.0, D(2026, 1, 3), "us", None),
    (4, "d", 40.0, D(2026, 1, 4), "us", "gold"),
    (5, "e", 50.0, D(2026, 1, 5), "eu", "silver"),
    (6, "f", None, D(2026, 1, 6), "eu", None),
    (7, "g", 70.0, D(2026, 1, 7), "us", None),
]
LAYOUTS: dict[str, list[tuple[int, Transform[Any, Any]]]] = {
    "unpartitioned": [],
    "identity": [(5, IdentityTransform())],
    "day": [(4, DayTransform())],
    "bucket": [(1, BucketTransform(4))],
}


def arrow(schema: pa.Schema, rows: list[tuple[Any, ...]]) -> pa.Table:
    return pa.Table.from_pylist([dict(zip(schema.names, r)) for r in rows], schema)


def read(table: Table) -> list[tuple[Any, ...]]:
    return sorted(tuple(r.values()) for r in table.scan().to_arrow().to_pylist())


def assert_files_hold_their_partition(table: Table) -> None:
    schema = table.schema()
    for task in table.scan().plan_files():
        data = pq.read_table(table.io.new_input(task.file.file_path).open())
        for i, field in enumerate(table.spec().fields):
            column = data.column(schema.find_column_name(field.source_id))
            if pa.types.is_date32(column.type):  # transforms take days since epoch
                column = column.cast(pa.int32())
            source_type = schema.find_type(field.source_id)
            transform = field.transform.transform(source_type)
            values = {transform(v) for v in column.to_pylist()}
            assert values == {task.file.partition[i]}, task.file.file_path


@pytest.fixture
def catalog(monkeypatch: pytest.MonkeyPatch) -> Catalog:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "spike")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "spike-s3-secret")
    return open_catalog(CATALOG, STORAGE)


@pytest.fixture(params=list(LAYOUTS))
def table(request: pytest.FixtureRequest, catalog: Catalog) -> Iterator[Table]:
    if not catalog.namespace_exists(NAMESPACE):
        catalog.create_namespace(NAMESPACE)
    name = f"m{uuid.uuid4().hex[:8]}"
    spec = PartitionSpec(
        *(
            PartitionField(source, 1000 + i, transform, f"p{i}")
            for i, (source, transform) in enumerate(LAYOUTS[request.param])
        )
    )
    created = catalog.create_table((NAMESPACE, name), SCHEMA, partition_spec=spec)
    created.append(arrow(ARROW, TARGET))
    yield created
    catalog.drop_table((NAMESPACE, name))


def test_scd1_golden_then_rerun_changes_nothing(catalog: Catalog, table: Table) -> None:
    con = open_duckdb(CATALOG, STORAGE)  # opened before the schema change on purpose
    table = ensure_table(
        catalog, NAMESPACE, table.name()[1], WIDER, "e", schema_evolution="add_columns"
    )
    commits = len(table.metadata.metadata_log)
    batch = arrow(BATCH_ARROW, BATCH)

    result = merge_scd1(con, table, batch.to_reader(), ["id"])

    assert read(table) == EXPECTED
    assert len(table.metadata.metadata_log) == commits + 1  # one catalog commit
    current = table.current_snapshot()
    assert current is not None and result.snapshot_id == current.snapshot_id
    assert result.files_written >= 1
    assert_files_hold_their_partition(table)

    snapshots = len(table.snapshots())
    rerun = merge_scd1(con, table, batch.to_reader(), ["id"])

    assert read(table) == EXPECTED
    assert len(table.snapshots()) == snapshots
    assert rerun.files_written == 0


def test_ignored_columns_keep_target_values(catalog: Catalog, table: Table) -> None:
    con = open_duckdb(CATALOG, STORAGE)
    batch = arrow(ARROW, [(1, "a2", 11.0, D(2026, 1, 1), "eu")])

    merge_scd1(con, table, batch.to_reader(), ["id"], ignore_columns={"amount"})

    assert read(table)[0] == (1, "a2", 10.0, D(2026, 1, 1), "eu")
