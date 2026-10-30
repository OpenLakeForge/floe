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
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

from floe.config.storage import CatalogDefinition, StorageDefinition
from floe.iceberg.catalog import ensure_table, open_catalog, open_duckdb
from floe.iceberg.merge import merge_scd1, merge_scd2
from floe.iceberg.write import write_table

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


def create(
    catalog: Catalog, layout: str, schema: Schema, data: pa.Table
) -> Iterator[Table]:
    if not catalog.namespace_exists(NAMESPACE):
        catalog.create_namespace(NAMESPACE)
    name = f"m{uuid.uuid4().hex[:8]}"
    spec = PartitionSpec(
        *(
            PartitionField(source, 1000 + i, transform, f"p{i}")
            for i, (source, transform) in enumerate(LAYOUTS[layout])
        )
    )
    created = catalog.create_table((NAMESPACE, name), schema, partition_spec=spec)
    created.append(data)
    yield created
    catalog.drop_table((NAMESPACE, name))


@pytest.fixture(params=list(LAYOUTS))
def table(request: pytest.FixtureRequest, catalog: Catalog) -> Iterator[Table]:
    yield from create(catalog, request.param, SCHEMA, arrow(ARROW, TARGET))


def test_scd1_golden_then_rerun_changes_nothing(catalog: Catalog, table: Table) -> None:
    con = open_duckdb(CATALOG, STORAGE)  # opened before the schema change on purpose
    table, _ = ensure_table(
        catalog, NAMESPACE, table.name()[1], WIDER, "e", schema_evolution="add_columns"
    )
    commits = len(table.metadata.metadata_log)
    batch = arrow(BATCH_ARROW, BATCH)

    result = merge_scd1(con, table, batch.to_reader(), ["id"])

    assert read(table) == EXPECTED
    assert len(table.metadata.metadata_log) == commits + 1  # one catalog commit
    current = table.current_snapshot()
    assert current is not None and result.write.snapshot_id == current.snapshot_id
    assert result.write.files_written >= 1
    assert (result.inserted_count, result.updated_count) == (1, 3)
    assert_files_hold_their_partition(table)

    snapshots = len(table.snapshots())
    rerun = merge_scd1(con, table, batch.to_reader(), ["id"])

    assert read(table) == EXPECTED
    assert len(table.snapshots()) == snapshots
    assert rerun.write.files_written == 0
    assert (rerun.inserted_count, rerun.updated_count) == (0, 0)


def test_ignored_columns_keep_target_values(catalog: Catalog, table: Table) -> None:
    con = open_duckdb(CATALOG, STORAGE)
    batch = arrow(ARROW, [(1, "a2", 11.0, D(2026, 1, 1), "eu")])

    merge_scd1(con, table, batch.to_reader(), ["id"], ignore_columns={"amount"})

    assert read(table)[0] == (1, "a2", 10.0, D(2026, 1, 1), "eu")


def test_a_full_refresh_replaces_a_merged_table(catalog: Catalog, table: Table) -> None:
    """#1091: a `merge_scd1` full refresh writes as `overwrite` over a merge-on-read table, in one commit."""
    con = open_duckdb(CATALOG, STORAGE)
    merge_scd1(con, table, arrow(ARROW, [(1, "a2", 11.0, D(2026, 1, 1), "eu")]).to_reader(), ["id"])  # fmt: skip
    commits = len(table.metadata.metadata_log)
    inputs = [(1, "a3", 12.0, D(2026, 1, 1), "eu"), (9, "z", None, D(2026, 1, 9), "us")]

    write_table(table, arrow(ARROW, inputs).to_reader(), "overwrite", "refresh", "hash")

    # rows gone from the inputs leave the table, with their deletes
    assert read(table) == inputs
    assert len(table.metadata.metadata_log) == commits + 1
    assert_files_hold_their_partition(table)


# SCD2: #598's golden fixture; the target also holds a closed historical version of key 1
SYSTEM = [
    NestedField(6, "__floe_is_current", BooleanType(), required=True),
    NestedField(7, "__floe_valid_from", TimestampType(), required=True),
    NestedField(8, "__floe_valid_to", TimestampType(), required=False),
]
SCD2_SCHEMA = Schema(*SCHEMA.fields, *SYSTEM)
SCD2_WIDER = Schema(*SCD2_SCHEMA.fields, NestedField(9, "tier", StringType(), required=False))  # fmt: skip
SCD2_ARROW = ARROW.append(pa.field("__floe_is_current", pa.bool_(), nullable=False)).append(pa.field("__floe_valid_from", pa.timestamp("us"), nullable=False)).append(pa.field("__floe_valid_to", pa.timestamp("us")))  # fmt: skip
T0, T_HIST, T = dt.datetime(2026, 1, 1), dt.datetime(2025, 12, 1), "T"
SCD2_TARGET = [(*r, True, T0, None) for r in TARGET] + [(1, "a-old", 1.0, D(2025, 12, 1), "eu", False, T_HIST, T0)]  # fmt: skip
SCD2_EXPECTED = sorted([
    (1, "a-old", 1.0, D(2025, 12, 1), "eu", False, T_HIST, T0, None),
    (1, "a", 10.0, D(2026, 1, 1), "eu", True, T0, None, None),
    (3, "c", 30.0, D(2026, 1, 3), "us", True, T0, None, None),
    (6, "f", None, D(2026, 1, 6), "eu", True, T0, None, None),
    (2, "b", 20.0, D(2026, 1, 2), "eu", False, T0, T, None),
    (4, "d", 40.0, D(2026, 1, 4), "us", False, T0, T, None),
    (7, "g", None, D(2026, 1, 7), "us", False, T0, T, None),
    (2, "b2", 20.0, D(2026, 1, 2), "us", True, T, None, None),
    (4, "d", 40.0, D(2026, 1, 4), "us", True, T, None, "gold"),
    (5, "e", 50.0, D(2026, 1, 5), "eu", True, T, None, "silver"),
    (7, "g", 70.0, D(2026, 1, 7), "us", True, T, None, None),
], key=repr)  # fmt: skip


@pytest.fixture(params=list(LAYOUTS))
def scd2_table(request: pytest.FixtureRequest, catalog: Catalog) -> Iterator[Table]:
    yield from create(
        catalog, request.param, SCD2_SCHEMA, arrow(SCD2_ARROW, SCD2_TARGET)
    )


def read_scd2(table: Table) -> list[tuple[Any, ...]]:
    """Rows with the merge timestamp shown as `T`: one value, after every earlier one."""
    rows = [tuple(r.values()) for r in table.scan().to_arrow().to_pylist()]
    stamps = {v for r in rows for v in r[6:8] if v not in (None, T0, T_HIST)}
    assert len(stamps) <= 1 and all(s > T0 for s in stamps)
    return sorted(
        (
            tuple(T if i in (6, 7) and v in stamps else v for i, v in enumerate(r))
            for r in rows
        ),
        key=repr,
    )


def test_scd2_golden_then_rerun_changes_nothing(
    catalog: Catalog, scd2_table: Table
) -> None:
    con = open_duckdb(CATALOG, STORAGE)
    table, _ = ensure_table(
        catalog,
        NAMESPACE,
        scd2_table.name()[1],
        SCD2_WIDER,
        "e",
        schema_evolution="add_columns",
    )
    commits = len(table.metadata.metadata_log)
    batch = arrow(BATCH_ARROW, BATCH)

    result = merge_scd2(con, table, batch.to_reader(), ["id"])

    assert read_scd2(table) == SCD2_EXPECTED
    assert (
        len(table.metadata.metadata_log) == commits + 1
    )  # one catalog commit, per-bucket too
    current = table.current_snapshot()
    assert current is not None and result.write.snapshot_id == current.snapshot_id
    assert result.write.files_written >= 1
    counts = (result.inserted_count, result.updated_count, result.closed_count, result.unchanged_count)  # fmt: skip
    assert counts == (4, 3, 3, 2)
    assert_files_hold_their_partition(table)

    snapshots = len(table.snapshots())
    rerun = merge_scd2(con, table, batch.to_reader(), ["id"])

    assert read_scd2(table) == SCD2_EXPECTED
    assert len(table.snapshots()) == snapshots  # no new versions, no snapshot
    assert rerun.write.files_written == 0
    assert (rerun.inserted_count, rerun.closed_count, rerun.unchanged_count) == (
        0,
        0,
        6,
    )
