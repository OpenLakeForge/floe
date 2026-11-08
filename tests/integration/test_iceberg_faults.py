"""#561 fault scenarios against a real Polaris: commit conflict, rival copy of a batch, writer failure and retry,
read-after-commit. Skipped unless `FLOE_POLARIS_URI` is set (harness: see `test_iceberg_catalog.py`).

Credential expiry and missing permissions are not repeatable on the local harness: see docs/support-matrix.md.
"""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportMissingTypeStubs=false
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
from pyiceberg.types import LongType, NestedField

from floe.config.storage import CatalogDefinition, StorageDefinition
from floe.errors import FloeRunError
from floe.iceberg.catalog import ensure_table, open_catalog
from floe.iceberg.write import write_table

URI = os.environ.get("FLOE_POLARIS_URI")

pytestmark = [
    pytest.mark.polaris,
    pytest.mark.skipif(URI is None, reason="FLOE_POLARIS_URI is not set"),
]

SCHEMA = Schema(NestedField(1, "id", LongType(), required=True))
ARROW = pa.schema([pa.field("id", pa.int64(), nullable=False)])


def open_test_catalog() -> Catalog:
    return open_catalog(
        CatalogDefinition(
            name="polaris",
            uri=URI or "",
            credential=SecretStr("spike-client:spike-secret"),
            warehouse="spike",
            oauth2_server_uri=None,
            scope="PRINCIPAL_ROLE:ALL",
            warehouse_storage=None,
            warehouse_prefix=None,
        ),
        StorageDefinition(
            name="s3",
            type="s3",
            bucket="warehouse",
            region="us-east-1",
            account=None,
            container=None,
            prefix=None,
            endpoint="http://localhost:8333",
            path_style_access=True,
        ),
    )


def ids(*values: int) -> pa.RecordBatchReader:
    return pa.RecordBatchReader.from_batches(
        ARROW, [pa.record_batch([pa.array(values, pa.int64())], schema=ARROW)]
    )


def stored(table: Table) -> list[int]:
    fresh = open_test_catalog().load_table(table.name())
    return sorted(fresh.scan().to_arrow()["id"].to_pylist())


@pytest.fixture
def table(monkeypatch: pytest.MonkeyPatch) -> Iterator[Table]:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "spike")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "spike-s3-secret")
    catalog = open_test_catalog()
    name = f"f{uuid.uuid4().hex[:8]}"
    created, _ = ensure_table(catalog, "fault_tests", name, SCHEMA, "e")
    yield created
    catalog.drop_table(("fault_tests", name))


def test_a_stale_writer_conflicts_then_commits_on_the_fresh_table(table: Table) -> None:
    write_table(open_test_catalog().load_table(table.name()), ids(1), "append", "rival", "h1")  # fmt: skip
    result = write_table(
        table, ids(2), "append", "mine", "h2"
    )  # `table` still points at the empty snapshot
    assert not result.already_committed
    assert stored(table) == [1, 2]
    assert len(open_test_catalog().load_table(table.name()).snapshots()) == 2


def test_a_rival_copy_of_the_same_batch_is_not_appended_twice(table: Table) -> None:
    write_table(open_test_catalog().load_table(table.name()), ids(1, 2), "append", "rival", "same")  # fmt: skip
    result = write_table(table, ids(1, 2), "append", "mine", "same")
    assert result.already_committed
    assert stored(table) == [1, 2]


def test_a_failed_writer_commits_nothing_and_a_retry_lands_once(table: Table) -> None:
    def broken() -> Iterator[pa.RecordBatch]:
        yield pa.record_batch([pa.array([1, 2], pa.int64())], schema=ARROW)
        raise OSError("connection reset")

    with pytest.raises(FloeRunError, match="iceberg data write failed"):
        write_table(table, pa.RecordBatchReader.from_batches(ARROW, broken()), "append", "r1", "h")  # fmt: skip
    assert not list(open_test_catalog().load_table(table.name()).snapshots())
    write_table(table, ids(1, 2), "append", "r1", "h")
    assert stored(table) == [1, 2]


def test_a_commit_is_visible_to_a_fresh_reader_at_once(table: Table) -> None:
    result = write_table(table, ids(7), "append", "r", "h")
    snapshot = open_test_catalog().load_table(table.name()).current_snapshot()
    assert snapshot is not None
    assert snapshot.snapshot_id == result.snapshot_id
    assert stored(table) == [7]
