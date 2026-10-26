"""#920: a commit conflict refreshes the table and recommits the data files already written."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs

from pathlib import Path
from typing import cast

import pyarrow as pa
import pytest
from pyiceberg.catalog import Catalog
from pyiceberg.io import load_file_io
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import CommitTableResponse, Table
from pyiceberg.table.metadata import TableMetadata, new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.table.update import (
    TableRequirement,
    TableUpdate,
    update_table_metadata,
)
from pyiceberg.types import LongType, NestedField

import floe.iceberg.write as write_module
from floe.errors import FloeRunError
from floe.iceberg.write import INPUT_HASH, write_table

SCHEMA = Schema(NestedField(1, "id", LongType()))
IDENTIFIER = ("sales", "orders")


class SharedCatalog:
    """The REST catalog's compare-and-swap: every `load_table` is a new handle on one stored metadata."""

    def __init__(self, root: Path) -> None:
        self.root, self.commits = root, 0
        self.metadata: TableMetadata = new_table_metadata(
            SCHEMA, PartitionSpec(), UNSORTED_SORT_ORDER, f"file://{root}", {}
        )
        self.location = f"file://{root}/metadata/00000-init.metadata.json"
        self.rivals, self.rival_hash = (
            0,
            "rival",
        )  # commits another handle lands just before ours, and their hash
        self.rival_id: int | None = None  # the row they hold; default 100 + n

    def load_table(self, _: object) -> Table:
        return Table(
            IDENTIFIER,
            self.metadata,
            self.location,
            load_file_io(location=str(self.root)),
            cast("Catalog", self),  # duck-typed stand-in
        )

    def supports_server_side_planning(self) -> bool:
        return False

    def _delete_old_metadata(self, *_: object) -> None:
        pass

    def commit_table(
        self,
        table: Table,
        requirements: tuple[TableRequirement, ...],
        updates: tuple[TableUpdate, ...],
    ) -> CommitTableResponse:
        if self.rivals:
            self.rivals -= 1
            rival = self.load_table(IDENTIFIER)
            n = self.rivals
            self.rivals = 0  # the rival's own commit meets no rival
            write_table(
                rival,
                reader([100 + n if self.rival_id is None else self.rival_id]),
                "append",
                "other-run",
                self.rival_hash,
            )
            self.rivals = n
        for requirement in requirements:
            requirement.validate(
                self.metadata
            )  # raises CommitFailedException, the REST 409
        self.commits += 1
        self.metadata = update_table_metadata(self.metadata, updates)
        self.location = f"file://{self.root}/metadata/{self.commits:05}-x.metadata.json"
        return CommitTableResponse.model_validate(
            {"metadata": self.metadata, "metadata-location": self.location}
        )


def reader(ids: list[int]) -> pa.RecordBatchReader:  # pyright: ignore[reportUnknownParameterType] -- pyarrow ships no stubs
    schema = pa.schema([pa.field("id", pa.int64(), nullable=True)])
    return pa.RecordBatchReader.from_batches(
        schema, [pa.record_batch([pa.array(ids, pa.int64())], schema=schema)]
    )


def ids(catalog: SharedCatalog) -> list[int]:
    return sorted(catalog.load_table(IDENTIFIER).scan().to_arrow()["id"].to_pylist())


@pytest.fixture
def catalog(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SharedCatalog:
    monkeypatch.setattr(write_module, "_backoff", lambda _: None)  # pyright: ignore[reportUnknownLambdaType] -- stub
    return SharedCatalog(tmp_path)


def test_a_commit_between_load_and_commit_is_retried_on_the_fresh_table(catalog: SharedCatalog) -> None:  # fmt: skip
    mine = catalog.load_table(IDENTIFIER)
    catalog.rivals = 1
    result = write_table(mine, reader([1, 2]), "append", "run", "mine")
    assert result.files_written == 1 and not result.already_committed
    assert ids(catalog) == [1, 2, 100]  # the rival's row and ours, once each
    snapshots = catalog.load_table(IDENTIFIER).metadata.snapshots
    assert len(snapshots) == 2  # exactly one extra snapshot
    assert snapshots[-1].snapshot_id == result.snapshot_id
    assert snapshots[-1].summary and snapshots[-1].summary[INPUT_HASH] == "mine"


def test_a_conflict_with_the_same_batch_stops_and_reports_it_committed(catalog: SharedCatalog) -> None:  # fmt: skip
    mine = catalog.load_table(IDENTIFIER)
    catalog.rivals, catalog.rival_hash = 1, "mine"  # an overlapping retry of one run
    result = write_table(mine, reader([1, 2]), "append", "run", "mine")
    assert result.already_committed and result.files_written == 0
    assert ids(catalog) == [100]
    assert len(catalog.load_table(IDENTIFIER).metadata.snapshots) == 1


def test_five_conflicts_in_a_row_fail_and_leave_no_partial_snapshot(catalog: SharedCatalog) -> None:  # fmt: skip
    mine = catalog.load_table(IDENTIFIER)

    catalog.rivals = 5  # a rival wins every commit
    with pytest.raises(FloeRunError, match="conflicted 5 times"):
        write_table(mine, reader([1]), "append", "run", "mine")
    snapshots = catalog.load_table(IDENTIFIER).metadata.snapshots
    assert len(snapshots) == catalog.commits == 5  # only the rivals' five
    assert all(s.summary and s.summary[INPUT_HASH] == "rival" for s in snapshots)
    assert 1 not in ids(catalog)


def test_a_reload_of_an_old_batch_is_not_mistaken_for_a_rival_s_copy(catalog: SharedCatalog) -> None:  # fmt: skip
    """After `floe state reset` the same inputs (same hash) are loaded on purpose; only commits since the base count."""
    write_table(catalog.load_table(IDENTIFIER), reader([1]), "append", "old", "mine")
    mine = catalog.load_table(IDENTIFIER)
    catalog.rivals = 1
    result = write_table(mine, reader([1]), "append", "run", "mine")
    assert not result.already_committed
    assert ids(catalog) == [1, 1, 100]


def test_two_writers_with_one_key_on_one_base_commit_one_row(catalog: SharedCatalog) -> None:  # fmt: skip
    """#549: both validated `unique` against the same snapshot; the one that loses the commit fails closed."""
    mine = catalog.load_table(IDENTIFIER)
    catalog.rivals, catalog.rival_id = 1, 7
    with pytest.raises(FloeRunError, match=r"unique key \(id\) already committed"):
        write_table(mine, reader([7]), "append", "run", "mine", unique_keys=[["id"]])
    assert ids(catalog) == [7]
    assert len(catalog.load_table(IDENTIFIER).metadata.snapshots) == 1


def test_a_conflict_without_a_shared_key_still_recommits(catalog: SharedCatalog) -> None:  # fmt: skip
    mine = catalog.load_table(IDENTIFIER)
    catalog.rivals, catalog.rival_id = 1, 8
    write_table(mine, reader([7]), "append", "run", "mine", unique_keys=[["id"]])
    assert ids(catalog) == [7, 8]
