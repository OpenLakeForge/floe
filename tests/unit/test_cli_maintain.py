"""`floe maintain` on a local warehouse behind a stub catalog (#950)."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs

import sys
import time
from pathlib import Path

import pyarrow as pa
import pytest
from pyiceberg.io import load_file_io
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import CommitTableResponse, Table
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.table.update import (
    TableRequirement,
    TableUpdate,
    update_table_metadata,
)
from pyiceberg.types import LongType, NestedField

import floe
import floe.iceberg.sink as sink_module
from floe._cli import main

DAY = 86_400
CONFIG = """\
version: "0.3"
storages:
  default: local
  definitions:
    - {name: local, type: local}
    - {name: lake, type: s3, bucket: warehouse, region: us-east-1, prefix: floe}
catalogs:
  default: polaris
  definitions:
    - {name: polaris, type: rest, uri: "http://polaris", warehouse: w}
domains:
  - name: Sales
entities:
  - name: Orders
    domain: Sales
    source: {format: csv, path: ./in}
    sink:
      write_mode: append
      accepted: {format: iceberg, path: tables/orders, storage: lake, iceberg: {catalog: polaris}}
    policy: {severity: warn}
    schema: {columns: [{name: id, type: int64}]}
  - name: Plain
    source: {format: csv, path: ./in}
    sink: {accepted: {format: parquet, path: ./out}}
    policy: {severity: warn}
    schema: {columns: [{name: id, type: int64}]}
"""


class StubCatalog:
    """Just enough catalog for PyIceberg appends and expiry: one table, commits applied in memory."""

    def __init__(self) -> None:
        self.table: Table | None = None

    def table_exists(self, _: object) -> bool:
        return self.table is not None

    def load_table(self, _: object) -> Table:
        assert self.table is not None
        return self.table

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
        for requirement in requirements:
            requirement.validate(table.metadata)
        return CommitTableResponse.model_validate(
            {
                "metadata": update_table_metadata(table.metadata, updates),
                "metadata-location": f"{table.location()}/metadata/x.metadata.json",
            }
        )


@pytest.fixture
def catalog(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> StubCatalog:
    stub = StubCatalog()
    monkeypatch.setattr(sink_module, "open_catalog", lambda *_: stub)  # pyright: ignore[reportUnknownLambdaType] -- stub
    (tmp_path / "config.yml").write_text(CONFIG)
    monkeypatch.chdir(tmp_path)
    return stub


# 12 snapshots older than 7 days, then 3 recent ones
AGES = [30 - i for i in range(12)] + [2, 1, 0]


def seed(tmp_path: Path, catalog: StubCatalog) -> Table:
    """One append per age in `AGES` (oldest first), each snapshot stamped that many days ago."""
    schema = Schema(NestedField(1, "id", LongType()))
    now, where = time.time(), tmp_path / "warehouse"
    # PyIceberg stamps snapshots with `time.time()`
    with pytest.MonkeyPatch.context() as clock:
        clock.setattr(time, "time", lambda: now - AGES[0] * DAY - 1)
        metadata = new_table_metadata(schema, PartitionSpec(), UNSORTED_SORT_ORDER, f"file://{where}", {})  # fmt: skip
        table = Table(("sales", "orders"), metadata, f"file://{where}/metadata/0.metadata.json",
                      load_file_io(location=str(where)), catalog)  # fmt: skip  # pyright: ignore[reportArgumentType] -- duck-typed stub
        catalog.table = table
        for i, age in enumerate(AGES):
            clock.setattr(time, "time", lambda age=age: now - age * DAY)
            table.append(pa.table({"id": pa.array([i], pa.int64())}))
    return table


def floe_cli(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], *args: str) -> tuple[int, str, str]:  # fmt: skip
    monkeypatch.setattr(sys, "argv", ["floe", "maintain", "-c", "config.yml", *args])
    code = main()
    out, err = capsys.readouterr()
    return code, out, err


def test_expiry_keeps_the_last_7_days_and_at_least_10_snapshots(tmp_path: Path, catalog: StubCatalog, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:  # fmt: skip
    table = seed(tmp_path, catalog)
    before = sorted(table.metadata.snapshots, key=lambda s: s.timestamp_ms)
    code, out, err = floe_cli(monkeypatch, capsys)
    assert (code, err) == (0, ""), err
    after = table.metadata.snapshots
    assert [s.snapshot_id for s in after] == [s.snapshot_id for s in before[5:]]
    assert "Entity: Orders\n  Table: sales.orders\n  Snapshots expired: 5, kept: 10" in out  # fmt: skip
    assert "Plain" not in out
    # expiry is metadata only
    assert sorted(table.scan().to_arrow()["id"].to_pylist()) == list(range(15))


def test_expiry_with_a_wider_window_keeps_more(tmp_path: Path, catalog: StubCatalog, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:  # fmt: skip
    table = seed(tmp_path, catalog)
    code, _, err = floe_cli(monkeypatch, capsys, "--older-than-days", "20", "--retain-last", "11")  # fmt: skip
    assert (code, err) == (0, ""), err
    # of the 10 snapshots older than 20 days, 4 fall outside the 11 newest
    assert len(table.metadata.snapshots) == 11


@pytest.mark.parametrize("flags", [["--older-than-days", "1"], ["--retain-last", "9"]])
def test_a_request_below_the_retention_floor_fails_closed(tmp_path: Path, catalog: StubCatalog, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], flags: list[str]) -> None:  # fmt: skip
    table = seed(tmp_path, catalog)
    code, _, err = floe_cli(monkeypatch, capsys, *flags)
    assert code == 1
    assert "retention floor" in err
    assert len(table.metadata.snapshots) == 15


def test_protected_refs_are_never_expired(tmp_path: Path, catalog: StubCatalog, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:  # fmt: skip
    table = seed(tmp_path, catalog)
    oldest = min(table.metadata.snapshots, key=lambda s: s.timestamp_ms).snapshot_id
    table.manage_snapshots().create_tag(oldest, "audit").commit()
    code, out, err = floe_cli(monkeypatch, capsys)
    assert (code, err) == (0, ""), err
    assert oldest in {s.snapshot_id for s in table.metadata.snapshots}
    assert "Snapshots expired: 4, kept: 11" in out


def test_a_missing_table_expires_nothing(catalog: StubCatalog, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:  # fmt: skip
    code, out, _ = floe_cli(monkeypatch, capsys)
    assert code == 0
    assert "Snapshots expired: 0, kept: 0" in out


def test_public_api(tmp_path: Path, catalog: StubCatalog) -> None:
    seed(tmp_path, catalog)
    expected = {"entity": "Orders", "table": "sales.orders", "expired_snapshots": 5, "kept_snapshots": 10}  # fmt: skip
    assert floe.maintain("config.yml") == [expected]
    with pytest.raises(floe.FloeConfigError, match="retention floor"):
        floe.maintain("config.yml", older_than_days=6)
