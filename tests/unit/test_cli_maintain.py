"""`floe maintain` on a local warehouse behind a stub catalog (#950)."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs

import os
import sys
import time
from pathlib import Path

import pyarrow as pa
import pytest
from pyiceberg.io import load_file_io
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.serializers import ToOutputFile
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
from floe.maintain import _list  # pyright: ignore[reportPrivateUsage] -- the S3 branch of the listing
from floe.storage.types import ObjectRef

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
    """Just enough catalog for PyIceberg appends and expiry: one table, each commit writes its metadata file."""

    def __init__(self) -> None:
        self.table: Table | None = None
        self.commits = 0

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
        self.commits += 1
        metadata = update_table_metadata(
            table.metadata, updates, metadata_location=table.metadata_location
        )
        location = f"{table.location()}/metadata/{self.commits:05}.metadata.json"
        ToOutputFile.table_metadata(metadata, table.io.new_output(location))
        return CommitTableResponse.model_validate(
            {"metadata": metadata, "metadata-location": location}
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
        ToOutputFile.table_metadata(metadata, table.io.new_output(table.metadata_location))  # fmt: skip
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
    expected = {
        "entity": "Orders", "table": "sales.orders", "expired_snapshots": 5, "kept_snapshots": 10,
        "orphan_files": [], "orphan_bytes": 0,
    }  # fmt: skip
    # the profile arguments keep their positions from #950
    assert floe.maintain("config.yml", None, 7, 10, {}) == [expected]
    with pytest.raises(floe.FloeConfigError, match="retention floor"):
        floe.maintain("config.yml", older_than_days=6)


def age_files(tmp_path: Path, days: float) -> None:
    then = time.time() - days * DAY
    for p in (tmp_path / "warehouse").rglob("*"):
        os.utime(p, (then, then))


def reachable(table: Table) -> set[str]:
    m, inspect = table.metadata, table.inspect
    paths = [table.metadata_location, *(e.metadata_file for e in m.metadata_log), *(s.manifest_list for s in m.snapshots)]  # fmt: skip
    paths += inspect.all_manifests()["path"].to_pylist() + inspect.all_files()["file_path"].to_pylist()  # fmt: skip
    return {p.removeprefix("file://") for p in paths}


def test_orphans_older_than_the_delay_are_removed(tmp_path: Path, catalog: StubCatalog, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:  # fmt: skip
    table = seed(tmp_path, catalog)
    data = tmp_path / "warehouse/data"
    (data / "old-orphan.parquet").write_bytes(b"x" * 10)
    age_files(tmp_path, 4)
    (data / "young-orphan.parquet").write_bytes(b"y")  # an in-flight `floe run` write
    code, out, err = floe_cli(monkeypatch, capsys)
    assert (code, err) == (0, ""), err
    assert not (data / "old-orphan.parquet").exists()
    assert (data / "young-orphan.parquet").exists()
    left = {str(p) for p in (tmp_path / "warehouse").rglob("*") if p.is_file()}
    assert reachable(table) <= left
    # the old orphan, and the manifest lists only the 5 expired snapshots referenced
    assert left == reachable(table) | {str(data / "young-orphan.parquet")}
    assert "  Orphan files removed: 6" in out
    assert sorted(table.scan().to_arrow()["id"].to_pylist()) == list(range(15))


def test_dry_run_lists_the_candidates_and_changes_nothing(tmp_path: Path, catalog: StubCatalog, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:  # fmt: skip
    table = seed(tmp_path, catalog)
    orphan = tmp_path / "warehouse/data/old-orphan.parquet"
    orphan.write_bytes(b"x" * 10)
    age_files(tmp_path, 4)
    code, out, err = floe_cli(monkeypatch, capsys, "--dry-run")
    assert (code, err) == (0, ""), err
    assert orphan.exists() and len(table.metadata.snapshots) == 15
    assert "  Snapshots to expire: 5, kept: 10\n  Orphan files to remove: 1 (10 bytes)\n" in out  # fmt: skip
    assert f"    file://{orphan}\n" in out


def test_an_orphan_delay_below_one_day_fails_closed(tmp_path: Path, catalog: StubCatalog, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:  # fmt: skip
    seed(tmp_path, catalog)
    orphan = tmp_path / "warehouse/data/old-orphan.parquet"
    orphan.write_bytes(b"x")
    age_files(tmp_path, 4)
    code, _, err = floe_cli(monkeypatch, capsys, "--orphan-delay-days", "0")
    assert code == 1 and "orphan delay" in err
    assert orphan.exists()


def test_orphan_removal_refuses_a_listing_without_the_current_metadata(tmp_path: Path, catalog: StubCatalog, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:  # fmt: skip
    table = seed(tmp_path, catalog)
    orphan = tmp_path / "warehouse/data/old-orphan.parquet"
    orphan.write_bytes(b"x")
    age_files(tmp_path, 4)
    Path(table.metadata_location.removeprefix("file://")).unlink()
    code, _, err = floe_cli(monkeypatch, capsys, "--older-than-days", "100")
    assert code == 1 and "current metadata" in err
    assert orphan.exists()


@pytest.mark.parametrize(("location", "prefix"), [("s3://w/t/orders", "t/orders/"), ("s3://w/t/orders/", "t/orders/"), ("s3://w", ""), ("s3://w/", "")])  # fmt: skip
def test_an_s3_location_is_listed_through_the_storage_client(monkeypatch: pytest.MonkeyPatch, location: str, prefix: str) -> None:  # fmt: skip
    calls: list[tuple[object, ...]] = []

    class FakeS3:
        def __init__(self, *args: object) -> None:
            calls.append(args)

        def list(self, prefix: str) -> list[ObjectRef]:
            calls.append((prefix,))
            return [ObjectRef("s3://w/t/orders/data/a.parquet", "t/orders/data/a.parquet", "2026-10-01T00:00:00Z", 7)]  # fmt: skip

    # `floe.maintain` is also the public function, so the module comes from sys.modules
    monkeypatch.setattr(sys.modules["floe.maintain"], "S3Client", FakeS3)
    listed = _list(location, None)
    assert calls == [("w", None, None, None), (prefix,)]
    assert listed == [("s3://w/t/orders/data/a.parquet", 1790812800.0, 7)]
