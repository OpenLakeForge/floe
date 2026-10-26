"""`run_entity` with an Iceberg accepted sink, on a local warehouse behind a stub catalog."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs

import json
import sys
from collections.abc import Callable, Collection, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pytest
from jsonschema import Draft202012Validator
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

import floe
import floe.iceberg.sink as sink_module
from floe._cli import main
from floe.config.parse import parse_config
from floe.config.project import RootConfig
from floe.errors import FloeConfigError, FloeRunError
from floe.iceberg.merge import MergeResult
from floe.iceberg.sink import input_hash, partition_fields
from floe.iceberg.write import INPUT_HASH, RUN_ID, find_committed, write_result
from floe.run import entity as entity_module  # `floe.run` is the public function
from floe.run import run as run_project
from floe.run.entity import run_entity
from floe.run.events import NO_EVENTS, Emitter
from floe.storage.inputs import InputFile

CONFIG = """\
version: "0.3"
storages:
  default: local
  definitions:
    - {{name: local, type: local}}
    - {{name: lake, type: s3, bucket: warehouse, prefix: floe}}
catalogs:
  default: polaris
  definitions:
    - {{name: polaris, type: rest, uri: "http://polaris", warehouse: w}}
domains:
  - name: Sales
entities:
  - name: Orders
    domain: Sales
    source:
      format: csv
      path: ./in
      options: {{header: true, separator: ","}}
    sink:
      write_mode: {mode}
      accepted:
        format: iceberg
        path: tables/orders
        storage: lake
        iceberg: {{catalog: polaris}}
        {partition}
      rejected: {{format: csv, path: ./out/rejected}}
    policy: {{severity: {severity}}}
    schema:
      {evolution}
      primary_key: [id]
      columns:
        - {{name: id, type: int64, nullable: false}}
        - {{name: name, type: string}}
        {extra}
report: {{path: ./report}}
"""


class LocalCatalog:
    """Just enough REST catalog for `ensure_table` and `write_table`: tables live under `root`."""

    def __init__(self, root: Path) -> None:
        self.root, self.tables, self.locations = root, dict[str, Table](), list[str | None]()  # fmt: skip
        self.commits = 0

    def namespace_exists(self, _: object) -> bool:
        return True

    def table_exists(self, identifier: tuple[str, str]) -> bool:
        return ".".join(identifier) in self.tables

    def load_table(self, identifier: tuple[str, str]) -> Table:
        return self.tables[".".join(identifier)]

    def create_table(
        self,
        identifier: tuple[str, str],
        schema: Schema,
        location: str | None,
        partition_spec: PartitionSpec,
    ) -> Table:
        self.locations.append(location)
        where = self.root / ".".join(identifier)
        metadata = new_table_metadata(
            schema, partition_spec, UNSORTED_SORT_ORDER, f"file://{where}", {}
        )
        table = Table(
            identifier,
            metadata,
            f"file://{where}/metadata/00000-init.metadata.json",
            load_file_io(location=str(where)),
            self,  # pyright: ignore[reportArgumentType] -- duck-typed stand-in for a catalog
        )
        self.tables[".".join(identifier)] = table
        return table

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
        location = f"{table.location()}/metadata/{self.commits:05}-x.metadata.json"
        return CommitTableResponse.model_validate(
            {
                "metadata": update_table_metadata(table.metadata, updates),
                "metadata-location": location,
            }
        )


@pytest.fixture
def catalog(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> LocalCatalog:
    local = LocalCatalog(tmp_path / "warehouse")
    monkeypatch.setattr(sink_module, "open_catalog", lambda *_: local)  # pyright: ignore[reportUnknownLambdaType] -- stub
    return local


def config(tmp_path: Path, **values: str) -> RootConfig:
    defaults = {"mode": "overwrite", "severity": "reject", "partition": "", "evolution": "", "extra": ""}  # fmt: skip
    (tmp_path / "config.yml").write_text(CONFIG.format(**(defaults | values)))
    return parse_config(tmp_path / "config.yml")


def run(tmp_path: Path, rows: str, **values: str) -> dict[str, Any]:
    (tmp_path / "in").mkdir(exist_ok=True)
    (tmp_path / "in" / "orders.csv").write_text(rows)
    root = config(tmp_path, **values)
    return run_entity(
        root.entities[0], tmp_path, "0.3", storages=root.storages,
        emit=NO_EVENTS, catalogs=root.catalogs,
    ).report  # fmt: skip


def rows(catalog: LocalCatalog) -> list[tuple[Any, ...]]:
    data = catalog.tables["sales.orders"].scan().to_arrow().sort_by("id")
    return list(zip(*data.to_pydict().values(), strict=True))


def test_iceberg_entity_overwrites_the_table(tmp_path: Path, catalog: LocalCatalog) -> None:  # fmt: skip
    report = run(tmp_path, "id,name\n1,a\nx,b\n2,c\n")
    assert rows(catalog) == [(1, "a"), (2, "c")]
    assert catalog.locations == ["s3://warehouse/floe/tables/orders"]
    table = catalog.tables["sales.orders"]
    snapshot = table.current_snapshot()
    assert snapshot is not None
    out = report["accepted_output"]
    assert list(out)[:13] == [
        "path", "table_root_uri", "write_mode", "accepted_rows", "files_written",
        "parts_written", "part_files", "table_version", "snapshot_id",
        "iceberg_catalog_name", "iceberg_namespace", "iceberg_table", "total_bytes_written",
    ]  # fmt: skip
    assert out["path"] == out["table_root_uri"] == "s3://warehouse/floe/tables/orders"
    assert (out["snapshot_id"], out["table_version"]) == (snapshot.snapshot_id, 1)
    assert (out["iceberg_namespace"], out["iceberg_table"]) == ("sales", "orders")
    assert out["files_written"] == len(out["part_files"]) == 1
    assert out["total_bytes_written"] > 0 and out["small_files_count"] == 1
    assert report["files"][0]["output"]["accepted_path"] == out["path"]
    assert report["sink"]["accepted"]["path"] == out["path"]
    summary = snapshot.summary
    assert summary is not None and summary[RUN_ID] == NO_EVENTS.run_id
    assert len(str(summary[INPUT_HASH])) == 64
    assert [f.required for f in table.schema().fields] == [True, False]

    run(tmp_path, "id,name\n3,d\n")
    assert rows(catalog) == [(3, "d")]


def test_append_adds_rows(tmp_path: Path, catalog: LocalCatalog) -> None:
    run(tmp_path, "id,name\n1,a\n", mode="append")
    report = run(tmp_path, "id,name\n2,b\n", mode="append")
    assert rows(catalog) == [(1, "a"), (2, "b")]
    assert report["accepted_output"]["table_version"] == 2


@pytest.mark.parametrize("mode", ["overwrite", "append"])
def test_a_failed_commit_leaves_the_input_unarchived(tmp_path: Path, catalog: LocalCatalog, monkeypatch: pytest.MonkeyPatch, mode: str) -> None:  # fmt: skip
    """#914: archiving waits for the Iceberg commit, so a failed commit loses no rows."""
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "orders.csv").write_text("id,name\n1,a\n")
    config(tmp_path, mode=mode)
    path = tmp_path / "config.yml"
    path.write_text(path.read_text().replace("      rejected:", "      archive: {path: ./arch}\n      rejected:"))  # fmt: skip
    root = parse_config(path)

    def boom(*_: object) -> None:
        raise FloeRunError("commit failed")

    monkeypatch.setattr(catalog, "commit_table", boom)
    with pytest.raises(FloeRunError, match="commit failed"):
        run_entity(root.entities[0], tmp_path, "0.3", storages=root.storages, catalogs=root.catalogs)  # fmt: skip
    assert (tmp_path / "in/orders.csv").exists()
    assert not (tmp_path / "arch").exists()


def test_append_rejects_keys_already_in_the_table(tmp_path: Path, catalog: LocalCatalog) -> None:  # fmt: skip
    run(tmp_path, "id,name\n1,a\n", mode="append")
    (tmp_path / "in" / "b.csv").write_text("id,name\n2,x\n")  # read before orders.csv
    report = run(tmp_path, "id,name\n2,b\n1,c\n", mode="append")
    assert rows(catalog) == [(1, "a"), (2, "x")]
    (unique,) = report["unique_constraints"]
    # 1 is in the table; 2 only repeats the batch, though orders.csv also seeds 1
    assert (unique["duplicates_count"], unique["target_duplicates_count"]) == (2, 1)
    assert unique["batch_duplicates_count"] == 1


def test_append_to_a_missing_table_seeds_nothing(tmp_path: Path, catalog: LocalCatalog) -> None:  # fmt: skip
    report = run(tmp_path, "id,name\n1,a\n1,b\n", mode="append")
    assert rows(catalog) == [(1, "a")]
    (unique,) = report["unique_constraints"]
    assert "target_duplicates_count" not in unique


def test_add_columns_evolves_the_table(tmp_path: Path, catalog: LocalCatalog) -> None:
    evolution = "schema_evolution: {mode: add_columns}"
    run(tmp_path, "id,name\n1,a\n", mode="append", evolution=evolution)
    run(
        tmp_path, "id,name,qty\n2,b,5\n", mode="append", evolution=evolution,
        extra="- {name: qty, type: int32}",
    )  # fmt: skip
    assert rows(catalog) == [(1, "a", None), (2, "b", 5)]


def test_partition_spec_is_applied(tmp_path: Path, catalog: LocalCatalog) -> None:
    spec = "partition_spec: [{column: id, transform: bucket, num_buckets: 4}, {column: name, transform: identity}]"  # fmt: skip
    run(tmp_path, "id,name\n1,a\n2,b\n", partition=spec)
    run(tmp_path, "id,name\n3,c\n", mode="append", partition=spec)
    fields = catalog.tables["sales.orders"].spec().fields
    assert [(f.name, str(f.transform)) for f in fields] == [("id_bucket", "bucket[4]"), ("name", "identity")]  # fmt: skip
    assert rows(catalog) == [(1, "a"), (2, "b"), (3, "c")]


def test_bucket_maps_to_pyiceberg_notation(tmp_path: Path) -> None:
    entity = config(
        tmp_path,
        partition="partition_spec: [{column: id, transform: bucket, num_buckets: 16}, {column: name, transform: identity}]",
    ).entities[0]
    assert partition_fields(entity) == [("id", "bucket[16]"), ("name", "identity")]


def test_nulls_in_a_required_column_fail_the_write(tmp_path: Path, catalog: LocalCatalog) -> None:  # fmt: skip
    with pytest.raises(FloeRunError, match="^iceberg write rejected nulls for non-nullable column id$"):  # fmt: skip
        run(tmp_path, "id,name\n,a\n", severity="warn")
    assert not catalog.tables


Merges = list[tuple[list[str], set[str]]]


@pytest.fixture
def merges(monkeypatch: pytest.MonkeyPatch) -> Merges:
    """DuckDB can't attach the stub catalog: PyIceberg's upsert stands in for `merge_scd1` (Polaris runs the real one)."""
    calls: Merges = []

    def merge_scd1(_: object, table: Table, reader: pa.RecordBatchReader, key: Sequence[str], ignore: Collection[str] = ()) -> MergeResult:  # fmt: skip
        calls.append((list(key), set(ignore)))
        done = table.upsert(reader.read_all(), join_cols=list(key))
        return MergeResult(write_result(table, []), done.rows_inserted, done.rows_updated)  # fmt: skip

    monkeypatch.setattr(sink_module, "open_duckdb", lambda *_: duckdb.connect())  # pyright: ignore[reportUnknownLambdaType] -- stub
    monkeypatch.setattr(sink_module, "merge_scd1", merge_scd1)
    return calls


def test_merge_scd1_upserts_and_reports_the_counts(tmp_path: Path, catalog: LocalCatalog, merges: Merges) -> None:  # fmt: skip
    ignore = "merge: {ignore_columns: [name]}"
    first = run(tmp_path, "id,name\n1,a\n2,b\n", mode="merge_scd1", partition=ignore)
    report = run(tmp_path, "id,name\n2,B\n3,c\n", mode="merge_scd1", partition=ignore)
    assert rows(catalog) == [
        (1, "a"),
        (2, "B"),
        (3, "c"),
    ]  # the stand-in ignores nothing
    assert merges == [(["id"], {"name"})] * 2
    assert (first["accepted_output"]["inserted_count"], first["accepted_output"]["updated_count"]) == (2, 0)  # fmt: skip
    out = report["accepted_output"]
    assert list(out)[-4:] == ["merge_key", "inserted_count", "updated_count", "merge_elapsed_ms"]  # fmt: skip
    assert (out["write_mode"], out["merge_key"]) == ("merge_scd1", ["id"])
    assert (out["inserted_count"], out["updated_count"]) == (1, 1)
    assert out["merge_elapsed_ms"] >= 0 and out["snapshot_id"] is not None


def test_merge_scd2_is_not_yet_supported(tmp_path: Path, catalog: LocalCatalog) -> None:  # fmt: skip
    with pytest.raises(FloeRunError, match="sink.write_mode=merge_scd2 is not yet supported for iceberg"):  # fmt: skip
        run(tmp_path, "id,name\n1,a\n", mode="merge_scd2")


def test_full_refresh_with_a_merge_is_a_config_error(tmp_path: Path, catalog: LocalCatalog) -> None:  # fmt: skip
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "orders.csv").write_text("id,name\n1,a\n")
    root = config(tmp_path, mode="merge_scd1")
    with pytest.raises(FloeConfigError, match="^entity 'Orders': --full-refresh is not supported with write_mode 'merge_scd1'$"):  # fmt: skip
        run_project(tmp_path / "config.yml", root, full_refresh=True)
    assert not catalog.tables


def test_input_hash_covers_the_entity_and_the_claimed_files(tmp_path: Path) -> None:  # fmt: skip
    entity = config(tmp_path).entities[0]
    a = InputFile("local:///in/a.csv", "a.csv", "a", 1, "t1")
    b = InputFile("local:///in/b.csv", "b.csv", "b", 2, "t2")
    assert input_hash(entity, [a, b]) == input_hash(entity, [b, a])
    assert input_hash(entity, [a]) != input_hash(entity, [a, b])
    changed = InputFile("local:///in/a.csv", "a.csv", "a", 1, "t9")
    assert input_hash(entity, [changed]) != input_hash(entity, [a])
    other = entity.model_copy(update={"id": "other"})
    assert input_hash(other, [a]) != input_hash(entity, [a])


def test_warn_rejects_merge_key_duplicates_before_the_merge(tmp_path: Path, catalog: LocalCatalog, merges: Merges) -> None:  # fmt: skip
    """Rust `enforce_reject`: under warn, later rows repeating the merge key are rejected, never merged."""
    report = run(tmp_path, "id,name\n1,a\n1,b\n", mode="merge_scd1", severity="warn")
    assert rows(catalog) == [(1, "a")]
    file = report["files"][0]
    assert (file["status"], file["accepted_count"], file["rejected_count"]) == ("success", 1, 1)  # fmt: skip


def test_merge_ignore_columns_are_trimmed(tmp_path: Path, catalog: LocalCatalog, merges: Merges) -> None:  # fmt: skip
    run(tmp_path, "id,name\n1,a\n", mode="merge_scd1", partition='merge: {ignore_columns: [" name "]}')  # fmt: skip
    assert merges == [(["id"], {"name"})]


def test_a_merge_key_column_cannot_be_added_by_evolution(tmp_path: Path, catalog: LocalCatalog, merges: Merges) -> None:  # fmt: skip
    evolution = "schema_evolution: {mode: add_columns}"
    run(tmp_path, "id,name\n1,a\n", mode="merge_scd1", evolution=evolution)
    text = CONFIG.format(mode="merge_scd1", severity="reject", partition="", evolution=evolution, extra="- {name: qty, type: int32}")  # fmt: skip
    (tmp_path / "config.yml").write_text(text.replace("primary_key: [id]", "primary_key: [id, qty]"))  # fmt: skip
    (tmp_path / "in" / "orders.csv").write_text("id,name,qty\n1,b,5\n")
    root = parse_config(tmp_path / "config.yml")
    with pytest.raises(FloeRunError, match="iceberg schema evolution failed: merge key columns cannot be added: qty$"):  # fmt: skip
        run_entity(root.entities[0], tmp_path, "0.3", storages=root.storages, emit=NO_EVENTS, catalogs=root.catalogs)  # fmt: skip
    assert rows(catalog) == [(1, "a")]


def test_d28_partition_spec_on_the_derived_date(tmp_path: Path, catalog: LocalCatalog) -> None:  # fmt: skip
    (tmp_path / "in/2026/10/24/a1").mkdir(parents=True)
    (tmp_path / "in/2026/10/24/a1/orders.csv").write_text("id,name\n1,a\n")
    config(tmp_path, partition="partition_spec: [{column: ingest_date, transform: day}]")  # fmt: skip
    text = (tmp_path / "config.yml").read_text().replace('separator: ","}', 'separator: ","}\n      partitioning: {pattern: "{yyyy}/{MM}/{dd}/{batch}", date_column: ingest_date}')  # fmt: skip
    (tmp_path / "config.yml").write_text(text)
    root = parse_config(tmp_path / "config.yml")
    run_entity(root.entities[0], tmp_path, "0.3", storages=root.storages, emit=NO_EVENTS, catalogs=root.catalogs)  # fmt: skip
    table = catalog.tables["sales.orders"]
    assert [(f.name, str(f.transform)) for f in table.spec().fields] == [("ingest_date_day", "day")]  # fmt: skip
    assert rows(catalog) == [(1, "a", date(2026, 10, 24))]


def test_a_manifest_run_keeps_the_partition_spec(tmp_path: Path, catalog: LocalCatalog, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    """#911: `run --manifest` creates the table with the config's spec (D28 derived column, D12 bucket)."""
    (tmp_path / "in/2026/10/24/a1").mkdir(parents=True)
    (tmp_path / "in/2026/10/24/a1/orders.csv").write_text("id,name\n1,a\n")
    config(tmp_path, partition="partition_spec: [{column: ingest_date, transform: day}, {column: id, transform: bucket, num_buckets: 4}]")  # fmt: skip
    text = (tmp_path / "config.yml").read_text().replace('separator: ","}', 'separator: ","}\n      partitioning: {pattern: "{yyyy}/{MM}/{dd}/{batch}", date_column: ingest_date}')  # fmt: skip
    (tmp_path / "config.yml").write_text(text.replace("prefix: floe}", "prefix: floe, region: r}"))  # fmt: skip
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["floe", "manifest", "generate", "-c", "config.yml", "-o", "m.json"])  # fmt: skip
    assert main() == 0
    doc = json.loads((tmp_path / "m.json").read_text())
    spec = doc["entities"][0]["sinks"]["accepted"]["partition_spec"]
    assert spec[0].pop("num_buckets") is None  # the schema lets a manifest omit it
    schema = Path(__file__).parents[3] / "orchestrators/schemas/floe.manifest.schema.json"  # fmt: skip
    Draft202012Validator(json.loads(schema.read_text())).validate(doc)
    (tmp_path / "m.json").write_text(json.dumps(doc))
    floe.run_manifest(str(tmp_path / "m.json"))
    table = catalog.tables["sales.orders"]
    assert [(f.name, str(f.transform)) for f in table.spec().fields] == [("ingest_date_day", "day"), ("id_bucket", "bucket[4]")]  # fmt: skip
    assert rows(catalog) == [(1, "a", date(2026, 10, 24))]


def dead_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Callable[[str], dict[str, Any]], Path]:  # fmt: skip
    """#545: an append in `file` mode commits, dies before promote, and its claims expire; returns a runner and the state."""
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "orders.csv").write_text("id,name\n1,a\n2,b\n")
    config(tmp_path, mode="append")
    path = tmp_path / "config.yml"
    path.write_text(path.read_text().replace("    domain: Sales\n", "    domain: Sales\n    incremental_mode: file\n"))  # fmt: skip
    root = parse_config(path)

    def go(run_id: str) -> dict[str, Any]:
        return run_entity(root.entities[0], tmp_path, "0.3", storages=root.storages, emit=Emitter(None, run_id), catalogs=root.catalogs).report  # fmt: skip

    def killed(*_: object) -> None:
        raise FloeRunError("killed")

    promote = entity_module.promote
    monkeypatch.setattr(entity_module, "promote", killed)
    with pytest.raises(FloeRunError, match="killed"):
        go("dead")
    monkeypatch.setattr(entity_module, "promote", promote)
    (state,) = (tmp_path / "in").rglob("state.json")
    doc = json.loads(state.read_text())
    for claim in doc["claims"].values():
        claim["expires_at"] = "2000-01-01T00:00:00Z"
    state.write_text(json.dumps(doc))
    return go, state


def test_a_retry_skips_the_batch_a_dead_run_committed(tmp_path: Path, catalog: LocalCatalog, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    """#545 S3: the run died after its commit, before promote; once its claim expires, the retry reads nothing."""
    go, state = dead_run(tmp_path, monkeypatch)
    report = go("retry")
    assert rows(catalog) == [(1, "a"), (2, "b")]
    assert not (tmp_path / "out" / "rejected").exists()
    (file,) = report["files"]
    assert (file["status"], file["skip_reason"]) == ("skipped", "already_committed")
    assert list(json.loads(state.read_text())["files"]) == [file["input_file"]]


def test_a_retry_with_a_new_input_skips_only_the_dead_runs_batch(tmp_path: Path, catalog: LocalCatalog, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    """#545 S4: file B arrived before the retry; the dead run's batch is re-derived from its expired claims."""
    go, state = dead_run(tmp_path, monkeypatch)
    (tmp_path / "in" / "b.csv").write_text("id,name\n3,c\n")
    report = go("retry")
    assert rows(catalog) == [(1, "a"), (2, "b"), (3, "c")]
    assert not (tmp_path / "out" / "rejected").exists()
    got = [(Path(f["input_file"]).name, f["status"], f.get("skip_reason")) for f in report["files"]]  # fmt: skip
    assert got == [("orders.csv", "skipped", "already_committed"), ("b.csv", "success", None)]  # fmt: skip
    assert sorted(Path(u).name for u in json.loads(state.read_text())["files"]) == ["b.csv", "orders.csv"]  # fmt: skip


def test_find_committed_walks_back_to_the_claim(tmp_path: Path, catalog: LocalCatalog) -> None:  # fmt: skip
    run(tmp_path, "id,name\n1,a\n", mode="append")
    run(tmp_path, "id,name\n2,bb\n", mode="append")
    table = catalog.tables["sales.orders"]
    first, last = table.metadata.snapshots
    digest = str(first.summary and first.summary[INPUT_HASH])
    at = datetime.fromtimestamp(first.timestamp_ms / 1000, UTC)
    assert find_committed(table, digest) is True
    assert find_committed(table, digest, at) is True
    assert find_committed(table, digest, at + timedelta(days=1)) is False
    expired = Table(table.name(), table.metadata.model_copy(update={"snapshots": [last]}), table.metadata_location, table.io, catalog)  # pyright: ignore[reportArgumentType] -- duck-typed stand-in for a catalog  # fmt: skip
    # the history since the claim is gone
    assert find_committed(expired, "other", at) is None
    assert find_committed(expired, "other") is False
