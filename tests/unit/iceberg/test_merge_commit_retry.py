"""#922: DuckDB-Iceberg surfaces a commit conflict as a catchable error; the MERGE then runs again.

DuckDB talks to a minimal in-process REST catalog that keeps the real compare-and-swap (the commit
requirements are validated against the stored metadata) and can hold commits at a gate, so two
writers are both stale when they commit.
"""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs

import datetime as dt
import json
import re
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pytest
from pydantic import TypeAdapter
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadata, new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.table.update import (
    TableRequirement,
    TableUpdate,
    update_table_metadata,
)
from pyiceberg.types import (
    BooleanType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

import floe.iceberg.merge as merge_module
from floe.config.storage import CatalogDefinition
from floe.errors import FloeRunError
from floe.iceberg.catalog import open_catalog, open_duckdb
from floe.iceberg.merge import (
    MergeResult,
    merge_scd1,
    merge_scd2,
    run_merge,
    scd1_sql,
    stage_batch,
)

SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=True), NestedField(2, "v", StringType())
)
ARROW = pa.schema([pa.field("id", pa.int64(), nullable=False), ("v", pa.string())])
NOT_FOUND = {
    "error": {"message": "not found", "type": "NotFoundException", "code": 404}
}


class RestCatalog:
    """One table behind the Iceberg REST commit endpoints DuckDB and PyIceberg call."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.metadata: TableMetadata | None = None
        self.lock = threading.Lock()
        self.commits: list[int] = []  # the status of every commit request
        self.gate: threading.Barrier | None = (
            None  # holds the first commits until every writer arrives
        )
        self.reject = 0  # commits answered 409 before the next one goes through

    def table(self) -> dict[str, Any]:
        assert self.metadata is not None
        return {
            "metadata-location": self.metadata.location,
            "metadata": json.loads(self.metadata.model_dump_json()),
            "config": {},
        }

    def create(self, body: dict[str, Any]) -> dict[str, Any]:
        (self.root / "metadata").mkdir(parents=True)  # DuckDB writes its manifests here
        self.metadata = new_table_metadata(
            Schema.model_validate(body["schema"]),
            PartitionSpec(),
            UNSORTED_SORT_ORDER,
            f"file://{self.root}",
            {},
        )
        return self.table()

    def commit(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        if gate := self.gate:
            gate.wait(timeout=30)
            self.gate = None  # the retry is not held back
        with self.lock:
            assert self.metadata is not None
            try:
                if self.reject:
                    self.reject -= 1
                    raise CommitFailedException("rejected")
                for r in TypeAdapter(list[TableRequirement]).validate_python(body["requirements"]):  # fmt: skip
                    r.validate(self.metadata)
            except CommitFailedException as e:
                self.commits.append(409)
                return 409, {"error": {"message": str(e), "type": "CommitFailedException", "code": 409}}  # fmt: skip
            updates = TypeAdapter(list[TableUpdate]).validate_python(body["updates"])
            self.metadata = update_table_metadata(self.metadata, tuple(updates))
            self.commits.append(200)
            return 200, self.table()


@pytest.fixture
def server(tmp_path: Path) -> Iterator[tuple[RestCatalog, CatalogDefinition]]:
    catalog = RestCatalog(tmp_path / "warehouse")

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            pass

        def reply(self, status: int, body: object = None) -> None:
            data = b"" if body is None else json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def route(self, method: str) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            body: dict[str, Any] = json.loads(self.rfile.read(length)) if length else {}
            path = self.path.split("?")[0]
            if path == "/v1/config":
                self.reply(200, {"defaults": {}, "overrides": {}})
            elif path == "/v1/namespaces" and method == "POST":
                self.reply(200, {"namespace": body["namespace"], "properties": {}})
            elif re.fullmatch(r"/v1/namespaces/\w+", path):
                self.reply(
                    200, {"namespace": [path.rsplit("/", 1)[1]], "properties": {}}
                )
            elif re.fullmatch(r"/v1/namespaces/\w+/tables", path):
                self.reply(200, catalog.create(body))
            elif re.fullmatch(r"/v1/namespaces/\w+/tables/\w+", path):
                self.reply(*((200, catalog.table()) if catalog.metadata else (404, NOT_FOUND)))  # fmt: skip
            elif path == "/v1/transactions/commit":
                self.reply(*catalog.commit(body["table-changes"][0]))
            else:
                self.reply(404, NOT_FOUND)

        def do_GET(self) -> None:
            self.route("GET")

        def do_POST(self) -> None:
            self.route("POST")

        def do_HEAD(self) -> None:
            self.route("HEAD")

    srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    definition = CatalogDefinition(
        name="fake",
        uri=f"http://127.0.0.1:{srv.server_port}",
        credential=None,
        warehouse="wh",
        oauth2_server_uri=None,
        scope=None,
        warehouse_storage=None,
        warehouse_prefix=None,
    )
    yield catalog, definition
    srv.shutdown()


def no_backoff(attempt: int) -> None:
    pass


def reader(*rows: tuple[int, str]) -> pa.RecordBatchReader:  # pyright: ignore[reportUnknownParameterType] -- pyarrow ships no stubs
    batch = pa.RecordBatch.from_pylist([{"id": i, "v": v} for i, v in rows], ARROW)
    return pa.RecordBatchReader.from_batches(ARROW, [batch])


def new_table(definition: CatalogDefinition, schema: Schema = SCHEMA) -> Table:
    catalog = open_catalog(definition)
    catalog.create_namespace("ns")
    return catalog.create_table(("ns", "t"), schema)


def race(definition: CatalogDefinition, writers: list[Any]) -> list[Any]:
    """Each writer runs on its own table handle and DuckDB connection; the results, or the errors."""
    results: list[Any] = [None] * len(writers)

    def run(i: int) -> None:
        table = open_catalog(definition).load_table(("ns", "t"))
        try:
            with open_duckdb(definition) as con:
                results[i] = writers[i](con, table)
        except Exception as e:  # noqa: BLE001 -- the writer's error is the result under test
            results[i] = e

    threads = [threading.Thread(target=run, args=(i,)) for i in range(len(writers))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results


def rows(table: Table) -> list[dict[str, Any]]:
    table.refresh()
    return table.scan().to_arrow().to_pylist()


def test_duckdb_raises_the_commit_conflict_and_does_not_retry_it(
    server: tuple[RestCatalog, CatalogDefinition], tmp_path: Path
) -> None:
    catalog, definition = server
    table = new_table(definition)
    catalog.gate = threading.Barrier(2)
    count, merge = scd1_sql('"floe_catalog"."ns"."t"', ["id", "v"], ["id"], ())

    def writer(value: str) -> Any:
        def write(con: duckdb.DuckDBPyConnection, _: Table) -> None:
            (tmp_path / value).mkdir()
            files = stage_batch(tmp_path / value, reader((7, value)), table.spec(), table.schema())  # fmt: skip
            run_merge(con, count, merge, files)

        return write

    results = race(definition, [writer("x"), writer("y")])
    failed = [r for r in results if r is not None]
    assert len(failed) == 1
    assert isinstance(failed[0], duckdb.TransactionException)
    assert "Conflict_409" in str(failed[0])
    assert sorted(catalog.commits) == [
        200,
        409,
    ]  # one request each: DuckDB sent no second one
    assert len(rows(table)) == 1


def test_concurrent_scd1_merges_inserting_one_key_end_with_one_row(
    server: tuple[RestCatalog, CatalogDefinition],
) -> None:
    catalog, definition = server
    table = new_table(definition)
    catalog.gate = threading.Barrier(2)  # both writers are stale when they commit

    def writer(value: str) -> Any:
        def write(con: duckdb.DuckDBPyConnection, t: Table) -> MergeResult:
            return merge_scd1(con, t, reader((7, value)), ["id"])

        return write

    results = race(definition, [writer("x"), writer("y")])
    assert not [r for r in results if isinstance(r, Exception)], results
    # the loser's counts are recomputed on the fresh table: its insert became an update
    assert sorted((r.inserted_count, r.updated_count) for r in results) == [(0, 1), (1, 0)]  # fmt: skip
    assert len(rows(table)) == 1
    assert sorted(catalog.commits) == [200, 200, 409]


def test_five_conflicts_in_a_row_fail_the_merge(
    server: tuple[RestCatalog, CatalogDefinition], monkeypatch: pytest.MonkeyPatch
) -> None:
    catalog, definition = server
    table = new_table(definition)
    catalog.reject = 99
    monkeypatch.setattr(merge_module, "_backoff", no_backoff)
    with (
        open_duckdb(definition) as con,
        pytest.raises(FloeRunError, match="conflicted 5 times in a row"),
    ):
        merge_scd1(con, table, reader((7, "x")), ["id"])
    assert catalog.commits == [409] * 5
    assert rows(table) == []


def test_each_scd2_attempt_stamps_its_own_time(
    server: tuple[RestCatalog, CatalogDefinition], monkeypatch: pytest.MonkeyPatch
) -> None:
    catalog, definition = server
    system = ["is_current", "valid_from", "valid_to"]
    table = new_table(
        definition,
        Schema(
            *SCHEMA.fields,
            NestedField(3, system[0], BooleanType()),
            NestedField(4, system[1], TimestampType()),
            NestedField(5, system[2], TimestampType()),
        ),
    )
    catalog.reject = 99
    monkeypatch.setattr(merge_module, "_backoff", no_backoff)
    stamps: list[dt.datetime] = []
    scd2_sql = merge_module.scd2_sql

    def spy(*args: Any) -> Any:
        stamps.append(args[-1])
        return scd2_sql(*args)

    monkeypatch.setattr(merge_module, "scd2_sql", spy)
    with (
        open_duckdb(definition) as con,
        pytest.raises(FloeRunError, match="conflicted 5 times in a row"),
    ):
        merge_scd2(con, table, reader((7, "x")), ["id"], system_columns=system)
    assert (
        len(stamps) == 5
    )  # a retry reusing the first attempt's time could close a rival's newer version
