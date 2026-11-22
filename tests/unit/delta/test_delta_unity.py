"""Registering a Delta table in Unity Catalog, against a local HTTP stub of the REST API."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
# ^ deltalake and pyarrow ship no type stubs

import json
import threading
from collections.abc import Iterator
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake
from pydantic import SecretStr

from floe.config.parse import parse_config
from floe.config.storage import CatalogDefinition, CatalogsConfig
from floe.delta.unity import register, spark_type
from floe.errors import FloeRunError
from floe.run.entity import run_entity


class Stub:
    """Just enough of the Unity REST API; `tables` maps a full name to the stored table."""

    def __init__(self) -> None:
        self.tables: dict[str, dict[str, Any]] = {}
        self.schemas: set[str] = set()
        self.calls: list[tuple[str, str, dict[str, Any] | None, str]] = []
        self.fail_post: int | None = None
        self.race: dict[str, Any] | None = (
            None  # a table a rival registers just before our POST
        )


@pytest.fixture
def stub() -> Iterator[tuple[Stub, str]]:
    state = Stub()

    class Handler(BaseHTTPRequestHandler):
        def reply(self, status: int, body: dict[str, Any]) -> None:
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def serve(self, method: str) -> None:
            size = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(size)) if size else None
            state.calls.append(
                (method, self.path, body, self.headers.get("Authorization", ""))
            )
            path = self.path.removeprefix("/api/2.1/unity-catalog")
            if method == "GET" and path.startswith("/tables/"):
                found = state.tables.get(path.removeprefix("/tables/"))
                return self.reply(200, found) if found else self.reply(404, {"error_code": "TABLE_DOES_NOT_EXIST"})  # fmt: skip
            if method == "GET":
                return self.reply(200 if path.removeprefix("/schemas/") in state.schemas else 404, {})  # fmt: skip
            if path == "/schemas":
                state.schemas.add(f"{body['catalog_name']}.{body['name']}")  # pyright: ignore[reportOptionalSubscript]
                return self.reply(200, {})
            assert body is not None
            full = f"{body['catalog_name']}.{body['schema_name']}.{body['name']}"
            if state.fail_post:
                return self.reply(state.fail_post, {"error_code": "PERMISSION_DENIED", "message": "no EXTERNAL USE LOCATION"})  # fmt: skip
            if state.race:
                state.tables[full], state.race = state.race, None
            if full in state.tables:
                return self.reply(409, {"error_code": "TABLE_ALREADY_EXISTS"})
            state.tables[full] = body
            self.reply(200, body)

        def do_GET(self) -> None:
            self.serve("GET")

        def do_POST(self) -> None:
            self.serve("POST")

        def log_message(self, format: str, *args: object) -> None: ...

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield state, f"http://127.0.0.1:{server.server_port}"
    server.shutdown()


def definition(host: str, schema_if_missing: bool = True) -> CatalogDefinition:
    return CatalogDefinition(name="uc", type="unity", host=host, catalog="main", schema="raw", token=SecretStr("${UC_TOKEN}"), create_schema_if_missing=schema_if_missing, warehouse_storage=None, warehouse_prefix=None)  # type: ignore[call-arg]  # pyright: ignore[reportCallIssue] -- `schema` is the alias of `schema_`  # fmt: skip


@pytest.fixture(autouse=True)
def token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UC_TOKEN", "s3cret")


def delta(tmp_path: Path) -> DeltaTable:
    write_deltalake(str(tmp_path / "t"), pa.table({"id": pa.array([1], pa.int64()), "name": ["a"], "price": pa.array([Decimal("1.50")], pa.decimal128(10, 2))}))  # fmt: skip
    return DeltaTable(str(tmp_path / "t"))


LOCATION = "s3://bucket/orders"


def test_a_new_table_is_created_with_the_exact_request(
    stub: tuple[Stub, str], tmp_path: Path
) -> None:
    state, host = stub
    register(definition(host), "sales", "orders", LOCATION, delta(tmp_path))
    posts = [
        (path, body, auth)
        for method, path, body, auth in state.calls
        if method == "POST"
    ]
    assert [p[0] for p in posts] == [
        "/api/2.1/unity-catalog/schemas",
        "/api/2.1/unity-catalog/tables",
    ]
    assert posts[0][1] == {"name": "sales", "catalog_name": "main"}
    assert posts[1][1] == {
        "name": "orders", "catalog_name": "main", "schema_name": "sales",
        "table_type": "EXTERNAL", "data_source_format": "DELTA", "storage_location": LOCATION, "properties": {},
        "columns": [
            {"name": "id", "type_text": "bigint", "type_name": "LONG", "type_json": '{"name": "id", "type": "long", "nullable": true, "metadata": {}}', "position": 0, "nullable": True},
            {"name": "name", "type_text": "string", "type_name": "STRING", "type_json": '{"name": "name", "type": "string", "nullable": true, "metadata": {}}', "position": 1, "nullable": True},
            {"name": "price", "type_text": "decimal(10,2)", "type_name": "DECIMAL", "type_json": '{"name": "price", "type": "decimal(10,2)", "nullable": true, "metadata": {}}', "position": 2, "nullable": True, "type_precision": 10, "type_scale": 2},
        ],
    }  # fmt: skip
    assert {auth for *_, auth in posts} == {"Bearer s3cret"}


def test_an_existing_table_at_the_same_location_is_left_alone(
    stub: tuple[Stub, str], tmp_path: Path
) -> None:
    state, host = stub
    state.tables["main.sales.orders"] = {"table_type": "EXTERNAL", "data_source_format": "DELTA", "storage_location": LOCATION + "/"}  # fmt: skip
    register(definition(host), "sales", "orders", LOCATION, delta(tmp_path))
    assert [m for m, *_ in state.calls] == ["GET"]


def test_an_existing_table_at_another_location_is_refused(
    stub: tuple[Stub, str], tmp_path: Path
) -> None:
    state, host = stub
    state.tables["main.sales.orders"] = {"table_type": "EXTERNAL", "data_source_format": "DELTA", "storage_location": "s3://bucket/other"}  # fmt: skip
    with pytest.raises(FloeRunError, match="s3://bucket/other") as err:
        register(definition(host), "sales", "orders", LOCATION, delta(tmp_path))
    assert err.value.code == "run_state_conflict"


def test_a_managed_table_is_refused(stub: tuple[Stub, str], tmp_path: Path) -> None:
    state, host = stub
    state.tables["main.sales.orders"] = {"table_type": "MANAGED", "data_source_format": "DELTA", "storage_location": LOCATION}  # fmt: skip
    with pytest.raises(FloeRunError, match="MANAGED DELTA") as err:
        register(definition(host), "sales", "orders", LOCATION, delta(tmp_path))
    assert err.value.code == "run_state_conflict"


def test_a_racing_registration_is_success(
    stub: tuple[Stub, str], tmp_path: Path
) -> None:
    state, host = stub
    state.race = {
        "table_type": "EXTERNAL",
        "data_source_format": "DELTA",
        "storage_location": LOCATION,
    }
    register(definition(host), "sales", "orders", LOCATION, delta(tmp_path))
    assert [m for m, *_ in state.calls].count("GET") >= 2  # the re-read after the 409


def test_a_missing_schema_stays_missing_without_create_schema_if_missing(stub: tuple[Stub, str], tmp_path: Path) -> None:  # fmt: skip
    state, host = stub
    register(
        definition(host, schema_if_missing=False),
        "sales",
        "orders",
        LOCATION,
        delta(tmp_path),
    )
    assert not state.schemas


def test_an_http_error_is_coded_and_never_names_the_token(
    stub: tuple[Stub, str], tmp_path: Path
) -> None:
    state, host = stub
    state.fail_post = 403
    with pytest.raises(FloeRunError, match="HTTP 403 .*PERMISSION_DENIED") as err:
        register(definition(host), "sales", "orders", LOCATION, delta(tmp_path))
    assert err.value.code == "run_catalog_error"
    assert "s3cret" not in str(err.value)


def test_an_unset_token_variable_is_refused(stub: tuple[Stub, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    _, host = stub
    monkeypatch.delenv("UC_TOKEN")
    with pytest.raises(FloeRunError, match="UC_TOKEN") as err:
        register(definition(host), "sales", "orders", LOCATION, delta(tmp_path))
    assert err.value.code == "run_invalid_setup"


@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        (pa.int8(), "tinyint"), (pa.int16(), "smallint"), (pa.int32(), "int"), (pa.int64(), "bigint"),
        (pa.float32(), "float"), (pa.float64(), "double"), (pa.bool_(), "boolean"), (pa.string(), "string"),
        (pa.large_string(), "string"), (pa.date32(), "date"), (pa.timestamp("us"), "timestamp_ntz"),
        (pa.timestamp("us", "UTC"), "timestamp"), (pa.decimal128(38, 4), "decimal(38,4)"),
    ],
)  # fmt: skip
def test_spark_types_of_every_floe_column_type(kind: Any, expected: str) -> None:
    assert spark_type(kind)[0] == expected


def test_a_type_without_a_spark_mapping_is_refused() -> None:
    with pytest.raises(FloeRunError) as err:
        spark_type(pa.list_(pa.int32()))
    assert err.value.code == "run_not_supported"


CONFIG = """\
version: "0.3"
catalogs:
  default: uc
  definitions:
    - {{name: uc, type: unity, host: "{host}", catalog: main, schema: raw, token: "${{UC_TOKEN}}"}}
domains:
  - name: d
entities:
  - name: orders
    domain: d
    source: {{format: csv, path: ./in, options: {{header: true, separator: ","}}}}
    sink:
      write_mode: append
      accepted: {{format: delta, path: ./out/accepted, delta: {{schema: sales, table: orders}}}}
      rejected: {{format: csv, path: ./out/rejected}}
    policy: {{severity: reject}}
    schema:
      columns:
        - {{name: id, type: int64}}
report: {{path: ./report}}
"""


def test_a_retry_that_finds_the_commit_still_registers(
    stub: tuple[Stub, str], tmp_path: Path
) -> None:
    """Run 1 commits but registration fails; run 2 reports `already_committed` and registers."""
    state, host = stub
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "orders.csv").write_text("id\n1\n")
    (tmp_path / "config.yml").write_text(CONFIG.format(host=host))
    config = parse_config(tmp_path / "config.yml")

    def run() -> dict[str, Any]:
        return run_entity(
            config.entities[0], tmp_path, "0.3", catalogs=config.catalogs
        ).report

    state.fail_post = 500
    with pytest.raises(FloeRunError):
        run()
    assert (
        DeltaTable(str(tmp_path / "out" / "accepted")).version() == 0
    )  # the commit stays
    state.fail_post = None
    report = run()
    assert report["files"][0]["status"] == "skipped"
    assert (
        DeltaTable(str(tmp_path / "out" / "accepted")).version() == 0
    )  # no second commit
    assert "main.sales.orders" in state.tables
    assert state.tables["main.sales.orders"]["storage_location"].endswith(
        "/out/accepted"
    )
    out = report["accepted_output"]
    assert (out["delta_catalog_name"], out["delta_schema"], out["delta_table"]) == (
        "uc",
        "sales",
        "orders",
    )
    assert (out["total_bytes_written"], out["avg_file_size_mb"], out["small_files_count"]) == (None, None, None)  # nothing was written: the registered fields do not make these 0  # fmt: skip
    assert isinstance(config.catalogs, CatalogsConfig)
