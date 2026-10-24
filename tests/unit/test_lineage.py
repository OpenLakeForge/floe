"""OpenLineage observer: events, datasets, facets, auth and the circuit breaker (Rust: `tests/unit/run/lineage.rs`)."""

import json
import re
import sys
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest
from pydantic import SecretStr

from floe import lineage
from floe._cli import main
from floe.config.parse import parse_config
from floe.config.project import LineageConfig
from floe.config.types import EntityConfig
from floe.lineage import OpenLineageObserver, resolve_api_key

UUID4 = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}")  # fmt: skip


class Server:
    def __init__(self) -> None:
        self.requests: list[tuple[str, dict[str, str], dict[str, Any]]] = []
        self.statuses: list[int] = []
        server = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                body = self.rfile.read(int(self.headers["Content-Length"]))
                server.requests.append(
                    (self.path, dict(self.headers), json.loads(body))
                )
                self.send_response(server.statuses.pop(0) if server.statuses else 200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, format: str, *args: Any) -> None:
                pass

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"

    @property
    def bodies(self) -> list[dict[str, Any]]:
        return [b for _, _, b in self.requests]


@pytest.fixture
def server(monkeypatch: pytest.MonkeyPatch) -> Iterator[Server]:
    monkeypatch.setattr(lineage, "RETRY_DELAYS_S", (0.0, 0.0, 0.0))
    for var in ("OPENLINEAGE_API_KEY", "AIRFLOW_CTX_DAG_RUN_ID", "DAGSTER_RUN_ID"):
        monkeypatch.delenv(var, raising=False)
    s = Server()
    thread = threading.Thread(target=s.httpd.serve_forever, args=(0.01,), daemon=True)
    thread.start()
    yield s
    s.httpd.shutdown()
    s.httpd.server_close()


def config(url: str, max_failures: int | None = None, **kw: Any) -> LineageConfig:
    fields: dict[str, Any] = {
        **dict.fromkeys(("endpoint", "api_key", "dataset_namespace", "producer", "job_name")),
        "url": url, "timeout_secs": 2, "namespace": "test-ns", "max_failures": max_failures,
    }  # fmt: skip
    return LineageConfig(**(fields | kw))


ENTITY = """\
  - name: {name}
    {domain}
    source: {{format: csv, path: "{source}"}}
    sink:
      accepted: {{format: {fmt}, path: "{accepted}"{iceberg}}}
      {rejected}
    policy: {{severity: warn}}
    schema:
      columns: [{columns}]
"""


def entity(
    tmp_path: Path,
    source: str = "/data/in/",
    accepted: str = "/data/out/",
    rejected: str | None = None,
    *,
    name: str = "orders",
    domain: str | None = None,
    iceberg: str = "",
    columns: str = "",
) -> EntityConfig:
    text = ENTITY.format(
        name=name, source=source, accepted=accepted, columns=columns,
        domain=f"domain: {domain}" if domain else "",
        fmt="iceberg" if iceberg else "parquet",
        iceberg=f", iceberg: {iceberg}" if iceberg else "",
        rejected=f'rejected: {{format: csv, path: "{rejected}"}}' if rejected else "",
    )  # fmt: skip
    domains = f"domains:\n  - name: {domain}\n" if domain else ""
    path = tmp_path / f"{name}-{domain}.yml"
    path.write_text(
        f'version: "0.3"\n{domains}entities:\n{text}report: {{path: ./r}}\n'
    )
    return parse_config(path, {}).entities[0]


RUN_STARTED = {"event": "run_started", "run_id": "test-run-1", "ts_ms": 1_000_000}
ENTITY_STARTED = {"event": "entity_started", "run_id": "test-run-1", "name": "orders", "ts_ms": 1_001_000}  # fmt: skip


def finished(name: str = "orders", status: str = "success") -> dict[str, Any]:
    counts = {"files": 2, "rows": 100, "accepted": 90, "rejected": 10, "warnings": 1, "errors": 0}  # fmt: skip
    return {"event": "entity_finished", "run_id": "test-run-1", "name": name,
            "status": status, "ts_ms": 1_002_000, **counts}  # fmt: skip


def complete(server: Server, obs: OpenLineageObserver) -> dict[str, Any]:
    obs(ENTITY_STARTED)
    obs(finished())
    start, done = server.bodies
    assert (start["eventType"], done["eventType"]) == ("START", "COMPLETE")
    return done


def test_custom_endpoint_is_used_for_posts(server: Server) -> None:
    cfg = config(f"{server.url}/", endpoint="/api/v1/openlineage/lineage")
    OpenLineageObserver(cfg, [], "config.yml")(RUN_STARTED)
    assert [p for p, _, _ in server.requests] == ["/api/v1/openlineage/lineage"]


def test_default_endpoint(server: Server) -> None:
    OpenLineageObserver(config(server.url), [], "config.yml")(RUN_STARTED)
    assert [p for p, _, _ in server.requests] == ["/api/v1/lineage"]


def test_circuit_opens_after_threshold(
    server: Server, capsys: pytest.CaptureFixture[str]
) -> None:
    server.statuses = [500] * 10
    obs = OpenLineageObserver(config(server.url, 1), [], "config.yml")
    obs(RUN_STARTED)
    assert obs.circuit_open
    obs(ENTITY_STARTED)
    obs(ENTITY_STARTED)
    assert len(server.requests) == 3  # one event x three attempts, then nothing
    err = capsys.readouterr().err
    assert (
        f"warn: lineage: POST {server.url}/api/v1/lineage failed — event dropped" in err
    )
    assert err.count("disabled for this run after 1 consecutive failures") == 1


def test_success_resets_failure_counter(server: Server) -> None:
    obs = OpenLineageObserver(config(server.url, 3), [], "config.yml")
    obs(RUN_STARTED)
    obs(ENTITY_STARTED)
    assert len(server.requests) == 2
    assert not obs.circuit_open and obs.consecutive_failures == 0


def test_non_retryable_error_counts_without_retry(server: Server) -> None:
    server.statuses = [401]
    obs = OpenLineageObserver(config(server.url, 3), [], "config.yml")
    obs(RUN_STARTED)
    assert len(server.requests) == 1
    assert obs.consecutive_failures == 1 and not obs.circuit_open


def test_rate_limit_response_is_retried(server: Server) -> None:
    server.statuses = [429] * 3
    obs = OpenLineageObserver(config(server.url, 5), [], "config.yml")
    obs(RUN_STARTED)
    assert len(server.requests) == 3
    assert obs.consecutive_failures == 1


def test_connection_error_is_retried_and_never_raises(server: Server) -> None:
    obs = OpenLineageObserver(config("http://127.0.0.1:1"), [], "config.yml")
    obs(RUN_STARTED)
    assert obs.consecutive_failures == 1


def test_circuit_stays_closed_after_recovery(server: Server) -> None:
    server.statuses = [500] * 3
    obs = OpenLineageObserver(config(server.url, 3), [], "config.yml")
    obs(RUN_STARTED)
    assert obs.consecutive_failures == 1
    obs(ENTITY_STARTED)
    assert len(server.requests) == 4
    assert obs.consecutive_failures == 0 and not obs.circuit_open


def test_circuit_resets_on_new_run_started(server: Server) -> None:
    server.statuses = [500] * 3
    obs = OpenLineageObserver(config(server.url, 1), [], "config.yml")
    obs(RUN_STARTED)
    assert obs.circuit_open
    obs(RUN_STARTED | {"run_id": "test-run-2", "ts_ms": 2_000_000})
    assert not obs.circuit_open and obs.consecutive_failures == 0
    assert len(server.requests) == 4


def test_entity_complete_event_has_source_input_and_accepted_output(
    server: Server, tmp_path: Path
) -> None:
    obs = OpenLineageObserver(config(server.url), [entity(tmp_path)], "config.yml")
    done = complete(server, obs)
    assert done["inputs"] == [{"namespace": "file", "name": "/data/in/"}]
    assert [(o["namespace"], o["name"]) for o in done["outputs"]] == [("file", "/data/out/")]  # fmt: skip
    assert done["job"] == {"namespace": "test-ns", "name": "orders", "facets": {}}


def test_rejected_output_and_data_quality_metrics(
    server: Server, tmp_path: Path
) -> None:
    e = entity(tmp_path, rejected="/data/rejected/")
    done = complete(server, OpenLineageObserver(config(server.url), [e], "config.yml"))
    accepted, rejected = done["outputs"]
    assert accepted["facets"]["dataQualityMetrics"] | {"_producer": ""} == {
        "rowCount": 90, "validCount": 90, "invalidCount": 0, "_producer": "",
        "_schemaURL": lineage.DQ_SCHEMA,
    }  # fmt: skip
    assert rejected == {
        "namespace": "file",
        "name": "/data/rejected/",
        "facets": {"dataQualityMetrics": {
            "rowCount": 10, "validCount": 0, "invalidCount": 10,
            "_producer": lineage.DEFAULT_PRODUCER, "_schemaURL": lineage.DQ_SCHEMA,
        }},
    }  # fmt: skip


def test_accepted_dataset_has_schema_and_floe_quality_run_facets(
    server: Server, tmp_path: Path
) -> None:
    done = complete(server, OpenLineageObserver(config(server.url), [entity(tmp_path)], ""))  # fmt: skip
    facets = done["outputs"][0]["facets"]
    assert facets["schema"]["fields"] == []
    assert "columnLineage" not in facets  # no columns, no column lineage
    assert {k: v for k, v in facets["floeQualityRun"].items() if k[0] != "_"} == {
        "entity": "orders", "rejectionRate": 0.1, "files": 2, "rows": 100,
        "warnings": 1, "errors": 0,
    }  # fmt: skip


@pytest.mark.parametrize(
    ("source", "namespace", "name"),
    [
        ("s3://my-bucket/data/raw/", "s3://my-bucket", "data/raw/"),
        ("abfss://container@acct.dfs.core.windows.net/raw/", "abfss://container@acct.dfs.core.windows.net", "raw/"),
        ("abfs://container@acct.dfs.core.windows.net/raw/", "abfs://container@acct.dfs.core.windows.net", "raw/"),
    ],
)  # fmt: skip
def test_cloud_uris_split_into_namespace_and_name(
    server: Server, tmp_path: Path, source: str, namespace: str, name: str
) -> None:
    e = entity(tmp_path, source, "s3://my-bucket/data/out/")
    done = complete(server, OpenLineageObserver(config(server.url), [e], "config.yml"))
    assert done["inputs"] == [{"namespace": namespace, "name": name}]
    assert done["outputs"][0]["namespace"] == "s3://my-bucket"
    assert done["outputs"][0]["name"] == "data/out/"


def test_run_job_name_is_config_stem_or_job_name(server: Server) -> None:
    OpenLineageObserver(config(server.url), [], "pipelines/orders.yml")(RUN_STARTED)
    OpenLineageObserver(config(server.url, job_name="my-pipeline"), [], "orders.yml")(RUN_STARTED)  # fmt: skip
    OpenLineageObserver(config(server.url), [], "")(RUN_STARTED)
    names = [b["job"]["name"] for b in server.bodies]
    assert names == ["orders", "my-pipeline", "floe-run"]


def test_column_lineage_maps_to_source_field(server: Server, tmp_path: Path) -> None:
    cols = "{name: order_id, type: string}, {name: amount, type: float64, source: raw_amt}"  # fmt: skip
    e = entity(tmp_path, columns=cols)
    facets = complete(server, OpenLineageObserver(config(server.url), [e], ""))["outputs"][0]["facets"]  # fmt: skip
    assert facets["schema"]["fields"] == [
        {"name": "order_id", "type": "string"}, {"name": "amount", "type": "float64"},
    ]  # fmt: skip
    src = {"namespace": "file", "name": "/data/in/"}
    assert facets["columnLineage"]["fields"] == {
        "order_id": {"inputFields": [src | {"field": "order_id"}]},
        "amount": {"inputFields": [src | {"field": "raw_amt"}]},
    }


def test_run_and_entity_events_carry_uuid_run_ids(server: Server) -> None:
    obs = OpenLineageObserver(config(server.url), [], "config.yml")
    obs(RUN_STARTED)
    obs(ENTITY_STARTED)
    obs(finished(status="failed"))
    obs({"event": "run_finished", "run_id": "test-run-1", "status": "aborted", "ts_ms": 1_003_000})  # fmt: skip
    run_start, ent_start, ent_end, run_end = server.bodies
    assert UUID4.fullmatch(run_start["run"]["runId"])
    assert UUID4.fullmatch(ent_start["run"]["runId"])
    assert ent_start["run"]["runId"] != run_start["run"]["runId"]
    assert ent_end["run"]["runId"] == ent_start["run"]["runId"]
    assert run_end["run"]["runId"] == run_start["run"]["runId"]
    assert [b["eventType"] for b in server.bodies] == ["START", "START", "FAIL", "FAIL"]
    assert ent_end["inputs"] == [] and ent_end["outputs"] == []  # unknown entity
    assert run_start["eventTime"] == "1970-01-01T00:16:40Z"
    assert run_start["schemaURL"] == lineage.RUN_EVENT_SCHEMA
    assert run_start["producer"] == lineage.DEFAULT_PRODUCER
    assert run_start["run"]["facets"] == {}


def test_entity_finished_without_start_derives_run_id(server: Server) -> None:
    OpenLineageObserver(config(server.url), [], "")(finished())
    assert server.bodies[0]["run"]["runId"] == "test-run-1.entity.orders"


@pytest.mark.parametrize(("dataset_namespace", "expected"), [("iceberg.prod", "iceberg.prod"), (None, "test-ns")])  # fmt: skip
def test_iceberg_accepted_uses_dataset_namespace_and_table_name(
    server: Server, tmp_path: Path, dataset_namespace: str | None, expected: str
) -> None:
    e = entity(
        tmp_path, "s3://iceberg-data/bronze/sales/customers", "s3://warehouse/silver/customers",
        name="customers", iceberg="{namespace: sales_dev, table: customers}",
    )  # fmt: skip
    obs = OpenLineageObserver(config(server.url, dataset_namespace=dataset_namespace), [e], "")  # fmt: skip
    obs(finished("customers"))
    (done,) = server.bodies
    assert done["inputs"] == [{"namespace": "s3://iceberg-data", "name": "bronze/sales/customers"}]  # fmt: skip
    out = done["outputs"][0]
    assert (out["namespace"], out["name"]) == (expected, "sales_dev.customers")


def test_qualified_and_bare_entity_ids_resolve_their_own_datasets(
    server: Server, tmp_path: Path
) -> None:
    bare = entity(tmp_path)
    qualified = entity(tmp_path, "/data/sales/in/", "/data/sales/out/", domain="sales")
    obs = OpenLineageObserver(config(server.url), [bare, qualified], "config.yml")
    obs(finished("orders"))
    obs(finished("sales.orders"))
    assert [(b["job"]["name"], b["inputs"][0]["name"]) for b in server.bodies] == [
        ("orders", "/data/in/"), ("sales.orders", "/data/sales/in/"),
    ]  # fmt: skip


def test_parent_run_facet_from_airflow_then_dagster(
    server: Server, monkeypatch: pytest.MonkeyPatch
) -> None:
    obs = OpenLineageObserver(config(server.url, producer="p"), [], "")
    monkeypatch.setenv("DAGSTER_RUN_ID", "dg-1")
    monkeypatch.setenv("DAGSTER_JOB_NAME", "job")
    obs(RUN_STARTED)
    monkeypatch.setenv("AIRFLOW_CTX_DAG_RUN_ID", "af-1")
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "dag")
    monkeypatch.setenv("AIRFLOW_CTX_TASK_ID", "task")
    obs(ENTITY_STARTED)
    schema = "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json"
    assert [b["run"]["facets"]["parent"] for b in server.bodies] == [
        {"run": {"runId": "dg-1"}, "job": {"namespace": "test-ns", "name": "job"},
         "_producer": "p", "_schemaURL": schema},
        {"run": {"runId": "af-1"}, "job": {"namespace": "test-ns", "name": "dag.task"},
         "_producer": "p", "_schemaURL": schema},
    ]  # fmt: skip
    assert server.bodies[0]["producer"] == "p"


def test_api_key_resolution_over_the_wire(
    server: Server, monkeypatch: pytest.MonkeyPatch
) -> None:
    OpenLineageObserver(config(server.url), [], "")(RUN_STARTED)
    monkeypatch.setenv("OPENLINEAGE_API_KEY", "env-token")
    OpenLineageObserver(config(server.url), [], "")(RUN_STARTED)
    with_key = config(server.url, api_key=SecretStr("cfg-token"))
    OpenLineageObserver(with_key, [], "")(RUN_STARTED)
    auth = [h.get("Authorization") for _, h, _ in server.requests]
    assert auth == [None, "Bearer env-token", "Bearer cfg-token"]


@pytest.mark.parametrize(
    ("configured", "ambient", "expected"),
    [
        ("cfg-token", "env-token", "cfg-token"),
        (None, "env-token", "env-token"),
        ("{{OPENLINEAGE_API_KEY}}", "env-token", "env-token"),
        ("{{OPENLINEAGE_API_KEY}}", None, None),
        (None, None, None),
        ("   ", "   ", None),
    ],
)
def test_resolve_api_key(
    configured: str | None, ambient: str | None, expected: str | None
) -> None:
    assert resolve_api_key(configured, ambient) == expected


CLI_CONFIG = """\
version: "0.3"
lineage: {{url: "{url}", namespace: ns, max_failures: 1}}
entities:
  - name: orders
    source: {{format: csv, path: ./in/{file}, options: {{header: true, separator: ","}}}}
    sink:
      accepted: {{format: parquet, path: ./out}}
    policy: {{severity: warn}}
    schema:
      columns: [{{name: id, type: int64}}]
report: {{path: ./report}}
"""


def cli_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, url: str, file: str
) -> int:
    (tmp_path / "in").mkdir()
    (tmp_path / "in/orders.csv").write_text("id\n1\n")
    (tmp_path / "orders.yml").write_text(CLI_CONFIG.format(url=url, file=file))
    monkeypatch.setattr(sys, "argv", ["floe", "run", "-c", str(tmp_path / "orders.yml")])  # fmt: skip
    return main()


def test_cli_run_emits_the_run_lifecycle(
    server: Server, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert cli_run(tmp_path, monkeypatch, server.url, "orders.csv") == 0
    events = [(b["eventType"], b["job"]["name"]) for b in server.bodies]
    assert events == [("START", "orders"), ("START", "orders"), ("COMPLETE", "orders"), ("COMPLETE", "orders")]  # fmt: skip
    assert server.bodies[2]["outputs"][0]["facets"]["dataQualityMetrics"]["rowCount"] == 1  # fmt: skip


def test_cli_failed_run_emits_fail(
    server: Server, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert cli_run(tmp_path, monkeypatch, server.url, "missing.csv") == 1
    types = [b["eventType"] for b in server.bodies]
    assert types == ["START", "START", "FAIL", "FAIL"]


def test_lineage_endpoint_down_never_fails_the_run(
    server: Server, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert cli_run(tmp_path, monkeypatch, "http://127.0.0.1:1", "orders.csv") == 0
