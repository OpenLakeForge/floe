"""OpenLineage observer: transport, auth, redirects, deadline, circuit breaker and CLI wiring (Rust: `tests/unit/run/lineage.rs`)."""

import json
import re
import sys
import threading
import time
import urllib.request
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest
from pydantic import SecretStr

from floe._cli import main
from floe.config.project import LineageConfig
from floe.lineage import events, observer
from floe.lineage.observer import OpenLineageObserver

UUID4 = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}")  # fmt: skip


class Server:
    def __init__(self) -> None:
        self.requests: list[tuple[str, dict[str, str], dict[str, Any]]] = []
        self.statuses: list[int] = []
        self.redirect: str | None = None  # Location for POSTs to the default endpoint
        self.redirect_code = 302
        self.trickle = False  # dribble response headers that never end
        self.trickle_to: str | None = None  # dribble a 307 to here, ended by `release`
        self.release = threading.Event()  # ends every dribble
        self.hangups: list[str] = []  # dribbles cut short by the client
        server = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                server.requests.append((self.path, dict(self.headers), {}))
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def do_POST(self) -> None:
                body = self.rfile.read(int(self.headers["Content-Length"]))
                server.requests.append(
                    (self.path, dict(self.headers), json.loads(body))
                )
                if server.trickle_to:
                    location = f"Location: {server.trickle_to}\r\n".encode()
                    head = b"HTTP/1.1 307 Temporary Redirect\r\n" + location
                    self.dribble(head + b"Content-Length: 0\r\nX-Slow: ", b"\r\n\r\n")
                    return
                if server.trickle:
                    self.dribble(b"HTTP/1.1 200 OK\r\nX-Slow: ", b"")
                    return
                if server.redirect and self.path == "/api/v1/lineage":
                    self.send_response(server.redirect_code)
                    self.send_header("Location", server.redirect)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                self.send_response(server.statuses.pop(0) if server.statuses else 200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def dribble(self, head: bytes, tail: bytes) -> None:
                """`head`, a byte every 0.1 s (each beats the socket timeout) for up to 30 s or until released, then `tail`."""
                try:
                    self.wfile.write(head)
                    for _ in range(300):
                        if server.release.wait(0.1):
                            break
                        self.wfile.write(b"x")
                        self.wfile.flush()
                    self.wfile.write(tail)
                except OSError:
                    server.hangups.append(self.path)

            def log_message(self, format: str, *args: Any) -> None:
                pass

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"

    @property
    def bodies(self) -> list[dict[str, Any]]:
        return [b for _, _, b in self.requests]


def serve() -> Iterator[Server]:
    s = Server()
    thread = threading.Thread(target=s.httpd.serve_forever, args=(0.01,), daemon=True)
    thread.start()
    yield s
    s.release.set()
    s.httpd.shutdown()
    s.httpd.server_close()


@pytest.fixture
def server(monkeypatch: pytest.MonkeyPatch) -> Iterator[Server]:
    monkeypatch.setattr(observer, "RETRY_DELAYS_S", (0.0, 0.0, 0.0))
    for var in ("OPENLINEAGE_API_KEY", "AIRFLOW_CTX_DAG_RUN_ID", "DAGSTER_RUN_ID"):
        monkeypatch.delenv(var, raising=False)
    yield from serve()


@pytest.fixture
def other_server() -> Iterator[Server]:
    yield from serve()


def config(url: str, max_failures: int | None = None, **kw: Any) -> LineageConfig:
    fields: dict[str, Any] = {
        **dict.fromkeys(("endpoint", "api_key", "dataset_namespace", "producer", "job_name")),
        "url": url, "timeout_secs": 2, "namespace": "test-ns", "max_failures": max_failures,
    }  # fmt: skip
    return LineageConfig(**(fields | kw))


RUN_STARTED = {"event": "run_started", "run_id": "test-run-1", "ts_ms": 1_000_000}
ENTITY_STARTED = {"event": "entity_started", "run_id": "test-run-1", "name": "orders", "ts_ms": 1_001_000}  # fmt: skip


def finished(name: str = "orders", status: str = "success") -> dict[str, Any]:
    counts = {"files": 2, "rows": 100, "accepted": 90, "rejected": 10, "warnings": 1, "errors": 0}  # fmt: skip
    return {"event": "entity_finished", "run_id": "test-run-1", "name": name,
            "status": status, "ts_ms": 1_002_000, **counts}  # fmt: skip


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


def test_zero_max_failures_opens_the_circuit_on_the_first_failure(
    server: Server,
) -> None:
    server.statuses = [500] * 3
    obs = OpenLineageObserver(config(server.url, 0), [], "config.yml")
    obs(RUN_STARTED)
    assert obs.circuit_open


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
    assert run_start["schemaURL"] == events.RUN_EVENT_SCHEMA
    assert run_start["producer"] == events.DEFAULT_PRODUCER
    assert run_start["run"]["facets"] == {}


def test_entity_finished_without_start_derives_run_id(server: Server) -> None:
    OpenLineageObserver(config(server.url), [], "")(finished())
    assert server.bodies[0]["run"]["runId"] == "test-run-1.entity.orders"


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


def test_bearer_token_is_dropped_on_cross_origin_redirect_only(
    server: Server, other_server: Server
) -> None:
    cfg = config(server.url, api_key=SecretStr("cfg-token"))
    server.redirect = f"{other_server.url}/elsewhere"
    OpenLineageObserver(cfg, [], "")(RUN_STARTED)
    server.redirect = f"{server.url}/landed"
    OpenLineageObserver(cfg, [], "")(RUN_STARTED)
    assert [(p, h.get("Authorization")) for p, h, _ in other_server.requests] == [
        ("/elsewhere", None)
    ]
    assert [(p, h.get("Authorization")) for p, h, _ in server.requests][-1] == (
        "/landed",
        "Bearer cfg-token",
    )


@pytest.mark.parametrize("code", [307, 308])
def test_307_and_308_keep_the_post_and_drop_the_token_cross_origin(
    server: Server, other_server: Server, code: int
) -> None:
    cfg = config(server.url, api_key=SecretStr("cfg-token"))
    server.redirect_code = code
    server.redirect = f"{other_server.url}/elsewhere"
    OpenLineageObserver(cfg, [], "")(RUN_STARTED)
    server.redirect = f"{server.url}/landed"
    OpenLineageObserver(cfg, [], "")(RUN_STARTED)
    ((cross_path, cross_headers, cross_body),) = other_server.requests
    assert cross_path == "/elsewhere" and "Authorization" not in cross_headers
    assert cross_headers["Content-Type"] == "application/json"
    assert cross_body == server.bodies[0] and cross_body["eventType"] == "START"
    same_path, same_headers, same_body = server.requests[-1]
    assert same_path == "/landed" and same_body["eventType"] == "START"
    assert same_headers["Authorization"] == "Bearer cfg-token"


def test_api_key_with_a_line_break_is_never_sent_or_printed(
    server: Server, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    hooked: list[threading.ExceptHookArgs] = []
    monkeypatch.setattr(threading, "excepthook", hooked.append)
    obs = OpenLineageObserver(
        config(server.url, api_key=SecretStr("tok\nsecret-xyz")), [], ""
    )
    obs(RUN_STARTED)
    obs.fail_run()
    assert server.requests == [] and hooked == []
    err = capsys.readouterr().err
    assert "warn: lineage: api_key contains a line break" in err
    assert "secret-xyz" not in err


def test_a_worker_exception_never_reaches_the_thread_hook(
    server: Server, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def leak(*args: Any, **kwargs: Any) -> None:
        raise ValueError("Bearer secret-xyz is not a valid header")

    hooked: list[threading.ExceptHookArgs] = []
    monkeypatch.setattr(threading, "excepthook", hooked.append)
    monkeypatch.setattr(urllib.request.OpenerDirector, "open", leak)
    obs = OpenLineageObserver(
        config(server.url, api_key=SecretStr("secret-xyz")), [], ""
    )
    obs(RUN_STARTED)
    assert hooked == [] and obs.consecutive_failures == 1
    assert "secret-xyz" not in capsys.readouterr().err


def wait_for(condition: Callable[[], bool], secs: float = 10) -> bool:
    deadline = time.monotonic() + secs
    while not condition() and time.monotonic() < deadline:
        time.sleep(0.01)
    return condition()


def test_timeout_bounds_the_whole_request(
    server: Server, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(observer, "RETRY_DELAYS_S", (0.0, 0.0))
    server.trickle = True
    obs = OpenLineageObserver(config(server.url, 2, timeout_secs=1), [], "")
    started = time.monotonic()
    obs(RUN_STARTED)
    assert time.monotonic() - started < 20  # two 1 s deadlines, not two 30 s dribbles
    assert wait_for(lambda: len(server.hangups) == 2)  # both attempts were cut off
    assert len(server.requests) == 2  # a timeout is retried
    assert obs.consecutive_failures == 1


def test_an_expired_attempt_never_follows_its_redirect(
    server: Server, other_server: Server, monkeypatch: pytest.MonkeyPatch
) -> None:
    workers: list[threading.Thread] = []
    send = OpenLineageObserver._send  # pyright: ignore[reportPrivateUsage] -- spy on the worker threads

    def tracked(self: OpenLineageObserver, *args: Any) -> bool | None:
        workers.append(threading.current_thread())
        return send(self, *args)

    monkeypatch.setattr(OpenLineageObserver, "_send", tracked)
    server.trickle_to = f"{other_server.url}/elsewhere"
    obs = OpenLineageObserver(config(server.url, 1, timeout_secs=1), [], "")
    obs(RUN_STARTED)
    assert obs.circuit_open
    server.release.set()  # end every 307 now: an abandoned worker would follow it
    for worker in workers:
        worker.join(10)
    assert len(workers) == 3 and not any(w.is_alive() for w in workers)
    assert other_server.requests == []


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
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    url: str,
    file: str,
    template: str = CLI_CONFIG,
) -> int:
    (tmp_path / "in").mkdir()
    (tmp_path / "in/orders.csv").write_text("id\n1\n")
    (tmp_path / "orders.yml").write_text(template.format(url=url, file=file))
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
    assert types == ["START", "FAIL"]  # no entity starts on unresolved inputs (#864)


def test_cli_config_failing_validation_emits_start_then_fail(
    server: Server, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    columns = "      columns: [{{name: id, type: int64}}]\n"
    template = CLI_CONFIG.replace(columns, columns + "      primary_key: [missing]\n")
    assert cli_run(tmp_path, monkeypatch, server.url, "orders.csv", template) == 1
    assert [b["eventType"] for b in server.bodies] == ["START", "FAIL"]


def _lineage(server: Server) -> list[tuple[Any, ...]]:
    """Each event without its uuids and times."""

    def names(datasets: list[dict[str, Any]]) -> list[str]:
        return [d["name"] for d in datasets]

    events = [(b["eventType"], b["job"]["name"], names(b.get("inputs", [])), names(b.get("outputs", []))) for b in server.bodies]  # fmt: skip
    server.requests.clear()
    return events


@pytest.mark.parametrize(
    ("file", "code", "failing_observer"),
    [("orders.csv", 0, False), ("missing.csv", 1, False), ("orders.csv", 0, True)],
)
def test_api_run_manifest_posts_the_same_lineage_as_the_cli(
    file: str,
    code: int,
    failing_observer: bool,
    server: Server,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import floe

    def generate(work: Path) -> None:
        (work / "in").mkdir(parents=True)
        (work / "in/orders.csv").write_text("id\n1\n")
        (work / "orders.yml").write_text(CLI_CONFIG.format(url=server.url, file=file))
        monkeypatch.chdir(work)
        monkeypatch.setattr(sys, "argv", ["floe", "manifest", "generate", "-c", "orders.yml", "-o", "m.json"])  # fmt: skip
        assert main() == 0

    generate(tmp_path / "cli")
    monkeypatch.setattr(sys, "argv", ["floe", "run", "--manifest", "m.json"])
    assert main() == code
    by_cli = _lineage(server)
    generate(tmp_path / "api")

    def boom(event: floe.RunEvent) -> None:
        raise RuntimeError("user observer")

    # its error is swallowed, as before; lineage still gets every event
    if failing_observer:
        floe.set_observer(boom)
    try:
        assert floe.run_manifest("m.json").summary["run"]["exit_code"] == code
    except floe.FloeError:
        assert code == 1
    finally:
        floe.clear_observer()
    assert _lineage(server) == by_cli
    assert by_cli[0][0] == "START" and by_cli[-1][0] == (
        "COMPLETE" if code == 0 else "FAIL"
    )


@pytest.mark.parametrize(("file", "code"), [("orders.csv", 0), ("missing.csv", 1)])
def test_api_run_posts_the_same_lineage_as_the_cli(
    file: str,
    code: int,
    server: Server,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#989: in-process `floe.run` posts what `floe run -c` posts."""
    import floe

    assert cli_run(tmp_path, monkeypatch, server.url, file) == code
    by_cli = _lineage(server)
    try:
        assert (
            floe.run(str(tmp_path / "orders.yml")).summary["run"]["exit_code"] == code
        )
    except floe.FloeError:
        assert code == 1
    assert _lineage(server) == by_cli
    assert by_cli[0][0] == "START" and by_cli[-1][0] == (
        "COMPLETE" if code == 0 else "FAIL"
    )


def test_lineage_endpoint_down_never_fails_the_run(
    server: Server, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert cli_run(tmp_path, monkeypatch, "http://127.0.0.1:1", "orders.csv") == 0


def test_lineage_error_on_the_failure_path_does_not_mask_the_run_error(
    server: Server,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def boom(self: OpenLineageObserver) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(OpenLineageObserver, "fail_run", boom)
    assert cli_run(tmp_path, monkeypatch, server.url, "missing.csv") == 1
    err = capsys.readouterr().err
    assert "warn: lineage: boom" in err
    assert "Error: " in err


def test_a_cancelled_attempt_counts_as_failed_even_if_its_worker_reports_success(monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    """#886: a socket shut down mid-headers can read as a 200; a cancelled attempt must still fail."""
    reported = threading.Event()

    def send(self: OpenLineageObserver, data: bytes, timeout: float, attempt: Any) -> None:  # fmt: skip
        while not attempt.cancelled:
            time.sleep(0.01)
        reported.set()  # the worker's "200" lands right after the cancel

    cancel = observer._Attempt.cancel  # pyright: ignore[reportPrivateUsage] -- the race under test

    def cancel_then_report(self: Any) -> None:
        cancel(self)
        reported.wait(5)
        time.sleep(0.05)  # the worker appends its result before `_attempt` reads it

    monkeypatch.setattr(OpenLineageObserver, "_send", send)
    monkeypatch.setattr(observer._Attempt, "cancel", cancel_then_report)  # pyright: ignore[reportPrivateUsage]
    obs = OpenLineageObserver(config("http://127.0.0.1:9", timeout_secs=1), [], "config.yml")  # fmt: skip
    assert obs._attempt(b"{}") is True  # pyright: ignore[reportPrivateUsage] -- retryable failure, never a delivery
