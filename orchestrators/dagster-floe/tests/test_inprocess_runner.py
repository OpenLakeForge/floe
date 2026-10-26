import json
import shutil
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from dagster import materialize

floe = pytest.importorskip("floe")
if not hasattr(floe, "run_manifest"):
    pytest.skip("needs the floe Python core", allow_module_level=True)

from floe_dagster.assets import build_floe_asset_defs  # noqa: E402 -- after the skip
from floe_dagster.events import parse_json_event_lines  # noqa: E402 -- after the skip
from floe_dagster.manifest import load_manifest  # noqa: E402 -- after the skip
from floe_dagster.runner import InProcessRunner, LocalRunner, RunResult  # noqa: E402 -- after the skip

EXAMPLE = Path(__file__).parents[3] / "example" / "standalone"
FLOE = f"{sys.executable} -m floe"


def _example(work: Path, extra_config: str = "") -> Path:
    shutil.copytree(EXAMPLE, work)
    config = work / "config.yml"
    config.write_text(config.read_text() + extra_config)
    cmd = [sys.executable, "-m", "floe", "manifest", "generate", "-c", "config.yml", "-o", "m.json"]
    subprocess.run(cmd, cwd=work, check=True, capture_output=True)
    return work


def _recording(cls: type[LocalRunner]) -> LocalRunner:
    class Recording(cls):
        results: list[RunResult] = []

        def run_floe_entity(self, *args, **kwargs) -> RunResult:
            result = super().run_floe_entity(*args, **kwargs)
            self.results.append(result)
            return result

    return Recording(FLOE)


def _materialize(cls: type[LocalRunner], work: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.chdir(work)
    runner = _recording(cls)
    assets, sources, _ = build_floe_asset_defs(manifest_path="m.json", runner=runner)
    res = materialize([*assets, *sources])
    [result] = runner.results
    events = parse_json_event_lines(result.stdout)
    finished = {k: v for k, v in events[-1].items() if k not in ("ts_ms", "schema", "level")}
    run = (result.exit_code, result.status, result.failure_reason, result.backend_metadata)
    metadata = {
        str(m.asset_key): {k: str(v.value).replace(res.run_id, "<run>") for k, v in m.metadata.items()}
        for m in res.asset_materializations_for_node("sales__orders")
    }
    finished = {k: str(v).replace(res.run_id, "<run>") for k, v in finished.items()}
    kinds = [e["event"] for e in events]
    return run, kinds, finished, metadata


def test_in_process_materialization_matches_the_local_runner(tmp_path, monkeypatch) -> None:
    local = _materialize(LocalRunner, _example(tmp_path / "w"), monkeypatch)
    shutil.rmtree(tmp_path / "w")
    in_process = _materialize(InProcessRunner, _example(tmp_path / "w"), monkeypatch)
    assert in_process == local
    assert local[1][-1] == "run_finished"


def test_a_floe_error_is_a_failed_run_result(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(_example(tmp_path / "w"))
    execution = load_manifest("m.json").execution
    kwargs = dict(config_uri="config.yml", run_id="r", entity="sales.orders", execution=execution, manifest_uri="nope.json")
    local = LocalRunner(FLOE).run_floe_entity(**kwargs)
    result = InProcessRunner(FLOE).run_floe_entity(**kwargs)
    assert (result.exit_code, local.exit_code) == (1, 1)
    assert result.stderr and result.stderr in local.stderr  # the CLI's message


def test_concurrent_runs_keep_their_own_events(tmp_path) -> None:
    results: dict[str, RunResult] = {}

    def run(name: str) -> None:
        work = _example(tmp_path / name)
        execution = load_manifest(str(work / "m.json")).execution
        results[name] = InProcessRunner("false").run_floe_entity(  # `false`: no subprocess fallback
            config_uri=str(work / "config.yml"), run_id=name, entity="sales.orders",
            execution=execution, manifest_uri=str(work / "m.json"),
        )

    threads = [threading.Thread(target=run, args=(name,)) for name in ("a", "b")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(results) == ["a", "b"]
    for name, result in results.items():
        assert result.exit_code == 0
        assert {e["run_id"] for e in parse_json_event_lines(result.stdout)} == {name}


def test_an_in_process_run_posts_the_same_lineage_as_the_subprocess(tmp_path, monkeypatch) -> None:
    bodies: list[dict] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            bodies.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    lineage = f'lineage: {{url: "http://127.0.0.1:{server.server_address[1]}", namespace: ns}}\n'
    monkeypatch.delenv("DAGSTER_RUN_ID", raising=False)

    def run(cls: type[LocalRunner], name: str) -> list[tuple]:
        work = _example(tmp_path / name, lineage)
        execution = load_manifest(str(work / "m.json")).execution
        result = cls(FLOE).run_floe_entity(
            config_uri=str(work / "config.yml"), run_id="r", entity="sales.orders",
            execution=execution, manifest_uri=str(work / "m.json"),
        )
        assert result.exit_code == 0
        events = [
            (b["eventType"], b["job"]["name"], [d["name"].replace(str(work), "<w>") for d in b.get("outputs", [])])
            for b in bodies
        ]
        bodies.clear()
        return events

    try:
        local = run(LocalRunner, "local")
        assert run(InProcessRunner, "in_process") == local
    finally:
        server.shutdown()
    assert [e[0] for e in local] == ["START", "START", "COMPLETE", "COMPLETE"]


def test_an_execution_without_the_entity_selector_runs_what_the_cli_runs(tmp_path) -> None:
    def run(cls: type[LocalRunner], name: str) -> list[str]:
        work = tmp_path / name
        shutil.copytree(EXAMPLE, work)
        config = work / "config.yml"
        config.write_text(config.read_text().replace("  - name: orders\n", "  - name: other\n    domain: sales\n    source: {format: csv, path: ./in/orders.csv, options: {header: true, separator: \",\"}}\n    sink: {accepted: {format: parquet, path: ./out/accepted/other}}\n    policy: {severity: warn}\n    schema: {columns: [{name: id, type: string}]}\n  - name: orders\n", 1))
        cmd = [sys.executable, "-m", "floe", "manifest", "generate", "-c", "config.yml", "-o", "m.json"]
        subprocess.run(cmd, cwd=work, check=True, capture_output=True)
        manifest = json.loads((work / "m.json").read_text())
        manifest["execution"]["per_entity_args"] = ["--quiet"]  # no `--entities`: the CLI runs every entity
        (work / "m.json").write_text(json.dumps(manifest))
        result = cls(FLOE).run_floe_entity(
            config_uri=str(work / "config.yml"), run_id="r", entity="sales.orders",
            execution=load_manifest(str(work / "m.json")).execution, manifest_uri=str(work / "m.json"),
        )
        return [e["name"] for e in parse_json_event_lines(result.stdout) if e["event"] == "entity_started"]

    local = run(LocalRunner, "local")
    assert sorted(local) == ["sales.orders", "sales.other"]
    assert run(InProcessRunner, "in_process") == local
