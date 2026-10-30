"""`floe.run.run` with `workers` > 1: entities overlap, shared targets never do, reports stay in config order, a failure stops new entities (#1117)."""

import json
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from floe.config.parse import parse_config
from floe.config.project import RootConfig
from floe.errors import FloeRunError
from floe.run import run
from floe.run.entity import EntityRun, run_entity
from floe.run.parallel import exclusive_keys, run_parallel

ENTITY = """\
  - name: {name}
    domain: d
    {extra}
    source:
      format: csv
      path: ./in/{name}.csv
      options: {{header: true, separator: ","}}
    sink:
      write_mode: {mode}
      accepted: {{format: parquet, path: ./out/accepted/{target}}}
      rejected: {{format: csv, path: ./out/rejected/{name}}}
    policy: {{severity: {severity}}}
    schema:
      columns:
        - {{name: id, type: int64}}
"""
SLEEP = 0.4


def setup(tmp_path: Path, names: list[str], **options: str) -> tuple[Path, RootConfig]:
    """`options` map an entity name to its `target` directory (default its own name)."""
    (tmp_path / "in").mkdir()
    text = 'version: "0.3"\ndomains:\n  - name: d\nentities:\n'
    for n in names:
        (tmp_path / "in" / f"{n}.csv").write_text("id\n1\n2\n")
        text += ENTITY.format(name=n, mode="overwrite", target=options.get(n, n), severity="reject", extra="")  # fmt: skip
    path = tmp_path / "config.yml"
    path.write_text(text + "report: {path: ./report}\n")
    return path, parse_config(path, {})


class Spy:
    """Events with the thread they came from; `run_entity` sleeps first, and fails the names in `fail`."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, delays: dict[str, float] | None = None, fail: dict[str, float] | None = None) -> None:  # fmt: skip
        self.events: list[dict[str, Any]] = []
        self.lock = threading.Lock()
        self.running = 0
        self.peak = 0
        delays, fail = delays or {}, fail or {}

        def slow(entity: Any, *args: Any) -> EntityRun:
            with self.lock:
                self.running += 1
                self.peak = max(self.peak, self.running)
            try:
                time.sleep(fail.get(entity.name, delays.get(entity.name, SLEEP)))
                if entity.name in fail:
                    raise FloeRunError(f"{entity.name} failed")
                return run_entity(entity, *args)
            finally:
                with self.lock:
                    self.running -= 1

        monkeypatch.setattr(sys.modules["floe.run"], "run_entity", slow)

    def __call__(self, event: dict[str, Any]) -> None:
        with self.lock:
            self.events.append(event)

    def window(self, name: str) -> tuple[int, int]:
        at = {
            e["event"]: e["ts_ms"] for e in self.events if e.get("name") == f"d.{name}"
        }
        return at["entity_started"], at["entity_finished"]

    def started(self) -> list[str]:
        return [e["name"] for e in self.events if e["event"] == "entity_started"]

    def finished(self) -> list[str]:
        return [e["name"] for e in self.events if e["event"] == "entity_finished"]


def overlap(spy: Spy, a: str, b: str) -> bool:
    (a0, a1), (b0, b1) = spy.window(a), spy.window(b)
    return a0 < b1 and b0 < a1


def test_entities_on_different_targets_overlap(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    path, root = setup(tmp_path, ["a", "b"])
    spy = Spy(monkeypatch)
    run(path, root, observer=spy, workers=2)
    assert overlap(spy, "a", "b")
    assert spy.peak == 2


def test_one_worker_runs_them_one_after_another(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    path, root = setup(tmp_path, ["a", "b"])
    spy = Spy(monkeypatch, {"a": 0.05, "b": 0.05})
    run(path, root, observer=spy)
    names = spy.started()
    assert spy.peak == 1 and spy.finished() == names
    assert [e["event"] for e in spy.events if "name" in e] == ["entity_started", "entity_finished"] * 2  # fmt: skip


def test_no_more_than_the_workers_run_at_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    path, root = setup(tmp_path, ["a", "b", "c", "d", "e"])
    spy = Spy(monkeypatch, dict.fromkeys("abcde", 0.1))
    run(path, root, observer=spy, workers=2)
    assert spy.peak == 2


def test_entities_with_the_same_accepted_path_never_overlap(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    path, root = setup(tmp_path, ["a", "b", "c"], b="a")
    spy = Spy(monkeypatch, dict.fromkeys("abc", 0.15))
    out = run(path, root, observer=spy, workers=3)
    assert not overlap(spy, "a", "b") and len(out.entity_reports) == 3


def test_reports_and_summary_stay_in_config_order(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    path, root = setup(tmp_path, ["a", "b", "c"])
    spy = Spy(monkeypatch, {"a": 0.4, "b": 0.2, "c": 0.01})
    out = run(path, root, observer=spy, workers=3)
    assert spy.finished() == ["d.c", "d.b", "d.a"]
    assert [r["entity"]["name"] for r in out.entity_reports] == ["d.a", "d.b", "d.c"]
    assert [e["name"] for e in out.summary["entities"]] == ["d.a", "d.b", "d.c"]
    summary = json.loads((Path(out.run_dir) / "run.summary.json").read_text())
    assert [e["name"] for e in summary["entities"]] == ["d.a", "d.b", "d.c"]
    assert all((Path(out.run_dir) / "d" / n / "run.json").exists() for n in "abc")


def test_a_failure_starts_no_new_entity_and_lets_the_running_ones_finish(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    path, root = setup(tmp_path, ["a", "b", "c", "d"])
    spy = Spy(monkeypatch, {"a": 0.3}, fail={"b": 0.05})
    with pytest.raises(FloeRunError, match="b failed"):
        run(path, root, observer=spy, workers=2)
    assert spy.started() == ["d.a", "d.b"]
    assert sorted(spy.finished()) == ["d.a", "d.b"]
    assert (tmp_path / "out/accepted/a").exists()  # `a` ran to the end


def test_the_first_error_in_config_order_is_raised(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    path, root = setup(tmp_path, ["a", "b"])
    spy = Spy(monkeypatch, fail={"a": 0.3, "b": 0.05})
    with pytest.raises(FloeRunError, match="a failed"):
        run(path, root, observer=spy, workers=2)


def test_a_failing_entity_releases_and_the_others_promote_their_claims(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    path, root = setup(tmp_path, ["a", "b"])
    root = root.model_copy(update={"entities": [e.model_copy(update={"incremental_mode": "file"}) for e in root.entities]})  # fmt: skip
    spy = Spy(monkeypatch, {"a": 0.3}, fail={"b": 0.05})
    with pytest.raises(FloeRunError, match="b failed"):
        run(path, root, observer=spy, workers=2)
    state = json.loads((tmp_path / "in/.floe/state/d/a/state.json").read_text())
    assert list(state["files"]) and not state.get("claims")


def test_a_policy_abort_fails_only_its_entity(tmp_path: Path) -> None:
    path, root = setup(tmp_path, ["a", "b"])
    (tmp_path / "in/a.csv").write_text("id\nx\n")
    entities = [e.model_copy(update={"policy": e.policy.model_copy(update={"severity": "abort"})}) if e.name == "a" else e for e in root.entities]  # fmt: skip
    out = run(path, root.model_copy(update={"entities": entities}), workers=2)
    assert [r["entity"]["name"] for r in out.entity_reports] == ["d.a", "d.b"]
    assert [r["results"]["errors_total"] > 0 for r in out.entity_reports] == [
        True,
        False,
    ]


def test_a_parallel_run_matches_the_sequential_one(tmp_path: Path) -> None:
    names = ["a", "b", "c", "d"]
    results: list[Any] = []
    for sub, workers in (("seq", 1), ("par", 4)):
        work = tmp_path / sub
        work.mkdir()
        path, root = setup(work, names)
        out = run(path, root, run_id="r", workers=workers)
        results.append([(r["entity"]["name"], r["results"], r["accepted_output"]["files_written"]) for r in out.entity_reports])  # fmt: skip
    assert results[0] == results[1]


def test_an_interrupt_in_a_worker_stops_new_entities_and_reaches_the_caller(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    path, root = setup(tmp_path, ["a", "b", "c"])
    spy = Spy(monkeypatch, dict.fromkeys("abc", 0.2))

    def interrupt(event: dict[str, Any]) -> None:
        spy(event)
        if event == {**event, "event": "entity_started", "name": "d.a"}:
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        run(path, root, observer=interrupt, workers=2)
    assert spy.started() == ["d.a", "d.b"] and spy.finished() == ["d.b"]


def test_an_observer_error_in_a_worker_does_not_stop_the_run(tmp_path: Path) -> None:
    path, root = setup(tmp_path, ["a", "b"])

    def broken(event: dict[str, Any]) -> None:
        raise RuntimeError("boom")

    assert run(path, root, observer=broken, workers=2).summary["run"]["exit_code"] == 0


def tasks(keys: list[set[str]], workers: int, fail: dict[int, float] | None = None) -> tuple[list[int], int, list[tuple[int, int]]]:  # fmt: skip
    """`run_parallel` over fake entities: the start order, the peak concurrency, and the (index, index) pairs that overlapped."""
    lock = threading.Lock()
    live: set[int] = set()
    overlaps: list[tuple[int, int]] = []
    order: list[int] = []
    peak = 0
    fail = fail or {}

    class Fake:
        def __init__(self, i: int) -> None:
            self.i = i

    def task(entity: Any) -> dict[str, Any]:
        nonlocal peak
        i = entity.i
        with lock:
            order.append(i)
            overlaps.extend((j, i) for j in live)
            live.add(i)
            peak = max(peak, len(live))
        time.sleep(fail.get(i, 0.05))
        with lock:
            live.discard(i)
        if i in fail:
            raise FloeRunError(f"{i} failed")
        return {"i": i}

    entities: list[Any] = [Fake(i) for i in range(len(keys))]
    try:
        result = run_parallel(entities, [frozenset(k) for k in keys], workers, task)
        assert [r["i"] for r in result] == list(range(len(keys)))
    except FloeRunError:
        pass
    return order, peak, overlaps


def test_a_blocked_entity_does_not_hold_back_the_ones_after_it() -> None:
    order, peak, overlaps = tasks([{"x"}, {"x"}, {"y"}], 2)
    assert order[:2] == [0, 2] and order[2] == 1 and peak == 2
    assert (0, 1) not in {tuple(sorted(p)) for p in overlaps}


def test_merge_entities_run_one_at_a_time() -> None:
    _, peak, overlaps = tasks(
        [{"merge"}, {"merge", "t1"}, {"merge", "t2"}, {"free"}], 4
    )
    assert all(3 in p for p in overlaps) and peak <= 2


def test_a_failure_stops_the_schedule() -> None:
    order, _, _ = tasks([set(), set(), {"x"}, {"x"}, set()], 2, fail={0: 0.01})
    assert 4 not in order and 3 not in order


def keys_of(tmp_path: Path, text: str) -> list[frozenset[str]]:
    (tmp_path / "config.yml").write_text(text)
    root = parse_config(tmp_path / "config.yml", {})
    return [
        exclusive_keys(e, tmp_path, root.storages, root.catalogs) for e in root.entities
    ]


def iceberg_entity(
    name: str, table: str, mode: str = "overwrite", catalog: str = "polaris"
) -> str:
    return f"""\
  - name: {name}
    domain: d
    source: {{format: csv, path: ./in/{name}, options: {{header: true, separator: ","}}}}
    sink:
      write_mode: {mode}
      accepted: {{format: iceberg, path: tables/{name}, iceberg: {{catalog: {catalog}, table: {table}}}}}
      rejected: {{format: csv, path: ./out/rejected/{name}}}
    policy: {{severity: reject}}
    schema:
      primary_key: [id]
      columns:
        - {{name: id, type: int64, nullable: false}}
"""


ICEBERG = """\
version: "0.3"
catalogs:
  default: polaris
  definitions:
    - {name: polaris, type: rest, uri: "http://polaris", warehouse: w}
domains: [{name: d}]
entities:
"""


def test_the_keys_of_two_entities_on_one_iceberg_table_overlap(tmp_path: Path) -> None:
    a, b, c = keys_of(tmp_path, ICEBERG + iceberg_entity("a", "t") + iceberg_entity("b", "t") + iceberg_entity("c", "other"))  # fmt: skip
    assert a & b and not a & c and not b & c
    assert "merge" not in a


def test_merge_entities_share_a_key(tmp_path: Path) -> None:
    a, b = keys_of(tmp_path, ICEBERG + iceberg_entity("a", "t1", "merge_scd1") + iceberg_entity("b", "t2", "merge_scd1"))  # fmt: skip
    assert a & b == {"merge"}


def test_entities_sharing_a_state_file_share_a_key(tmp_path: Path) -> None:
    state = "incremental_mode: file\n    state: {path: ./shared/state.json}"
    text = "".join(ENTITY.format(name=n, mode="overwrite", target=n, severity="reject", extra=state) for n in "ab")  # fmt: skip
    a, b = keys_of(tmp_path, 'version: "0.3"\ndomains: [{name: d}]\nentities:\n' + text)
    assert a & b and all(f"local://{tmp_path}/shared/state.json" in k for k in (a, b))


def test_an_entity_whose_target_cannot_be_resolved_has_no_keys(tmp_path: Path) -> None:
    text = ICEBERG + iceberg_entity("a", "t", catalog="nope")
    assert keys_of(tmp_path, text)[0] == frozenset()
