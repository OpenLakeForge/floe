"""#546: kill an entity run at each failpoint, rerun the same entity from what is persisted, and compare rows and state.

The kill is a `Crash` (a `BaseException`, so no `except Exception` turns it into an ordinary failure) raised at an
existing seam. Like a dead process, it runs no cleanup: `Incremental.release` and `finish` only stop the heartbeat, so
the claims stay in `state.json`. The TTL is spent by rewriting the claims' `expires_at`, so nothing sleeps.
"""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportMissingTypeStubs=false
# pyright: reportPrivateUsage=false
# ^ pyarrow ships no type stubs; the private seams are the point: a crash is injected where the code is called from

import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parent))  # the Iceberg catalog stub's module  # fmt: skip
from test_run_iceberg import (  # pyright: ignore[reportMissingImports] -- a sibling test module, put on the path above
    LocalCatalog,
    catalog,  # noqa: F401  # pyright: ignore[reportUnusedImport] -- the fixture, used by name
    config,
    rows,
)

import floe.iceberg.sink as sink_module
import floe.write.outputs as outputs_module
from floe.config.parse import parse_config
from floe.iceberg import write as write_module
from floe.run import entity as entity_module
from floe.run import incremental as incremental_module
from floe.run import run as run_project
from floe.run.incremental import Incremental

run_module = sys.modules["floe.run"]  # `floe.run` is the public function

PARQUET = """\
version: "0.3"
entities:
  - name: orders
    incremental_mode: file
    source: {{format: csv, path: ./in, options: {{header: true, separator: ","}}}}
    sink:
      write_mode: {mode}
      accepted: {{format: parquet, path: ./out/accepted}}
      rejected: {{format: csv, path: ./out/rejected}}
    policy: {{severity: reject}}
    schema:
      columns:
        - {{name: id, type: int64}}
        - {{name: name, type: string}}
report: {{path: ./report}}
"""
OLD, NEW = (
    "id,name\n100,z\n",
    "id,name\n1,a\n2,b\nx,bad\n",
)  # NEW holds one rejected row
POINTS = ["first_write", "partial_flush", "after_commit", "before_promote", "after_promote", "after_report"]  # fmt: skip
COMMITTED = {"after_commit", "before_promote", "after_promote", "after_report"}  # the accepted rows are in the sink  # fmt: skip
PROMOTED = {"after_promote", "after_report"}  # the state holds the file, not a claim


@pytest.fixture
def lake(catalog: LocalCatalog) -> LocalCatalog:  # noqa: F811 -- the imported fixture
    return catalog


class Crash(BaseException):
    """The process dies here."""


def die(*_: object, **__: object) -> None:
    raise Crash


def after(real: Callable[..., Any]) -> Callable[..., Any]:
    def wrapped(*args: Any, **kwargs: Any) -> None:
        real(*args, **kwargs)
        raise Crash

    return wrapped


class Pipeline:
    """One Floe project: an `orders` entity writing to Parquet or to the local Iceberg catalog."""

    def __init__(self, tmp_path: Path, sink: str, mode: str) -> None:
        self.dir, self.sink = tmp_path, sink
        (tmp_path / "in").mkdir()
        if sink == "parquet":
            (tmp_path / "config.yml").write_text(PARQUET.format(mode=mode))
        else:
            config(tmp_path, mode=mode)
            text = (tmp_path / "config.yml").read_text()
            (tmp_path / "config.yml").write_text(text.replace("    domain: Sales\n", "    domain: Sales\n    incremental_mode: file\n"))  # fmt: skip

    def run(self, run_id: str) -> dict[str, Any]:
        path = self.dir / "config.yml"
        return run_project(path, parse_config(path), run_id=run_id).entity_reports[0]

    def crash(self, run_id: str) -> dict[str, Any]:
        """The run dies; returns the `run.failure.json` it left (best effort, a real kill leaves none)."""
        with pytest.raises(Crash):
            self.run(run_id)
        (path,) = (self.dir / "report").rglob(f"run_{run_id}/run.failure.json")
        return json.loads(path.read_text())["entities"][0]

    def accepted(self, lake: LocalCatalog) -> list[int]:
        if self.sink == "iceberg":
            return [r[0] for r in rows(lake)] if lake.tables else []
        return sorted(pl.read_parquet(self.dir / "out/accepted/part-*.parquet")["id"]) if list((self.dir / "out/accepted").glob("part-*")) else []  # fmt: skip

    def rejected(self) -> list[str]:
        return sorted(v for p in (self.dir / "out/rejected").glob("*.csv") for v in pl.read_csv(p)["id"].cast(pl.String))  # fmt: skip

    def state(self) -> dict[str, Any]:
        (path,) = (self.dir / "in").rglob("state.json")
        return json.loads(path.read_text())

    def expire_claims(self) -> None:
        (path,) = (self.dir / "in").rglob("state.json")
        doc = json.loads(path.read_text())
        for claim in doc["claims"].values():
            claim["expires_at"] = "2000-01-01T00:00:00Z"
        path.write_text(json.dumps(doc))


def dead(real: Callable[..., Any]) -> Callable[..., Any]:
    """`real`, except when called while a `Crash` unwinds: the dead process cleans up nothing, and only its heartbeat stops."""

    def wrapped(self: Incremental, *args: Any) -> Any:
        if isinstance(sys.exc_info()[1], Crash):
            return self._end()  # pyright: ignore[reportPrivateUsage]
        return real(self, *args)

    return wrapped


def partial_flush(monkeypatch: pytest.MonkeyPatch, iceberg: bool) -> None:
    """One row per part (or chunk); the second dies, the first is written."""
    if iceberg:
        chunks, data_files = write_module._chunks, write_module._dataframe_to_data_files
        calls: list[int] = []

        def one_row_chunks(reader: Any, _: int) -> Any:
            return chunks(reader, 1)

        def second_chunk_dies(*args: Any) -> Any:
            calls.append(1)
            if len(calls) > 1:
                raise Crash
            return data_files(*args)

        monkeypatch.setattr(write_module, "_chunks", one_row_chunks)
        monkeypatch.setattr(write_module, "_dataframe_to_data_files", second_chunk_dies)
    else:
        parts = outputs_module._part_frames

        def first_part_only(frames: Any, _: int) -> Any:
            yield next(parts(frames, 1))
            raise Crash

        monkeypatch.setattr(outputs_module, "_part_frames", first_part_only)


def arm(monkeypatch: pytest.MonkeyPatch, sink: str, point: str) -> None:
    """Kill the next run at `point`; the process takes no cleanup with it."""
    for cleanup in ("release", "finish"):
        monkeypatch.setattr(Incremental, cleanup, dead(getattr(Incremental, cleanup)))
    module, name = (sink_module, "write_iceberg") if sink == "iceberg" else (entity_module, "write_accepted")  # fmt: skip
    if point == "partial_flush":
        partial_flush(monkeypatch, sink == "iceberg")
    elif point == "first_write":
        monkeypatch.setattr(module, name, die)
    elif point == "after_commit":
        monkeypatch.setattr(module, name, after(getattr(module, name)))
    elif point == "before_promote":
        monkeypatch.setattr(incremental_module, "promote", die)
    elif point == "after_promote":
        monkeypatch.setattr(incremental_module, "promote", after(incremental_module.promote))  # fmt: skip
    else:
        assert point == "after_report"
        monkeypatch.setattr(run_module, "write_entity_report", after(run_module.write_entity_report))  # fmt: skip


def crashed(tmp_path: Path, lake: LocalCatalog, monkeypatch: pytest.MonkeyPatch, sink: str, mode: str, point: str) -> tuple[Pipeline, list[int]]:  # fmt: skip
    """An earlier run took `OLD`; the run on `NEW` dies at `point`. Returns the project and what the sink held before."""
    p = Pipeline(tmp_path, sink, mode)
    (tmp_path / "in/old.csv").write_text(OLD)
    p.run("seed")
    held = p.accepted(lake)
    assert held == [100]
    (tmp_path / "in/new.csv").write_text(NEW)
    with monkeypatch.context() as dying:  # the catalog stub stays
        arm(dying, sink, point)
        failure = p.crash("dead")
    # run.failure.json says what each sink had confirmed: the accepted one is committed only after its write returned
    assert failure["sinks"] == {"rejected": "committed", "accepted": "committed" if point in {"before_promote", "after_promote", "after_report"} else "unknown"}  # fmt: skip
    return p, held


def assert_died_there(
    p: Pipeline, lake: LocalCatalog, point: str, mode: str, held: list[int]
) -> None:
    """Rows and state as the dead run left them."""
    new = [1, 2] if mode == "overwrite" else sorted([*held, 1, 2])
    assert p.accepted(lake) == (new if point in COMMITTED else held)
    assert p.rejected() == [
        "x"
    ]  # the rejected part is written per file, before the accepted sink
    state = p.state()
    files = {Path(u).name for u in state["files"]}
    if point in PROMOTED:
        assert (files, state["claims"]) == ({"old.csv", "new.csv"}, {})
    else:
        assert files == {"old.csv"}
        assert {Path(u).name for u in state["claims"]} == {"new.csv"}


@pytest.mark.parametrize("point", POINTS)
@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_iceberg_converges_after_a_crash_at_any_point(tmp_path: Path, lake: LocalCatalog, monkeypatch: pytest.MonkeyPatch, mode: str, point: str) -> None:  # fmt: skip
    p, held = crashed(tmp_path, lake, monkeypatch, "iceberg", mode, point)
    assert_died_there(p, lake, point, mode, held)
    if point in {"first_write", "partial_flush"}:
        assert len(lake.tables["sales.orders"].metadata.snapshots) == 1  # the data file of a partial flush is an orphan  # fmt: skip
    p.expire_claims()
    report = p.run("retry")
    assert p.accepted(lake) == ([1, 2] if mode == "overwrite" else [1, 2, 100])
    assert p.rejected() == [
        "x"
    ]  # one copy: a retry rewrites the part it named from the input hash
    state = p.state()
    assert ({Path(u).name for u in state["files"]}, state["claims"]) == ({"old.csv", "new.csv"}, {})  # fmt: skip
    (file,) = [f for f in report["files"] if f["input_file"].endswith("new.csv")]
    expected = {
        # a promoted file is skipped from the state
        **dict.fromkeys(PROMOTED, "already_ingested"),
        # the append guard finds the batch in the table's snapshots; an overwrite is idempotent and runs again
        **dict.fromkeys(
            COMMITTED - PROMOTED, "already_committed" if mode == "append" else None
        ),
    }
    assert file.get("skip_reason") == expected.get(point)


@pytest.mark.parametrize(
    ("mode", "point"),
    [(m, pt) for m in ("append", "overwrite") for pt in POINTS if m == "overwrite" or pt not in COMMITTED - PROMOTED],
)  # fmt: skip
def test_parquet_converges_after_a_crash_except_an_append_between_the_write_and_the_promote(tmp_path: Path, lake: LocalCatalog, monkeypatch: pytest.MonkeyPatch, mode: str, point: str) -> None:  # fmt: skip
    """That append is the weaker-guarantees test below."""
    p, held = crashed(tmp_path, lake, monkeypatch, "parquet", mode, point)
    assert_died_there(p, lake, point, mode, held)
    p.expire_claims()
    p.run("retry")
    assert p.accepted(lake) == ([1, 2] if mode == "overwrite" else [1, 2, 100])
    assert p.rejected() == ["x"]
    state = p.state()
    assert ({Path(u).name for u in state["files"]}, state["claims"]) == ({"old.csv", "new.csv"}, {})  # fmt: skip


@pytest.mark.parametrize("point", sorted(COMMITTED - PROMOTED))
def test_parquet_append_has_weaker_guarantees_a_retry_after_the_write_and_before_promote_duplicates_the_rows(tmp_path: Path, lake: LocalCatalog, monkeypatch: pytest.MonkeyPatch, point: str) -> None:  # fmt: skip
    """Parquet parts have no snapshot to carry `floe.input_hash`, so the guard (#545) cannot see the dead run's rows: at-least-once."""
    p, held = crashed(tmp_path, lake, monkeypatch, "parquet", "append", point)
    assert_died_there(p, lake, point, "append", held)
    p.expire_claims()
    p.run("retry")
    assert p.accepted(lake) == [1, 1, 2, 2, 100]
    state = p.state()
    assert ({Path(u).name for u in state["files"]}, state["claims"]) == ({"old.csv", "new.csv"}, {})  # fmt: skip


@pytest.mark.parametrize("sink", ["parquet", "iceberg"])
def test_a_retry_inside_the_claim_ttl_runs_nothing(tmp_path: Path, lake: LocalCatalog, monkeypatch: pytest.MonkeyPatch, sink: str) -> None:  # fmt: skip
    """The claims of the dead run are live for an hour (incremental-state-cas.md): its files are skipped, nothing is written."""
    p, held = crashed(tmp_path, lake, monkeypatch, sink, "append", "before_promote")
    before = p.state()
    report = p.run("early")
    assert [f.get("skip_reason") for f in report["files"] if f["input_file"].endswith("new.csv")] == ["active_claim"]  # fmt: skip
    assert p.accepted(lake) == sorted([*held, 1, 2])
    assert p.state()["claims"] == before["claims"]  # the dead run's claims still stand


SUPERSET = "id,name\n7,q\ny,bad2\n"  # arrives after the crash: the retry's input set is no longer the dead run's


@pytest.mark.xfail(
    strict=True,
    reason="#1197: the rejected part is named from the input hash, so a retry with a new input writes the dead run's rejected rows again",
)
@pytest.mark.parametrize("sink", ["parquet", "iceberg"])
def test_a_retry_with_a_new_input_does_not_duplicate_the_dead_runs_rejected_rows(tmp_path: Path, lake: LocalCatalog, monkeypatch: pytest.MonkeyPatch, sink: str) -> None:  # fmt: skip
    p, _ = crashed(tmp_path, lake, monkeypatch, sink, "append", "first_write")
    (tmp_path / "in/zextra.csv").write_text(SUPERSET)
    p.expire_claims()
    p.run("retry")
    assert p.accepted(lake) == [
        1,
        2,
        7,
        100,
    ]  # the accepted rows are right either way
    assert p.rejected() == ["x", "y"]


@pytest.mark.xfail(
    strict=True,
    reason="#1198: Parquet append seeds the unique keys from the dead run's own parts, so the retry rejects them as duplicates",
)
def test_parquet_append_with_a_primary_key_does_not_reject_the_dead_runs_rows_on_retry(tmp_path: Path, lake: LocalCatalog, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    p = Pipeline(tmp_path, "parquet", "append")
    config_file = tmp_path / "config.yml"
    config_file.write_text(config_file.read_text().replace("    schema:\n", "    schema:\n      primary_key: [id]\n"))  # fmt: skip
    (tmp_path / "in/old.csv").write_text(OLD)
    p.run("seed")
    (tmp_path / "in/new.csv").write_text(NEW)
    with monkeypatch.context() as dying:
        arm(dying, "parquet", "after_commit")
        p.crash("dead")
    p.expire_claims()
    p.run("retry")
    assert p.accepted(lake) == [1, 2, 100]  # no duplicate: the unique check holds
    assert p.rejected() == ["x"]  # but the retry also rejected 1 and 2
