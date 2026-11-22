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
from floe.state import forget
from floe.storage.local import LocalClient

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

        def first_part_only(frames: Any, *_: Any) -> Any:
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
    report = p.run("retry")
    assert [f.get("skip_reason") for f in report["files"] if f["input_file"].endswith("new.csv")] == ["already_committed"]  # fmt: skip
    assert p.accepted(lake) == [1, 2, 100]
    assert p.rejected() == ["x"]


# #1203: a unique-key Parquet append retry skips the inputs whose marker part the dead run published, and replays the rest


def configure(
    p: Pipeline, severity: str = "reject", max_size: int | None = None
) -> None:
    """`primary_key: [id]`, with `severity` and an optional `max_size_per_file`."""
    text = PARQUET.format(mode="append").replace("    schema:\n", "    schema:\n      primary_key: [id]\n")  # fmt: skip
    text = text.replace("{severity: reject}", f"{{severity: {severity}}}")
    if max_size:
        text = text.replace("path: ./out/accepted}", f"path: ./out/accepted, options: {{max_size_per_file: {max_size}}}}}")  # fmt: skip
    (p.dir / "config.yml").write_text(text)


def keyed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, severity: str = "reject", max_size: int | None = None) -> Pipeline:  # fmt: skip
    """A unique-key Parquet append that already took `OLD`; from here on no accepted part is deleted or published twice."""
    copy, delete, published = LocalClient.copy, LocalClient.delete, set[str]()

    def copy_once(self: LocalClient, src: str, dst: str) -> None:
        if "/out/accepted/part-" in dst:
            assert dst not in published, f"{dst} published twice"
            published.add(dst)
        copy(self, src, dst)

    def keep_parts(self: LocalClient, uri: str) -> None:
        assert "/out/accepted/part-" not in uri, f"{uri} deleted"
        delete(self, uri)

    monkeypatch.setattr(LocalClient, "copy", copy_once)
    monkeypatch.setattr(LocalClient, "delete", keep_parts)
    p = Pipeline(tmp_path, "parquet", "append")
    configure(p, severity, max_size)
    (tmp_path / "in/old.csv").write_text(OLD)
    p.run("seed")
    return p


def dies_after_commit(
    p: Pipeline, monkeypatch: pytest.MonkeyPatch, run_id: str = "dead"
) -> None:
    with monkeypatch.context() as dying:
        arm(dying, "parquet", "after_commit")
        p.crash(run_id)
    p.expire_claims()


def rejected_by_file(p: Pipeline) -> dict[str, list[str]]:
    """The rejected row ids per input file; the raw copy of an aborted file has no source column and is left out."""
    out: dict[str, list[str]] = {}
    for path in (p.dir / "out/rejected").glob("*.csv"):
        df = pl.read_csv(path, infer_schema=False)
        if "__floe_source_uri" in df.columns:
            for uri, i in zip(df["__floe_source_uri"], df["id"], strict=True):
                out.setdefault(Path(uri).name, []).append(i)
    return {k: sorted(v) for k, v in out.items()}


def reasons(report: dict[str, Any]) -> dict[str, str | None]:
    return {Path(f["input_file"]).name: f.get("skip_reason") for f in report["files"]}


def parts(p: Pipeline) -> set[str]:
    return {f.name for f in (p.dir / "out/accepted").glob("part-*.parquet")}


def test_parquet_append_with_a_primary_key_skips_the_dead_runs_file_when_the_retry_holds_a_new_input(tmp_path: Path, lake: LocalCatalog, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    p = keyed(tmp_path, monkeypatch)
    (tmp_path / "in/new.csv").write_text(NEW)
    dies_after_commit(p, monkeypatch)
    (tmp_path / "in/zextra.csv").write_text(SUPERSET)
    report = p.run("retry")
    assert p.accepted(lake) == [1, 2, 7, 100]
    assert rejected_by_file(p) == {"new.csv": ["x"], "zextra.csv": ["y"]}
    assert reasons(report) == {"old.csv": "already_ingested", "new.csv": "already_committed", "zextra.csv": None}  # fmt: skip


def test_parquet_append_with_a_primary_key_keeps_the_parts_of_a_removed_input(tmp_path: Path, lake: LocalCatalog, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    p = keyed(tmp_path, monkeypatch)
    (tmp_path / "in/new.csv").write_text(NEW)
    dies_after_commit(p, monkeypatch)
    (tmp_path / "in/new.csv").unlink()
    (tmp_path / "in/zextra.csv").write_text("id,name\n1,dup\n7,q\n")
    p.run("retry")
    assert p.accepted(lake) == [
        1,
        2,
        7,
        100,
    ]  # the removed file's parts stay, and seed the unique check
    assert rejected_by_file(p) == {"new.csv": ["x"], "zextra.csv": ["1"]}


def test_parquet_append_with_a_primary_key_replays_a_replaced_input_against_its_old_parts(tmp_path: Path, lake: LocalCatalog, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    p = keyed(tmp_path, monkeypatch)
    (tmp_path / "in/new.csv").write_text(NEW)
    dies_after_commit(p, monkeypatch)
    (tmp_path / "in/new.csv").write_text("id,name\n2,b2\n3,c\n")  # same URI, new size
    report = p.run("retry")
    assert (
        reasons(report)["new.csv"] is None
    )  # read again: the marker names the old content
    assert p.accepted(lake) == [
        1,
        2,
        3,
        100,
    ]  # the old parts stay and seed: 2 is a duplicate
    assert rejected_by_file(p) == {"new.csv": ["2", "x"]}


def crash_mid_publish(p: Pipeline, monkeypatch: pytest.MonkeyPatch) -> None:
    """The dead run dies after publishing one of its two single-part files: `z.csv`, written last and published first (#1423)."""
    copy, copies = LocalClient.copy, list[str]()

    def second_copy_dies(self: LocalClient, src: str, dst: str) -> None:
        copies.append(dst)
        if len(copies) > 1:
            raise Crash
        copy(self, src, dst)

    with monkeypatch.context() as dying:
        for cleanup in ("release", "finish"):
            dying.setattr(Incremental, cleanup, dead(getattr(Incremental, cleanup)))
        dying.setattr(LocalClient, "copy", second_copy_dies)
        p.crash("dead")
    p.expire_claims()


@pytest.mark.parametrize(
    "bad", ["a_bad.csv", "p_bad.csv"]
)  # aborts before, or after, the retry reads `new.csv`
def test_parquet_append_with_a_primary_key_survives_a_retry_that_aborts(tmp_path: Path, lake: LocalCatalog, monkeypatch: pytest.MonkeyPatch, bad: str) -> None:  # fmt: skip
    p = keyed(tmp_path, monkeypatch, "abort")
    (tmp_path / "in/new.csv").write_text("id,name\n1,a\n2,b\n")
    (tmp_path / "in/z.csv").write_text("id,name\n7,q\n")
    crash_mid_publish(p, monkeypatch)
    assert p.accepted(lake) == [7, 100]
    (tmp_path / f"in/{bad}").write_text("id,name\nq,bad\n")
    report = p.run("aborts")
    assert reasons(report)["z.csv"] == "already_committed"
    assert p.accepted(lake) == ([7, 100] if bad == "a_bad.csv" else [1, 2, 7, 100])
    (tmp_path / f"in/{bad}").write_text("id,name\n8,r\n")
    p.run(
        "fixed"
    )  # `new.csv` kept its claim's name: `release` restored the dead run's claim
    assert p.accepted(lake) == [1, 2, 7, 8, 100]
    assert rejected_by_file(p) == {}
    state = p.state()
    assert (sorted(Path(u).name for u in state["files"]), state["claims"]) == (sorted([bad, "new.csv", "old.csv", "z.csv"]), {})  # fmt: skip


def test_parquet_append_with_a_primary_key_replays_the_file_the_dead_run_aborted_on(tmp_path: Path, lake: LocalCatalog, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    p = keyed(tmp_path, monkeypatch, "abort")
    (tmp_path / "in/a.csv").write_text("id,name\n1,a\n2,b\n")
    (tmp_path / "in/y.csv").write_text("id,name\n3,c\nq,bad\n")
    dies_after_commit(
        p, monkeypatch
    )  # the dead run committed `a.csv` and aborted on `y.csv`
    assert p.accepted(lake) == [1, 2, 100]
    report = p.run("again")
    assert reasons(report) == {"old.csv": "already_ingested", "a.csv": "already_committed", "y.csv": None}  # fmt: skip
    assert p.accepted(lake) == [
        1,
        2,
        100,
    ]  # `y.csv` has no marker: read again, it aborts again
    (tmp_path / "in/y.csv").write_text("id,name\n3,c\n4,d\n")
    p.run("fixed")
    assert p.accepted(lake) == [1, 2, 3, 4, 100]
    assert rejected_by_file(p) == {}


def test_parquet_append_with_a_primary_key_skips_a_multi_part_file_when_an_earlier_input_repeats_its_keys(tmp_path: Path, lake: LocalCatalog, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    p = keyed(tmp_path, monkeypatch, max_size=1)  # one row per part
    (tmp_path / "in/new.csv").write_text(NEW)
    before = parts(p)
    dies_after_commit(p, monkeypatch)
    assert len(parts(p) - before) == 2
    (tmp_path / "in/a_w.csv").write_text(
        "id,name\n1,w\n2,w\n"
    )  # read ahead of `new.csv`
    report = p.run("retry")
    assert reasons(report)["new.csv"] == "already_committed"
    assert p.accepted(lake) == [1, 2, 100]
    assert rejected_by_file(p) == {"a_w.csv": ["1", "2"], "new.csv": ["x"]}


def test_parquet_append_with_a_primary_key_skips_a_fully_rejected_file(tmp_path: Path, lake: LocalCatalog, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    p = keyed(tmp_path, monkeypatch)
    (tmp_path / "in/bad.csv").write_text("id,name\nx,bad\ny,bad2\n")
    before = parts(p)
    dies_after_commit(p, monkeypatch)
    (marker,) = parts(p) - before
    assert (
        pl.read_parquet(tmp_path / "out/accepted" / marker).height == 0
    )  # the file's empty marker part
    (tmp_path / "in/zextra.csv").write_text("id,name\n7,q\n")
    report = p.run("retry")
    assert reasons(report)["bad.csv"] == "already_committed"
    assert p.accepted(lake) == [7, 100]
    assert rejected_by_file(p) == {"bad.csv": ["x", "y"]}


def test_parquet_append_with_a_primary_key_skips_by_marker_after_max_size_per_file_changes(tmp_path: Path, lake: LocalCatalog, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    p = keyed(tmp_path, monkeypatch, max_size=1)
    (tmp_path / "in/new.csv").write_text(NEW)
    dies_after_commit(p, monkeypatch)
    configure(p)
    (tmp_path / "in/zextra.csv").write_text(SUPERSET)
    report = p.run("retry")
    assert reasons(report)["new.csv"] == "already_committed"
    assert p.accepted(lake) == [1, 2, 7, 100]
    assert rejected_by_file(p) == {"new.csv": ["x"], "zextra.csv": ["y"]}


def test_parquet_append_with_a_primary_key_converges_after_the_retry_dies_too(tmp_path: Path, lake: LocalCatalog, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    p = keyed(tmp_path, monkeypatch)
    (tmp_path / "in/new.csv").write_text(NEW)
    dies_after_commit(p, monkeypatch)
    (tmp_path / "in/zextra.csv").write_text(SUPERSET)
    dies_after_commit(p, monkeypatch, "retry")
    report = p.run("again")
    assert reasons(report) == {"old.csv": "already_ingested", "new.csv": "already_ingested", "zextra.csv": "already_committed"}  # fmt: skip
    assert p.accepted(lake) == [1, 2, 7, 100]
    assert rejected_by_file(p) == {"new.csv": ["x"], "zextra.csv": ["y"]}
    state = p.state()
    assert ({Path(u).name for u in state["files"]}, state["claims"]) == ({"old.csv", "new.csv", "zextra.csv"}, {})  # fmt: skip


def test_parquet_append_with_a_primary_key_reads_a_forgotten_file_again(tmp_path: Path, lake: LocalCatalog, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    p = keyed(tmp_path, monkeypatch)
    (tmp_path / "in/new.csv").write_text(NEW)
    p.run("first")
    (state_file,) = (tmp_path / "in").rglob("state.json")
    (uri,) = [u for u in p.state()["files"] if u.endswith("new.csv")]
    forget(LocalClient(), f"local://{state_file}", "orders", [uri])
    with (
        monkeypatch.context() as dying
    ):  # a dead run leaves an expired claim, so the guard looks
        arm(dying, "parquet", "first_write")
        p.crash("dead")
    p.expire_claims()
    before = parts(p)
    report = p.run("retry")
    assert (
        reasons(report)["new.csv"] is None
    )  # a fresh claim names a new marker: read, not skipped
    assert len(parts(p) - before) == 1
    assert p.accepted(lake) == [1, 2, 100]
    assert rejected_by_file(p) == {
        "new.csv": ["1", "2", "x"]
    }  # a re-ingest checks its keys against the committed rows
