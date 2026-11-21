"""#1340: the retry guard of a Delta append (the `txn` action)."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
# ^ deltalake and pyarrow ship no type stubs

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from deltalake import CommitProperties, DeltaTable, write_deltalake

from floe import reset_entity_state
from floe.config.parse import parse_config
from floe.delta import write as write_module
from floe.delta.guard import committed, stamp
from floe.errors import FloeRunError
from floe.run import incremental as incremental_module
from floe.run.entity import run_entity
from floe.run.events import Emitter

CONFIG = """\
version: "0.3"
domains:
  - name: d
entities:
  - name: orders
    domain: d
    {mode}
    source: {{format: csv, path: ./in, options: {{header: true, separator: ","}}}}
    sink:
      write_mode: append
      accepted: {{format: delta, path: ./out/accepted}}
    policy: {{severity: reject}}
    schema:
      columns:
        - {{name: id, type: int64}}
        - {{name: name, type: string}}
report: {{path: ./report}}
"""


def project(tmp_path: Path, incremental: str = "") -> Any:
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "orders.csv").write_text("id,name\n1,a\n2,b\n")
    (tmp_path / "config.yml").write_text(CONFIG.format(mode=incremental))
    return parse_config(tmp_path / "config.yml").entities[0]


def ids(tmp_path: Path) -> list[int]:
    return sorted(DeltaTable(str(tmp_path / "out" / "accepted")).to_pyarrow_table()["id"].to_pylist())  # fmt: skip


def die_after_commit(entity: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, age_days: int = 0) -> None:  # fmt: skip
    """A run commits and dies before it promotes its claim; the claim is then expired and `age_days` old."""

    def killed(*_: object, **__: object) -> None:
        raise FloeRunError("killed", code="run_commit_failed")

    monkeypatch.setattr(incremental_module, "promote", killed)
    with pytest.raises(FloeRunError, match="killed"):
        run_entity(entity, tmp_path, "0.3", emit=Emitter(None, "dead"))
    monkeypatch.undo()
    (state,) = (tmp_path / "in").rglob("state.json")
    doc = json.loads(state.read_text())
    for claim in doc["claims"].values():
        claim["expires_at"] = "2000-01-01T00:00:00Z"
        claim["acquired_at"] = (datetime.now(UTC) - timedelta(days=age_days)).isoformat().replace("+00:00", "Z")  # fmt: skip
    state.write_text(json.dumps(doc))


def test_a_retry_after_a_commit_that_was_not_promoted_adds_no_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    """The run died after its commit, before promote; once its claim expires, the retry reads nothing."""
    entity = project(tmp_path, "incremental_mode: file")
    die_after_commit(entity, tmp_path, monkeypatch)
    (file,) = run_entity(entity, tmp_path, "0.3", emit=Emitter(None, "retry")).report["files"]  # fmt: skip
    assert (file["status"], file["skip_reason"]) == ("skipped", "already_committed")
    assert ids(tmp_path) == [1, 2]
    assert DeltaTable(str(tmp_path / "out" / "accepted")).version() == 0


def test_a_transaction_older_than_its_retention_is_unknown(tmp_path: Path) -> None:
    path = str(tmp_path / "t")
    write_deltalake(path, pa.table({"a": [1]}), configuration={"delta.setTransactionRetentionDuration": "interval 1 days"}, commit_properties=stamp("r", "h"))  # fmt: skip
    table = DeltaTable(path)
    since = datetime.now(UTC) - timedelta(days=2)
    assert committed(table, "h", since) is True
    assert committed(table, "other") is False
    assert committed(table, "other", datetime.now(UTC) - timedelta(hours=1)) is False
    assert committed(table, "other", since) is None


def test_an_abort_severity_append_is_refused_before_anything_is_claimed(tmp_path: Path) -> None:  # fmt: skip
    project(tmp_path, "incremental_mode: file")
    path = tmp_path / "config.yml"
    path.write_text(path.read_text().replace("severity: reject", "severity: abort"))
    entity = parse_config(path).entities[0]
    with pytest.raises(FloeRunError) as e:
        run_entity(entity, tmp_path, "0.3")
    assert str(e.value) == "entity.name=orders policy.severity=abort on a delta append is not supported yet: the retry guard cannot cover it"  # fmt: skip
    assert e.value.code == "run_not_supported"
    assert not list(tmp_path.rglob("state.json"))
    assert not (tmp_path / "out").exists()


def test_a_preflight_failure_of_an_append_is_a_coded_error(tmp_path: Path) -> None:
    project(tmp_path)
    run_entity(parse_config(tmp_path / "config.yml").entities[0], tmp_path, "0.3")
    first = tmp_path / "out" / "accepted" / "_delta_log" / "00000000000000000000.json"
    lines = [json.loads(line) for line in first.read_text().splitlines()]
    for line in lines:
        if "protocol" in line:
            line["protocol"] = {"minReaderVersion": 1, "minWriterVersion": 7, "writerFeatures": ["identityColumns"]}  # fmt: skip
    first.write_text("\n".join(json.dumps(line) for line in lines) + "\n")
    (tmp_path / "in" / "orders.csv").write_text("id,name\n3,c\n")
    with pytest.raises(FloeRunError, match="table feature delta-rs cannot write") as e:
        run_entity(parse_config(tmp_path / "config.yml").entities[0], tmp_path, "0.3")
    assert e.value.code == "run_not_supported"


@pytest.mark.parametrize("unit", ["microseconds", "nanoseconds", "milliseconds", "seconds", "minutes", "weeks", "second"])  # fmt: skip
def test_every_unit_delta_rs_accepts_is_read_as_a_retention(tmp_path: Path, unit: str) -> None:  # fmt: skip
    """A claim older than the retention is unknown (`None`), whatever the unit the table spells it in."""
    path = str(tmp_path / "t")
    write_deltalake(path, pa.table({"a": [1]}), configuration={"delta.setTransactionRetentionDuration": f"interval 1 {unit}"}, commit_properties=stamp("r", "h"))  # fmt: skip
    assert committed(DeltaTable(path), "other", datetime.now(UTC) - timedelta(days=30)) is None  # fmt: skip


def test_a_retention_in_microseconds_fails_the_retry_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    entity = project(tmp_path, "incremental_mode: file")
    path = str(tmp_path / "out" / "accepted")
    write_deltalake(path, pa.table({"id": pa.array([0], pa.int64()), "name": ["seed"]}), configuration={"delta.setTransactionRetentionDuration": "interval 86400000000 microseconds"})  # fmt: skip

    def unstamped(
        run_id: str, _hash: str | None
    ) -> CommitProperties:  # the dead run's `txn` is gone, as if expired
        return CommitProperties(custom_metadata={"floe.run_id": run_id})

    monkeypatch.setattr(write_module, "stamp", unstamped)
    die_after_commit(entity, tmp_path, monkeypatch, age_days=2)
    with pytest.raises(FloeRunError, match="cannot tell whether a run that died") as e:
        run_entity(entity, tmp_path, "0.3")
    assert e.value.code == "run_state_conflict"
    assert ids(tmp_path) == [0, 1, 2]


def test_an_unparseable_retention_fails_the_retry_closed(tmp_path: Path) -> None:
    path = str(tmp_path / "t")
    write_deltalake(path, pa.table({"a": [1]}), configuration={"delta.setTransactionRetentionDuration": "interval 2 years"})  # fmt: skip
    table, since = DeltaTable(path), datetime.now(UTC) - timedelta(hours=1)
    assert committed(table, "h", since) is None
    assert (
        committed(table, "h") is False
    )  # no claim to age: the whole-set modes ask without `since`


def test_a_reload_after_a_state_reset_appends_again(tmp_path: Path) -> None:
    """No claims are left to retry, so the same batch is loaded on purpose (docs/concepts/incremental.md)."""
    entity = project(tmp_path, "incremental_mode: file")
    run_entity(entity, tmp_path, "0.3")
    assert reset_entity_state(str(tmp_path / "config.yml"), "orders")
    (file,) = run_entity(entity, tmp_path, "0.3").report["files"]
    assert file["status"] == "success"
    assert ids(tmp_path) == [1, 1, 2, 2]


def test_a_commit_reports_its_own_version_and_carries_the_stamp(tmp_path: Path) -> None:  # fmt: skip
    entity = project(tmp_path)
    run_entity(entity, tmp_path, "0.3", emit=Emitter(None, "r1"))
    (tmp_path / "in" / "orders.csv").write_text(
        "id,name\n3,c\n"
    )  # a new batch: the same one is skipped
    report = run_entity(entity, tmp_path, "0.3", emit=Emitter(None, "r2")).report
    assert report["accepted_output"]["table_version"] == 1
    table = DeltaTable(str(tmp_path / "out" / "accepted"))
    last = table.history(1)[0]
    assert (last["floe.run_id"], len(last["floe.input_hash"])) == ("r2", 64)
    assert table.transaction_version(f"floe:{last['floe.input_hash']}") == 1


@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_an_entity_whose_rows_are_all_rejected_reports_normally(tmp_path: Path, mode: str) -> None:  # fmt: skip
    entity = project(tmp_path)
    run_entity(entity, tmp_path, "0.3", emit=Emitter(None, "first"))  # the table exists
    (tmp_path / "in" / "orders.csv").write_text("id,name\nx,a\n")
    path = tmp_path / "config.yml"
    text = path.read_text().replace("write_mode: append", f"write_mode: {mode}")
    path.write_text(text.replace("      accepted:", "      rejected: {format: csv, path: ./out/rejected}\n      accepted:"))  # fmt: skip
    report = run_entity(parse_config(path).entities[0], tmp_path, "0.3", emit=Emitter(None, "second")).report  # fmt: skip
    assert report["accepted_output"]["accepted_rows"] == 0
    assert report["accepted_output"]["files_written"] == 0


def test_an_unreadable_file_is_settled_with_its_batch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    """An unreadable input is rejected (its raw copy is written) and consumed like any rejected file, so the batch's txn may name it: the retry skips both."""
    project(tmp_path, "incremental_mode: file")
    path = tmp_path / "config.yml"
    path.write_text(path.read_text().replace("      accepted:", "      rejected: {format: csv, path: ./out/rejected}\n      accepted:"))  # fmt: skip
    entity = parse_config(path).entities[0]
    (tmp_path / "in" / "bad.csv").write_bytes(b'id,name\n\xff\xfe\x00,"unterminated\n')
    die_after_commit(entity, tmp_path, monkeypatch)
    files = run_entity(entity, tmp_path, "0.3", emit=Emitter(None, "retry")).report["files"]  # fmt: skip
    assert {f["skip_reason"] for f in files} == {"already_committed"} and len(
        files
    ) == 2
    assert ids(tmp_path) == [1, 2]
    assert [p.name for p in (tmp_path / "out" / "rejected").rglob("*.csv")] == ["bad.csv"]  # fmt: skip


def seed(tmp_path: Path) -> None:
    rows = pa.table({"id": pa.array([0], pa.int64()), "name": ["z"]})
    write_deltalake(str(tmp_path / "out" / "accepted"), rows, mode="append")


def test_a_rival_committing_the_batch_after_the_guard_looked_is_not_duplicated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    """Both files are settled and archived, the unread one too, and a follow-up run finds no input."""
    project(tmp_path)
    (tmp_path / "in" / "more.csv").write_text("id,name\n3,c\n")
    path = tmp_path / "config.yml"
    text = path.read_text().replace(
        "    source:", "    incremental_mode: archive\n    source:"
    )
    path.write_text(text.replace("      accepted:", "      archive: {path: ./archive}\n      accepted:"))  # fmt: skip
    entity = parse_config(path).entities[0]
    seed(tmp_path)
    rows = pa.table({"id": pa.array([1, 2, 3], pa.int64()), "name": ["a", "b", "c"]})
    real, calls = write_module.write_deltalake, [0]

    def racing(target: Any, data: Any, **kwargs: Any) -> None:
        calls[0] += 1
        if (
            calls[0] == 1
        ):  # a rival commits the same batch (same `txn`) after the guard looked
            real(str(tmp_path / "out" / "accepted"), rows, mode="append", commit_properties=kwargs["commit_properties"])  # fmt: skip
        real(target, data, **kwargs)

    monkeypatch.setattr(write_module, "write_deltalake", racing)
    report = run_entity(entity, tmp_path, "0.3", emit=Emitter(None, "mine")).report
    assert {f["skip_reason"] for f in report["files"]} == {"already_committed"}
    assert len(report["files"]) == 2
    assert report["accepted_output"]["table_version"] == 1
    assert ids(tmp_path) == [0, 1, 2, 3]
    assert sorted(p.name.split("__")[0] for p in (tmp_path / "archive").rglob("*.csv")) == ["more", "orders"]  # fmt: skip
    assert not list((tmp_path / "in").glob("*.csv"))
    monkeypatch.undo()
    with pytest.raises(FloeRunError):  # nothing is left to run
        run_entity(entity, tmp_path, "0.3", emit=Emitter(None, "again"))
    assert ids(tmp_path) == [0, 1, 2, 3]


def test_a_reload_after_a_reset_still_appends_when_no_rival_commits(tmp_path: Path) -> None:  # fmt: skip
    """The batch's own earlier `txn` is in the guarded snapshot: it is not a rival's."""
    entity = project(tmp_path, "incremental_mode: file")
    run_entity(entity, tmp_path, "0.3", emit=Emitter(None, "first"))
    assert reset_entity_state(str(tmp_path / "config.yml"), "orders")
    report = run_entity(entity, tmp_path, "0.3", emit=Emitter(None, "reload")).report
    assert report["files"][0]["status"] == "success"
    assert report["accepted_output"]["table_version"] == 1
    assert ids(tmp_path) == [1, 1, 2, 2]


def test_concurrent_runs_of_one_batch_commit_it_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    """`incremental_mode: none` has no claims: N runs pass the guard, then all write: one commit wins, the rest see it and add nothing (a barrier, not timing)."""
    entity = project(tmp_path)
    seed(tmp_path)
    n, real = 4, write_module.write_deltalake
    barrier, one_at_a_time = threading.Barrier(n), threading.Lock()

    def after_every_guard(*args: Any, **kwargs: Any) -> None:
        barrier.wait(timeout=60)  # every run has passed its guard on the same snapshot
        with one_at_a_time:  # a later run loads its table after the winner committed, unless it kept the guarded one
            real(*args, **kwargs)

    monkeypatch.setattr(write_module, "write_deltalake", after_every_guard)

    def go(i: int) -> Any:
        return run_entity(entity, tmp_path, "0.3", emit=Emitter(None, f"r{i}"))

    with ThreadPoolExecutor(n) as pool:
        runs = list(pool.map(go, range(n)))
    files = [f for r in runs for f in r.report["files"]]
    assert ids(tmp_path) == [0, 1, 2]
    assert [f["status"] for f in files].count("success") == 1
    assert [f.get("skip_reason") for f in files].count("already_committed") == n - 1
