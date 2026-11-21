"""#1308: the retry guard of a Delta append (the `txn` action)."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
# ^ deltalake and pyarrow ship no type stubs

import json
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from floe.config.parse import parse_config
from floe.delta import write as write_module
from floe.delta.guard import committed, stamp
from floe.delta.write import write_table
from floe.errors import FloeRunError
from floe.run import incremental as incremental_module
from floe.run.entity import run_entity
from floe.run.events import Emitter
from floe.storage.inputs import NoInputsError

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


def test_a_retry_after_a_commit_that_was_not_promoted_adds_no_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    """P2: the run died after its commit, before promote; once its claim expires, the retry reads nothing."""
    entity = project(tmp_path, "incremental_mode: file")

    def go(run_id: str) -> dict[str, Any]:
        return run_entity(entity, tmp_path, "0.3", emit=Emitter(None, run_id)).report

    def killed(*_: object, **__: object) -> None:
        raise FloeRunError("killed", code="run_commit_failed")

    monkeypatch.setattr(incremental_module, "promote", killed)
    with pytest.raises(FloeRunError, match="killed"):
        go("dead")
    monkeypatch.undo()
    (state,) = (tmp_path / "in").rglob("state.json")
    doc = json.loads(state.read_text())
    for claim in doc["claims"].values():
        claim["expires_at"] = "2000-01-01T00:00:00Z"
    state.write_text(json.dumps(doc))
    (file,) = go("retry")["files"]
    assert (file["status"], file["skip_reason"]) == ("skipped", "already_committed")
    assert ids(tmp_path) == [1, 2]
    assert DeltaTable(str(tmp_path / "out" / "accepted")).version() == 0


def test_threads_appending_the_same_batch_commit_once(tmp_path: Path) -> None:
    entity = project(tmp_path)
    # ponytail: two runs creating the table race past the guard (delta-rs sees no `txn` on a table that has no state yet), so it exists first
    write_deltalake(str(tmp_path / "out" / "accepted"), pa.table({"id": pa.array([0], pa.int64()), "name": ["seed"]}))  # fmt: skip
    reports: list[dict[str, Any]] = []

    def go() -> None:
        reports.append(run_entity(entity, tmp_path, "0.3").report)

    threads = [threading.Thread(target=go) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(reports) == 6
    assert ids(tmp_path) == [0, 1, 2]
    assert DeltaTable(str(tmp_path / "out" / "accepted")).version() == 1
    skips = [f["skip_reason"] for r in reports for f in r["files"] if f["status"] == "skipped"]  # fmt: skip
    assert skips == ["already_committed"] * 5


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


def test_a_competing_commit_after_the_table_was_loaded_is_not_duplicated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    """The commit is based on the loaded snapshot, so a same-batch commit that lands after it conflicts (#1308)."""
    path = str(tmp_path / "t")
    batch = pa.table({"id": pa.array([1], pa.int64())})
    write_deltalake(path, pa.table({"id": pa.array([0], pa.int64())}))
    real, rivals = write_module.committed, []

    def rival(*args: Any) -> bool | None:
        found = real(
            *args
        )  # the guard looks, then a rival commits the same batch (once)
        if not rivals:
            rivals.append(1)
            write_deltalake(
                path, batch, mode="append", commit_properties=stamp("rival", "h")
            )
        return found

    monkeypatch.setattr(write_module, "committed", rival)
    reader = pa.RecordBatchReader.from_batches(batch.schema, batch.to_batches())
    written, version, raced = write_table("w", path, {}, reader, "append", "mine", "h")
    assert (written.files_written, version, raced) == (0, 1, True)
    assert sorted(DeltaTable(path).to_pyarrow_table()["id"].to_pylist()) == [0, 1]


def test_a_commit_already_in_the_loaded_snapshot_is_not_repeated(
    tmp_path: Path,
) -> None:
    path = str(tmp_path / "t")
    batch = pa.table({"id": pa.array([1], pa.int64())})
    write_deltalake(path, batch, commit_properties=stamp("rival", "h"))
    reader = pa.RecordBatchReader.from_batches(batch.schema, batch.to_batches())
    assert write_table("w", path, {}, reader, "append", "mine", "h")[1:] == (0, True)
    assert DeltaTable(path).version() == 0


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


@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_an_entity_whose_rows_are_all_rejected_reports_normally(
    tmp_path: Path, mode: str
) -> None:
    project(tmp_path)
    (tmp_path / "in" / "orders.csv").write_text("id,name\nx,a\n")
    path = tmp_path / "config.yml"
    text = path.read_text().replace("write_mode: append", f"write_mode: {mode}")
    path.write_text(text.replace("      accepted:", "      rejected: {format: csv, path: ./out/rejected}\n      accepted:"))  # fmt: skip
    report = run_entity(parse_config(path).entities[0], tmp_path, "0.3").report
    assert report["accepted_output"]["accepted_rows"] == 0
    assert report["accepted_output"]["files_written"] == 0


def test_a_raced_batch_archives_every_input_including_the_unread(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    """The rival committed this batch after the guard looked: no pending file may stay behind to be appended again."""
    project(tmp_path, "incremental_mode: archive")
    (tmp_path / "in" / "b.csv").write_text("id,name\n3,c\n")
    path = tmp_path / "config.yml"
    path.write_text(path.read_text().replace("      accepted:", "      archive: {path: ./arch}\n      accepted:"))  # fmt: skip
    entity = parse_config(path).entities[0]
    table = str(tmp_path / "out" / "accepted")
    seed = pa.table({"id": pa.array([0], pa.int64()), "name": ["seed"]})
    write_deltalake(table, seed)
    real, rivals = write_module.committed, []

    def rival(*args: Any) -> bool | None:
        found = real(*args)
        if not rivals:  # the run's own hash, committed by another run
            rivals.append(1)
            write_deltalake(table, seed, mode="append", commit_properties=stamp("rival", args[1]))  # fmt: skip
        return found

    monkeypatch.setattr(write_module, "committed", rival)
    report = run_entity(entity, tmp_path, "0.3").report
    assert {f["skip_reason"] for f in report["files"]} == {"already_committed"}
    assert len(report["files"]) == 2
    assert not list((tmp_path / "in").glob("*.csv"))
    assert sorted(p.name.split("__")[0] for p in (tmp_path / "arch").rglob("*.csv")) == ["b", "orders"]  # fmt: skip
    assert ids(tmp_path) == [0, 0]
    with pytest.raises(NoInputsError):  # nothing is left to append again
        run_entity(entity, tmp_path, "0.3")
    assert ids(tmp_path) == [0, 0]
