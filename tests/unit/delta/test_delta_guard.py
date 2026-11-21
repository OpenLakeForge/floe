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
