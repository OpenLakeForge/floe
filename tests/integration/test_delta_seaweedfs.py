"""The Delta accepted sink on the S3-compatible service of the #602 harness (SeaweedFS), through `floe.run.run` (#1311).

Skipped unless `FLOE_S3_ENDPOINT` is set, e.g. `FLOE_S3_ENDPOINT=http://localhost:8333`. The source is local, the table is on S3.
"""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
# ^ deltalake and pyarrow ship no type stubs

import json
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from floe.config.parse import parse_config
from floe.delta import write as write_module
from floe.delta.sink import storage_options
from floe.errors import FloeRunError
from floe.run import incremental as incremental_module
from floe.run import run

pytestmark = [
    pytest.mark.s3,
    pytest.mark.skipif(
        not os.environ.get("FLOE_S3_ENDPOINT"), reason="FLOE_S3_ENDPOINT not set"
    ),
]
BUCKET = os.environ.get("FLOE_S3_BUCKET", "warehouse")

CONFIG = """\
version: "0.3"
storages:
  default: local
  definitions:
    - {{name: local, type: local}}
    - {{name: lake, type: s3, bucket: {bucket}, region: us-east-1, endpoint: "{endpoint}", path_style_access: true, prefix: {prefix}}}
domains:
  - name: d
entities:
  - name: orders
    domain: d
    incremental_mode: {mode}
    source: {{format: csv, path: ./in, options: {{header: true, separator: ","}}}}
    sink:
      write_mode: {write_mode}
      accepted: {{format: delta, path: out/accepted, storage: lake}}
    policy: {{severity: reject}}
    schema:
      columns:
        - {{name: id, type: int64}}
        - {{name: name, type: string}}
report: {{path: ./report}}
"""


class Project:
    def __init__(self, tmp_path: Path, mode: str = "none", write_mode: str = "append") -> None:  # fmt: skip
        self.root, self.prefix = tmp_path, f"it-{uuid.uuid4().hex}"
        (tmp_path / "in").mkdir()
        self.config = tmp_path / "config.yml"
        self.config.write_text(CONFIG.format(bucket=BUCKET, endpoint=os.environ["FLOE_S3_ENDPOINT"], prefix=self.prefix, mode=mode, write_mode=write_mode))  # fmt: skip
        self.uri = f"s3://{BUCKET}/{self.prefix}/out/accepted"
        storages = parse_config(self.config).storages
        assert storages is not None
        lake = next(s for s in storages.definitions if s.name == "lake")
        self.options = storage_options(lake)

    def source(self, rows: list[tuple[int, str]]) -> None:
        (self.root / "in" / "orders.csv").write_text("id,name\n" + "".join(f"{i},{n}\n" for i, n in rows))  # fmt: skip

    def run(self, run_id: str) -> dict[str, Any]:
        return run(self.config, parse_config(self.config), run_id=run_id).summary

    def files(self, run_id: str) -> list[dict[str, Any]]:
        (report,) = (self.root / "report").rglob(f"run_{run_id}/**/run.json")
        return json.loads(report.read_text())["files"]

    def table(self) -> DeltaTable:
        return DeltaTable(self.uri, storage_options=self.options)

    def ids(self) -> list[int]:
        return sorted(self.table().to_pyarrow_table()["id"].to_pylist())

    def seed(self) -> None:
        rows = pa.table({"id": pa.array([0], pa.int64()), "name": ["z"]})
        write_deltalake(self.uri, rows, mode="append", storage_options=self.options)


@pytest.fixture(autouse=True)
def credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "spike")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "spike-s3-secret")


def test_a_run_creates_the_table_on_s3(tmp_path: Path) -> None:
    p = Project(tmp_path)
    p.source([(1, "a"), (2, "b")])
    assert p.run("r1")["run"]["status"] == "success"
    assert p.ids() == [1, 2]
    assert p.table().version() == 0


def test_overwrite_twice_leaves_only_the_second_batch(tmp_path: Path) -> None:
    p = Project(tmp_path, write_mode="overwrite")
    p.source([(1, "a"), (2, "b")])
    p.run("r1")
    p.source([(3, "c")])
    p.run("r2")
    assert p.ids() == [3]
    assert p.table().version() == 1


def test_an_append_adds_to_the_table(tmp_path: Path) -> None:
    p = Project(tmp_path)
    p.source([(1, "a")])
    p.run("r1")
    p.source([(2, "b")])
    p.run("r2")
    assert p.ids() == [1, 2]
    assert p.table().version() == 1


def test_a_retry_after_a_commit_that_was_not_promoted_is_already_committed(tmp_path: Path) -> None:  # fmt: skip
    """The commit landed on S3, the claims were not promoted; once they expire the retry reads nothing."""
    p = Project(tmp_path, mode="file")
    p.source([(1, "a"), (2, "b")])

    def killed(*_: object, **__: object) -> None:
        raise FloeRunError("killed", code="run_commit_failed")

    with (
        pytest.MonkeyPatch.context() as patch
    ):  # not the fixture: `undo` would drop the credentials too
        patch.setattr(incremental_module, "promote", killed)
        with pytest.raises(FloeRunError, match="killed"):
            p.run("dead")
    (state,) = (tmp_path / "in").rglob("state.json")
    doc = json.loads(state.read_text())
    for claim in doc["claims"].values():
        claim["expires_at"] = "2000-01-01T00:00:00Z"
        claim["acquired_at"] = (datetime.now(UTC) - timedelta(minutes=1)).isoformat().replace("+00:00", "Z")  # fmt: skip
    state.write_text(json.dumps(doc))
    assert p.ids() == [1, 2]
    p.run("retry")
    (file,) = p.files("retry")
    assert (file["status"], file["skip_reason"]) == ("skipped", "already_committed")
    assert p.ids() == [1, 2]
    assert p.table().version() == 0


def test_racing_appends_of_one_batch_commit_it_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    """`incremental_mode: none`: N runs pass the guard on one snapshot, then write; one conditional PUT wins, the rest see it."""
    p = Project(tmp_path)
    p.source([(1, "a"), (2, "b")])
    p.seed()
    n, real = 4, write_module.write_deltalake
    barrier = threading.Barrier(n)

    def after_every_guard(*args: Any, **kwargs: Any) -> None:
        barrier.wait(timeout=60)  # every run has passed its guard on the same snapshot
        real(*args, **kwargs)

    monkeypatch.setattr(write_module, "write_deltalake", after_every_guard)
    with ThreadPoolExecutor(n) as pool:
        for future in [pool.submit(p.run, f"r{i}") for i in range(n)]:
            future.result()
    files = [f for i in range(n) for f in p.files(f"r{i}")]
    assert p.ids() == [0, 1, 2]
    assert [f["status"] for f in files].count("success") == 1
    assert [f.get("skip_reason") for f in files].count("already_committed") == n - 1
