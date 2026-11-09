"""#1155: `run.failure.json` says what each sink of the failed entity had confirmed (committed), never more."""

import json
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from floe.config.parse import parse_config
from floe.run import run
from floe.run.sources import list_source, resolve_source
from floe.storage.inputs import input_hash

CONFIG = """\
version: "0.3"
entities:
  - name: a
    source: {format: csv, path: ./in, options: {header: true, separator: ","}}
    sink:
      accepted: {format: parquet, path: ./out/accepted}
      rejected: {format: csv, path: ./out/rejected}
    policy: {severity: reject}
    schema:
      columns:
        - {name: id, type: int64}
report: {path: ./report}
"""


def fail_run(tmp_path: Path) -> dict[str, Any]:
    (tmp_path / "in").mkdir()
    (tmp_path / "in/a.csv").write_text("id\n1\nsecret-bad-value\n")
    path = tmp_path / "config.yml"
    path.write_text(CONFIG)
    with pytest.raises(RuntimeError, match="boom"):
        run(path, parse_config(path, {}), run_id="r1")
    text = (tmp_path / "report/run_r1/run.failure.json").read_text()
    assert "secret-bad-value" not in text
    return json.loads(text)["entities"][0]


def boom(*_: object, **__: object) -> None:
    raise RuntimeError("boom")


def test_a_failed_rejected_write_is_unknown_and_the_accepted_sink_not_started(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    monkeypatch.setattr(sys.modules["floe.run.file"], "write_rejected", boom)
    assert fail_run(tmp_path)["sinks"] == {"rejected": "unknown", "accepted": "not_started"}  # fmt: skip


def test_a_failed_accepted_write_after_the_rejected_one_is_unknown(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    def drain_then_fail(frames: Iterable[pl.DataFrame], *_: object, **__: object) -> None:  # fmt: skip
        list(frames)
        raise RuntimeError("boom")

    monkeypatch.setattr(
        sys.modules["floe.run.entity"], "write_accepted", drain_then_fail
    )
    assert fail_run(tmp_path)["sinks"] == {"rejected": "committed", "accepted": "unknown"}  # fmt: skip


def test_a_failure_after_the_accepted_write_reports_it_committed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    monkeypatch.setattr(sys.modules["floe.run.entity"], "build_report", boom)
    assert fail_run(tmp_path)["sinks"] == {"rejected": "committed", "accepted": "committed"}  # fmt: skip


def test_the_failure_file_holds_no_credential_from_the_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "s3cr3t-key-value")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIA-id-value")
    monkeypatch.setattr(sys.modules["floe.run.file"], "write_rejected", boom)
    fail_run(tmp_path)
    text = (tmp_path / "report/run_r1/run.failure.json").read_text()
    assert "s3cr3t-key-value" not in text and "AKIA-id-value" not in text


def test_a_failed_entity_records_the_fingerprint_of_the_inputs_it_claimed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    monkeypatch.setattr(sys.modules["floe.run.entity"], "build_report", boom)
    entry = fail_run(tmp_path)
    config = parse_config(tmp_path / "config.yml", {})
    entity = config.entities[0]
    uri, client = resolve_source(entity, tmp_path, None)
    listed = list_source(entity, tmp_path, None, uri, client)
    assert entry["input_hash"] == input_hash(entity, listed.files)
    assert "input_hash" not in entry["sinks"]


def test_an_entity_that_failed_before_claiming_has_no_fingerprint(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    monkeypatch.setattr(sys.modules["floe.run.incremental"].Incremental, "claim", boom)
    entry = fail_run(tmp_path)
    assert "input_hash" not in entry and "input_hash" not in entry["sinks"]
