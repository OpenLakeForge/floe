"""D28: `floe run` on a `source.partitioning` layout (local; S3 is in test_run_s3.py)."""

import importlib
import json
import os
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from floe.config.parse import parse_config
from floe.errors import FloeRunError
from floe.run import entity as entity_module  # `floe.run` is the public function
from floe.run import run

run_module = importlib.import_module("floe.run")
CONFIG = """\
version: "0.3"
domains:
  - name: d
entities:
  - name: o
    domain: d
    {mode}
    source:
      format: csv
      path: ./in
      options: {{header: true, separator: ","}}
      partitioning: {{pattern: "{{yyyy}}/{{MM}}/{{dd}}/{{batch}}", date_column: ingest_date, batch_column: batch_id}}
    sink:
      accepted: {{format: parquet, path: ./out/accepted}}
      rejected: {{format: csv, path: ./out/rejected}}
      {archive}
    policy: {{severity: reject}}
    schema:
      primary_key: [id, ingest_date]
      columns:
        - {{name: id, type: int64}}
report: {{path: ./report}}
"""


def put(tmp_path: Path, rel: str, body: str, mtime: int) -> None:
    path = tmp_path / "in" / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    os.utime(path, (mtime, mtime))


def go(tmp_path: Path, mode: str = "", archive: str = "", **kw: Any) -> Any:
    path = tmp_path / "config.yml"
    path.write_text(CONFIG.format(mode=mode, archive=archive))
    return run(path, parse_config(path, {}), **kw)


def report(tmp_path: Path) -> Any:
    return json.loads(max(tmp_path.glob("report/run_*/d/o/run.json")).read_text())


def test_two_days_two_batches_load_in_date_then_batch_order(tmp_path: Path) -> None:
    put(tmp_path, "2026/10/25/c3d4/y.csv", "id\n1\n", 300)
    put(
        tmp_path, "2026/10/24/ff00/x.csv", "id\n2\n", 100
    )  # earlier batch, though it sorts last by name
    put(tmp_path, "2026/10/24/a1b2/x.csv", "id\n1\n1\n", 200)
    go(tmp_path)
    got = pl.read_parquet(tmp_path / "out/accepted")
    assert got.rows() == [(2, date(2026, 10, 24), "ff00"), (1, date(2026, 10, 24), "a1b2"), (1, date(2026, 10, 25), "c3d4")]  # fmt: skip
    assert got.schema == {"id": pl.Int64, "ingest_date": pl.Date, "batch_id": pl.String}
    r = report(tmp_path)
    order = ["2026/10/24/ff00/x.csv", "2026/10/24/a1b2/x.csv", "2026/10/25/c3d4/y.csv"]
    assert [f["input_file"].split("/in/")[1] for f in r["files"]] == order
    assert [
        u.split("/in/")[1] for u in r["source"]["resolved_inputs"]["files"]
    ] == order
    # the second (1, 2026-10-24) is a primary-key duplicate; (1, 2026-10-25) is not
    assert [f["rejected_count"] for f in r["files"]] == [0, 1, 0]
    mismatch = r["files"][0]["mismatch"]
    assert (mismatch["declared_columns_count"], mismatch["input_columns_count"]) == (
        3,
        1,
    )
    assert mismatch["missing_columns"] == mismatch["extra_columns"] == []


def test_a_file_outside_the_pattern_fails_the_run_before_any_entity_starts(tmp_path: Path) -> None:  # fmt: skip
    put(tmp_path, "2026/10/24/a1b2/x.csv", "id\n1\n", 100)
    put(tmp_path, "2026/10/24/stray.csv", "id\n1\n", 100)
    events: list[str] = []

    def observe(event: dict[str, Any]) -> None:
        events.append(event["event"])

    with pytest.raises(FloeRunError) as err:
        go(tmp_path, observer=observe)
    assert str(err.value).endswith(
        "/in/2026/10/24/stray.csv does not match source.partitioning.pattern={yyyy}/{MM}/{dd}/{batch}"
    )
    assert "run_started" in events and "entity_started" not in events


def test_full_refresh_fails_on_archived_inputs_until_d29(tmp_path: Path) -> None:
    put(tmp_path, "2026/10/24/a1b2/x.csv", "id\n1\n", 100)
    archive = "archive: {path: ./arch}"
    go(tmp_path, "incremental_mode: archive", archive)
    put(tmp_path, "2026/10/25/c3d4/y.csv", "id\n2\n", 200)
    with pytest.raises(FloeRunError, match=r"archived inputs keep their partition path only with D29 \(#884\): local://.*/arch/o/x__run-"):  # fmt: skip
        go(tmp_path, "incremental_mode: archive", archive, full_refresh=True)


def test_derived_columns_never_name_headerless_csv_fields(tmp_path: Path) -> None:
    """An extra field of a headerless file stays an extra column; it is not taken for `ingest_date`."""
    put(tmp_path, "2026/10/24/a1b2/x.csv", "1,unexpected\n", 100)
    path = tmp_path / "config.yml"
    text = CONFIG.format(mode="", archive="").replace("header: true", "header: false")
    path.write_text(text.replace("      primary_key:", "      mismatch: {extra_columns: reject_file}\n      primary_key:"))  # fmt: skip
    run(path, parse_config(path, {}))
    (f,) = report(tmp_path)["files"]
    assert (f["status"], f["mismatch"]["extra_columns"]) == (
        "rejected",
        ["extra_column_2"],
    )


XML = """\
version: "0.3"
entities:
  - name: o
    source:
      format: xml
      path: ./in
      options: {row_tag: row}
      partitioning: {pattern: "{yyyy}/{MM}/{dd}", date_column: ingest.date}
    sink: {accepted: {format: parquet, path: ./out/accepted}, rejected: {format: csv, path: ./out/rejected}}
    policy: {severity: reject}
    schema: {columns: [{name: id, type: int64}]}
report: {path: ./report}
"""


def test_an_xml_derived_column_is_declared_whatever_its_name(tmp_path: Path) -> None:
    """A derived name is an output column, not an XML selector: `ingest.date` is not taken for a nested one."""
    put(tmp_path, "2026/10/24/x.xml", "<root><row><id>1</id></row></root>", 100)
    path = tmp_path / "config.yml"
    path.write_text(XML)
    run(path, parse_config(path, {}))
    assert pl.read_parquet(tmp_path / "out/accepted").rows() == [
        (1, date(2026, 10, 24))
    ]


def state_of(tmp_path: Path) -> Any:
    [path] = (tmp_path / "in/.floe").rglob("state.json")
    return json.loads(path.read_text())


def test_d30_file_mode_lists_from_the_watermark_minus_lookback_and_prunes_the_state(tmp_path: Path) -> None:  # fmt: skip
    for day in range(1, 25):
        put(tmp_path, f"2026/10/{day:02d}/b{day:02d}/x.csv", f"id\n{day}\n", 100 + day)
    go(tmp_path, "incremental_mode: file")
    assert state_of(tmp_path)["watermark"] == "2026-10-24"
    put(
        tmp_path, "2026/10/24/c3d4/y.csv", "id\n99\n", 200
    )  # a second batch on the last day
    put(
        tmp_path, "2026/10/05/stray.csv", "id\n1\n", 200
    )  # never walked: it would fail the pattern
    events: list[dict[str, Any]] = []
    go(tmp_path, "incremental_mode: file", observer=events.append)
    r = report(tmp_path)
    listed = [u.split("/in/")[1] for u in r["source"]["resolved_inputs"]["files"]]
    assert listed == ["2026/10/23/b23/x.csv", "2026/10/24/b24/x.csv", "2026/10/24/c3d4/y.csv"]  # fmt: skip
    assert [f["status"] for f in r["files"]] == ["skipped", "skipped", "success"]
    state = state_of(tmp_path)
    assert sorted(u.split("/in/")[1] for u in state["files"]) == listed
    assert state["watermark"] == "2026-10-24"
    [warn] = [e for e in events if e["event"] == "log"]
    assert (warn["log_level"], warn["entity"]) == ("warn", "o")
    assert "dates from 2026-10-23 (watermark 2026-10-24, lookback_days 1)" in warn["message"]  # fmt: skip


def test_d30_a_late_file_waits_for_a_full_refresh(tmp_path: Path) -> None:
    put(tmp_path, "2026/10/23/a/x.csv", "id\n1\n", 100)
    put(tmp_path, "2026/10/24/a/x.csv", "id\n2\n", 100)
    go(tmp_path, "incremental_mode: file")
    put(tmp_path, "2026/10/20/late/x.csv", "id\n3\n", 200)
    go(tmp_path, "incremental_mode: file")
    assert {f["status"] for f in report(tmp_path)["files"]} == {"skipped"}
    go(tmp_path, "incremental_mode: file", full_refresh=True)
    assert sorted(pl.read_parquet(tmp_path / "out/accepted")["id"]) == [1, 2, 3]
    state = state_of(tmp_path)  # the refresh prunes the late day again
    assert sorted(u.split("/in/")[1] for u in state["files"]) == ["2026/10/23/a/x.csv", "2026/10/24/a/x.csv"]  # fmt: skip


def test_d30_a_claim_holds_the_window_open(tmp_path: Path) -> None:
    """A crashed run's claim on an older day keeps that day listed, so its files are retried, not lost."""
    put(tmp_path, "2026/10/24/a/x.csv", "id\n1\n", 100)
    go(tmp_path, "incremental_mode: file")
    [path] = (tmp_path / "in/.floe").rglob("state.json")
    state = json.loads(path.read_text())
    put(tmp_path, "2026/10/10/dead/x.csv", "id\n2\n", 100)
    dead = f"local://{tmp_path}/in/2026/10/10/dead/x.csv"
    state["claims"][dead] = {"run_id": "dead", "acquired_at": "2026-01-01T00:00:00Z", "expires_at": "2026-01-01T01:00:00Z"}  # fmt: skip
    path.write_text(json.dumps(state))
    go(tmp_path, "incremental_mode: file")
    assert [f["input_file"] for f in report(tmp_path)["files"] if f["status"] == "success"] == [dead]  # fmt: skip


def boom(*_: object) -> None:
    raise FloeRunError("boom")


def test_d30_a_failed_run_keeps_an_older_day_listed_for_its_retry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    """The run holding the window open fails and releases; its released claim, expired, still holds it open."""
    put(tmp_path, "2026/10/24/a/x.csv", "id\n1\n", 100)
    go(tmp_path, "incremental_mode: file")
    [path] = (tmp_path / "in/.floe").rglob("state.json")
    state = json.loads(path.read_text())
    put(tmp_path, "2026/10/10/old/x.csv", "id\n2\n", 100)
    old = f"local://{tmp_path}/in/2026/10/10/old/x.csv"
    state["claims"][old] = {"run_id": "dead", "acquired_at": "2026-01-01T00:00:00Z", "expires_at": "2026-01-01T01:00:00Z"}  # fmt: skip
    path.write_text(json.dumps(state))
    with monkeypatch.context() as m:
        m.setattr(entity_module, "_run_file", boom)
        with pytest.raises(FloeRunError, match="boom"):
            go(tmp_path, "incremental_mode: file")
    go(tmp_path, "incremental_mode: file")
    assert [f["input_file"] for f in report(tmp_path)["files"] if f["status"] == "success"] == [old]  # fmt: skip


def test_d30_a_stale_listing_does_not_reclaim_a_pruned_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    """A run that listed before another moved the watermark checks the window again when it claims."""
    put(tmp_path, "2026/10/10/a/x.csv", "id\n1\n", 100)
    put(tmp_path, "2026/10/24/a/x.csv", "id\n2\n", 100)
    go(tmp_path, "incremental_mode: file")
    assert all("/10/10/" not in u for u in state_of(tmp_path)["files"])
    real = run_module.list_source

    def unwindowed(
        *a: Any,
    ) -> Any:  # the whole source, as listed before the watermark moved
        return real(*a[:-1], True)

    monkeypatch.setattr(run_module, "list_source", unwindowed)
    go(tmp_path, "incremental_mode: file")
    assert [f["status"] for f in report(tmp_path)["files"]] == ["skipped"]


def test_d30_a_full_refresh_keeps_the_watermark_until_it_promotes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    put(tmp_path, "2026/10/24/a/x.csv", "id\n1\n", 100)
    go(tmp_path, "incremental_mode: file")
    with monkeypatch.context() as m:
        m.setattr(entity_module, "_run_file", boom)
        with pytest.raises(FloeRunError, match="boom"):
            go(tmp_path, "incremental_mode: file", full_refresh=True)
    assert state_of(tmp_path)["watermark"] == "2026-10-24"


def test_d30_a_nested_glob_override_keeps_its_meaning(tmp_path: Path) -> None:
    put(tmp_path, "2026/10/24/a/x.csv", "id\n1\n", 100)
    path = tmp_path / "config.yml"
    path.write_text(CONFIG.format(mode="incremental_mode: file", archive="").replace('separator: ","}', 'separator: ",", glob: "2026/**/*.csv"}'))  # fmt: skip
    run(path, parse_config(path, {}))
    put(tmp_path, "2026/10/24/b/x.csv", "id\n2\n", 200)
    run(path, parse_config(path, {}))
    assert [f["status"] for f in report(tmp_path)["files"]] == ["skipped", "success"]
