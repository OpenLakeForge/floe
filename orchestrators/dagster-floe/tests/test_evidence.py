from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dagster import materialize

from floe_dagster.assets import build_floe_asset_defs
from test_asset_checks import _StaticRunner

MANIFEST = Path(__file__).parent / "fixtures" / "manifest.json"
TIMINGS = {
    "list_ms": 3,
    "download_wait_ms": None,
    "read_validate_ms": 40,
    "rejected_write_ms": 5,
    "accepted_write_ms": 12,
    "archive_ms": None,
}


def _value(meta: Any) -> Any:
    return getattr(meta, "value", getattr(meta, "text", getattr(meta, "path", meta)))


def _run(tmp_path: Path, events: list[dict[str, Any]], *, exit_code: int, report: dict[str, Any] | None):
    if report is not None:
        summary = tmp_path / "run.summary.json"
        entity_report = tmp_path / "run.json"
        entity_report.write_text(json.dumps(report), encoding="utf-8")
        summary.write_text(
            json.dumps({"entities": [{"name": "employees", "status": "rejected", "report_file": str(entity_report)}]}),
            encoding="utf-8",
        )
        events = [
            *events,
            {"event": "run_finished", "run_id": "r1", "status": "rejected", "exit_code": exit_code,
             "summary_uri": f"local://{summary}"},
        ]  # fmt: skip
    stdout = "\n".join(json.dumps(e) for e in events)
    assets, _, _ = build_floe_asset_defs(
        manifest_path=str(MANIFEST), runner=_StaticRunner(stdout, exit_code=exit_code), entities=["employees"]
    )
    return materialize(assets, raise_on_error=False)


def _outputs(result) -> dict[str, dict[str, Any]]:
    return {
        "rejected" if "rejected" in e.asset_key.path else "accepted": {
            k: _value(v) for k, v in e.materialization.metadata.items()
        }
        for e in result.get_asset_materialization_events()
    }


def _failure_metadata(result) -> dict[str, Any]:
    [event] = [e for e in result.all_events if e.event_type_value == "STEP_FAILURE"]
    return {k: _value(v) for k, v in event.event_specific_data.user_failure_data.metadata.items()}


def test_timings_and_rejected_sink_become_metadata(tmp_path: Path) -> None:
    report = {
        "timings_ms": TIMINGS,
        "files": [],
        "sink": {"rejected": {"format": "parquet", "path": "s3://b/rejected/employees"}},
    }
    out = _outputs(_run(tmp_path, [], exit_code=0, report=report))

    accepted = out["accepted"]
    assert {k: v for k, v in accepted.items() if k.startswith("floe/timings/")} == {
        "floe/timings/list_ms": 3,
        "floe/timings/read_validate_ms": 40,
        "floe/timings/rejected_write_ms": 5,
        "floe/timings/accepted_write_ms": 12,
    }
    rejected = out["rejected"]
    assert rejected["floe/rejected_format"] == "parquet"
    assert rejected["floe/rejected_uri"] == "s3://b/rejected/employees"
    assert rejected["entity_report_uri"] == accepted["entity_report_uri"]


def test_report_without_timings_or_sink_parses_unchanged(tmp_path: Path) -> None:
    result = _run(tmp_path, [], exit_code=0, report={"files": []})

    assert result.success
    out = _outputs(result)
    assert not [k for k in out["accepted"] if k.startswith("floe/timings/")]
    assert "floe/rejected_format" not in out["rejected"]


def _failure_events(uri: str) -> list[dict[str, Any]]:
    return [{"event": "log", "code": "run_failure_report", "message": f"run failure report: {uri}"}]


def test_failed_run_surfaces_the_failure_report(tmp_path: Path) -> None:
    failure = tmp_path / "run.failure.json"
    failure.write_text(
        json.dumps(
            {
                "error": {"type": "FloeStorageError", "code": None, "message": "boom"},
                "entities": [
                    {"name": "hr.employees", "state": "failed"},
                    {"name": "hr.done", "state": "completed", "report_file": "x"},
                    {"name": "hr.later", "state": "not_started"},
                    {"name": "hr.lost", "state": "unknown"},
                ],
            }
        ),
        encoding="utf-8",
    )
    result = _run(tmp_path, _failure_events(str(failure)), exit_code=1, report=None)

    assert not result.success
    meta = _failure_metadata(result)
    assert meta["floe/failure_report_uri"] == str(failure)
    assert meta["floe/error_type"] == "FloeStorageError"
    assert meta["floe/entity_states"] == {
        "hr.employees": "failed",
        "hr.done": "completed",
        "hr.later": "not_started",
        "hr.lost": "unknown",
    }
    assert "floe/failure_evidence" not in meta
    assert not any(ev.passed for ev in result.get_asset_check_evaluations())  # no check passes on a failed run


def test_failed_run_without_a_failure_report_says_evidence_is_unavailable(tmp_path: Path) -> None:
    result = _run(tmp_path, [], exit_code=1, report=None)

    assert not result.success
    assert _failure_metadata(result)["floe/failure_evidence"].startswith("unavailable:")


def test_failed_run_with_an_unreadable_failure_report_keeps_the_failure(tmp_path: Path) -> None:
    missing = tmp_path / "gone.json"
    result = _run(tmp_path, _failure_events(str(missing)), exit_code=1, report=None)

    assert not result.success
    meta = _failure_metadata(result)
    assert meta["floe/failure_report_uri"] == str(missing)
    assert meta["floe/failure_evidence"].startswith("unavailable:")
