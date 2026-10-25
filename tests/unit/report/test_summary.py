"""`run.summary.json`, run outcome and report layout vs the recorded Rust 0.7 `expected/reports`."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs; check.py and record.py are scripts on the conformance sys.path

import json
import sys
from pathlib import Path
from typing import Any

import pytest
from test_entity_report import CASES, CONFORMANCE, run_entity

from floe.config.parse import parse_config
from floe.config.project import ProjectMetadata
from floe.report.summary import build_summary, id_path, run_outcome, write_reports
from floe.storage.local import LocalClient

sys.path.insert(0, str(CONFORMANCE))
from check import comparable, load_expected  # pyright: ignore[reportMissingImports]  # script on the sys.path entry above
from record import normalize  # pyright: ignore[reportMissingImports]

RUN_ID = "2026-01-01T00-00-00Z"
SUMMARY_CASES = list(dict.fromkeys(case for case, _ in CASES))


@pytest.mark.parametrize("case", SUMMARY_CASES)
def test_summary_matches_recorded(case: str, tmp_path: Path) -> None:
    config = parse_config(CONFORMANCE / case / "config.yml")
    ids = {e for c, e in CASES if c == case}
    reports: list[tuple[str, dict[str, Any]]] = []
    texts: dict[str, str] = {}
    for entity in (e for e in config.entities if e.id in ids):
        text, _ = run_entity(case, entity, tmp_path)
        reports.append((id_path(entity), json.loads(text)))
        texts[id_path(entity)] = text
    summary = build_summary(
        config.version,
        reports,
        run_id=RUN_ID,
        started_at=RUN_ID,
        finished_at=RUN_ID,
        duration_ms=1,
        config_path=str(tmp_path / "config.yml"),
        report_path=str(tmp_path / "report"),
    )
    write_reports(summary, reports, LocalClient())

    expected = load_expected(CONFORMANCE / case, "python")
    prefix = "reports/report/run_ID/"
    run_dir = tmp_path / "report" / f"run_{RUN_ID}"
    written = {p.relative_to(run_dir).as_posix(): p for p in run_dir.rglob("*.json")}
    assert set(written) == {
        k.removeprefix(prefix) for k in expected if k.startswith(prefix)
    }
    name = f"{prefix}run.summary.json"
    actual = normalize(
        json.loads(written["run.summary.json"].read_text()), str(tmp_path)
    )
    assert isinstance(actual, dict)
    actual["tool"]["version"] = (
        "0.7.0"  # the Rust release that was recorded; Python versions itself
    )
    assert comparable(name, json.dumps(actual)) == comparable(name, expected[name])
    for path, text in texts.items():
        assert written[f"{path}/run.json"].read_text() == text.removesuffix("\n")


@pytest.mark.parametrize(
    ("statuses", "warnings", "outcome"),
    [
        (["success", "success"], 0, ("success", 0)),
        (["success", "rejected"], 0, ("rejected", 0)),
        (["rejected", "aborted"], 0, ("aborted", 2)),
        (["aborted", "failed"], 0, ("failed", 1)),
        (["success", "skipped"], 0, ("success", 0)),
        (["success"], 3, ("success_with_warnings", 0)),
        (["rejected"], 3, ("rejected", 0)),
        ([], 0, ("success", 0)),
    ],
)
def test_run_outcome(
    statuses: list[str], warnings: int, outcome: tuple[str, int]
) -> None:
    assert run_outcome(statuses, warnings) == outcome


def _report(
    name: str, file_status: str, warnings: int = 0
) -> tuple[str, dict[str, Any]]:
    keys = (
        "files_total",
        "files_skipped",
        "rows_total",
        "accepted_total",
        "rejected_total",
        "errors_total",
    )
    results = dict.fromkeys(keys, 0) | {"warnings_total": warnings}
    return name, {
        "entity": {"name": name},
        "files": [{"status": file_status}],
        "results": results,
    }


@pytest.mark.parametrize(
    ("reports", "status", "code", "entity_statuses"),
    [
        (
            [_report("a", "success"), _report("b", "aborted")],
            "aborted",
            2,
            ["success", "aborted"],
        ),
        (
            [_report("a", "success", warnings=2)],
            "success_with_warnings",
            0,
            ["success_with_warnings"],
        ),
        (
            [_report("a", "success", warnings=2), _report("b", "rejected")],
            "rejected",
            0,
            ["success_with_warnings", "rejected"],
        ),
    ],
)
def test_summary_status(
    reports: list[tuple[str, dict[str, Any]]],
    status: str,
    code: int,
    entity_statuses: list[str],
) -> None:
    s = build_summary(
        "0.3",
        reports,
        run_id="r",
        started_at="t",
        finished_at="t",
        duration_ms=0,
        config_path="c",
        report_path="p",
    )
    assert (s["run"]["status"], s["run"]["exit_code"]) == (status, code)
    assert [e["status"] for e in s["entities"]] == entity_statuses


def test_summary_config_metadata_and_overwrite(tmp_path: Path) -> None:
    meta = ProjectMetadata(project="p", description=None, owner="o", tags=["a"])
    kw: dict[str, Any] = {
        "run_id": "r",
        "started_at": "t",
        "finished_at": "t",
        "duration_ms": 0,
        "config_path": "c",
        "report_path": str(tmp_path),
    }
    s = build_summary("0.3", [], metadata=meta, **kw)
    assert s["config"] == {
        "path": "c",
        "version": "0.3",
        "metadata": {"project": "p", "owner": "o", "tags": ["a"]},
    }
    assert "metadata" not in build_summary("0.3", [], **kw)["config"]
    write_reports(s, [], LocalClient())
    write_reports(s, [], LocalClient())  # Rust overwrites
    assert (tmp_path / "run_r" / "run.summary.json").read_text().startswith("{")
