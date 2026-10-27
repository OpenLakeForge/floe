"""`floe run` output and log formats (Rust: `crates/floe-cli/tests/output.rs`, `logging_contract.rs`)."""

import json
import shutil
import sys
from pathlib import Path

import pytest

from floe._cli import main
from floe._cli_output import event_json, run_output
from floe.run import RunOutcome

REPO = Path(__file__).resolve().parents[2]

REPORT = {
    "entity": {"name": "customer"},
    "source": {
        "format": "csv",
        "path": "/tmp/input",
        "resolved_inputs": {"files": ["/tmp/input/customers.csv"]},
    },
    "sink": {
        "accepted": {"format": "parquet", "path": "/tmp/out/accepted"},
        "rejected": {"format": "csv", "path": "/tmp/out/rejected"},
    },
    "policy": {"severity": "reject"},
    "files": [
        {
            "input_file": "/tmp/input/customers.csv",
            "status": "rejected",
            "row_count": 10,
            "accepted_count": 8,
            "rejected_count": 2,
            "output": {
                "accepted_path": "/tmp/out/accepted",
                "rejected_path": "/tmp/out/rejected/customers.rejected.csv",
            },
        }
    ],
}
SUMMARY = {
    "run": {"status": "rejected", "exit_code": 0},
    "results": {
        "files_total": 1,
        "files_skipped": 0,
        "rows_total": 10,
        "accepted_total": 8,
        "rejected_total": 2,
    },
}


def outcome() -> RunOutcome:
    return RunOutcome(
        "run-123", SUMMARY, None, "/tmp/reports", "/tmp/reports/run_run-123", [REPORT]
    )


def test_format_run_output_default_mode() -> None:
    timings = [{"/tmp/input/customers.csv": 12}]
    expected = [
        "run id: run-123",
        "report base: /tmp/reports",
        "",
        "==> entity customer (severity=reject, format=csv)",
        "  REJECTED customers.csv rows=10 accepted=8 rejected=2 elapsed_ms=12 accepted_out=accepted rejected_out=customers.rejected.csv",
        "",
        "Totals: files=1 rows=10 accepted=8 rejected=2",
        "Overall: rejected (exit_code=0)",
        "Run summary: /tmp/reports/run_run-123/run.summary.json",
    ]
    assert run_output(outcome(), [], "default", timings).split("\n") == expected


def test_format_run_output_quiet_mode_keeps_only_the_summary() -> None:
    expected = [
        "run id: run-123",
        "report base: /tmp/reports",
        "Totals: files=1 rows=10 accepted=8 rejected=2",
        "Overall: rejected (exit_code=0)",
        "Run summary: /tmp/reports/run_run-123/run.summary.json",
    ]
    assert run_output(outcome(), [], "quiet", [{}]).split("\n") == expected


def test_format_run_output_verbose_mode_echoes_source_and_sinks() -> None:
    lines = run_output(outcome(), [], "verbose", [{}]).splitlines()
    assert lines[4:10] == [
        "  source: csv (/tmp/input)",
        "  sink accepted: parquet (/tmp/out/accepted)",
        "  sink rejected: csv (/tmp/out/rejected)",
        "  inputs:",
        "    /tmp/input/customers.csv",
        "  REJECTED customers.csv rows=10 accepted=8 rejected=2 accepted_out=accepted rejected_out=customers.rejected.csv",
    ]


def standalone(tmp_path: Path) -> Path:
    shutil.copytree(REPO / "example/standalone", tmp_path, dirs_exist_ok=True)
    return tmp_path / "config.yml"


def floe(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], *args: str
) -> tuple[int, str, str]:
    monkeypatch.setattr(sys, "argv", ["floe", *args])
    code = main()
    out, err = capsys.readouterr()
    return code, out, err


@pytest.mark.parametrize("verbose", [False, True])
def test_dry_run_previews_each_entity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    verbose: bool,
) -> None:
    config = str(standalone(tmp_path))
    args = ["run", "-c", config, "--run-id", "r1", "--dry-run"]
    code, out, _ = floe(monkeypatch, capsys, *args, *(["--verbose"] * verbose))
    report = f"  report: {tmp_path}/report/sales/run_r1/sales/orders/run.json"
    assert code == 0
    assert out == "\n".join(
        [
            "DRY RUN MODE - No actual execution performed",
            "run id: r1",
            f"report base: {tmp_path}/report",
            "",
            "==> entity sales.orders (format=csv)",
            "  Input: ./in/orders.csv (csv)",
            "  Accepted Output: ./out/accepted/orders (parquet)",
            "  Rejected Output: ./out/rejected/orders (csv)",
            "  Resolved Inputs: 1",
            "  Resolved Files:",
            f"    local://{tmp_path}/in/orders.csv",
            *([report] * verbose),
            "",
            "Totals: files=0 rows=0 accepted=0 rejected=0",
            "Overall: success (exit_code=0)",
            f"Run summary: {tmp_path}/report/sales/run_r1/run.summary.json\n",
        ]
    )
    assert not (tmp_path / "out").exists()


def test_log_format_json_stdout_is_ndjson_only_and_summary_is_stderr(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config = str(standalone(tmp_path))
    code, out, err = floe(monkeypatch, capsys, "run", "-c", config, "--log-format", "json")  # fmt: skip
    assert code == 0
    assert "Run summary:" in err
    events = [json.loads(line) for line in out.splitlines()]
    assert {e["schema"] for e in events} == {"floe.log.v1"}
    assert [e["event"] for e in events] == [
        *("run_started", "entity_started", "file_started", "file_finished"),
        *("entity_finished", "run_finished"),
    ]
    assert [e["level"] for e in events] == ["info"] * 3 + ["warn"] * 3  # rejected
    assert list(events[-1])[3:] == [
        *("run_id", "status", "exit_code", "files", "files_skipped", "rows"),
        *("accepted", "rejected", "warnings", "errors", "summary_uri"),
        *("report_base", "entity_report_uris", "ts_ms"),
    ]
    assert list(events[-1]["entity_report_uris"]) == ["sales.orders"]


def test_default_mode_prints_human_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    code, out, err = floe(monkeypatch, capsys, "run", "-c", str(standalone(tmp_path)))
    assert (code, err) == (0, "")
    assert "\n==> entity sales.orders (severity=reject, format=csv)\n" in out
    assert "\nTotals: files=1 rows=2 accepted=1 rejected=1\n" in out
    assert "\nRun summary: " in out


def test_quiet_json_still_emits_run_finished(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config = str(standalone(tmp_path))
    args = ("run", "-c", config, "--quiet", "--log-format", "json")
    code, out, _ = floe(monkeypatch, capsys, *args)
    assert code == 0
    assert json.loads(out.splitlines()[-1])["event"] == "run_finished"


def test_a_failed_run_logs_the_error_and_a_failed_run_finished(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config = standalone(tmp_path)
    args = ("run", "-c", str(config), "--run-id", "r1", "--entities", "zz")
    code, out, err = floe(monkeypatch, capsys, *args, "--log-format", "text")
    assert code == 1
    assert out == (
        f"run_started run_id=r1 config={config} report_base=disabled\n"
        "log level=error code=config_error entities not found: zz\n"
        "run_finished status=failed exit_code=1 summary=disabled\n"
    )
    assert err == "Error: entities not found: zz\n"


def test_a_missing_config_logs_no_run_started(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    args = ("run", "-c", str(tmp_path / "nope.yml"), "--log-format", "text")
    code, out, _ = floe(monkeypatch, capsys, *args)
    assert code == 1
    assert out.splitlines()[0] == (
        "log level=error code=error No such file or directory (os error 2)"
    )


def test_event_json_matches_the_rust_envelope() -> None:
    event = {"event": "entity_started", "run_id": "r", "name": "é", "ts_ms": 1}
    assert event_json(event) == (
        '{"schema":"floe.log.v1","level":"info","event":"entity_started","run_id":"r","name":"é","ts_ms":1}'
    )
