from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from dagster import AssetCheckSeverity, materialize

from floe_dagster.asset_checks import FLOE_CHECK_NAMES, build_asset_check_results
from floe_dagster.assets import build_floe_asset_defs
from floe_dagster.events import FloeRunFinished
from floe_dagster.runner import Runner, RunResult


class _StaticRunner(Runner):
    def __init__(self, stdout: str, *, exit_code: int = 0, stderr: str = "") -> None:
        self._result = RunResult(stdout=stdout, stderr=stderr, exit_code=exit_code)

    def run_floe_entity(
        self,
        config_uri: str,
        run_id: str | None,
        entity: str,
        log_format: str = "json",
        execution=None,
        runner_definition=None,
        manifest_uri: str | None = None,
        dagster_job_name: str | None = None,
        log=None,
    ) -> RunResult:
        del config_uri, run_id, entity, log_format, execution, runner_definition, manifest_uri, dagster_job_name, log
        return self._result


def test_build_asset_check_results_maps_rule_schema_and_file_statuses() -> None:
    finished = FloeRunFinished(
        run_id="run-123",
        status="rejected",
        exit_code=0,
        summary_uri="local:///tmp/run.summary.json",
    )
    entity_report = {
        "files": [
            {
                "status": "rejected",
                "mismatch": {
                    "mismatch_action": "rejected_file",
                    "missing_columns": ["created_at"],
                    "extra_columns": [],
                    "error": {"rule": "schema_mismatch", "message": "missing created_at"},
                },
                "validation": {"rules": []},
            },
            {
                "status": "rejected",
                "mismatch": {"mismatch_action": "none", "missing_columns": [], "extra_columns": []},
                "validation": {
                    "rules": [
                        {
                            "rule": "cast_error",
                            "severity": "reject",
                            "violations": 4,
                            "columns": [{"column": "created_at", "violations": 4}],
                        },
                        {
                            "rule": "not_null",
                            "severity": "reject",
                            "violations": 1,
                            "columns": [{"column": "customer_id", "violations": 1}],
                        },
                    ]
                },
            },
        ]
    }

    checks = build_asset_check_results(
        asset_key=["sales", "customers"],
        entity_name="customers",
        finished=finished,
        entity_report=entity_report,
        entity_stats={
            "entity_status": "rejected",
            "files": 2,
            "rejected": 2,
            "accepted": 0,
            "warnings": 0,
            "errors": 5,
        },
        summary_uri=finished.summary_uri,
        entity_report_uri="/tmp/customer/run.json",
    )

    by_name = {check.check_name: check for check in checks}
    assert set(by_name) == set(FLOE_CHECK_NAMES)
    assert by_name["floe_cast_error"].passed is False
    assert by_name["floe_not_null"].passed is False
    assert by_name["floe_unique"].passed is True
    assert by_name["floe_schema_mismatch"].passed is False
    assert by_name["floe_file_status"].passed is False
    new_checks = ("floe_out_of_range", "floe_not_allowed", "floe_pattern_mismatch", "floe_length", "floe_min_rows_per_file", "floe_min_rows", "floe_row_predicates")  # fmt: skip
    assert all(by_name[name].passed for name in new_checks)

    cast_meta = by_name["floe_cast_error"].metadata or {}
    assert _meta_value(cast_meta["entity"]) == "customers"
    assert _meta_value(cast_meta["run_id"]) == "run-123"
    assert _meta_value(cast_meta["violations_total"]) == 4
    assert "top_columns" in cast_meta


def test_build_asset_check_results_uses_warn_severity_for_warn_only_outcomes() -> None:
    checks = build_asset_check_results(
        asset_key=["hr", "employees"],
        entity_name="employees",
        finished=FloeRunFinished(run_id="run-1", status="success", exit_code=0, summary_uri=None),
        entity_report={
            "files": [
                {
                    "status": "warning",
                    "mismatch": {"mismatch_action": "none", "missing_columns": [], "extra_columns": []},
                    "validation": {
                        "rules": [
                            {
                                "rule": "unique",
                                "severity": "warn",
                                "violations": 2,
                                "columns": [{"column": "email", "violations": 2}],
                            }
                        ]
                    },
                }
            ]
        },
    )
    by_name = {check.check_name: check for check in checks}
    unique_check = by_name["floe_unique"]
    file_status_check = by_name["floe_file_status"]
    assert unique_check.passed is False
    assert unique_check.severity == AssetCheckSeverity.WARN
    assert file_status_check.passed is False
    assert file_status_check.severity == AssetCheckSeverity.WARN


def test_abort_severity_violations_count_as_reject_violations() -> None:
    checks = build_asset_check_results(
        asset_key=["hr", "employees"],
        entity_name="employees",
        finished=FloeRunFinished(run_id="run-1", status="success", exit_code=0, summary_uri=None),
        entity_report={
            "files": [
                {
                    "status": "rejected",
                    "mismatch": {"mismatch_action": "none", "missing_columns": [], "extra_columns": []},
                    "validation": {
                        "rules": [{"rule": "not_null", "severity": "abort", "violations": 1}],
                    },
                }
            ]
        },
    )
    metadata = {check.check_name: check for check in checks}["floe_not_null"].metadata
    assert metadata["violations_total"].value == 1
    assert metadata["reject_violations"].value == 1
    assert metadata["warn_violations"].value == 0


def test_build_asset_check_results_fallbacks_when_entity_report_fields_missing() -> None:
    checks = build_asset_check_results(
        asset_key=["sales", "orders"],
        entity_name="orders",
        finished=FloeRunFinished(run_id="run-2", status="rejected", exit_code=0, summary_uri=None),
        entity_report={"spec_version": "0.1"},
        entity_stats={"entity_status": "rejected", "rejected": 1, "errors": 1},
    )
    by_name = {check.check_name: check for check in checks}
    assert by_name["floe_file_status"].passed is False
    assert by_name["floe_schema_mismatch"].passed is True
    assert by_name["floe_cast_error"].passed is True
    fallback_meta = by_name["floe_file_status"].metadata or {}
    assert _meta_value(fallback_meta["fallback"]) == "summary_only"


def test_build_asset_check_results_marks_all_checks_failed_for_aborted_runs() -> None:
    checks = build_asset_check_results(
        asset_key=["sales", "orders"],
        entity_name="orders",
        finished=FloeRunFinished(run_id="run-3", status="aborted", exit_code=1, summary_uri=None),
        entity_report={"files": []},
    )
    assert all(check.passed is False for check in checks)


def test_asset_emits_native_dagster_asset_checks_from_floe_reports(tmp_path: Path) -> None:
    report_dir = tmp_path / "report" / "run_123" / "employees"
    report_dir.mkdir(parents=True)
    summary_path = tmp_path / "report" / "run_123" / "run.summary.json"
    entity_report_path = report_dir / "run.json"

    entity_report = {
        "entity": {"name": "employees"},
        "results": {
            "files_total": 2,
            "rows_total": 10,
            "accepted_total": 8,
            "rejected_total": 2,
            "warnings_total": 1,
            "errors_total": 3,
        },
        "files": [
            {
                "input_file": str(tmp_path / "in" / "employees_bad.csv"),
                "status": "rejected",
                "mismatch": {"mismatch_action": "none", "missing_columns": [], "extra_columns": []},
                "validation": {
                    "rules": [
                        {
                            "rule": "cast_error",
                            "severity": "reject",
                            "violations": 3,
                            "columns": [{"column": "hire_date", "violations": 3}],
                        }
                    ]
                },
            },
            {
                "input_file": str(tmp_path / "in" / "employees_ok.csv"),
                "status": "success",
                "mismatch": {"mismatch_action": "none", "missing_columns": [], "extra_columns": []},
                "validation": {"rules": []},
            },
        ],
    }
    summary_json = {
        "run": {"run_id": "run-123", "status": "rejected", "exit_code": 0},
        "entities": [
            {
                "name": "employees",
                "status": "rejected",
                "results": {
                    "files_total": 2,
                    "rows_total": 10,
                    "accepted_total": 8,
                    "rejected_total": 2,
                    "warnings_total": 1,
                    "errors_total": 3,
                },
                "report_file": str(entity_report_path),
            }
        ],
    }
    entity_report_path.write_text(json.dumps(entity_report), encoding="utf-8")
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary_json), encoding="utf-8")

    stdout = "\n".join(
        [
            json.dumps(
                {
                    "event": "run_started",
                    "run_id": "run-123",
                    "status": "running",
                    "exit_code": 0,
                }
            ),
            json.dumps(
                {
                    "event": "run_finished",
                    "run_id": "run-123",
                    "status": "rejected",
                    "exit_code": 0,
                    "summary_uri": f"local://{summary_path}",
                }
            ),
            "run id: run-123",
            "Totals: files=2 rows=10 accepted=8 rejected=2",
            "Overall: rejected (exit_code=0)",
            f"Run summary: {summary_path}",
        ]
    )

    manifest_path = Path(__file__).parent / "fixtures" / "manifest.json"
    assets_defs, _source_assets, _ = build_floe_asset_defs(
        manifest_path=str(manifest_path),
        runner=_StaticRunner(stdout),
        entities=["employees"],
    )
    result = materialize(assets_defs)
    assert result.success is True

    evaluations = result.get_asset_check_evaluations()
    assert {ev.asset_check_key.name for ev in evaluations} == set(FLOE_CHECK_NAMES)

    by_name = {ev.asset_check_key.name: ev for ev in evaluations}
    assert by_name["floe_cast_error"].passed is False
    assert by_name["floe_schema_mismatch"].passed is True
    assert by_name["floe_file_status"].passed is False

    cast_meta = by_name["floe_cast_error"].metadata
    assert _meta_value(cast_meta["run_id"]) == "run-123"
    assert _meta_value(cast_meta["entity"]) == "employees"
    assert "entity_report_uri" in cast_meta


def _meta_value(value: Any) -> Any:
    for attr in ("value", "text", "path"):
        if hasattr(value, attr):
            return getattr(value, attr)
    if hasattr(value, "data"):
        return value.data
    return value


def test_file_status_check_carries_the_rejection_threshold() -> None:
    policy = {"severity": "reject", "max_rejection_rate": 0.25, "rejection_rate": 0.5, "max_rejection_rate_exceeded": True}  # fmt: skip
    finished = FloeRunFinished(run_id="run-4", status="aborted", exit_code=2, summary_uri=None)
    files = [{"status": "aborted", "mismatch": {"mismatch_action": "none"}, "validation": {"rules": []}}]

    def file_status(report: dict[str, Any]) -> Any:
        checks = build_asset_check_results(asset_key=["sales", "orders"], entity_name="orders", finished=finished, entity_report=report)  # fmt: skip
        return next(c for c in checks if c.check_name == "floe_file_status")

    check = file_status({"files": files, "policy": policy})
    meta = {k: _meta_value(v) for k, v in (check.metadata or {}).items()}
    assert meta["floe/max_rejection_rate"] == 0.25
    assert meta["floe/rejection_rate"] == 0.5
    assert meta["floe/max_rejection_rate_exceeded"] is True
    assert "0.25" in (check.description or "") and "0.5" in (check.description or "")

    plain = file_status({"files": files, "policy": {"severity": "reject"}})
    assert not [k for k in plain.metadata or {} if k.startswith("floe/max_rejection") or k == "floe/rejection_rate"]
    assert "rate" not in (plain.description or "")


def test_empty_file_report_keeps_the_rejection_threshold_on_the_file_status_check() -> None:
    policy = {"max_rejection_rate": 0.25, "rejection_rate": 0.0, "max_rejection_rate_exceeded": False}
    checks = build_asset_check_results(
        asset_key=["sales", "orders"],
        entity_name="orders",
        finished=FloeRunFinished(run_id="run-5", status="success", exit_code=0, summary_uri=None),
        entity_report={"files": [], "policy": policy},
    )
    check = next(c for c in checks if c.check_name == "floe_file_status")
    assert _meta_value((check.metadata or {})["floe/max_rejection_rate"]) == 0.25


def _checks_for(rules: list[dict[str, Any]], policy: dict[str, Any] | None = None) -> dict[str, Any]:
    report: dict[str, Any] = {"files": [{"status": "success", "validation": {"rules": rules}}]}
    if policy is not None:
        report["policy"] = policy
    checks = build_asset_check_results(
        asset_key=["sales", "orders"],
        entity_name="orders",
        finished=FloeRunFinished(run_id="run-6", status="success", exit_code=0, summary_uri=None),
        entity_report=report,
    )
    return {check.check_name: check for check in checks}


def test_out_of_range_rule_fails_its_own_check() -> None:
    rule = {"rule": "out_of_range", "severity": "reject", "violations": 3, "columns": [{"column": "amount", "violations": 3}]}  # fmt: skip
    check = _checks_for([rule])["floe_out_of_range"]
    assert check.passed is False
    assert check.severity == AssetCheckSeverity.ERROR
    assert _meta_value((check.metadata or {})["violations_total"]) == 3


@pytest.mark.parametrize(
    ("rule_name", "check_name", "severity", "expected"),
    [
        ("not_allowed", "floe_not_allowed", "warn", AssetCheckSeverity.WARN),
        ("pattern_mismatch", "floe_pattern_mismatch", "abort", AssetCheckSeverity.ERROR),
        ("length", "floe_length", "reject", AssetCheckSeverity.ERROR),
        ("min_rows_per_file", "floe_min_rows_per_file", "reject", AssetCheckSeverity.ERROR),
    ],
)
def test_new_rules_fail_their_matching_check(
    rule_name: str, check_name: str, severity: str, expected: AssetCheckSeverity
) -> None:
    by_name = _checks_for([{"rule": rule_name, "severity": severity, "violations": 1, "columns": []}])
    assert by_name[check_name].passed is False
    assert by_name[check_name].severity == expected
    assert by_name["floe_row_predicates"].passed is True


def test_unknown_rule_names_count_as_row_predicates_only() -> None:
    rules = [
        {
            "rule": "end_after_start",
            "severity": "reject",
            "violations": 2,
            "columns": [{"column": "end_date", "violations": 2}],
        },
        {
            "rule": "discount_within_amount",
            "severity": "warn",
            "violations": 1,
            "columns": [{"column": "discount", "violations": 1}],
        },
        {"rule": "schema_error", "severity": "reject", "violations": 5, "columns": []},
    ]
    by_name = _checks_for(rules)
    check = by_name["floe_row_predicates"]
    meta = {k: _meta_value(v) for k, v in (check.metadata or {}).items()}
    assert check.passed is False
    assert check.severity == AssetCheckSeverity.ERROR
    assert meta["violations_total"] == 3
    assert meta["predicate_violations"] == {"end_after_start": 2, "discount_within_amount": 1}
    assert all(c.passed for name, c in by_name.items() if name != "floe_row_predicates")


@pytest.mark.parametrize(
    ("policy", "passed", "severity", "evaluated"),
    [
        ({"severity": "reject", "min_rows": 10, "min_rows_violated": True}, False, AssetCheckSeverity.ERROR, True),
        (
            {"severity": "reject", "min_rows": 10, "min_rows_violated": True, "rule_severity": {"min_rows": "warn"}},
            False,
            AssetCheckSeverity.WARN,
            True,
        ),
        ({"severity": "warn", "min_rows": 10, "min_rows_violated": True}, False, AssetCheckSeverity.WARN, True),
        ({"severity": "reject", "min_rows": 10, "min_rows_violated": False}, True, AssetCheckSeverity.ERROR, True),
        ({"severity": "reject", "min_rows": 10, "min_rows_violated": None}, True, AssetCheckSeverity.ERROR, False),
        ({"severity": "reject"}, True, AssetCheckSeverity.ERROR, False),
    ],
)
def test_min_rows_check_reads_the_policy_block(
    policy: dict[str, Any], passed: bool, severity: AssetCheckSeverity, evaluated: bool
) -> None:
    check = _checks_for([], policy)["floe_min_rows"]
    assert check.passed is passed
    assert check.severity == severity
    assert _meta_value((check.metadata or {})["min_rows_evaluated"]) is evaluated
