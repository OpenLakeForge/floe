from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dagster import AssetCheckResult, AssetCheckSeverity, AssetCheckSpec

from .events import FloeRunFinished

FLOE_CHECK_NAMES: tuple[str, ...] = (
    "floe_cast_error",
    "floe_not_null",
    "floe_unique",
    "floe_schema_mismatch",
    "floe_file_status",
)

_RULE_TO_CHECK_NAME: dict[str, str] = {
    "cast_error": "floe_cast_error",
    "not_null": "floe_not_null",
    "unique": "floe_unique",
}

_TERMINAL_RUN_STATUSES = {"failed", "aborted"}
_FAIL_FILE_STATUSES = {"rejected", "failed", "aborted", "error"}
_WARN_FILE_STATUSES = {"warning", "warn"}


@dataclass(frozen=True)
class FloeCheckContext:
    asset_key: Sequence[str]
    entity_name: str
    run_id: str
    run_status: str
    summary_uri: str | None
    entity_report_uri: str | None
    entity_status: str | None
    entity_stats: Mapping[str, Any]


def build_asset_check_specs(asset_key: Sequence[str]) -> list[AssetCheckSpec]:
    return [AssetCheckSpec(name=name, asset=list(asset_key)) for name in FLOE_CHECK_NAMES]


def build_asset_check_results(
    *,
    asset_key: Sequence[str],
    entity_name: str,
    finished: FloeRunFinished,
    entity_report: Mapping[str, Any] | None,
    entity_stats: Mapping[str, Any] | None = None,
    summary_uri: str | None = None,
    entity_report_uri: str | None = None,
) -> list[AssetCheckResult]:
    stats = entity_stats or {}
    ctx = FloeCheckContext(
        asset_key=list(asset_key),
        entity_name=entity_name,
        run_id=finished.run_id,
        run_status=finished.status,
        summary_uri=summary_uri,
        entity_report_uri=entity_report_uri,
        entity_status=_as_str(stats.get("entity_status")),
        entity_stats=stats,
    )
    report_files = _as_list(entity_report.get("files")) if isinstance(entity_report, Mapping) else []
    if not report_files:
        return _build_summary_fallback_results(ctx)

    rule_summary = _summarize_rules(report_files)
    schema_summary = _summarize_schema_mismatches(report_files)
    file_status_summary = _summarize_file_statuses(report_files)

    results: list[AssetCheckResult] = []
    for rule_name, check_name in _RULE_TO_CHECK_NAME.items():
        rule_info = rule_summary.get(rule_name) or {}
        results.append(
            _make_rule_check_result(
                check_name=check_name,
                rule_name=rule_name,
                ctx=ctx,
                rule_info=rule_info,
            )
        )
    results.append(_make_schema_check_result(ctx=ctx, schema_summary=schema_summary))
    results.append(_make_file_status_check_result(ctx=ctx, file_status_summary=file_status_summary))
    return results


def _build_summary_fallback_results(ctx: FloeCheckContext) -> list[AssetCheckResult]:
    common = _base_metadata(ctx)
    common["fallback"] = "summary_only"
    results: list[AssetCheckResult] = []

    run_failed = ctx.run_status in _TERMINAL_RUN_STATUSES
    file_status_failed = run_failed or (ctx.entity_status in _FAIL_FILE_STATUSES)
    file_status_warn = (ctx.entity_status in _WARN_FILE_STATUSES) and not file_status_failed

    for check_name in FLOE_CHECK_NAMES:
        passed = True
        severity = AssetCheckSeverity.ERROR
        description = "Floe entity report unavailable; check derived from summary only."
        metadata = dict(common)

        if check_name == "floe_file_status":
            if file_status_failed:
                passed = False
            elif file_status_warn:
                passed = False
                severity = AssetCheckSeverity.WARN
            description = "Floe file status check derived from summary only (entity report unavailable)."
        elif run_failed:
            passed = False
            description = "Floe run status indicates failure/abort; detailed entity report unavailable."

        results.append(
            AssetCheckResult(
                asset_key=list(ctx.asset_key),
                check_name=check_name,
                passed=passed,
                severity=severity,
                metadata=metadata,
                description=description,
            )
        )
    return results


def _make_rule_check_result(
    *,
    check_name: str,
    rule_name: str,
    ctx: FloeCheckContext,
    rule_info: Mapping[str, Any],
) -> AssetCheckResult:
    reject_violations = _as_int(rule_info.get("reject_violations")) or 0
    warn_violations = _as_int(rule_info.get("warn_violations")) or 0
    total_violations = _as_int(rule_info.get("violations_total")) or 0
    files_impacted = _as_int(rule_info.get("files_impacted")) or 0

    passed = True
    severity = AssetCheckSeverity.ERROR
    if ctx.run_status in _TERMINAL_RUN_STATUSES or reject_violations > 0:
        passed = False
    elif warn_violations > 0:
        passed = False
        severity = AssetCheckSeverity.WARN

    metadata = _base_metadata(ctx)
    metadata.update(
        {
            "rule": rule_name,
            "violations_total": total_violations,
            "reject_violations": reject_violations,
            "warn_violations": warn_violations,
            "files_impacted": files_impacted,
        }
    )
    top_columns = rule_info.get("top_columns")
    if isinstance(top_columns, list) and top_columns:
        metadata["top_columns"] = top_columns

    description = (
        f"Floe {rule_name} violations={total_violations} (reject={reject_violations}, warn={warn_violations})."
    )
    if ctx.run_status in _TERMINAL_RUN_STATUSES:
        description = f"Floe run status is {ctx.run_status}; treating {rule_name} check as failed."

    return AssetCheckResult(
        asset_key=list(ctx.asset_key),
        check_name=check_name,
        passed=passed,
        severity=severity,
        metadata=metadata,
        description=description,
    )


def _make_schema_check_result(
    *,
    ctx: FloeCheckContext,
    schema_summary: Mapping[str, Any],
) -> AssetCheckResult:
    files_impacted = _as_int(schema_summary.get("files_impacted")) or 0
    rejected_files = _as_int(schema_summary.get("rejected_files")) or 0

    passed = True
    severity = AssetCheckSeverity.ERROR
    if ctx.run_status in _TERMINAL_RUN_STATUSES or rejected_files > 0:
        passed = False
    elif files_impacted > 0:
        passed = False
        severity = AssetCheckSeverity.WARN

    metadata = _base_metadata(ctx)
    metadata.update(
        {
            "files_impacted": files_impacted,
            "rejected_files": rejected_files,
            "mismatch_actions": schema_summary.get("mismatch_actions", {}),
        }
    )
    if schema_summary.get("missing_columns"):
        metadata["missing_columns"] = schema_summary["missing_columns"]
    if schema_summary.get("extra_columns"):
        metadata["extra_columns"] = schema_summary["extra_columns"]
    if schema_summary.get("sample_messages"):
        metadata["sample_messages"] = schema_summary["sample_messages"]

    description = "Floe schema mismatch summary from entity report."
    if ctx.run_status in _TERMINAL_RUN_STATUSES:
        description = f"Floe run status is {ctx.run_status}; treating schema mismatch check as failed."

    return AssetCheckResult(
        asset_key=list(ctx.asset_key),
        check_name="floe_schema_mismatch",
        passed=passed,
        severity=severity,
        metadata=metadata,
        description=description,
    )


def _make_file_status_check_result(
    *,
    ctx: FloeCheckContext,
    file_status_summary: Mapping[str, Any],
) -> AssetCheckResult:
    counts = file_status_summary.get("status_counts")
    if not isinstance(counts, Mapping):
        counts = {}

    failed_file_count = sum(count for status, count in counts.items() if _as_str(status) in _FAIL_FILE_STATUSES)
    warn_file_count = sum(count for status, count in counts.items() if _as_str(status) in _WARN_FILE_STATUSES)

    passed = True
    severity = AssetCheckSeverity.ERROR
    if ctx.run_status in _TERMINAL_RUN_STATUSES or failed_file_count > 0:
        passed = False
    elif warn_file_count > 0:
        passed = False
        severity = AssetCheckSeverity.WARN

    metadata = _base_metadata(ctx)
    metadata["file_status_counts"] = dict(counts)
    metadata["failed_files"] = failed_file_count
    metadata["warn_files"] = warn_file_count

    description = "Floe file-level statuses from entity report."
    if ctx.run_status in _TERMINAL_RUN_STATUSES:
        description = f"Floe run status is {ctx.run_status}; treating file status check as failed."

    return AssetCheckResult(
        asset_key=list(ctx.asset_key),
        check_name="floe_file_status",
        passed=passed,
        severity=severity,
        metadata=metadata,
        description=description,
    )


def _summarize_rules(files: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    summary: dict[str, dict[str, Any]] = {
        "cast_error": {
            "violations_total": 0,
            "reject_violations": 0,
            "warn_violations": 0,
            "files_impacted": 0,
            "column_counts": Counter(),
        },
        "not_null": {
            "violations_total": 0,
            "reject_violations": 0,
            "warn_violations": 0,
            "files_impacted": 0,
            "column_counts": Counter(),
        },
        "unique": {
            "violations_total": 0,
            "reject_violations": 0,
            "warn_violations": 0,
            "files_impacted": 0,
            "column_counts": Counter(),
        },
    }

    for file_item in files:
        file_had_rule: set[str] = set()
        validation = _as_dict(file_item.get("validation"))
        rules = _as_list(validation.get("rules"))
        for rule_item in rules:
            rule_name = _as_str(rule_item.get("rule"))
            if rule_name not in summary:
                continue
            bucket = summary[rule_name]
            violations = _as_int(rule_item.get("violations")) or 0
            severity = _as_str(rule_item.get("severity"))
            bucket["violations_total"] = (bucket.get("violations_total") or 0) + violations
            if severity == "reject":
                bucket["reject_violations"] = (bucket.get("reject_violations") or 0) + violations
            elif severity in {"warn", "warning"}:
                bucket["warn_violations"] = (bucket.get("warn_violations") or 0) + violations
            if violations > 0:
                file_had_rule.add(rule_name)

            column_counts = bucket["column_counts"]
            if isinstance(column_counts, Counter):
                for column_item in _as_list(rule_item.get("columns")):
                    column_name = _as_str(column_item.get("column"))
                    if not column_name:
                        continue
                    column_violations = _as_int(column_item.get("violations")) or 0
                    column_counts[column_name] += max(column_violations, 0)

        for rule_name in file_had_rule:
            bucket = summary[rule_name]
            bucket["files_impacted"] = (bucket.get("files_impacted") or 0) + 1

    for bucket in summary.values():
        column_counts = bucket.pop("column_counts", Counter())
        if isinstance(column_counts, Counter):
            bucket["top_columns"] = [
                {"column": name, "violations": count} for name, count in column_counts.most_common(3)
            ]
        else:
            bucket["top_columns"] = []

    return summary


def _summarize_schema_mismatches(files: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    mismatch_actions: Counter[str] = Counter()
    missing_columns: Counter[str] = Counter()
    extra_columns: Counter[str] = Counter()
    sample_messages: list[str] = []
    files_impacted = 0
    rejected_files = 0

    for file_item in files:
        mismatch = _as_dict(file_item.get("mismatch"))
        action = _as_str(mismatch.get("mismatch_action"))
        missing = [_as_str(v) for v in _as_list(mismatch.get("missing_columns"))]
        extra = [_as_str(v) for v in _as_list(mismatch.get("extra_columns"))]
        error = _as_dict(mismatch.get("error"))
        error_rule = _as_str(error.get("rule"))
        message = _as_str(error.get("message"))

        has_schema_mismatch = bool(
            (error_rule == "schema_mismatch") or (action and action != "none" and (any(missing) or any(extra)))
        )
        if not has_schema_mismatch:
            continue

        files_impacted += 1
        if _as_str(file_item.get("status")) in _FAIL_FILE_STATUSES or action == "rejected_file":
            rejected_files += 1
        if action:
            mismatch_actions[action] += 1
        for col in missing:
            if col:
                missing_columns[col] += 1
        for col in extra:
            if col:
                extra_columns[col] += 1
        if message and len(sample_messages) < 3:
            sample_messages.append(message)

    return {
        "files_impacted": files_impacted,
        "rejected_files": rejected_files,
        "mismatch_actions": dict(mismatch_actions),
        "missing_columns": [name for name, _ in missing_columns.most_common(5)],
        "extra_columns": [name for name, _ in extra_columns.most_common(5)],
        "sample_messages": sample_messages,
    }


def _summarize_file_statuses(files: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    for file_item in files:
        status = _as_str(file_item.get("status"))
        if status:
            counts[status] += 1
    return {"status_counts": dict(counts)}


def _base_metadata(ctx: FloeCheckContext) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "entity": ctx.entity_name,
        "run_id": ctx.run_id,
        "floe_run_status": ctx.run_status,
    }
    if ctx.summary_uri:
        metadata["summary_uri"] = ctx.summary_uri
    if ctx.entity_report_uri:
        metadata["entity_report_uri"] = ctx.entity_report_uri

    for source_key, target_key in (
        ("files", "files"),
        ("rows", "rows"),
        ("accepted", "accepted"),
        ("rejected", "rejected"),
        ("warnings", "warnings"),
        ("errors", "errors"),
    ):
        value = ctx.entity_stats.get(source_key)
        if value is not None:
            metadata[target_key] = value
    if ctx.entity_status:
        metadata["entity_status"] = ctx.entity_status
    return metadata


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    return []


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)
