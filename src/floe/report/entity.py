"""Entity `run.json`. Dicts in the report's field order, serialised like serde."""

import json
from collections.abc import Mapping, Sequence
from itertools import product
from typing import Any

from floe.checks.mismatch import MismatchOutcome
from floe.checks.normalize import resolve_strategy, source_names
from floe.checks.rows import RowCheck
from floe.checks.unique import UniqueResult
from floe.config.types import EntityConfig, SchemaEvolutionConfig, WriteMode, rule_level
from floe.storage.inputs import ResolvedInputs
from floe.write.outputs import AcceptedWrite

MAX_RESOLVED_INPUTS = 50
_RULES = ("not_null", "cast_error", "out_of_range", "not_allowed", "pattern_mismatch", "length", "unique", "schema_error")  # fmt: skip
# `serde_json::Map` is a BTreeMap, so the echoed options and metadata come out key-sorted.
_OPTION_KEYS = ("header", "separator", "encoding", "null_values", "recursive", "glob",
                "json_mode", "row_tag", "namespace", "value_tag")  # fmt: skip
_META_KEYS = ("data_product", "domain", "owner", "description", "tags")
_UNIQUE_EFFECT = {
    "warn": ("warn", "warning"),
    "reject": ("reject_rows", "rows_rejected"),
    "abort": ("abort_run", "run_aborted"),
}


def serialize(report: dict[str, Any]) -> str:
    """`serde_json::to_string_pretty`: two-space indent, raw UTF-8, no trailing newline."""
    return json.dumps(report, indent=2, ensure_ascii=False)


def summarize_rules(
    checks: Sequence[RowCheck], entity: EntityConfig
) -> list[dict[str, Any]]:
    """Per rule (fixed order) and resolved severity (abort, reject, warn), per column (sorted) violation counts."""
    columns = entity.schema_.columns
    names = source_names(columns, resolve_strategy(entity.schema_))
    types = {n: c.column_type for n, c in zip(names, columns, strict=True)}
    sources = {n: c.source for n, c in zip(names, columns, strict=True) if c.source}
    rules: list[dict[str, Any]] = []
    for rule, level in product(_RULES, ("abort", "reject", "warn")):
        counts: dict[str, int] = {}
        for c in checks:
            if c.severity == level and (c.rule == rule or (rule == "schema_error" and c.rule not in _RULES)):  # fmt: skip
                counts[c.column] = counts.get(c.column, 0) + int(c.mask.sum())
        cols: list[dict[str, Any]] = []
        for name, n in sorted(counts.items()):
            if n:
                col: dict[str, Any] = {"column": name, "violations": n}
                if rule == "cast_error" and name in types:
                    col["target_type"] = types[name]
                if name in sources:
                    col["source"] = sources[name]
                cols.append(col)
        if cols:
            total = sum(c["violations"] for c in cols)
            rules.append(
                {"rule": rule, "severity": level, "violations": total, "columns": cols}
            )  # fmt: skip
    return rules


def file_report(
    entity: EntityConfig,
    input_file: str,
    row_count: int,
    rejected_count: int,
    checks: Sequence[RowCheck],
    mismatch: MismatchOutcome,
    rejected_path: str | None = None,
    archived_path: str | None = None,
    errors_path: str | None = None,
    forced_rejects: int = 0,
) -> dict[str, Any]:
    """One `files[]` entry. `rejected_count` is the split's, `forced_rejects` the merge-key duplicates forced out under warn."""
    # a rejected/aborted mismatch decides the file before any row check runs
    precheck = mismatch.aborted or mismatch.rejected
    rules = [] if precheck else summarize_rules(checks, entity)
    violations = sum(r["violations"] for r in rules)
    warnings = sum(r["violations"] for r in rules if r["severity"] == "warn")
    rejected = rejected_count + forced_rejects
    if precheck:
        status = "aborted" if mismatch.aborted else "rejected"
        accepted, rejected = 0, row_count
    elif any(r["severity"] == "abort" for r in rules):
        status, accepted, rejected = "aborted", 0, row_count
    else:  # merge-key duplicates forced out under warn leave the file a success
        status, accepted = (
            "rejected" if rejected_count else "success",
            row_count - rejected,
        )
    return {
        "input_file": input_file,
        "status": status,
        "row_count": row_count,
        "accepted_count": accepted,
        "rejected_count": rejected,
        "mismatch": mismatch.report,
        "output": {
            "accepted_path": None,
            "rejected_path": rejected_path,
            "errors_path": errors_path,
            "archived_path": archived_path,
        },
        "validation": {
            "errors": violations - warnings + mismatch.errors,
            "warnings": warnings + mismatch.warnings,
            "rules": rules,
        },
    }  # fmt: skip


def _unique_report(severity: str, r: UniqueResult) -> dict[str, Any]:
    action, effect = (
        _UNIQUE_EFFECT[severity] if r.duplicates_count else ("none", "none")
    )
    out: dict[str, Any] = {"columns": r.columns, "duplicates_count": r.duplicates_count}
    if batch := r.duplicates_count - r.target_duplicates_count:
        out["batch_duplicates_count"] = batch
    if r.target_duplicates_count:
        out["target_duplicates_count"] = r.target_duplicates_count
    out |= {
        "affected_rows_count": r.duplicates_count,
        "action": action,
        "status_effect": effect,
    }
    if r.samples:
        out["samples"] = [
            {"values": dict(sorted(s.values.items())), "count": s.count}
            for s in r.samples
        ]
    return out  # fmt: skip


def _rate_report(limit: float | None, totals: dict[str, int]) -> dict[str, Any]:
    if limit is None:
        return {}
    rate = (
        totals["rejected_total"] / totals["rows_total"] if totals["rows_total"] else 0.0
    )
    return {"max_rejection_rate": limit, "rejection_rate": rate, "max_rejection_rate_exceeded": rate > limit}  # fmt: skip


def build_report(
    entity: EntityConfig,
    version: str,
    inputs: ResolvedInputs,
    files: list[dict[str, Any]],
    write: AcceptedWrite,
    *,
    source_uri: str,
    accepted_uri: str,
    rejected_uri: str | None,
    unique: Sequence[UniqueResult] = (),
    write_mode: WriteMode | None = None,
    table_root_uri: str | None = None,
    table: Mapping[str, Any] | None = None,
    merge: Mapping[str, Any] | None = None,
    added_columns: Sequence[str] = (),
) -> dict[str, Any]:
    """The entity's `run.json`; `files` are the `file_report`s, in input order.

    `table` holds a table sink's fields (Iceberg: version, snapshot, catalog), whose metrics are reported even for no files.
    """
    src, sink, severity = entity.source, entity.sink, entity.policy.severity
    if write.files_written:
        for f in files:
            if f["status"] != "skipped":
                f["output"]["accepted_path"] = accepted_uri
    totals: dict[str, int] = {
        "files_total": len(files),
        "files_skipped": sum(f["status"] == "skipped" for f in files),
        "rows_total": sum(f["row_count"] for f in files),
        "accepted_total": sum(f["accepted_count"] for f in files),
        "rejected_total": sum(f["rejected_count"] for f in files),
        "warnings_total": sum(f["validation"]["warnings"] for f in files),
        "errors_total": sum(f["validation"]["errors"] for f in files),
    }
    entity_echo: dict[str, Any] = {"name": entity.id}
    if entity.metadata:
        entity_echo["metadata"] = {
            k: v
            for k in sorted(_META_KEYS)
            if (v := getattr(entity.metadata, k)) is not None
        }
    sink_echo: dict[str, Any] = {
        "accepted": {"format": sink.accepted.format, "path": accepted_uri}
    }
    if sink.rejected:
        sink_echo["rejected"] = {
            "format": sink.rejected.format,
            "path": rejected_uri or sink.rejected.path,
        }
    archive: dict[str, Any] = {
        "enabled": (entity.incremental_mode in ("none", "archive"))
        and sink.archive is not None
    }
    if sink.archive:
        archive["path"] = sink.archive.path
    sink_echo["archive"] = archive
    evolution = entity.schema_.schema_evolution or SchemaEvolutionConfig()
    report: dict[str, Any] = {
        "spec_version": version,
        "entity": entity_echo,
        "source": {
            "format": src.format,
            "path": source_uri,
            "options": {
                k: v for k in sorted(_OPTION_KEYS) if (v := getattr(src.options, k)) is not None
            },
            "cast_mode": src.cast_mode,
            "read_plan": "raw_and_typed",
            "resolved_inputs": {
                "mode": inputs.mode,
                "file_count": len(inputs.listed),
                "files": inputs.listed[:MAX_RESOLVED_INPUTS],
            },
        },
        "sink": sink_echo,
        "policy": {"severity": severity}
        | ({"rule_severity": dict(sorted(levels.items()))} if (levels := entity.policy.rule_severity) else {})
        | _rate_report(entity.policy.max_rejection_rate, totals),
        "accepted_output": {
            "path": accepted_uri,
            "table_root_uri": table_root_uri or accepted_uri,
            "write_mode": write_mode or sink.write_mode,
            "accepted_rows": totals["accepted_total"],
            "files_written": write.files_written,
            "parts_written": write.files_written,
            **({"part_files": write.part_files} if write.part_files else {}),
            **(table or {}),
            # the default output (nothing written) has no byte metrics
            "total_bytes_written": write.total_bytes_written if write.files_written or table else None,
            "avg_file_size_mb": write.avg_file_size_mb if write.files_written or table else None,
            "small_files_count": write.small_files_count if write.files_written or table else None,
            **(merge or {}),
        },
        "schema_evolution": {
            "enabled": evolution.mode == "add_columns",
            "mode": evolution.mode,
            "applied": bool(added_columns),
            **({"added_columns": list(added_columns)} if added_columns else {}),
            "incompatible_changes_detected": False,
        },
    }  # fmt: skip
    if unique:
        report["unique_constraints"] = [
            _unique_report(rule_level(entity, "unique"), r) for r in unique
        ]
    return report | {"results": totals, "files": files}
