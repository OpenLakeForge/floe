"""Business-contract quality checks: severities and column value checks as Floe entity keys.

Pure: records in, a plain dict out (`policy` and per-column keys), problems appended to `errors`
with the Excel cell they come from. Value checks are validated by `check_value_checks`.
"""

import re
from collections.abc import Sequence
from typing import Any

from floe.checks.values import literal
from floe.config.types import ColumnConfig, PolicySeverity
from floe.config.validate import check_value_checks
from floe.contract.workbook import ColumnRow, TableRow
from floe.errors import FloeConfigError

_TABLE_RULES = (
    ("unique_severity", "unique"),
    ("not_null_severity", "not_null"),
    ("cast_severity", "cast_error"),
)
_KEY_RULE = {
    "min": "out_of_range",
    "max": "out_of_range",
    "min_length": "length",
    "max_length": "length",
    "allowed_values": "not_allowed",
    "pattern": "pattern_mismatch",
}


def _text(value: str | None) -> str | None:
    return value if value is not None and value.strip() else None


def _i64(n: int) -> bool:
    return (
        -(2**63) <= n < 2**63
    )  # Floe's YAML parser reads wider integers back as floats


def _bound(text: str | None) -> int | str | None:
    text = _text(text)
    if text is None:
        return None
    text = text.strip()
    if re.fullmatch(r"-?(0|[1-9]\d*)", text) and _i64(int(text)):
        return int(text)
    return text


def _length(c: ColumnRow, key: str, errors: list[str]) -> int | None:
    text = _text(getattr(c, key))
    if text is None:
        return None
    if re.fullmatch(r"\d+", text.strip()):
        return int(text)
    errors.append(
        f"{c.refs[key]}: {key} must be a whole number >= 0, got {text.strip()}"
    )
    return None


def _column(c: ColumnRow, severity: str | None, errors: list[str]) -> dict[str, Any]:
    keys: dict[str, Any] = {
        "min": _bound(c.min),
        "max": _bound(c.max),
        "min_length": _length(c, "min_length", errors),
        "max_length": _length(c, "max_length", errors),
        "allowed_values": list(c.allowed_values) if c.allowed_values else None,
        "pattern": _text(c.pattern),
    }
    keys = {k: v for k, v in keys.items() if v is not None}
    try:
        check_value_checks(
            c.target_table,
            ColumnConfig(
                name=c.target_column,
                source=None,
                column_type=c.target_type,
                nullable=None,
                **keys,
            ),
        )
        if "allowed_values" in keys:
            typed = [literal(c.target_type, v) for v in keys["allowed_values"]]
            # one list is all-int or all-str: keep the text when any int would not survive YAML
            if all(not isinstance(v, int) or _i64(v) for v in typed):
                keys["allowed_values"] = typed
    except ValueError as err:  # cast_column on an unknown or malformed target_type
        errors.append(f"{c.refs['target_type']}: target_type {c.target_type}: {err}")
        return {}
    except FloeConfigError as err:
        message = str(err)
        found = re.search(rf"\[name={re.escape(c.target_column)}\]\.(\w+)", message)
        key = found.group(1) if found else "target_type"
        errors.append(
            f"{c.refs[key]}: {message.split('] ', 1)[-1] if found else message}"
        )
        return {}
    rules: dict[str, str] = {}
    required = c.not_null or c.primary_key
    if c.not_null_severity:
        if required:
            rules["not_null"] = c.not_null_severity
        else:
            errors.append(f"{c.refs['not_null_severity']}: not_null_severity needs not_null=X or primary_key=X on this column")  # fmt: skip
    if severity:
        enabled = {_KEY_RULE[k] for k in keys}
        if enabled:
            rules.update({r: severity for r in sorted(enabled)})
        else:
            errors.append(f"{c.refs['value_severity']}: value_severity is set but no check is enabled on this column")  # fmt: skip
    return {**keys, **({"rule_severity": rules} if rules else {})}


def compile_checks(
    table: TableRow,
    columns: Sequence[ColumnRow],
    default_severity: str,
    errors: list[str],
) -> dict[str, Any]:
    """`{"policy": {"severity", "rule_severity"?}, "columns": {target_column: {...}}}` for one table."""
    severity: PolicySeverity = table.default_severity or default_severity  # type: ignore[assignment]  # ponytail: workbook validated it
    rules = {
        rule: getattr(table, cell)
        for cell, rule in _TABLE_RULES
        if getattr(table, cell)
    }
    policy: dict[str, Any] = {
        "severity": severity,
        **({"rule_severity": rules} if rules else {}),
    }
    out = {c.target_column: _column(c, c.value_severity, errors) for c in columns if c.target_table == table.target_table}  # fmt: skip
    return {"policy": policy, "columns": {k: v for k, v in out.items() if v}}
