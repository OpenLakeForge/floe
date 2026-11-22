"""not_null, row error columns and the accept/reject split."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

import polars as pl

from floe.checks.cast import json_escape
from floe.config.types import PolicySeverity

NOT_NULL_MESSAGE = "required value missing"
ROW_INDEX = "__floe_row_index"
ERRORS = "__floe_errors"

Formatter = Literal[
    "json", "csv", "text"
]  # `report.formatter`, validated with the config

# (prefix, separator, suffix) around the joined entries. csv and text are JSON strings: the
# whole value is escaped and quoted, so the csv separator `\n` is written as the two characters.
_WRAP: dict[Formatter, tuple[str, str, str]] = {
    "json": ("[", ",", "]"),
    "csv": ('"', "\n", '"'),
    "text": ('"', "; ", '"'),
}


@dataclass(frozen=True)
class RowCheck:
    """One check over the rows: `mask` is true where the row violates it.

    Pass the checks in the error order: not_null (required columns), cast_error (declared
    non-string columns), the value checks (out_of_range, not_allowed, pattern_mismatch, length),
    the row predicates (`schema.checks`, in declaration order), then unique.
    """

    rule: str
    column: str
    message: str
    mask: pl.Series
    severity: PolicySeverity


def not_null_check(df: pl.DataFrame, column: str, severity: PolicySeverity) -> RowCheck:
    """A required column. `df` holds the typed values, so a failed cast also violates it."""
    return RowCheck(
        "not_null", column, NOT_NULL_MESSAGE, df[column].is_null(), severity
    )


def error_entry(rule: str, column: str, message: str, source: str | None = None) -> str:
    """One row error as JSON, with its source column when given."""
    src = "" if source is None else f'"source":"{json_escape(source)}",'
    return (
        f'{{"rule":"{json_escape(rule)}","column":"{json_escape(column)}",{src}'
        f'"message":"{json_escape(message)}"}}'
    )


def _csv_escape(value: str) -> str:
    escaped = value.replace('"', '""')
    return f'"{escaped}"' if any(c in escaped for c in ",\n\r") else escaped


def _entry(
    formatter: Formatter, check: RowCheck, sources: Mapping[str, str] | None
) -> str:
    """What one violated check adds to a row's errors."""
    source = sources.get(check.column) if sources else None
    rule, column, message = check.rule, check.column, check.message
    if formatter == "json":
        return error_entry(rule, column, message, source)
    if formatter == "csv":
        fields = [rule, column, message]
        if (
            sources
        ):  # with a source map every line has the source field, empty if unmapped
            fields.insert(2, source or "")
        return ",".join(_csv_escape(f) for f in fields)
    src = "" if source is None else f" source={source}"
    return f"{rule}:{column}{src} {message}"


def error_column(
    height: int,
    checks: Sequence[RowCheck],
    formatter: Formatter = "json",
    sources: Mapping[str, str] | None = None,
) -> pl.Series:
    """`__floe_errors`: the formatted errors of each row, null for rows that pass every check.

    `sources` maps a runtime column name to its source name.
    """
    if not checks:
        return pl.Series(ERRORS, [None] * height, dtype=pl.String)
    head, sep, tail = _WRAP[formatter]
    escape = str if formatter == "json" else json_escape
    entries = [
        pl.when(c.mask).then(pl.lit(escape(_entry(formatter, c, sources))))
        for c in checks
    ]
    joined = pl.concat_str(entries, separator=escape(sep), ignore_nulls=True)
    failed = pl.any_horizontal([c.mask for c in checks])
    return pl.select(
        pl.when(failed).then(pl.lit(head) + joined + pl.lit(tail)).alias(ERRORS)
    )[ERRORS]


def reject_mask(checks: Sequence[RowCheck], height: int) -> pl.Series:
    """True where a reject- or abort-level check fails: the most severe failure decides the row."""
    masks = [c.mask for c in checks if c.severity != "warn"]
    if not masks:
        return pl.repeat(False, height, eager=True)
    return pl.select(pl.any_horizontal(masks).fill_null(False)).to_series()


def split_rows(
    df: pl.DataFrame,
    checks: Sequence[RowCheck],
    formatter: Formatter = "json",
    sources: Mapping[str, str] | None = None,
    raw: pl.DataFrame | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """`(accepted, rejected, warned)`; rejected adds `__floe_row_index` (UInt64, row position in `df`) and `__floe_errors`.

    A row is rejected when a reject- or abort-level check fails on it. `__floe_errors` lists every failed check, warn
    included. `warned` holds `__floe_row_index` and `__floe_errors` of the accepted rows that failed a warn-level check.
    `raw` is the row-aligned frame before the casts: the rejected rows are taken from it, so a failed cast keeps its source value.
    """
    src = df if raw is None else raw
    # a null mask passes the row, as in `error_column`
    if not any(c.mask.any() for c in checks):
        none = [pl.Series(ROW_INDEX, [], pl.UInt64), pl.Series(ERRORS, [], pl.String)]
        # with_columns replaces a clashing column, as below
        return df, src.clear().with_columns(none), pl.DataFrame(none)
    errors = error_column(df.height, checks, formatter, sources)
    rejects = reject_mask(checks, df.height)
    indexed = src.with_columns(
        pl.int_range(pl.len(), dtype=pl.UInt64).alias(ROW_INDEX), errors
    )
    warned = indexed.filter(~rejects & errors.is_not_null()).select(ROW_INDEX, ERRORS)
    return df.filter(~rejects), indexed.filter(rejects), warned
