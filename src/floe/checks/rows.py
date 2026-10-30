"""not_null, row error columns and the accept/reject split (Rust: `checks/not_null.rs`, `checks/mod.rs`)."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

import polars as pl

from floe.checks.cast import json_escape

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

    Pass the checks in Rust's error order: not_null (required columns), cast_error (declared
    non-string columns), then unique.
    """

    rule: str
    column: str
    message: str
    mask: pl.Series


def not_null_check(df: pl.DataFrame, column: str) -> RowCheck:
    """A required column. `df` holds the typed values, so a failed cast also violates it."""
    return RowCheck("not_null", column, NOT_NULL_MESSAGE, df[column].is_null())


def error_entry(rule: str, column: str, message: str, source: str | None = None) -> str:
    """Rust's `RowError::to_json_with_source`."""
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
    """What one violated check adds to a row's errors, per Rust `RowErrorFormatter`."""
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

    `sources` maps a runtime column name to its source name (Rust's `source_column_map`).
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


def split_rows(
    df: pl.DataFrame,
    checks: Sequence[RowCheck],
    formatter: Formatter = "json",
    sources: Mapping[str, str] | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """`(accepted, rejected)`; rejected adds `__floe_row_index` (UInt64, row position in `df`) and `__floe_errors`."""
    # a null mask passes the row, as in `error_column`
    if not any(c.mask.any() for c in checks):
        none = [pl.Series(ROW_INDEX, [], pl.UInt64), pl.Series(ERRORS, [], pl.String)]
        return df, df.clear().with_columns(none)  # replaces a clashing column, as below
    errors = error_column(df.height, checks, formatter, sources)
    rejected = df.with_columns(
        pl.int_range(pl.len(), dtype=pl.UInt64).alias(ROW_INDEX), errors
    ).filter(errors.is_not_null())
    return df.filter(errors.is_null()), rejected
