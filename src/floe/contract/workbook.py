"""Business-contract records and their validation, from cells that already carry their Excel ref.

Every problem is reported with its cell (`Columns!C248`) and aggregated.
"""

import re
from dataclasses import dataclass
from typing import NamedTuple

from floe.errors import FloeConfigError

SEVERITIES = ("warn", "reject", "abort")
_SAFE_NAME = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_-]*")


class Cell(NamedTuple):
    value: str | None
    ref: str


@dataclass(frozen=True)
class TableRow:
    refs: dict[str, str]
    target_table: str
    source_dataset: str | None
    business_name: str | None
    description: str | None
    owner: str | None
    grain: str | None
    default_severity: str | None
    unique_severity: str | None
    not_null_severity: str | None
    cast_severity: str | None


@dataclass(frozen=True)
class ColumnRow:
    refs: dict[str, str]
    target_table: str
    source_column: str | None
    target_column: str
    target_type: str
    business_label: str | None
    primary_key: bool
    unique: bool
    unique_group: str | None
    fk_target_table: str | None
    fk_target_column: str | None
    not_null: bool
    not_null_severity: str | None
    min: str | None
    max: str | None
    min_length: str | None
    max_length: str | None
    allowed_values: tuple[str, ...] | None
    pattern: str | None
    value_severity: str | None


def _flag(cell: Cell, errors: list[str]) -> bool:
    text = (cell.value or "").strip()
    if text and text.upper() != "X":
        errors.append(f"{cell.ref}: expected X or blank, got {text}")
    return text.upper() == "X"


def _severity(cell: Cell, errors: list[str]) -> str | None:
    value = (cell.value or "").strip() or None
    if value is not None and value not in SEVERITIES:
        errors.append(
            f"{cell.ref}: severity must be one of {'|'.join(SEVERITIES)}, got {value}"
        )
    return value


def _required(cell: Cell, errors: list[str]) -> str:
    if cell.value is None or not cell.value.strip():
        errors.append(f"{cell.ref}: required cell is blank")
        return ""
    return cell.value


def _allowed(cell: Cell, errors: list[str]) -> tuple[str, ...] | None:
    if cell.value is None:
        return None
    items = tuple(i.strip() for i in cell.value.split("|"))
    if "" in items:
        errors.append(
            f"{cell.ref}: allowed_values has an empty item (separate values with |)"
        )
    return items


def _table(row: dict[str, Cell], errors: list[str]) -> TableRow:
    name = _required(row["target_table"], errors)
    if name and not _SAFE_NAME.fullmatch(name):
        errors.append(
            f"{row['target_table'].ref}: target_table {name} cannot be a file name ({name}.yml)"
        )
    return TableRow(
        {k: c.ref for k, c in row.items()},
        name,
        row["source_dataset"].value,
        row["business_name"].value,
        row["description"].value,
        row["owner"].value,
        row["grain"].value,
        _severity(row["default_severity"], errors),
        _severity(row["unique_severity"], errors),
        _severity(row["not_null_severity"], errors),
        _severity(row["cast_severity"], errors),
    )


def _column(row: dict[str, Cell], errors: list[str]) -> ColumnRow:
    return ColumnRow(
        {k: c.ref for k, c in row.items()},
        _required(row["target_table"], errors),
        row["source_column"].value,
        _required(row["target_column"], errors),
        _required(row["target_type"], errors),
        row["business_label"].value,
        _flag(row["primary_key"], errors),
        _flag(row["unique"], errors),
        row["unique_group"].value,
        row["fk_target_table"].value,
        row["fk_target_column"].value,
        _flag(row["not_null"], errors),
        _severity(row["not_null_severity"], errors),
        row["min"].value,
        row["max"].value,
        row["min_length"].value,
        row["max_length"].value,
        _allowed(row["allowed_values"], errors),
        row["pattern"].value,
        _severity(row["value_severity"], errors),
    )


def _cross_check(
    tables: list[TableRow],
    columns: list[ColumnRow],
    errors: list[str],
    warnings: list[str],
) -> None:
    first_table: dict[str, TableRow] = {}
    for t in tables:
        if t.target_table in first_table:
            errors.append(
                f"{t.refs['target_table']}: duplicate target_table {t.target_table} "
                f"(first at {first_table[t.target_table].refs['target_table']})"
            )
        first_table.setdefault(t.target_table, t)
    first_column: dict[tuple[str, str], ColumnRow] = {}
    for c in columns:
        if c.target_table and c.target_table not in first_table:
            errors.append(
                f"{c.refs['target_table']}: unknown target_table {c.target_table} (not in Tables)"
            )
        key = (c.target_table, c.target_column)
        if c.target_column and key in first_column:
            errors.append(
                f"{c.refs['target_column']}: duplicate target_column {c.target_column} in {c.target_table} "
                f"(first at {first_column[key].refs['target_column']})"
            )
        first_column.setdefault(key, c)
    for c in columns:
        if (c.fk_target_table is None) != (c.fk_target_column is None):
            blank = (
                "fk_target_column" if c.fk_target_column is None else "fk_target_table"
            )
            errors.append(
                f"{c.refs[blank]}: {blank} is blank but the other fk_target_* cell is set"
            )
        elif c.fk_target_table is not None and c.fk_target_column is not None:
            if c.fk_target_table not in first_table:
                warnings.append(
                    f"{c.refs['fk_target_table']}: foreign key points outside the workbook ({c.fk_target_table})"
                )
            elif (c.fk_target_table, c.fk_target_column) not in first_column:
                errors.append(
                    f"{c.refs['fk_target_column']}: {c.fk_target_table} has no column {c.fk_target_column}"
                )


def invalid(errors: list[str]) -> FloeConfigError:
    return FloeConfigError(
        "invalid business-contract workbook:\n" + "\n".join(errors),
        code="config_invalid_value",
        rule="contract_workbook",
    )


def build_rows(
    table_cells: list[dict[str, Cell]],
    column_cells: list[dict[str, Cell]],
    errors: list[str],
) -> tuple[tuple[TableRow, ...], tuple[ColumnRow, ...], tuple[str, ...]]:
    """Records in input order, with every row and cross-row problem appended to `errors`."""
    tables = [_table(r, errors) for r in table_cells]
    columns = [_column(r, errors) for r in column_cells]
    warnings: list[str] = []
    _cross_check(tables, columns, errors, warnings)
    return tuple(tables), tuple(columns), tuple(warnings)
