"""Business-contract records and their validation, from cells that already carry their Excel ref.

Every problem is reported with its cell (`Columns!C248`) and aggregated.
"""

import re
from dataclasses import dataclass, fields
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

import polars as pl

from floe.errors import FloeConfigError, FloeIoError
from floe.extras import missing

if TYPE_CHECKING:
    import fastexcel

SEVERITIES = ("warn", "reject", "abort")
GUIDE_LABELS = ("domain", "template_version", "default_severity")
_SAFE_NAME = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9_-]*"
)  # no leading `_`: `_domain.yml` is the domain file
_DATE_CELL = re.compile(
    r"(\d{4}-\d{2}-\d{2}) 00:00:00"
)  # how calamine renders a date cell as text


class Cell(NamedTuple):
    value: str | None
    ref: str


@dataclass(frozen=True)
class Domain:
    name: str
    template_version: str
    default_severity: str | None


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


def _bound(cell: Cell) -> str | None:
    match = _DATE_CELL.fullmatch(cell.value or "")
    return match[1] if match else cell.value


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
        _bound(row["min"]),
        _bound(row["max"]),
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


TABLES_HEADERS = tuple(f.name for f in fields(TableRow))[1:]
COLUMNS_HEADERS = tuple(f.name for f in fields(ColumnRow))[1:]


@dataclass(frozen=True)
class Workbook:
    domain: Domain
    tables: tuple[TableRow, ...]
    columns: tuple[ColumnRow, ...]
    warnings: tuple[str, ...] = ()


def _letters(index: int) -> str:
    name, index = "", index + 1
    while index:
        index, rem = divmod(index - 1, 26)
        name = chr(65 + rem) + name
    return name


def _sheet_rows(book: "fastexcel.ExcelReader", name: str) -> list[list[str | None]]:
    # ponytail: calamine hides where the used range starts, so refs assume it starts at A1.
    import fastexcel  # the optional xlsx extra, checked in parse_workbook

    try:
        # the eager=True overload of fastexcel's stubs returns an untyped pyarrow table
        loaded = book.load_sheet(  # pyright: ignore[reportUnknownMemberType]
            name, header_row=None, dtypes="string"
        )
        frame: pl.DataFrame = loaded.to_polars()
    except fastexcel.FastExcelError as err:
        raise FloeIoError(
            f"xlsx_read_error: failed to read sheet {name}: {err}",
            code="io_read_error",
            rule="xlsx_read_error",
        ) from err
    return [[(c.strip() or None) if c else None for c in row] for row in frame.rows()]


def _data_rows(
    sheet: str,
    rows: list[list[str | None]],
    expected: tuple[str, ...],
    errors: list[str],
) -> list[dict[str, Cell]]:
    """Rows below the header row; the group row and anything above the header row is skipped."""
    head = next((i for i, r in enumerate(rows) if "target_table" in r), None)
    if head is None:
        errors.append(
            f"{sheet}: no header row (expected a row containing target_table)"
        )
        return []
    seen: dict[str, int] = {}
    for col, name in enumerate(rows[head]):
        if name in seen:
            errors.append(
                f"{sheet}!{_letters(col)}{head + 1}: duplicate header {name} "
                f"(first at {_letters(seen[name])}{head + 1})"
            )
        elif name:
            seen[name] = col
    absent = [h for h in expected if h not in seen]
    if absent:
        errors.append(f"{sheet}: missing header(s): {', '.join(absent)}")
    if absent or len(seen) < len([n for n in rows[head] if n]):
        return []
    return [
        {
            h: Cell(
                r[seen[h]] if seen[h] < len(r) else None,
                f"{sheet}!{_letters(seen[h])}{n + 1}",
            )
            for h in expected
        }
        for n, r in enumerate(rows[head + 1 :], head + 1)
        if any(r)
    ]


def _guide(rows: list[list[str | None]], errors: list[str]) -> Domain:
    found: dict[str, Cell] = {}
    # labels live in one column (the first label's); a value like a domain named `default_severity` is not a label
    label_col = next(
        (c for row in rows for c, t in enumerate(row) if t in GUIDE_LABELS), 0
    )
    for n, row in enumerate(rows):
        text = row[label_col] if label_col < len(row) else None
        if text in GUIDE_LABELS and text not in found:
            right = row[label_col + 1] if label_col + 1 < len(row) else None
            found[text] = Cell(right, f"Guide!{_letters(label_col + 1)}{n + 1}")
    for label in GUIDE_LABELS:
        if label not in found:
            errors.append(f"Guide: label {label} not found")
    blank = Cell(None, "Guide")
    return Domain(
        _required(found.get("domain", blank), errors) if "domain" in found else "",
        _required(found["template_version"], errors)
        if "template_version" in found
        else "",
        _severity(found["default_severity"], errors)
        if "default_severity" in found
        else None,
    )


def parse_workbook(path: Path) -> Workbook:
    """Read `Guide`, `Tables` and `Columns`; any other sheet (a legacy `Rules`) is ignored."""
    try:
        import fastexcel  # imported here so the records work without the xlsx extra
    except ImportError as e:
        raise missing("xlsx", "the business-contract workbook") from e
    try:
        book = fastexcel.read_excel(path)
    except fastexcel.FastExcelError as err:
        raise FloeIoError(
            f"xlsx_read_error: failed to open xlsx at {path}: {err}",
            code="io_read_error",
            rule="xlsx_read_error",
        ) from err
    absent = [s for s in ("Guide", "Tables", "Columns") if s not in book.sheet_names]
    if absent:
        raise invalid(
            [
                f"{s}: sheet not found (available: {', '.join(book.sheet_names)})"
                for s in absent
            ]
        )
    errors: list[str] = []
    domain = _guide(_sheet_rows(book, "Guide"), errors)
    table_cells = _data_rows(
        "Tables", _sheet_rows(book, "Tables"), TABLES_HEADERS, errors
    )
    column_cells = _data_rows(
        "Columns", _sheet_rows(book, "Columns"), COLUMNS_HEADERS, errors
    )
    tables, columns, warnings = build_rows(table_cells, column_cells, errors)
    if errors:
        raise invalid(errors)
    return Workbook(domain, tables, columns, warnings)
