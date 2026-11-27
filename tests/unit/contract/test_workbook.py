from collections.abc import Sequence
from datetime import date
from pathlib import Path

import pytest
from xlsx_book import Cell, Day, Num, Rows, write_book

from floe.contract.workbook import COLUMNS_HEADERS, TABLES_HEADERS, parse_workbook
from floe.errors import FloeConfigError

GUIDE: Rows = [
    ["Instructions"],
    ["domain", "sales"],
    ["template_version", "1"],
    ["default_severity", "warn"],
]


def _row(headers: tuple[str, ...], **cells: Cell) -> list[Cell]:
    return [cells.get(h) for h in headers]


def tables(*rows: dict[str, Cell]) -> Rows:
    group: Sequence[Cell] = ["01 / TABLE"] + [None] * 9
    return [group, list(TABLES_HEADERS)] + [_row(TABLES_HEADERS, **r) for r in rows]


def columns(*rows: dict[str, Cell]) -> Rows:
    group: Sequence[Cell] = ["01 / NAMES & TYPES"] + [None] * 18
    return [group, list(COLUMNS_HEADERS)] + [_row(COLUMNS_HEADERS, **r) for r in rows]


def book(
    tmp_path: Path,
    t: Rows | None = None,
    c: Rows | None = None,
    **extra: list[list[Cell]],
) -> Path:
    sheets = {
        "Guide": GUIDE,
        "Tables": t if t is not None else tables({"target_table": "orders"}),
        "Columns": c if c is not None else columns(),
        **extra,
    }
    return write_book(tmp_path / "b.xlsx", sheets)


def col(table: str, name: str, **kw: Cell) -> dict[str, Cell]:
    return {"target_table": table, "target_column": name, "target_type": "string", **kw}


def errors_of(path: Path) -> str:
    with pytest.raises(FloeConfigError) as e:
        parse_workbook(path)
    return str(e.value)


def test_missing_tables_sheet(tmp_path: Path) -> None:
    path = write_book(tmp_path / "b.xlsx", {"Guide": GUIDE, "Columns": columns()})
    assert "Tables: sheet not found" in errors_of(path)


def test_rejects_unknown_target_table_with_cell_location(tmp_path: Path) -> None:
    c = columns(col("orders", "id"), col("ghost", "id"))
    assert "Columns!A4: unknown target_table ghost" in errors_of(book(tmp_path, c=c))


def test_preserves_composite_pk_row_order(tmp_path: Path) -> None:
    c = columns(
        col("orders", "b", primary_key="X"),
        col("orders", "z"),
        col("orders", "a", primary_key=" x "),
    )
    wb = parse_workbook(book(tmp_path, c=c))
    assert [r.target_column for r in wb.columns] == ["b", "z", "a"]
    assert [r.target_column for r in wb.columns if r.primary_key] == ["b", "a"]
    assert wb.columns[2].refs["target_column"] == "Columns!C5"


def test_reads_text_cells_without_numeric_coercion(tmp_path: Path) -> None:
    c = columns(
        col("orders", "id", min="007", max=Num("0.1"), min_length=Num("5")),
        col("orders", "day", min=Day(date(2024, 1, 31)), max="2024-12-31"),
    )
    wb = parse_workbook(book(tmp_path, c=c))
    assert (wb.columns[0].min, wb.columns[0].max, wb.columns[0].min_length) == (
        "007",
        "0.1",
        "5",
    )
    assert (wb.columns[1].min, wb.columns[1].max) == ("2024-01-31", "2024-12-31")


def test_rejects_duplicate_target_column(tmp_path: Path) -> None:
    c = columns(col("orders", "id"), col("orders", "id"))
    assert (
        "Columns!C4: duplicate target_column id in orders (first at Columns!C3)"
        in errors_of(book(tmp_path, c=c))
    )


def test_parse_without_technical_configuration(tmp_path: Path) -> None:
    t = tables(
        {"target_table": "orders", "source_dataset": "orders_export", "owner": "ann"}
    )
    c = columns(col("orders", "id", not_null="X", allowed_values="a | b|c"))
    wb = parse_workbook(book(tmp_path, t, c, Rules=[["ignored"]]))
    assert (wb.domain.name, wb.domain.template_version, wb.domain.default_severity) == (
        "sales",
        "1",
        "warn",
    )
    assert wb.tables[0].source_dataset == "orders_export"
    assert wb.columns[0].allowed_values == ("a", "b", "c")
    assert wb.columns[0].not_null is True


def test_blank_rows_are_ignored_and_sheet_order_does_not_matter(tmp_path: Path) -> None:
    t = tables({}, {"target_table": "orders"})
    path = write_book(
        tmp_path / "b.xlsx", {"Columns": columns(), "Tables": t, "Guide": GUIDE}
    )
    assert [r.target_table for r in parse_workbook(path).tables] == ["orders"]


def test_aggregates_cell_errors(tmp_path: Path) -> None:
    t = tables(
        {"target_table": "a/b"},
        {"target_table": "orders", "cast_severity": "fatal"},
        {"target_table": "orders"},
    )
    c = columns(
        {
            "target_table": "orders",
            "primary_key": "yes",
            "allowed_values": "a||b",
            "fk_target_table": "orders",
        },
    )
    msg = errors_of(book(tmp_path, t, c))
    for expected in (
        "Tables!A3: target_table a/b cannot be a file name",
        "Tables!J4: severity must be one of warn|reject|abort, got fatal",
        "Tables!A5: duplicate target_table orders (first at Tables!A4)",
        "Columns!C3: required cell is blank",
        "Columns!D3: required cell is blank",
        "Columns!F3: expected X or blank, got yes",
        "Columns!Q3: allowed_values has an empty item",
        "Columns!J3: fk_target_column is blank",
    ):
        assert expected in msg


def test_duplicate_and_missing_headers(tmp_path: Path) -> None:
    t = [["target_table", "owner", "owner"], ["orders"]]
    msg = errors_of(book(tmp_path, t))
    assert "Tables!C1: duplicate header owner (first at B1)" in msg
    assert "Tables: missing header(s): source_dataset" in msg


def test_foreign_key_inside_is_checked_outside_is_a_warning(tmp_path: Path) -> None:
    c = columns(
        col("orders", "id"),
        col("orders", "ext", fk_target_table="elsewhere", fk_target_column="id"),
        col("orders", "bad", fk_target_table="orders", fk_target_column="nope"),
    )
    assert "Columns!J5: orders has no column nope" in errors_of(book(tmp_path, c=c))
    wb = parse_workbook(book(tmp_path, c=c[:4]))
    assert wb.warnings == (
        "Columns!I4: foreign key points outside the workbook (elsewhere)",
    )
