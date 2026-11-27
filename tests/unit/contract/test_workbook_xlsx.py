import importlib
import sys
from collections.abc import Sequence
from datetime import date
from pathlib import Path

import pytest
from xlsx_book import Cell, Day, Num, Rows, write_book

from floe.contract.workbook import COLUMNS_HEADERS, TABLES_HEADERS, parse_workbook
from floe.errors import FloeConfigError

TEMPLATE = Path(__file__).parents[3] / "example/business-contract/domain-template.xlsx"
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


def col(table: str, name: str, **kw: Cell) -> dict[str, Cell]:
    return {"target_table": table, "target_column": name, "target_type": "string", **kw}


def book(
    tmp_path: Path, t: Rows | None = None, c: Rows | None = None, **extra: Rows
) -> Path:
    sheets = {
        "Guide": GUIDE,
        "Tables": t if t is not None else tables({"target_table": "orders"}),
        "Columns": c if c is not None else columns(),
        **extra,
    }
    return write_book(tmp_path / "b.xlsx", sheets)


def errors_of(path: Path) -> str:
    with pytest.raises(FloeConfigError) as e:
        parse_workbook(path)
    return str(e.value)


def test_missing_tables_sheet(tmp_path: Path) -> None:
    path = write_book(tmp_path / "b.xlsx", {"Guide": GUIDE, "Columns": columns()})
    assert "Tables: sheet not found" in errors_of(path)


def test_reads_text_cells_without_numeric_coercion(tmp_path: Path) -> None:
    c = columns(
        col("orders", "id", min="007", max=Num("0.1"), min_length=Num("5")),
        col("orders", "day", min=Day(date(2024, 1, 31)), max="2024-12-31"),
    )
    wb = parse_workbook(book(tmp_path, c=c))
    first, second = wb.columns
    assert (first.min, first.max, first.min_length) == ("007", "0.1", "5")
    assert (second.min, second.max) == ("2024-01-31", "2024-12-31")


def test_parse_without_technical_configuration(tmp_path: Path) -> None:
    t = tables(
        {"target_table": "orders", "source_dataset": "orders_export", "owner": "ann"}
    )
    c = columns(col("orders", "id", not_null=" x ", allowed_values="a | b|c"))
    wb = parse_workbook(book(tmp_path, t, c, Rules=[["ignored"]]))
    assert (wb.domain.name, wb.domain.template_version, wb.domain.default_severity) == (
        "sales",
        "1",
        "warn",
    )
    assert wb.tables[0].source_dataset == "orders_export"
    assert wb.columns[0].allowed_values == ("a", "b", "c")
    assert wb.columns[0].not_null is True
    assert wb.columns[0].refs["target_column"] == "Columns!C3"


def test_blank_rows_are_ignored_and_sheet_order_does_not_matter(tmp_path: Path) -> None:
    path = write_book(
        tmp_path / "b.xlsx",
        {
            "Columns": columns(),
            "Tables": tables({}, {"target_table": "orders"}),
            "Guide": GUIDE,
        },
    )
    assert [r.target_table for r in parse_workbook(path).tables] == ["orders"]


def test_duplicate_and_missing_headers_and_guide(tmp_path: Path) -> None:
    t = [["target_table", "owner", "owner"], ["orders"]]
    path = write_book(
        tmp_path / "b.xlsx",
        {"Guide": [["domain", "sales"]], "Tables": t, "Columns": columns()},
    )
    msg = errors_of(path)
    assert "Tables!C1: duplicate header owner (first at B1)" in msg
    assert "Tables: missing header(s): source_dataset" in msg
    assert "Guide: label template_version not found" in msg


def test_cell_errors_name_the_excel_cell(tmp_path: Path) -> None:
    c = columns(col("ghost", "id"), col("orders", "n", primary_key="yes"))
    msg = errors_of(book(tmp_path, c=c))
    assert "Columns!A3: unknown target_table ghost" in msg
    assert "Columns!F4: expected X or blank, got yes" in msg


def test_reads_the_shipped_template() -> None:
    wb = parse_workbook(TEMPLATE)
    assert wb.domain.name == "sales"
    assert [t.target_table for t in wb.tables][:2] == ["customers", "orders"]
    assert wb.columns


def test_records_import_without_the_xlsx_extra(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setitem(sys.modules, "fastexcel", None)
    monkeypatch.delitem(sys.modules, "floe.contract.workbook")
    workbook = importlib.import_module("floe.contract.workbook")
    errors: list[str] = []
    assert workbook.build_rows([], [], errors) == ((), (), ())
    with pytest.raises(FloeConfigError, match=r"floe\[xlsx\]"):
        workbook.parse_workbook(book(tmp_path))
