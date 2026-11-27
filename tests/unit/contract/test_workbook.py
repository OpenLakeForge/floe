from dataclasses import fields

import pytest

from floe.contract.workbook import Cell, ColumnRow, TableRow, build_rows, invalid
from floe.errors import FloeConfigError

LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def _cells(sheet: str, row: int, record: type, **values: str) -> dict[str, Cell]:
    names = [f.name for f in fields(record)][1:]
    return {
        n: Cell(values.get(n), f"{sheet}!{LETTERS[i]}{row}")
        for i, n in enumerate(names)
    }


def table(row: int, **kw: str) -> dict[str, Cell]:
    return _cells("Tables", row, TableRow, **kw)


def column(row: int, table: str, name: str, **kw: str) -> dict[str, Cell]:
    kw = {"target_table": table, "target_column": name, "target_type": "string", **kw}
    return _cells("Columns", row, ColumnRow, **kw)


def errors_of(tables: list[dict[str, Cell]], columns: list[dict[str, Cell]]) -> str:
    errors: list[str] = []
    build_rows(tables, columns, errors)
    with pytest.raises(FloeConfigError) as e:
        raise invalid(errors)
    return str(e.value)


def test_rejects_unknown_target_table_with_cell_location() -> None:
    msg = errors_of(
        [table(3, target_table="orders")],
        [column(3, "orders", "id"), column(4, "ghost", "id")],
    )
    assert "Columns!A4: unknown target_table ghost" in msg


def test_preserves_composite_pk_row_order() -> None:
    errors: list[str] = []
    _, columns, _ = build_rows(
        [table(3, target_table="orders")],
        [
            column(3, "orders", "b", primary_key="X"),
            column(4, "orders", "z"),
            column(5, "orders", "a", primary_key=" x "),
        ],
        errors,
    )
    assert errors == []
    assert [c.target_column for c in columns] == ["b", "z", "a"]
    assert [c.target_column for c in columns if c.primary_key] == ["b", "a"]
    assert columns[2].refs["target_column"] == "Columns!C5"


def test_rejects_duplicate_target_column() -> None:
    msg = errors_of(
        [table(3, target_table="orders")],
        [column(3, "orders", "id"), column(4, "orders", "id")],
    )
    assert (
        "Columns!C4: duplicate target_column id in orders (first at Columns!C3)" in msg
    )


def test_builds_typed_records_and_splits_allowed_values() -> None:
    errors: list[str] = []
    tables, columns, warnings = build_rows(
        [table(3, target_table="orders", source_dataset="orders_export", owner="ann")],
        [column(3, "orders", "id", not_null="X", allowed_values="a | b|c", min="007")],
        errors,
    )
    assert (errors, warnings) == ([], ())
    assert tables[0].source_dataset == "orders_export"
    assert columns[0].allowed_values == ("a", "b", "c")
    assert (columns[0].not_null, columns[0].min) == (True, "007")


def test_aggregates_cell_errors() -> None:
    msg = errors_of(
        [
            table(3, target_table="a/b"),
            table(4, target_table="orders", cast_severity="fatal"),
            table(5, target_table="orders"),
        ],
        [
            {**column(3, "orders", "x"), "target_type": Cell(None, "Columns!D3")},
            column(4, "orders", "y", primary_key="yes", allowed_values="a||b"),
            column(5, "orders", "z", fk_target_table="orders"),
        ],
    )
    for expected in (
        "Tables!A3: target_table a/b cannot be a file name",
        "Tables!J4: severity must be one of warn|reject|abort, got fatal",
        "Tables!A5: duplicate target_table orders (first at Tables!A4)",
        "Columns!D3: required cell is blank",
        "Columns!F4: expected X or blank, got yes",
        "Columns!Q4: allowed_values has an empty item",
        "Columns!J5: fk_target_column is blank",
    ):
        assert expected in msg


def test_foreign_key_inside_is_checked_outside_is_a_warning() -> None:
    tables = [table(3, target_table="orders")]
    ok = [
        column(3, "orders", "id"),
        column(4, "orders", "ext", fk_target_table="elsewhere", fk_target_column="id"),
    ]
    errors: list[str] = []
    assert build_rows(tables, ok, errors)[2] == (
        "Columns!I4: foreign key points outside the workbook (elsewhere)",
    )
    bad = [
        *ok,
        column(5, "orders", "bad", fk_target_table="orders", fk_target_column="nope"),
    ]
    assert "Columns!J5: orders has no column nope" in errors_of(tables, bad)


def test_blank_required_dotted_name_and_padded_severity() -> None:
    errors: list[str] = []
    tables, _, _ = build_rows(
        [table(3, target_table="orders", cast_severity=" reject ")],
        [column(3, "orders", "id", target_type="  ")],
        errors,
    )
    assert errors == ["Columns!D3: required cell is blank"]
    assert tables[0].cast_severity == "reject"
    assert "Tables!A3: target_table sales.orders cannot be a file name" in errors_of(
        [table(3, target_table="sales.orders")], []
    )
