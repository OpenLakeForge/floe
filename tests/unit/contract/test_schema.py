from dataclasses import fields
from typing import Any

from floe.contract.schema import compile_schema
from floe.contract.workbook import Cell, ColumnRow, TableRow, build_rows

LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def _cells(sheet: str, row: int, record: type, **values: str) -> dict[str, Cell]:
    names = [f.name for f in fields(record)][1:]
    return {
        n: Cell(values.get(n), f"{sheet}!{LETTERS[i]}{row}")
        for i, n in enumerate(names)
    }


def col(row: int, name: str, **kw: str) -> dict[str, Cell]:
    kw = {
        "target_table": "orders",
        "target_column": name,
        "target_type": "string",
        **kw,
    }
    return _cells("Columns", row, ColumnRow, **kw)


def compile_(
    columns: list[dict[str, Cell]], **table_kw: str
) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []
    tables, rows, _ = build_rows(
        [_cells("Tables", 3, TableRow, target_table="orders", **table_kw)],
        columns,
        errors,
    )
    assert errors == []
    return compile_schema("sales", tables[0], rows, errors), errors


def test_composite_pk_preserves_order() -> None:
    out, errors = compile_(
        [
            col(3, "b", primary_key="X"),
            col(4, "z"),
            col(5, "a", primary_key="X", not_null=""),
        ]
    )
    assert errors == []
    schema = out["schema"]
    assert schema["primary_key"] == ["b", "a"]
    nullable = {c["name"]: c["nullable"] for c in schema["columns"]}
    assert nullable == {"b": False, "z": True, "a": False}


def test_composite_unique_group() -> None:
    out, errors = compile_(
        [
            col(3, "id"),
            col(4, "a", unique_group="g1"),
            col(5, "email", unique="X"),
            col(6, "b", unique_group="g1"),
        ]
    )
    assert errors == []
    schema = out["schema"]
    assert schema["unique_keys"] == [["a", "b"], ["email"]]
    assert all("unique" not in c for c in schema["columns"])


def test_source_to_target_name_mapping() -> None:
    out, errors = compile_(
        [
            col(3, "order_id", source_column="COD_CDE", target_type="int64"),
            col(4, "amount", source_column="MNT_TTC", target_type="decimal(18,2)"),
            col(5, "same", source_column="same"),
            col(6, "blank", source_column="  "),
        ]
    )
    assert errors == []
    schema = out["schema"]
    assert schema["columns"] == [
        {"name": "order_id", "type": "int64", "source": "COD_CDE", "nullable": True},
        {
            "name": "amount",
            "type": "decimal(18,2)",
            "source": "MNT_TTC",
            "nullable": True,
        },
        {"name": "same", "type": "string", "nullable": True},
        {"name": "blank", "type": "string", "nullable": True},
    ]


def test_source_dataset_becomes_resource() -> None:
    out, _ = compile_(
        [col(3, "id")],
        source_dataset="ORDERS_EXT",
        owner="team-a",
        description="Orders",
        business_name="Commandes",
    )
    assert out["name"] == "orders"
    assert out["domain"] == "sales"
    assert out["source"] == {"resource": "ORDERS_EXT"}
    assert out["metadata"] == {"owner": "team-a", "description": "Orders"}


def test_fk_does_not_emit_key() -> None:
    out, _ = compile_(
        [
            col(3, "cust", fk_target_table="orders", fk_target_column="cust"),
            col(4, "x", business_label="Label", not_null="X", min="1"),
        ],
        grain="one row per order",
    )
    assert set(out) == {"name", "domain", "schema"}
    schema = out["schema"]
    assert set(schema) == {"columns"}
    assert all(
        set(c) <= {"name", "type", "source", "nullable"} for c in schema["columns"]
    )


def _errors(columns: list[dict[str, Cell]]) -> str:
    errors: list[str] = []
    tables, rows, _ = build_rows(
        [_cells("Tables", 3, TableRow, target_table="orders")], columns, errors
    )
    assert errors == []
    compile_schema("sales", tables[0], rows, errors)
    return "\n".join(errors)


def test_invalid_type_reports_cell() -> None:
    msg = _errors(
        [
            col(3, "a", target_type="varchar"),
            col(4, "b", target_type="decimal"),
            col(5, "c", target_type="decimal(40,2)"),
            col(6, "d", target_type="decimal(18,2)"),
        ]
    )
    assert "Columns!D3: target_type varchar is not a Floe type" in msg
    assert "Columns!D4: target_type decimal needs a precision and scale" in msg
    assert "Columns!D5:" in msg
    assert "Columns!D6" not in msg


def test_redundant_keys_are_rejected() -> None:
    msg = _errors(
        [
            col(3, "a", primary_key="X", unique="X"),
            col(4, "b", unique="X"),
            col(5, "c", unique_group="g"),
            col(6, "d", unique_group="g"),
            col(7, "e", unique="X", unique_group="h"),
            col(8, "f", unique="X"),
            col(9, "f2", unique_group="h"),
        ]
    )
    assert "Columns!G3: unique key ['a'] is identical to the primary key" in msg
    assert "Columns!G7" in msg and "already in unique_group" in msg
    assert "Columns!G4" not in msg
