from dataclasses import fields
from typing import Any

from floe.config.types import ColumnConfig
from floe.config.validate import check_value_checks
from floe.contract.checks import compile_checks
from floe.contract.workbook import Cell, ColumnRow, TableRow, build_rows

LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def _cells(sheet: str, row: int, record: type, **values: str) -> dict[str, Cell]:
    names = [f.name for f in fields(record)][1:]
    return {
        n: Cell(values.get(n), f"{sheet}!{LETTERS[i]}{row}")
        for i, n in enumerate(names)
    }


def compile_one(
    columns: list[dict[str, str]],
    table: dict[str, str] | None = None,
    default: str = "reject",
) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []
    t_cells = _cells("Tables", 2, TableRow, target_table="orders", **(table or {}))
    c_cells = [
        _cells(
            "Columns",
            i + 2,
            ColumnRow,
            **{"target_table": "orders", "target_type": "string", **c},
        )
        for i, c in enumerate(columns)
    ]
    tables, cols, _ = build_rows([t_cells], c_cells, errors)
    assert errors == []
    return compile_checks(tables[0], cols, default, errors), errors


def test_default_severity_inheritance() -> None:
    out, _ = compile_one([{"target_column": "id"}], default="warn")
    assert out["policy"] == {"severity": "warn"}
    out, _ = compile_one(
        [{"target_column": "id"}], {"default_severity": "abort"}, "warn"
    )
    assert out["policy"]["severity"] == "abort"
    assert out["columns"] == {}


def test_unique_severity_is_entity_wide() -> None:
    out, _ = compile_one(
        [
            {"target_column": "id", "primary_key": "X"},
            {"target_column": "sku", "unique": "X"},
        ],
        {"unique_severity": "warn"},
    )
    assert out["policy"]["rule_severity"] == {"unique": "warn"}
    assert all(
        "unique" not in c.get("rule_severity", {}) for c in out["columns"].values()
    )


def test_cast_severity_maps_to_cast_error() -> None:
    out, _ = compile_one(
        [{"target_column": "id"}],
        {"cast_severity": "abort", "not_null_severity": "warn"},
    )
    assert out["policy"]["rule_severity"] == {"cast_error": "abort", "not_null": "warn"}


def test_pk_not_null_without_explicit_x() -> None:
    out, errors = compile_one(
        [{"target_column": "id", "primary_key": "X", "not_null_severity": "abort"}]
    )
    assert errors == []
    assert out["columns"]["id"] == {"rule_severity": {"not_null": "abort"}}


def test_not_null_severity_on_optional_column_reports_cell() -> None:
    _, errors = compile_one([{"target_column": "id", "not_null_severity": "warn"}])
    assert len(errors) == 1 and errors[0].startswith("Columns!L2:")


def test_value_severity_only_for_enabled_checks() -> None:
    out, errors = compile_one(
        [
            {
                "target_column": "age",
                "target_type": "int64",
                "min": "0",
                "max": "120",
                "value_severity": "warn",
            },
            {"target_column": "code", "pattern": "[A-Z]+", "value_severity": "abort"},
        ]
    )
    assert errors == []
    assert out["columns"]["age"] == {
        "min": 0,
        "max": 120,
        "rule_severity": {"out_of_range": "warn"},
    }
    assert out["columns"]["code"] == {
        "pattern": "[A-Z]+",
        "rule_severity": {"pattern_mismatch": "abort"},
    }
    _, errors = compile_one([{"target_column": "x", "value_severity": "warn"}])
    assert len(errors) == 1 and errors[0].startswith("Columns!S2:")


def test_allowed_values_typed_per_target_type() -> None:
    out, _ = compile_one(
        [
            {"target_column": "n", "target_type": "int64", "allowed_values": "1|2"},
            {"target_column": "s", "allowed_values": "a|b", "value_severity": "warn"},
        ]
    )
    assert out["columns"]["n"]["allowed_values"] == [1, 2]
    assert out["columns"]["s"] == {
        "allowed_values": ["a", "b"],
        "rule_severity": {"not_allowed": "warn"},
    }
    _, errors = compile_one(
        [{"target_column": "f", "target_type": "float64", "allowed_values": "1|2"}]
    )
    assert len(errors) == 1 and errors[0].startswith("Columns!Q2:")


def test_length_bounds() -> None:
    out, errors = compile_one(
        [
            {
                "target_column": "name",
                "min_length": "1",
                "max_length": "50",
                "value_severity": "reject",
            }
        ]
    )
    assert errors == []
    col = out["columns"]["name"]
    assert (col["min_length"], col["max_length"]) == (1, 50)
    assert col["rule_severity"] == {"length": "reject"}
    check_value_checks(
        "orders",
        ColumnConfig(
            name="name",
            source=None,
            column_type="string",
            nullable=None,
            min_length=1,
            max_length=50,
        ),
    )
    _, errors = compile_one([{"target_column": "n", "min_length": "x"}])
    assert len(errors) == 1 and errors[0].startswith("Columns!O2:")


def test_invalid_rule_reports_column_cell() -> None:
    cases = [
        ({"target_type": "int64", "min": "9", "max": "1"}, "Columns!M2:"),
        ({"pattern": "("}, "Columns!R2:"),
        ({"target_type": "string", "min": "1"}, "Columns!M2:"),
        ({"target_type": "int64", "min": "abc"}, "Columns!M2:"),
    ]
    for cells, ref in cases:
        _, errors = compile_one([{"target_column": "c", **cells}])
        assert len(errors) == 1, cells
        assert errors[0].startswith(ref), errors
