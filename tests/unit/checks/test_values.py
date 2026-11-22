"""Column value checks (#1404): masks, messages, severity and the `literal()` conversion."""

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from floe.checks.cast import cast_column
from floe.checks.values import literal, value_checks
from floe.config.parse import parse_config
from floe.config.types import ColumnConfig, EntityConfig
from floe.errors import FloeConfigError

CONFORMANCE = Path(__file__).parents[3] / "conformance"


def _entity(**policy: Any) -> EntityConfig:
    entity = parse_config(CONFORMANCE / "example/standalone/config.yml").entities[0]
    return entity.model_copy(update={"policy": entity.policy.model_copy(update=policy)})  # fmt: skip


def _typed(column_type: str, values: list[str | None]) -> pl.DataFrame:
    typed, _ = cast_column("v", column_type)
    return pl.DataFrame({"v": values}, schema={"v": pl.String}).select(typed)


def _masks(column_type: str, values: list[str | None], **keys: Any) -> dict[str, list[bool]]:  # fmt: skip
    column = ColumnConfig(name="v", source=None, column_type=column_type, nullable=None, **keys)  # fmt: skip
    checks = value_checks(_typed(column_type, values), {"v": column}, _entity())
    return {c.rule: c.mask.to_list() for c in checks}


def test_out_of_range_is_inclusive_null_passes_and_nan_fails() -> None:
    values = ["5", "0", "10", "-1", "11", None, "NaN"]
    masks = _masks("float64", values, min=0, max=10)
    assert masks == {"out_of_range": [False, False, False, True, True, False, True]}


@pytest.mark.parametrize(
    ("keys", "expected"),
    [
        ({"min": 0}, [False, False, False, True, False, False]),
        ({"max": 10}, [False, False, False, False, True, False]),
    ],
)
def test_out_of_range_checks_only_the_sides_set(keys: dict[str, Any], expected: list[bool]) -> None:  # fmt: skip
    assert _masks("int64", ["5", "0", "10", "-1", "11", None], **keys) == {"out_of_range": expected}  # fmt: skip


@pytest.mark.parametrize(
    ("column_type", "lo", "hi", "values"),
    [
        ("decimal(10,2)", 0, "99.99", ["1.23", "0", "99.99", "-0.01", "100.00", None]),
        ("date", "2000-01-01", "2000-12-31", ["2000-06-01", "2000-01-01", "2000-12-31", "1999-12-31", "2001-01-01", None]),
        ("datetime", "2024-01-01T10:00:00+02:00", "2024-01-01T12:00:00Z", ["2024-01-01T09:00:00Z", "2024-01-01T08:00:00Z", "2024-01-01T12:00:00Z", "2024-01-01T07:59:59Z", "2024-01-01T12:00:01Z", None]),
    ],
)  # fmt: skip
def test_out_of_range_on_decimal_date_and_datetime(column_type: str, lo: object, hi: object, values: list[str | None]) -> None:  # fmt: skip
    masks = _masks(column_type, values, min=lo, max=hi)
    assert masks == {"out_of_range": [False, False, False, True, True, False]}


def test_not_allowed_is_exact_and_case_sensitive() -> None:
    masks = _masks("string", ["FR", "fr", " FR", "XX", None], allowed_values=["FR", "DE"])  # fmt: skip
    assert masks == {"not_allowed": [False, True, True, True, False]}
    assert _masks("int32", ["1", "3", None], allowed_values=[1, 2]) == {"not_allowed": [False, True, False]}  # fmt: skip


def test_pattern_is_a_full_match() -> None:
    masks = _masks("string", ["FR", "FRA", "xFR", "F", None], pattern="[A-Z]{2}|[A-Z]")  # fmt: skip
    assert masks == {"pattern_mismatch": [False, True, True, False, False]}


def test_length_counts_code_points() -> None:
    masks = _masks("string", ["ab", "é€", "a", "abc", "", None], min_length=2, max_length=2)  # fmt: skip
    assert masks == {"length": [False, False, True, True, True, False]}


def test_a_failed_cast_is_not_reported_again() -> None:
    assert _masks("int64", ["x", "99"], max=10) == {"out_of_range": [False, True]}


def test_checks_come_in_rule_order_with_config_messages_and_their_severity() -> None:
    column = ColumnConfig(
        name="c", source=None, column_type="string", nullable=None,
        allowed_values=["FR"], pattern="[A-Z]{2}", min_length=2, max_length=2,
        rule_severity={"pattern_mismatch": "reject"},
    )  # fmt: skip
    number = ColumnConfig(name="n", source=None, column_type="int64", nullable=None, min=0)  # fmt: skip
    typed = pl.DataFrame({"c": ["FR"], "n": [1]})
    checks = value_checks(typed, {"c": column, "n": number}, _entity(severity="warn"))
    assert [(c.rule, c.column, c.message, c.severity) for c in checks] == [
        ("out_of_range", "n", "value out of range (min=0)", "warn"),
        ("not_allowed", "c", "value not in allowed_values", "warn"),
        ("pattern_mismatch", "c", "value does not match pattern", "reject"),
        ("length", "c", "length out of range (min_length=2, max_length=2)", "warn"),
    ]
    both = ColumnConfig(name="n", source=None, column_type="int64", nullable=None, min=0, max=100)  # fmt: skip
    [check] = value_checks(typed, {"n": both}, _entity())
    assert check.message == "value out of range (min=0, max=100)"


def test_no_value_keys_no_checks() -> None:
    column = ColumnConfig(name="v", source=None, column_type="int64", nullable=None)
    assert value_checks(_typed("int64", ["1"]), {"v": column}, _entity()) == []


@pytest.mark.parametrize(
    ("column_type", "value", "expected"),
    [
        ("date", "2024-01-01", date(2024, 1, 1)),
        ("datetime", "2024-01-01T10:00:00+02:00", datetime(2024, 1, 1, 8, 0)),
        ("decimal(10,2)", "1.23", Decimal("1.23")),
        ("int64", 5, 5),
        ("float64", 1.5, 1.5),
    ],
)
def test_literal_converts_through_the_column_cast(column_type: str, value: Any, expected: object) -> None:  # fmt: skip
    assert literal(column_type, value) == expected


@pytest.mark.parametrize(
    ("column_type", "value"),
    [
        ("decimal(10,2)", "1.234"),
        ("int64", "5.5"),
        ("float64", float("nan")),
        ("float64", "inf"),
        ("float64", float("-inf")),
        ("date", "2024-13-01"),
    ],
)
def test_literal_refuses_a_value_the_column_would_not_accept(column_type: str, value: Any) -> None:  # fmt: skip
    with pytest.raises(FloeConfigError) as e:
        literal(column_type, value)
    assert e.value.code == "config_invalid_value"
    assert str(e.value) == f"{value} is not a valid {column_type} value"


def test_a_bad_bound_names_its_column_and_key() -> None:
    column = ColumnConfig(name="d", source=None, column_type="date", nullable=None, min="2024-13-01")  # fmt: skip
    with pytest.raises(FloeConfigError) as e:
        value_checks(_typed("date", ["2024-01-01"]).rename({"v": "d"}), {"d": column}, _entity())  # fmt: skip
    assert e.value.code == "config_invalid_value"
    assert str(e.value) == "schema.columns[name=d].min=2024-13-01 is not a valid date value"  # fmt: skip
    column = ColumnConfig(name="d", source=None, column_type="int64", nullable=None, allowed_values=["1", "x"])  # fmt: skip
    with pytest.raises(FloeConfigError, match=r"^schema\.columns\[name=d\]\.allowed_values\[1\]=x is not a valid int64 value$"):  # fmt: skip
        value_checks(pl.DataFrame({"d": [1]}), {"d": column}, _entity())
