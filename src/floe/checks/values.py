"""Column value checks (#1251): out_of_range, not_allowed, pattern_mismatch, length."""

import math
from collections.abc import Mapping

import polars as pl

from floe.checks.cast import cast_column
from floe.checks.rows import RowCheck
from floe.config.types import ColumnConfig, EntityConfig, rule_level
from floe.errors import FloeConfigError

_RULES = ("out_of_range", "not_allowed", "pattern_mismatch", "length")

Value = int | float | str


def literal(column_type: str, value: Value) -> object:
    """A config literal as the column's typed value: it goes through the column's own cast, as a CSV value would.

    Manifest runs skip validation, so a value the cast refuses raises instead of dropping the check.
    """
    typed, mask = cast_column("v", column_type)
    row = pl.DataFrame({"v": [str(value)]}).select(typed, mask.alias("m")).row(0)
    if row[1] or (isinstance(row[0], float) and not math.isfinite(row[0])):
        raise FloeConfigError(
            f"{value} is not a valid {column_type} value", code="config_invalid_value"
        )
    return row[0]


def _literal(c: ColumnConfig, key: str, value: Value) -> object:
    try:
        return literal(c.column_type, value)
    except FloeConfigError as err:
        raise FloeConfigError(
            f"schema.columns[name={c.name}].{key}={err}", code="config_invalid_value"
        ) from err


def _outside(value: pl.Expr, lo: pl.Expr | None, hi: pl.Expr | None) -> pl.Expr:
    sides = [value < lo] if lo is not None else []
    if hi is not None:
        sides.append(value > hi)
    return pl.any_horizontal(sides)


def _shown(sides: Mapping[str, Value]) -> str:
    return ", ".join(f"{k}={v}" for k, v in sides.items())


def _masks(n: str, c: ColumnConfig, dtype: pl.DataType) -> dict[str, tuple[pl.Expr, str]]:  # fmt: skip
    """Per rule the column sets: its violation mask (null where the value is null) and its message."""
    col, out = pl.col(n), dict[str, tuple[pl.Expr, str]]()
    bounds = {k: v for k, v in (("min", c.min), ("max", c.max)) if v is not None}
    if bounds:
        lo, hi = (pl.lit(_literal(c, k, bounds[k]), dtype=dtype) if k in bounds else None for k in ("min", "max"))  # fmt: skip
        mask = _outside(col, lo, hi)
        out["out_of_range"] = mask | col.is_nan() if dtype.is_float() else mask, f"value out of range ({_shown(bounds)})"  # fmt: skip
    if c.allowed_values is not None:
        values = [_literal(c, f"allowed_values[{i}]", v) for i, v in enumerate(c.allowed_values)]  # fmt: skip
        out["not_allowed"] = ~col.is_in(values), "value not in allowed_values"
    if c.pattern is not None:
        out["pattern_mismatch"] = ~col.str.contains(rf"\A(?:{c.pattern})\z"), "value does not match pattern"  # fmt: skip
    sizes = {k: v for k, v in (("min_length", c.min_length), ("max_length", c.max_length)) if v is not None}  # fmt: skip
    if sizes:
        lo, hi = (pl.lit(sizes[k]) if k in sizes else None for k in ("min_length", "max_length"))  # fmt: skip
        out["length"] = _outside(col.str.len_chars(), lo, hi), f"length out of range ({_shown(sizes)})"  # fmt: skip
    return out


def value_checks(
    typed: pl.DataFrame, config: Mapping[str, ColumnConfig], entity: EntityConfig
) -> list[RowCheck]:
    """One check per rule and column that sets it, in rule order. A null passes, so a failed cast is not reported twice."""
    # ponytail: the literals are converted on every chunk; compute them once on `Plan` (run/file.py) if that shows in profiles
    masks = {n: _masks(n, c, typed.schema[n]) for n, c in config.items()}
    found = [(rule, n) for rule in _RULES for n in config if rule in masks[n]]
    if not found:
        return []
    frame = typed.select(masks[n][rule][0].fill_null(False).alias(str(i)) for i, (rule, n) in enumerate(found))  # fmt: skip
    return [
        RowCheck(
            rule,
            n,
            masks[n][rule][1],
            frame[str(i)],
            rule_level(entity, rule, config[n]),
        )
        for i, (rule, n) in enumerate(found)
    ]
