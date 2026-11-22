"""Row predicates (#1252): one check per `schema.checks[]` entry, across the columns of a row."""

import operator
from collections.abc import Callable, Mapping

import polars as pl

from floe.checks.rows import RowCheck
from floe.checks.values import literal
from floe.config.types import ColumnConfig, EntityConfig, PredicateCondition
from floe.errors import FloeConfigError

_SYMBOLS = {"eq": "==", "ne": "!=", "lt": "<", "le": "<=", "gt": ">", "ge": ">=", "is_null": "is null", "not_null": "is not null"}  # fmt: skip
_COMPARE: dict[str, Callable[[pl.Expr, pl.Expr], pl.Expr]] = {
    "eq": operator.eq, "ne": operator.ne, "lt": operator.lt,
    "le": operator.le, "gt": operator.gt, "ge": operator.ge,
}  # fmt: skip


def _shown(c: PredicateCondition) -> str:
    right = c.right if c.value is None else c.value
    return f"{c.left} {_SYMBOLS[c.op]}" + ("" if c.op in ("is_null", "not_null") else f" {right}")  # fmt: skip


def _cond(c: PredicateCondition, where: str, typed: pl.DataFrame, columns: Mapping[str, ColumnConfig], runtime: Mapping[str, str]) -> pl.Expr:  # fmt: skip
    """The condition over the typed rows: null where a compared value is null."""

    def name(key: str, column: str | None) -> str:
        # manifest runs skip validation, so an unknown column fails here, before any row is routed
        if column is None or column not in runtime:
            raise FloeConfigError(f"{where}.{key}={column} is not a declared column", code="config_unknown_reference")  # fmt: skip
        return runtime[column]

    left = name("left", c.left)
    if c.op == "is_null":
        return pl.col(left).is_null()
    if c.op == "not_null":
        return pl.col(left).is_not_null()
    if c.value is None:
        return _COMPARE[c.op](pl.col(left), pl.col(name("right", c.right)))
    try:
        value = literal(columns[c.left].column_type, c.value)
    except FloeConfigError as err:
        raise FloeConfigError(f"{where}.value={err}", code="config_invalid_value") from err  # fmt: skip
    return _COMPARE[c.op](pl.col(left), pl.lit(value, dtype=typed.schema[left]))


def predicate_checks(typed: pl.DataFrame, entity: EntityConfig, runtime: Mapping[str, str]) -> list[RowCheck]:  # fmt: skip
    """One check per predicate, in declaration order. A predicate or `when` that is null passes the row, as in SQL `CHECK`.

    `runtime` maps a declared column name to its runtime name.
    """
    checks = entity.schema_.checks or []
    if not checks:
        return []
    columns = {c.name: c for c in entity.schema_.columns}
    masks: list[pl.Expr] = []
    for i, p in enumerate(checks):
        where = f"schema.checks[name={p.name}]"
        failed = ~_cond(p, where, typed, columns, runtime)
        if p.when is not None:
            failed = _cond(p.when, f"{where}.when", typed, columns, runtime) & failed
        masks.append(failed.fill_null(False).alias(str(i)))
    frame = typed.select(masks)
    return [
        RowCheck(
            p.name,
            runtime[p.left],
            _shown(p) + ("" if p.when is None else f" when {_shown(p.when)}"),
            frame[str(i)],
            p.severity or entity.policy.severity,
        )
        for i, p in enumerate(checks)
    ]
