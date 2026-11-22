"""Row predicates (#1406): masks, `when` gating, error entries, severity and the manifest-run failures."""

import json
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from floe.checks.predicates import predicate_checks
from floe.checks.rows import error_column
from floe.config.parse import parse_config
from floe.config.types import (
    ColumnConfig,
    EntityConfig,
    PredicateCheck,
    PredicateCondition,
    max_level,
)
from floe.config.validate import _level_source  # pyright: ignore[reportPrivateUsage] -- the scope this issue adds
from floe.errors import FloeConfigError

CONFORMANCE = Path(__file__).parents[3] / "conformance"
COLUMNS = {"start_date": "date", "end_date": "date", "status": "string", "amount": "int64"}  # fmt: skip
FRAME = pl.DataFrame(
    {
        "start_date": ["2024-01-01", "2024-01-05", None, "2024-01-01"],
        "end_date": ["2024-01-02", "2024-01-01", "2024-01-01", None],
        "status": ["open", "cancelled", "cancelled", None],
        "amount": [5, 3, 0, 7],
    }
).with_columns(pl.col("start_date", "end_date").str.to_date())


def _entity(*checks: PredicateCheck, severity: str = "reject") -> EntityConfig:
    entity = parse_config(CONFORMANCE / "example/standalone/config.yml").entities[0]
    columns = [ColumnConfig(name=n, source=None, column_type=t, nullable=None) for n, t in COLUMNS.items()]  # fmt: skip
    schema = entity.schema_.model_copy(update={"columns": columns, "checks": list(checks) or None})  # fmt: skip
    policy = entity.policy.model_copy(update={"severity": severity})
    return entity.model_copy(update={"schema_": schema, "policy": policy})


def _check(**keys: Any) -> PredicateCheck:
    return PredicateCheck.model_validate({"name": "p", **keys})


def _masks(*checks: PredicateCheck) -> list[list[bool]]:
    runtime = {n: n for n in COLUMNS}
    return [c.mask.to_list() for c in predicate_checks(FRAME, _entity(*checks), runtime)]  # fmt: skip


def test_only_a_false_predicate_fails_and_null_passes() -> None:
    check = _check(name="end_after_start", left="end_date", op="ge", right="start_date")  # fmt: skip
    assert _masks(check) == [[False, True, False, False]]


def test_when_gates_the_predicate_and_a_null_or_false_when_passes() -> None:
    when = PredicateCondition(left="status", op="eq", value="cancelled")
    assert _masks(_check(left="amount", op="eq", value=0, when=when)) == [[False, True, False, False]]  # fmt: skip


def test_unary_operators() -> None:
    when = PredicateCondition(left="status", op="not_null")
    checks = (_check(left="start_date", op="not_null", when=when), _check(left="status", op="is_null"))  # fmt: skip
    assert _masks(*checks) == [[False, False, True, False], [True, True, True, False]]


def test_the_error_entry_names_the_check_the_left_column_and_the_predicate() -> None:
    check = _check(name="end_after_start", left="end_date", op="ge", right="start_date")  # fmt: skip
    runtime = {n: n for n in COLUMNS}
    errors = error_column(
        FRAME.height, predicate_checks(FRAME, _entity(check), runtime)
    )
    assert json.loads(errors[1]) == [{"rule": "end_after_start", "column": "end_date", "message": "end_date >= start_date"}]  # fmt: skip


def test_the_message_renders_when_and_the_column_is_the_runtime_name() -> None:
    check = _check(left="status", op="not_null", when=PredicateCondition(left="amount", op="gt", value=4))  # fmt: skip
    runtime = {n: n.upper() for n in COLUMNS}
    [result] = predicate_checks(FRAME.rename(runtime), _entity(check), runtime)
    assert (result.column, result.message) == ("STATUS", "status is not null when amount > 4")  # fmt: skip
    assert result.mask.to_list() == [False, False, False, True]


def test_severity_falls_back_to_the_policy() -> None:
    checks = (_check(left="amount", op="gt", value=0), _check(left="amount", op="gt", value=0, severity="warn"))  # fmt: skip
    entity = _entity(*checks, severity="reject")
    assert [c.severity for c in predicate_checks(FRAME, entity, {n: n for n in COLUMNS})] == ["reject", "warn"]  # fmt: skip


@pytest.mark.parametrize(
    ("keys", "message", "code"),
    [
        (
            {"left": "nope", "op": "is_null"},
            "schema.checks[name=p].left=nope is not a declared column",
            "config_unknown_reference",
        ),
        (
            {"left": "amount", "op": "lt", "right": "nope"},
            "schema.checks[name=p].right=nope is not a declared column",
            "config_unknown_reference",
        ),
        (
            {"left": "amount", "op": "lt", "value": "x"},
            "schema.checks[name=p].value=x is not a valid int64 value",
            "config_invalid_value",
        ),
        (
            {
                "left": "amount",
                "op": "lt",
                "value": 1,
                "when": PredicateCondition(left="nope", op="is_null"),
            },
            "schema.checks[name=p].when.left=nope is not a declared column",
            "config_unknown_reference",
        ),
    ],
)
def test_an_unknown_column_or_a_bad_literal_raises(keys: dict[str, Any], message: str, code: str) -> None:  # fmt: skip
    with pytest.raises(FloeConfigError) as e:
        _masks(_check(**keys))
    assert (str(e.value), e.value.code) == (message, code)


def test_a_predicate_severity_counts_in_max_level_and_names_its_scope() -> None:
    entity = _entity(_check(name="big", left="amount", op="lt", value=9, severity="abort"), severity="warn")  # fmt: skip
    assert max_level(entity) == "abort"
    assert _level_source(entity, ("abort",)) == "schema.checks[name=big].severity=abort"
