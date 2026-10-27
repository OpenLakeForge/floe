"""not_null, row error columns and the accept/reject split.

Parity with Rust 0.7: the conformance cases `casts`, `example/standalone`, `mismatch` and
`unique_pii` are run through the casts, not_null and the split, and the `__floe_row_index` and
`__floe_errors` they produce are compared with the recorded `expected/rejected/*.csv`. The
cases join the Python pass-list when the run pipeline (#655) lands. Unique checks are a separate
task (#642), so rows with a unique error are left out of the comparison.
"""

import json
from collections.abc import Sequence
from pathlib import Path

import polars as pl
import pytest

from floe.checks.cast import CAST_ERROR_MESSAGE, cast_column
from floe.checks.rows import (
    Formatter,
    RowCheck,
    error_column,
    error_entry,
    not_null_check,
    split_rows,
)

CONFORMANCE = Path(__file__).parents[3] / "conformance"

NOT_NULL_ID = '{"rule":"not_null","column":"id","message":"required value missing"}'
NOT_NULL_N = '{"rule":"not_null","column":"n","message":"required value missing"}'
CAST_N = '{"rule":"cast_error","column":"n","message":"invalid value for target type"}'


def check_case(
    case: str,
    input_file: str,
    expected_file: str,
    columns: Sequence[tuple[str, str]],
    required: Sequence[str],
    separator: str = ",",
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Runs the checks of a case; returns `(rejected, expected)` without the unique-error rows."""
    names = [name for name, _ in columns]
    read = pl.read_csv(
        CONFORMANCE / case / "in" / input_file, separator=separator, infer_schema=False
    )
    raw = read.select(
        pl.col(n) if n in read.columns else pl.lit(None, pl.String).alias(n)
        for n in names
    )
    typed_columns = [(n, kind) for n, kind in columns if kind != "string"]
    casts = [cast_column(n, kind) for n, kind in typed_columns]
    typed = raw.with_columns(c[0] for c in casts)
    masks = raw.select(c[1] for c in casts)
    checks = [not_null_check(typed, n) for n in required] + [
        RowCheck("cast_error", n, CAST_ERROR_MESSAGE, masks[f"_e_cast_{n}"])
        for n, _ in typed_columns
    ]
    _, rejected = split_rows(typed, checks)
    expected = pl.read_csv(
        CONFORMANCE / case / "expected" / "rejected" / expected_file, infer_schema=False
    )
    no_unique = pl.col("__floe_errors").str.contains('"rule":"unique"').not_()
    return rejected.filter(no_unique), expected.filter(no_unique)


def assert_same_errors(rejected: pl.DataFrame, expected: pl.DataFrame) -> None:
    assert rejected["__floe_errors"].to_list() == expected["__floe_errors"].to_list()
    assert rejected["__floe_row_index"].cast(pl.String).to_list() == (
        expected["__floe_row_index"].to_list()
    )


def test_casts_case_matches_rust_rejected_rows() -> None:
    columns = [
        ("id", "string"), ("qty", "int64"), ("price", "number"), ("active", "boolean"),
        ("day", "date"), ("ts", "datetime"), ("small", "int32"), ("note", "string"),
    ]  # fmt: skip
    rejected, expected = check_case(
        "casts", "casts.csv", "casts/part-00000.csv", columns, ["id", "qty"]
    )
    assert expected.height == 18
    assert_same_errors(rejected, expected)
    assert rejected["id"].to_list() == expected["id"].to_list()


def test_not_null_case_matches_rust_rejected_rows() -> None:
    rejected, expected = check_case(
        "example/standalone",
        "orders.csv",
        "orders/part-00000.csv",
        [("id", "string"), ("item", "string")],
        ["id"],
    )
    assert_same_errors(rejected, expected)
    assert rejected["__floe_errors"].to_list() == [f"[{NOT_NULL_ID}]"]


def test_primary_key_case_matches_rust_rejected_rows() -> None:
    columns = [
        ("tenant", "string"), ("code", "int64"), ("phone", "string"),
        ("score", "number"), ("signup", "date"), ("missing_col", "string"),
    ]  # fmt: skip
    rejected, expected = check_case(
        "mismatch",
        "customers.csv",
        "customers/part-00000.csv",
        columns,
        ["tenant", "code"],
        separator=";",
    )
    assert expected.height == 4
    assert_same_errors(rejected, expected)


def test_unique_pii_case_matches_rust_rejected_rows() -> None:
    columns = [
        ("customer_id", "int64"), ("email", "string"), ("region", "string"),
        ("order_no", "int64"), ("amount", "number"),
    ]  # fmt: skip
    rejected, expected = check_case(
        "unique_pii",
        "a.csv",
        "orders/part-00000.csv",
        columns,
        ["customer_id", "amount"],
    )
    assert expected["__floe_row_index"].to_list() == ["9", "10"]
    assert_same_errors(rejected, expected)


def frame_and_checks() -> tuple[pl.DataFrame, list[RowCheck]]:
    df = pl.DataFrame({"id": ["a", None, "c", None], "n": [1, 2, None, None]})
    cast_mask = pl.Series([False, False, True, True])
    return df, [
        not_null_check(df, "id"),
        not_null_check(df, "n"),
        RowCheck("cast_error", "n", CAST_ERROR_MESSAGE, cast_mask),
    ]


def test_errors_follow_check_order_and_accepted_rows_are_null() -> None:
    df, checks = frame_and_checks()
    assert error_column(df.height, checks).to_list() == [
        None,
        f"[{NOT_NULL_ID}]",
        f"[{NOT_NULL_N},{CAST_N}]",
        f"[{NOT_NULL_ID},{NOT_NULL_N},{CAST_N}]",
    ]


def test_split_rows_keeps_source_row_index_and_column_order() -> None:
    df, checks = frame_and_checks()
    accepted, rejected = split_rows(df, checks)
    assert accepted.to_dict(as_series=False) == {"id": ["a"], "n": [1]}
    assert rejected.columns == ["id", "n", "__floe_row_index", "__floe_errors"]
    assert rejected["__floe_row_index"].dtype == pl.UInt64
    assert rejected["__floe_row_index"].to_list() == [1, 2, 3]
    assert rejected["__floe_errors"].null_count() == 0


def test_split_rows_without_checks_or_rows() -> None:
    df = pl.DataFrame({"id": ["a", "b"]})
    accepted, rejected = split_rows(df, [])
    assert accepted.equals(df)
    assert rejected.columns == ["id", "__floe_row_index", "__floe_errors"]
    assert rejected.height == 0
    empty = df.clear()
    accepted, rejected = split_rows(empty, [not_null_check(empty, "id")])
    assert accepted.height == rejected.height == 0


def test_error_entry_escapes_like_rust() -> None:
    assert error_entry('we"ird\\', "c\n", "m\t\r") == (
        '{"rule":"we\\"ird\\\\","column":"c\\n","message":"m\\t\\r"}'
    )
    df = pl.DataFrame({'a"b': [None]})
    assert error_column(1, [not_null_check(df, 'a"b')]).to_list() == [
        '[{"rule":"not_null","column":"a\\"b","message":"required value missing"}]'
    ]


def test_cast_error_entry_matches_rust_json() -> None:
    assert error_entry("cast_error", "ts", CAST_ERROR_MESSAGE) == (
        '{"rule":"cast_error","column":"ts","message":"invalid value for target type"}'
    )
    assert error_entry("cast_error", 'we"ird\\n\n', CAST_ERROR_MESSAGE) == (
        '{"rule":"cast_error","column":"we\\"ird\\\\n\\n","message":"invalid value for target type"}'
    )


def test_error_entry_escapes_every_control_character() -> None:
    """#983: \\b, \\f and U+0001 in a column name must not make `__floe_errors` invalid JSON."""
    entry = error_entry("cast_error", "a\b\f\x01\t\ré", CAST_ERROR_MESSAGE)
    assert json.loads(entry)["column"] == "a\b\f\x01\t\ré"
    assert "é" in entry  # non-ASCII stays raw, as before


@pytest.mark.parametrize(
    ("formatter", "sources", "expected"),
    [
        ("csv", None, '"not_null,id,required value missing\\nunique,id,dup"'),
        ("text", None, '"not_null:id required value missing; unique:id dup"'),
        (
            "json",
            {"id": "ID"},
            (
                '[{"rule":"not_null","column":"id","source":"ID","message":"required value missing"},'
                '{"rule":"unique","column":"id","source":"ID","message":"dup"}]'
            ),
        ),
        (
            "csv",
            {"id": "ID"},
            '"not_null,id,ID,required value missing\\nunique,id,ID,dup"',
        ),
        # with a source map every csv line has the source field, empty when unmapped
        ("csv", {"x": "X"}, '"not_null,id,,required value missing\\nunique,id,,dup"'),
        (
            "text",
            {"id": "ID"},
            '"not_null:id source=ID required value missing; unique:id source=ID dup"',
        ),
    ],
)
def test_formatters_match_rust(
    formatter: Formatter, sources: dict[str, str] | None, expected: str
) -> None:
    df = pl.DataFrame({"id": [None]}, schema={"id": pl.String})
    checks = [
        not_null_check(df, "id"),
        RowCheck("unique", "id", "dup", pl.Series([True])),
    ]
    assert error_column(1, checks, formatter, sources).to_list() == [expected]


def test_csv_and_text_formatters_escape_the_json_string() -> None:
    checks = [RowCheck("r", 'a,"b"', "m", pl.Series([True]))]
    assert error_column(1, checks, "csv").to_list() == ['"r,\\"a,\\"\\"b\\"\\"\\",m"']
    assert error_column(1, checks, "text").to_list() == ['"r:a,\\"b\\" m"']
