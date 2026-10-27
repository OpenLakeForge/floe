"""Casts and cast-mismatch errors.

Expected values come from the Rust 0.7 conformance cases `casts`, `casts_date`, `casts_datetime` and
`casts_coerce`, except the `D9_*` tables: inputs where Rust 0.7 misreads the value and Floe Python
deliberately differs (intentional delta D9, master issue #619).
"""

import math
from collections.abc import Sequence
from datetime import date, datetime
from decimal import Decimal

import polars as pl
import pytest

from floe.checks.cast import cast_column

Typed = list[object]


def run(
    values: Sequence[str | None], column_type: str, cast_mode: str = "strict"
) -> tuple[Typed, list[bool]]:
    df = pl.DataFrame({"v": values}, schema={"v": pl.String})
    typed, mask = cast_column("v", column_type, cast_mode)
    out = df.select(typed, mask)
    return out["v"].to_list(), out["_e_cast_v"].to_list()


def check_ok(column_type: str, cases: Sequence[tuple[str, object]]) -> None:
    typed, errors = run([raw for raw, _ in cases], column_type)
    assert typed == [expected for _, expected in cases]
    assert errors == [False] * len(cases)


def check_rejected(column_type: str, inputs: Sequence[str]) -> None:
    typed, errors = run(inputs, column_type)
    assert typed == [None] * len(inputs)
    assert errors == [True] * len(inputs)


DATE_OK = [
    ("2024-01-01", date(2024, 1, 1)),
    ("2024-02-29", date(2024, 2, 29)),
    ("2024-1-5", date(2024, 1, 5)),
    ("2024-01-5", date(2024, 1, 5)),
    ("2024-1-05", date(2024, 1, 5)),
    (" 2024-01-01", date(2024, 1, 1)),
    ("2024-01-01 ", date(2024, 1, 1)),
    ("  2024-01-01  ", date(2024, 1, 1)),
    ("\t2024-01-01\t", date(2024, 1, 1)),
    ("+2024-01-01", date(2024, 1, 1)),
    ("+0001-01-01", date(1, 1, 1)),
    ("0001-01-01", date(1, 1, 1)),
    ("9999-12-31", date(9999, 12, 31)),
    ("999-01-01", date(999, 1, 1)),
    ("24-01-01", date(24, 1, 1)),
]

DATE_REJECTED = [
    "2023-02-29", "10000-01-01", "12024-01-01", "99999-01-01", "2024-02-30", "2024-01-00",
    "2024-00-10", "2024-13-01", "2024-04-31", "2024-01-32", "   ", "2024-01-01x", "x2024-01-01",
    "20240101", "05/01/2024", "2024/01/01", "2024-01-01T10:00:00", "2024-01-01 10:00:00", "abc",
    "-", "2024-01", "2024-01-01-01", "2024-001-01", "2024-01-001", "1700000000",
    "２０２４-01-01",
]  # fmt: skip

# D9: Rust accepts these (years <= 0 stored one day late, spaces inside the value).
D9_DATE_REJECTED = [
    "2024- 01-01", "2024-01- 01", "2024 -01-01", "-2024-01-01", "-0001-01-01", "0000-01-01",
    "+10000-01-01",
]  # fmt: skip


def at(
    h: int, mi: int = 0, s: int = 0, ms: int = 0, d: tuple[int, int, int] = (2024, 1, 1)
):
    return datetime(*d, h, mi, s, ms * 1000)


DATETIME_OK = [
    ("2024-01-01T10:00:00", at(10)),
    ("2024-01-01T10:00", at(10)),
    ("2024-01-01T10:00:00.1", at(10, ms=100)),
    ("2024-01-01T10:00:00.12", at(10, ms=120)),
    ("2024-01-01T10:00:00.123", at(10, ms=123)),
    ("2024-01-01T10:00:00.1234", at(10, ms=123)),
    ("2024-01-01T10:00:00.123456789", at(10, ms=123)),
    ("2024-01-01T10:00:00.1234567891", at(10, ms=123)),
    ("2024-01-01T10:00:00.", at(10)),
    ("2024-01-01T10:00:00Z", at(10)),
    ("2024-01-01T10:00:00.5Z", at(10, ms=500)),
    ("2024-01-01T10:00Z", at(10)),
    ("2024-01-01T10:00:60", at(10, 1)),
    ("2024-01-01T23:59:60", at(0, d=(2024, 1, 2))),
    ("2024-01-01T10:00:60.5", at(10, 1, ms=500)),
    ("2024-12-31T23:59:60", at(0, d=(2025, 1, 1))),
    (" 2024-01-01T10:00:00", at(10)),
    ("2024-01-01T10:00:00 ", at(10)),
    ("2024-01-01T10:00:00\n", at(10)),
    ("2024-01-01T1:2:3", at(1, 2, 3)),
    ("2024-01-01T10:00:5", at(10, 0, 5)),
    ("2024-1-5T1:02", at(1, 2, d=(2024, 1, 5))),
    ("+2024-01-01T10:00:00", at(10)),
    ("0001-01-01T00:00:00", at(0, d=(1, 1, 1))),
    ("9999-12-31T23:59:59", at(23, 59, 59, d=(9999, 12, 31))),
    ("24-01-01T10:00:00", at(10, d=(24, 1, 1))),
]

# D9: Rust ignores the offset; Floe converts to UTC.
D9_DATETIME_OFFSETS = [
    ("2024-01-01T10:00:00+02:00", at(8)),
    ("2024-01-01T10:00:00-05:30", at(15, 30)),
    ("2024-01-01T10:00:00+0200", at(8)),
    ("2024-01-01T10:00:00+02", at(8)),
    ("2024-01-01T10:00:00-02", at(12)),
    ("2024-01-01T10:00:00.5+02:00", at(8, ms=500)),
    ("2024-01-01T10:00+02:00", at(8)),
    ("2024-01-01T10:00:60+02:00", at(8, 1)),
    ("2024-01-01T01:00:00+02:00", at(23, d=(2023, 12, 31))),
    ("2024-01-01T10:00:00-00:00", at(10)),
    ("2024-01-01T10:00:00z", at(10)),
    ("2024-01-01T10:00:00.5z", at(10, ms=500)),
    ("0001-01-01T02:00:00+02:00", at(0, d=(1, 1, 1))),
    ("9999-12-31T21:59:59-02:00", at(23, 59, 59, d=(9999, 12, 31))),
    (" 2024-01-01T10:00:00+02:00 ", at(8)),
    ("2024-01-01T10:00:00+23:59", at(10, 1, d=(2023, 12, 31))),
]

DATETIME_REJECTED = [
    "2024-01-01 10:00:00", "2024-01-01 10:00", "2024-01-01T10", "2024-01-01 10:00:00.5",
    "2024-01-01 10:00:00Z", "2024-01-01T24:00:00", "2024-01-01T24:00", "2024-01-01T23:60:00",
    "2024-01-01T10:60", "2024-01-01T25:00:00", "2024-01-01", "2024-01-01T", "2024-01-01 ",
    "2024-01-01t10:00:00", "2024-01-01  10:00:00", "10000-01-01T00:00:00",
    "12024-01-01T10:00:00", "2024-02-30T10:00:00", "2024-13-01T10:00:00", "2024-01-00T10:00:00",
    "1700000000", "abc", "2024-01-01T10:00:٣", "2024-01-01T10-00-00", "2024/01/01 10:00:00", "20240101T100000",
]  # fmt: skip

# D9: Rust accepts these (second 61 dropped, trailing characters ignored, space accepted
# before an offset or after the T, years <= 0 one day late, offset ignored).
D9_DATETIME_REJECTED = [
    "2024-01-01T10:00:61", "2024-01-01T10:00:00garbage", "2024-01-01T10:00:00.5garbage",
    "2024-01-01T10:00:00,123", "2024-01-01T10:00:00:00", "2024-01-01T10:00:00.123.456",
    "2024-01-01T10:00:00 +02:00", "2024-01-01T10:00:00 Z", "2024-01-01T 10:00:00",
    "2024- 01-01T10:00:00", "-2024-01-01T10:00:00", "0000-01-01T00:00:00",
    "+10000-01-01T00:00:00",
    # offsets outside hh:mm, hhmm, hh, Z (Rust ignores them; not in the case READMEs)
    "2024-01-01T10:00:00+25:00", "2024-01-01T10:00:00+02:60", "2024-01-01T10:00:00+2:00",
    "2024-01-01T10:00:00+02:0", "2024-01-01T10:00:00+020", "2024-01-01T10:00:00+",
]  # fmt: skip


@pytest.mark.parametrize("column_type", ["date"])
def test_date(column_type: str) -> None:
    check_ok(column_type, DATE_OK)
    check_rejected(column_type, DATE_REJECTED)


def test_date_d9_rejected() -> None:
    check_rejected("date", D9_DATE_REJECTED)


@pytest.mark.parametrize(
    "column_type", ["datetime", "timestamp", "DateTime", "date_time"]
)
def test_datetime(column_type: str) -> None:
    check_ok(column_type, DATETIME_OK)
    check_rejected(column_type, DATETIME_REJECTED)


def test_datetime_d9_offsets_converted_to_utc() -> None:
    check_ok("datetime", D9_DATETIME_OFFSETS)


def test_datetime_d9_rejected() -> None:
    check_rejected("datetime", D9_DATETIME_REJECTED)


def test_datetime_final_year_outside_1_9999_rejected() -> None:
    check_rejected(
        "datetime",
        [
            "9999-12-31T23:59:60",  # rollover to year 10000 (Rust stores it)
            "9999-12-31T22:00:00-02:00",  # offset to year 10000
            "0001-01-01T00:00:00+02:00",  # offset to year 0
        ],
    )


def test_datetime_is_millisecond_datetime() -> None:
    df = pl.DataFrame({"v": ["2024-01-01T10:00:00"]})
    assert df.select(cast_column("v", "datetime")[0]).schema["v"] == pl.Datetime("ms")


def test_date_dtype() -> None:
    df = pl.DataFrame({"v": ["2024-01-01"]})
    assert df.select(cast_column("v", "date")[0]).schema["v"] == pl.Date


@pytest.mark.parametrize(
    "column_type", ["int64", "int", "integer", "long", "Int-64", "INT_64"]
)
def test_int64(column_type: str) -> None:
    check_ok(
        column_type,
        [
            ("1", 1),
            ("-12", -12),
            ("007", 7),
            ("+9", 9),
            ("00", 0),
            ("9223372036854775807", 2**63 - 1),
        ],
    )
    check_rejected(
        column_type,
        [" 3", "4.0", "x", "1e3", "9223372036854775808", "0x10", "1_000", "+", ""],
    )


def test_int32_overflow_rejected() -> None:
    check_ok("int32", [("2147483647", 2**31 - 1), ("-1", -1)])
    check_rejected("int32", ["2147483648"])


def test_unsigned_and_small_ints() -> None:
    check_ok("uint8", [("255", 255), ("0", 0)])
    check_rejected("uint8", ["256", "-1"])
    check_ok("int8", [("-128", -128)])
    check_rejected("int8", ["128"])
    check_ok("uint16", [("65535", 65535)])
    check_ok("uint32", [("4294967295", 2**32 - 1)])
    check_ok("uint64", [("18446744073709551615", 2**64 - 1)])
    check_ok("int16", [("-32768", -(2**15))])


@pytest.mark.parametrize("column_type", ["number", "float64", "float", "double"])
def test_float64(column_type: str) -> None:
    check_ok(
        column_type,
        [
            ("1.5", 1.5), ("2", 2.0), ("1e3", 1000.0), ("-0.5", -0.5), ("+3", 3.0), (".5", 0.5),
            ("inf", math.inf), ("1.7976931348623157e309", math.inf),
        ],
    )  # fmt: skip
    check_rejected(column_type, ["abc", " 1_000", "1_000", "0x10"])


def test_float_nan_is_a_value_not_a_cast_error() -> None:
    typed, errors = run(["nan"], "number")
    assert math.isnan(typed[0])  # pyright: ignore[reportArgumentType] - typed is list[object]
    assert errors == [False]


def test_float32() -> None:
    check_ok("float32", [("1.5", 1.5)])
    check_rejected("float32", ["abc"])


@pytest.mark.parametrize("column_type", ["boolean", "bool"])
def test_boolean(column_type: str) -> None:
    check_ok(
        column_type,
        [
            ("true", True), ("TRUE", True), ("True", True), ("1", True), (" TRUE", True),
            ("false", False), ("False", False), ("0", False), ("0 ", False), (" false ", False),
        ],
    )  # fmt: skip
    check_rejected(column_type, ["yes", "t", "2", "", "no", "truee"])


@pytest.mark.parametrize("column_type", ["string", "str", "text", "String"])
def test_string_is_untouched(column_type: str) -> None:
    typed, errors = run([" a ", "", "1", None], column_type)
    assert typed == [" a ", "", "1", None]
    assert errors == [False] * 4


@pytest.mark.parametrize(
    "column_type", ["int64", "number", "boolean", "date", "datetime"]
)
def test_null_raw_is_null_without_error(column_type: str) -> None:
    assert run([None], column_type) == ([None], [False])


def test_cast_mode_coerce_nulls_without_errors() -> None:
    values = ["2024-01-01", "2023-02-29", "abc", "10000-01-01"]
    typed, errors = run(values, "date", cast_mode="coerce")
    assert typed == [date(2024, 1, 1), None, None, None]
    assert errors == [False] * 4
    typed, errors = run(["1", "x"], "int64", cast_mode="coerce")
    assert typed == [1, None]
    assert errors == [False, False]


def test_cast_mode_strict_flags_only_invalid_values() -> None:
    typed, errors = run(["1", "x", None], "int64")
    assert typed == [1, None, None]
    assert errors == [False, True, False]


def test_unsupported_column_type() -> None:
    with pytest.raises(ValueError, match="unsupported column type: bogus"):
        cast_column("v", "bogus")


def test_decimal_is_exact_beyond_float64_integer_precision() -> None:
    typed, errors = run(["12345678901234567.89", "-0.01"], "decimal(19,2)")
    assert typed == [Decimal("12345678901234567.89"), Decimal("-0.01")]
    assert errors == [False, False]


def test_decimal_dtype_is_pl_decimal() -> None:
    df = pl.DataFrame({"v": ["1.5"]}).select(cast_column("v", "Decimal(10, 2)")[0])
    assert df.schema["v"] == pl.Decimal(10, 2)


def test_decimal_high_scale() -> None:
    typed, _ = run(["0.123456789012345678901234567890"], "decimal(38,30)")
    assert typed == [Decimal("0.123456789012345678901234567890")]


def test_decimal_digits_beyond_scale_or_precision_are_cast_errors() -> None:
    typed, errors = run(["1.239", "1.230", "123.45", "abc"], "decimal(4,2)")
    assert typed[1] == Decimal(
        "1.23"
    )  # the flagged rows are rejected, their value is moot
    assert errors == [True, False, True, True]


def test_decimal_coerce_rounds_and_does_not_flag() -> None:
    typed, errors = run(["1.239"], "decimal(4,2)", "coerce")
    assert typed == [Decimal("1.24")]
    assert errors == [False]


def test_decimal_from_native_float_column_keeps_scale() -> None:
    typed, mask = cast_column("v", "decimal(10,2)", dtype=pl.Float64)
    out = pl.DataFrame({"v": [1.5, -2.25]}).select(typed, mask)
    assert out["v"].to_list() == [Decimal("1.50"), Decimal("-2.25")]
