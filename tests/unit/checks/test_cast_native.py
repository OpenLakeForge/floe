"""Native-typed cast path (D15, #758): Parquet, ORC and Avro columns keep their types.
Exception to Rust parity: D17, a fractional float into an integer type is rejected in strict mode.

`test_matches_rust` compares every native-column/declared-type pair of the `casts_typed`
conformance case with the cast_error rows Rust 0.7 recorded. Not in the matrix, because Rust
aborts the run: `uint8` ("cannot create series from UInt8"), date/datetime into `boolean`, lists.
"""

import csv
import json
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest

from floe.checks.cast import cast_column

CASE = Path(__file__).resolve().parents[3] / "conformance" / "casts_typed"
TYPES = [
    p.stem.removeprefix("typed_")
    for p in sorted((CASE / "expected" / "accepted").glob("typed_*.parquet"))
]
NATIVE = pl.read_parquet(CASE / "in" / "casts_typed.parquet")


def rust_rejected(declared: str) -> set[tuple[str, str]]:
    """`(row id, column)` of every cast_error in the recorded rejected rows."""
    # D17 changes the integer targets: expected_python/ replaces the Rust recording where present
    rel = Path("rejected") / f"typed_{declared}" / "part-00000.csv"
    path = next(
        (p for d in ("expected_python", "expected") if (p := CASE / d / rel).exists()),
        None,
    )
    if path is None:
        return set()
    with path.open(newline="") as f:
        return {
            (row["id"], e["column"])
            for row in csv.DictReader(f)
            for e in json.loads(row["__floe_errors"])
        }


def run(
    values: pl.Series, column_type: str, cast_mode: str = "strict"
) -> tuple[list[object], list[bool]]:
    typed, mask = cast_column("v", column_type, cast_mode, values.dtype)
    out = values.alias("v").to_frame().select(typed, mask)
    return out["v"].to_list(), out["_e_cast_v"].to_list()


@pytest.mark.parametrize("declared", TYPES)
def test_matches_rust(declared: str) -> None:
    rejected: set[tuple[str, str]] = set()
    for column in NATIVE.columns[1:]:
        if declared == "boolean" and column in ("d", "ts", "tstz", "tzp"):
            continue
        _, mask = run(NATIVE[column], declared)
        rejected |= {
            (i, column) for i, bad in zip(NATIVE["id"], mask, strict=True) if bad
        }
    assert rejected == rust_rejected(declared)


@pytest.mark.parametrize("column_type", ["int8", "int64", "uint32", "uint64"])
def test_float_fraction_rejected_in_strict_only(column_type: str) -> None:
    # D17: Rust 0.7 truncates 2.5 to 2 (casts_typed r1); Floe rejects it in strict mode
    values = pl.Series([2.0, 2.5, -2.5, 1e20, float("nan"), None])
    mask = [False, True, True, True, True, False]
    if column_type.startswith("u"):
        mask[2] = True  # negative anyway
    assert run(values, column_type)[1] == mask
    typed, errors = run(values, column_type, "coerce")
    assert errors == [False] * 6
    assert typed[:2] == [2, 2]  # truncated, as in Rust


def test_int_overflow() -> None:
    assert run(pl.Series([127, 128, -129, None], dtype=pl.Int64), "int8") == (
        [127, None, None, None],
        [False, True, True, False],
    )
    assert run(pl.Series([-1, 255], dtype=pl.Int16), "uint32")[1] == [True, False]
    assert run(pl.Series([2**63], dtype=pl.UInt64), "int64") == ([None], [True])


def test_coerce_never_flags() -> None:
    values = pl.Series([1, 5_000_000_000], dtype=pl.Int64)
    assert run(values, "int32", "coerce") == ([1, None], [False, False])


def test_bool_and_string_targets() -> None:
    assert run(pl.Series([0, 2, -1, None]), "boolean") == (
        [False, True, True, None],
        [False] * 4,
    )
    assert run(pl.Series([True, False]), "int64")[0] == [1, 0]
    assert run(pl.Series([2, None]), "string") == (["2", None], [False, False])


def test_temporal_targets() -> None:
    ts = pl.Series(
        [datetime(2024, 6, 1, 23, 59, 59, 999999, tzinfo=UTC)],
        dtype=pl.Datetime("us", "Asia/Kolkata"),
    )
    # the zone is dropped, the UTC instant is kept (same as Rust)
    assert run(ts, "datetime")[0] == [datetime(2024, 6, 1, 23, 59, 59, 999000)]
    assert run(ts, "date")[0] == [date(2024, 6, 1)]
    assert run(pl.Series([date(2024, 1, 1)]), "datetime")[0] == [datetime(2024, 1, 1)]
    # an int is days for a date, milliseconds for a datetime
    assert run(pl.Series([2]), "date")[0] == [date(1970, 1, 3)]
    assert run(pl.Series([2]), "datetime")[0] == [datetime(1970, 1, 1, 0, 0, 0, 2000)]
    assert run(pl.Series([5_000_000_000]), "date") == ([None], [True])


@pytest.mark.parametrize(
    "values, column_type",
    [
        (pl.Series([date(2024, 1, 1)]), "boolean"),
        (pl.Series([datetime(2024, 1, 1)]), "boolean"),
        (pl.Series([[1, 2]]), "int64"),
    ],
)
def test_unsupported_pair_raises(values: pl.Series, column_type: str) -> None:
    with pytest.raises(pl.exceptions.InvalidOperationError):
        run(values, column_type)
