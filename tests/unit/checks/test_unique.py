"""Unique keys within and across files.

Parity with Rust 0.7: the `unique_pii` conformance case is run through the casts, not_null and
unique, and the `__floe_row_index` and `__floe_errors` of both files, plus the unique report
samples, are compared with the recorded `expected/`. The case joins the Python pass-list when
the run pipeline (#655) lands.
"""

import json
import struct
from pathlib import Path

import polars as pl

from floe.checks.cast import CAST_ERROR_MESSAGE, cast_column
from floe.checks.rows import RowCheck, not_null_check, split_rows
from floe.checks.unique import UniqueTracker, resolve_unique_keys

CASE = Path(__file__).parents[3] / "conformance" / "unique_pii"
COLUMNS = {
    "customer_id": "int64", "email": "string", "region": "string",
    "order_no": "int64", "amount": "number",
}  # fmt: skip
KEYS = [["customer_id"], ["region", "order_no"]]


def run_case() -> tuple[list[pl.DataFrame], UniqueTracker]:
    tracker = UniqueTracker(KEYS)
    rejected: list[pl.DataFrame] = []
    for name in ("a.csv", "b.csv"):
        raw = pl.read_csv(CASE / "in" / name, infer_schema=False)
        casts = [cast_column(n, k) for n, k in COLUMNS.items() if k != "string"]
        typed = raw.with_columns(c[0] for c in casts)
        masks = raw.select(c[1] for c in casts)
        checks = [not_null_check(typed, n) for n in ("customer_id", "amount")]
        checks += [
            RowCheck("cast_error", n, CAST_ERROR_MESSAGE, masks[f"_e_cast_{n}"])
            for n, k in COLUMNS.items()
            if k != "string"
        ]
        rejected.append(split_rows(typed, checks + tracker.check(typed))[1])
    return rejected, tracker


def test_unique_pii_case_matches_rust_rejected_rows() -> None:
    rejected, _ = run_case()
    for frame, name in zip(rejected, ("part-00000.csv", "part-UUID.csv"), strict=True):
        expected = pl.read_csv(
            CASE / "expected" / "rejected" / "orders" / name, infer_schema=False
        )
        assert frame["__floe_errors"].to_list() == expected["__floe_errors"].to_list()
        assert frame["__floe_row_index"].cast(pl.String).to_list() == (
            expected["__floe_row_index"].to_list()
        )
    assert [f.height for f in rejected] == [4, 7]


def test_unique_pii_case_matches_rust_report_samples() -> None:
    _, tracker = run_case()
    report = json.loads(
        (CASE / "expected/reports/report/run_ID/spike/orders/run.json").read_text()
    )
    got = [
        {
            "columns": r.columns,
            "duplicates_count": r.duplicates_count,
            "samples": [{"values": s.values, "count": s.count} for s in r.samples],
        }
        for r in tracker.results()
    ]
    want = [
        {k: c[k] for k in ("columns", "duplicates_count", "samples")}
        for c in report["unique_constraints"]
    ]
    assert got == want


def test_null_keys_are_skipped_and_ties_sort_on_the_debug_string() -> None:
    tracker = UniqueTracker([["k"]])
    df = pl.DataFrame({"k": [None, None, 10, 10, 2, 2, 1, 1, 1]})
    mask = tracker.check(df)[0].mask.to_list()
    assert mask == [False, False, False, True, False, True, False, True, True]
    [result] = tracker.results()
    assert [(s.values["k"], s.count) for s in result.samples] == [
        ("1", 2),
        ("10", 1),
        ("2", 1),
    ]


def test_resolve_unique_keys_matches_rust() -> None:
    assert resolve_unique_keys(None, [" a ", "b"], [" b", ""]) == [
        ["a"],
        ["b"],
        ["b", ""],
    ]
    assert resolve_unique_keys([["a", "b"], [" a", "b "], []], ["z"], ["a", "b"]) == [
        ["a", "b"]
    ]


def test_key_columns_named_like_temporaries_or_selectors_stay_literal() -> None:
    for name in ("*", "^k.*$", "__floe_count", "__floe_seen", "__floe_debug", "k0"):
        tracker = UniqueTracker([[name]])
        df = pl.DataFrame({name: ["a", "a"], "other": [1, 2]})
        assert tracker.check(df)[0].mask.to_list() == [False, True]
        assert tracker.check(df)[0].mask.to_list() == [True, True]  # next file
        [result] = tracker.results()
        assert [(s.values, s.count) for s in result.samples] == [({name: "a"}, 3)]


def test_float_samples_use_rust_display() -> None:
    nan = float("nan")
    tracker = UniqueTracker([["f"]])
    tracker.check(pl.DataFrame({"f": [1e-7, 1e-7, 10.0, 10.0, nan, nan]}))
    values = [s.values["f"] for s in tracker.results()[0].samples]
    assert values == ["0.0000001", "10", "NaN"]


def test_float_keys_compare_by_bits() -> None:
    # Rust `UniqueKey::F64` compares `to_bits()`: 0.0 and -0.0 differ, as do NaN payloads
    quiet, payload = (
        struct.unpack("<d", struct.pack("<Q", bits))[0]
        for bits in (0x7FF8 << 48, 0x7FF8 << 48 | 1)
    )
    tracker = UniqueTracker([["f"]])
    df = pl.DataFrame({"f": [0.0, -0.0, quiet, payload, 0.0, quiet]})
    assert tracker.check(df)[0].mask.to_list() == [False] * 4 + [True] * 2
    assert tracker.results()[0].duplicates_count == 2


def test_string_ties_sort_on_rust_escapes() -> None:
    tracker = UniqueTracker([["s"]])
    keys = ["\x00", "A", "B", "C", "D", "E"]
    tracker.check(pl.DataFrame({"s": keys + keys}))
    assert [s.values["s"] for s in tracker.results()[0].samples] == list("ABCDE")


def test_datetime_samples_print_the_fraction_only_when_not_zero() -> None:
    raw = pl.DataFrame(
        {"t": ["2024-01-01T12:34:56.123"] * 2 + ["2024-01-02T00:00:00"] * 2}
    )
    typed, _ = cast_column("t", "datetime")
    tracker = UniqueTracker([["t"]])
    tracker.check(raw.with_columns(typed))
    values = [s.values["t"] for s in tracker.results()[0].samples]
    assert values == ["2024-01-01 12:34:56.123", "2024-01-02 00:00:00"]


def test_all_null_files_neither_fail_nor_remember_keys() -> None:
    tracker = UniqueTracker([["k"]])
    assert (
        tracker.check(pl.DataFrame({"k": [None, None]}))[0].mask.to_list()
        == [False] * 2
    )
    assert tracker.check(pl.DataFrame({"k": ["a", "a"]}))[0].mask.to_list() == [
        False,
        True,
    ]
    assert (
        tracker.check(pl.DataFrame({"k": [None, None]}))[0].mask.to_list()
        == [False] * 2
    )
