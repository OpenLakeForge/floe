"""Golden SCD1 / SCD2 fixture derived from docs/write_modes.md and
crates/floe-core/src/io/write/strategy/merge/{scd1,scd2,shared}.rs.

Key: `id`. Compare columns (SCD2) and update columns (SCD1): every non-key business column.
The batch carries one column the target lacks (`tier`): additive schema evolution, applied with
PyIceberg before the merge. Rust treats an added compare column as changed when the source value
is not null (`scd2_changed_predicate`); a null-safe "is distinct" compare against the evolved
target (null in old rows) gives the same answer.

Cases covered:
  1  not in batch                        -> untouched
  2  name + region changed               -> SCD1 update (moves identity partition) / SCD2 close + insert
  3  identical                           -> unchanged
  4  only the added column is non-null   -> changed
  5  new key                             -> insert
  6  amount NULL in target and batch     -> unchanged (null-safe compare)
  7  amount NULL -> 70                   -> changed
SCD2 target also holds a closed historical version of key 1, which must never be matched.

`T` stands for the merge timestamp, `T0` for the bootstrap timestamp of the target.
"""

import datetime as dt

import pyarrow as pa

T0 = dt.datetime(2026, 1, 1, 0, 0, 0)
T_HIST = dt.datetime(2025, 12, 1, 0, 0, 0)
T = "T"  # placeholder for the merge timestamp, resolved by check_golden.py

D = dt.date

BUSINESS = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("name", pa.string()),
        pa.field("amount", pa.float64()),
        pa.field("event_date", pa.date32()),
        pa.field("region", pa.string()),
    ]
)
BATCH = BUSINESS.append(pa.field("tier", pa.string()))
SYSTEM = [
    pa.field("__floe_is_current", pa.bool_(), nullable=False),
    pa.field("__floe_valid_from", pa.timestamp("us"), nullable=False),
    pa.field("__floe_valid_to", pa.timestamp("us")),
]
KEY = ["id"]

# id, name, amount, event_date, region
TARGET_ROWS = [
    (1, "a", 10.0, D(2026, 1, 1), "eu"),
    (2, "b", 20.0, D(2026, 1, 2), "eu"),
    (3, "c", 30.0, D(2026, 1, 3), "us"),
    (4, "d", 40.0, D(2026, 1, 4), "us"),
    (6, "f", None, D(2026, 1, 6), "eu"),
    (7, "g", None, D(2026, 1, 7), "us"),
]
# id, name, amount, event_date, region, tier
BATCH_ROWS = [
    (2, "b2", 20.0, D(2026, 1, 2), "us", None),
    (3, "c", 30.0, D(2026, 1, 3), "us", None),
    (4, "d", 40.0, D(2026, 1, 4), "us", "gold"),
    (5, "e", 50.0, D(2026, 1, 5), "eu", "silver"),
    (6, "f", None, D(2026, 1, 6), "eu", None),
    (7, "g", 70.0, D(2026, 1, 7), "us", None),
]

EXPECTED_SCD1 = [
    (1, "a", 10.0, D(2026, 1, 1), "eu", None),
    (2, "b2", 20.0, D(2026, 1, 2), "us", None),
    (3, "c", 30.0, D(2026, 1, 3), "us", None),
    (4, "d", 40.0, D(2026, 1, 4), "us", "gold"),
    (5, "e", 50.0, D(2026, 1, 5), "eu", "silver"),
    (6, "f", None, D(2026, 1, 6), "eu", None),
    (7, "g", 70.0, D(2026, 1, 7), "us", None),
]

# SCD2 target: business columns + is_current, valid_from, valid_to
SCD2_TARGET_ROWS = [(*r, True, T0, None) for r in TARGET_ROWS] + [
    (1, "a-old", 1.0, D(2025, 12, 1), "eu", False, T_HIST, T0),
]
EXPECTED_SCD2 = [
    # untouched history and unchanged current rows
    (1, "a-old", 1.0, D(2025, 12, 1), "eu", None, False, T_HIST, T0),
    (1, "a", 10.0, D(2026, 1, 1), "eu", None, True, T0, None),
    (3, "c", 30.0, D(2026, 1, 3), "us", None, True, T0, None),
    (6, "f", None, D(2026, 1, 6), "eu", None, True, T0, None),
    # closed versions of changed keys
    (2, "b", 20.0, D(2026, 1, 2), "eu", None, False, T0, T),
    (4, "d", 40.0, D(2026, 1, 4), "us", None, False, T0, T),
    (7, "g", None, D(2026, 1, 7), "us", None, False, T0, T),
    # new current versions (changed + new keys)
    (2, "b2", 20.0, D(2026, 1, 2), "us", None, True, T, None),
    (4, "d", 40.0, D(2026, 1, 4), "us", "gold", True, T, None),
    (5, "e", 50.0, D(2026, 1, 5), "eu", "silver", True, T, None),
    (7, "g", 70.0, D(2026, 1, 7), "us", None, True, T, None),
]
# SCD2 metrics as docs/write_modes.md defines them
EXPECTED_SCD2_METRICS = {"closed_count": 3, "inserted_count": 4, "unchanged_count": 2}


def table(schema, rows):
    return pa.Table.from_pylist([dict(zip(schema.names, r)) for r in rows], schema=schema)


def scd2_schema(business):
    return pa.schema(list(business) + SYSTEM)
