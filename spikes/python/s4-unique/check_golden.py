"""Exactness check at small scale: DuckDB anti-join (unique.py) vs brute force (reference.py).

1. A hand-written fixture whose expected flags are written out below, to pin the reference to the
   Rust contract (composite keys, NULLs, target vs batch duplicates, float bit equality).
2. Randomised fixtures (small key domains, ~10% NULLs, duplicates in history and in the batch).

Every fixture is loaded into a fresh Iceberg table in Polaris, twice: unpartitioned and
bucket(4, id). The reference reads the history with PyIceberg, the check with DuckDB.
Exits non-zero on any mismatch.
"""

import math
import pathlib
import random
import sys
import tempfile
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.schema import Schema
from pyiceberg.transforms import BucketTransform
from pyiceberg.types import DoubleType, LongType, NestedField, StringType

import reference
import unique
from common import NAMESPACE, catalog, duck, fq

SCHEMA = Schema(
    NestedField(1, "id", LongType()),
    NestedField(2, "account", StringType()),
    NestedField(3, "seq", LongType()),
    NestedField(4, "f", DoubleType()),
)
ARROW = pa.schema([("id", pa.int64()), ("account", pa.string()), ("seq", pa.int64()), ("f", pa.float64())])
NAN = math.nan

HAND_HISTORY = [
    (1, "a", 1, 0.0),
    (2, "a", 2, NAN),
    (2, "a", 2, 1.5),  # duplicate key inside the history
    (None, "a", 3, None),  # NULL id: not seeded for [id], seeded for [account, seq]
    (4, None, 4, 2.5),  # NULL account: not seeded for [account, seq]
]
HAND_INCOMING = [
    (1, "a", 1, -0.0),
    (3, "a", 3, NAN),
    (None, "a", 9, None),
    (3, "a", 9, -0.0),
    (4, None, 4, 2.5),
    (5, "A", 1, 0.0),
    (None, None, None, None),
    (None, "a", 9, None),
    (4, "a", 4, 3.5),
    (6, "a", 4, 3.5),
]
T, B = "target", "batch"
HAND_EXPECTED = {
    ("id",): {0: T, 3: B, 4: T, 8: T},
    ("account", "seq"): {0: T, 1: T, 3: B, 7: B, 9: B},
    # Rust compares floats by bits: -0.0 is a new key (history has 0.0), NaN matches NaN.
    ("f",): {1: T, 3: B, 4: T, 5: T, 9: B},
}
CONSTRAINTS = [("id",), ("account", "seq"), ("id", "account"), ("f",)]


def random_rows(rng, n):
    def maybe(v):
        return None if rng.random() < 0.1 else v

    return [
        (maybe(rng.randrange(5000)), maybe(rng.choice("abc")), maybe(rng.randrange(2000)),
         maybe(rng.choice([0.0, -0.0, NAN, 1.0, 2.0])))
        for _ in range(n)
    ]


def to_arrow(rows):
    return pa.Table.from_pylist([dict(zip(ARROW.names, r)) for r in rows], schema=ARROW)


def load_history(cat, con, rows, partitioned):
    name = f"golden_{uuid.uuid4().hex[:8]}"
    tbl = cat.create_table(f"{NAMESPACE}.{name}", schema=SCHEMA)
    if partitioned:
        with tbl.update_spec() as upd:
            upd.add_field("id", BucketTransform(4), "id_bucket")
    # PyIceberg 0.12 cannot append to bucketed tables without pyiceberg-core, so DuckDB writes both.
    hist = to_arrow(rows)  # noqa: F841 - read by DuckDB's replacement scan
    con.execute(f"INSERT INTO {fq(name)} SELECT * FROM hist")
    return name


def run_case(cat, con, tmp, label, history, incoming, partitioned, expected=None):
    name = load_history(cat, con, history, partitioned)
    failures = 0
    try:
        tbl = cat.load_table(f"{NAMESPACE}.{name}")
        hist_rows = tbl.scan().to_arrow().to_pylist()  # independent reader for the reference
        assert len(hist_rows) == len(history), (len(hist_rows), len(history))
        inc_path = tmp / f"{name}.parquet"
        pq.write_table(to_arrow(incoming), inc_path)
        inc_rows = pq.read_table(inc_path).to_pylist()
        for cols in CONSTRAINTS if expected is None else expected:
            want = reference.unique_check(hist_rows, inc_rows, list(cols))
            if expected is not None and want != expected[cols]:
                print(f"REFERENCE WRONG {label} {cols}: {want} != {expected[cols]}")
                failures += 1
            out = tmp / f"{name}_{'_'.join(cols)}.parquet"
            try:
                unique.unique_check(con, fq(name), str(inc_path), list(cols), str(out))
                got = {r["row_idx"]: r["kind"] for r in pq.read_table(out).to_pylist()}
            except Exception as e:  # noqa: BLE001 - reported as a failed case
                got = f"{type(e).__name__}: {e}"
            ok = got == want
            failures += not ok
            layout = "bucket(4,id)" if partitioned else "unpartitioned"
            print(f"{'PASS' if ok else 'FAIL'} {label} {layout} {list(cols)}: "
                  f"{len(want)} flagged ({sum(v == T for v in want.values())} target)"
                  + ("" if ok else f"\n  want {sorted(want.items())[:12]}\n  got  {str(got)[:400]}"))
    finally:
        cat.drop_table(f"{NAMESPACE}.{name}")
    return failures


def main():
    cat = catalog()
    con = duck(memory_limit="1GB")
    failures = 0
    with tempfile.TemporaryDirectory() as d:
        tmp = pathlib.Path(d)
        for partitioned in (False, True):
            failures += run_case(cat, con, tmp, "hand", HAND_HISTORY, HAND_INCOMING, partitioned, HAND_EXPECTED)
            for seed in range(5):
                rng = random.Random(seed)
                failures += run_case(cat, con, tmp, f"random-{seed}", random_rows(rng, 3000),
                                     random_rows(rng, 2000), partitioned)
    print("OK: all cases match the reference" if failures == 0 else f"FAIL: {failures} mismatches")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
