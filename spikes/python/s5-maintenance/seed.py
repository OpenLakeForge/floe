"""Seed an Iceberg table that looks like a #598 target after N merges: seed files, position deletes
from MERGE (SCD1-style update + insert), and many small appends (floe-run-style ingests).

    python seed.py --table s5_main --rows 50000000 --merges 5 --small-appends 30
    python seed.py --table s5_day  --rows 10000000 --merges 2 --partition day
"""

import argparse
import time

import pyarrow as pa
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import DateType, DoubleType, IntegerType, LongType, NestedField, StringType

from common import NAMESPACE, catalog, drop_and_purge, duck, file_counts, vmhwm_mb

SCHEMA = Schema(
    NestedField(1, "id", LongType(), False),
    NestedField(2, "cat", IntegerType(), False),
    NestedField(3, "d", DateType(), False),
    NestedField(4, "amount", DoubleType(), False),
    NestedField(5, "payload", StringType(), False),
)
# deterministic pseudo-random columns derived from id (no RNG state, reproducible)
ROW = "i AS id, (hash(i) % 1000)::INT AS cat, DATE '2024-01-01' + ((hash(i + 7) % 365))::INT AS d, (hash(i + 13) % 100000)::DOUBLE / 100 AS amount, 'p' || (hash(i + 29) % 1000000)::VARCHAR AS payload"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", required=True)
    ap.add_argument("--rows", type=int, required=True)
    ap.add_argument("--merges", type=int, default=5)
    ap.add_argument("--small-appends", type=int, default=0)
    ap.add_argument("--partition", choices=["none", "day"], default="none")
    ap.add_argument("--spread", choices=["contiguous", "all"], default="contiguous",
                    help="contiguous: each merge updates one id block (~#598 shape, few delete files); all: ids spread over every file")
    ap.add_argument("--insert-chunk", type=int, default=10_000_000)
    ap.add_argument("--memory-limit", default="4GB")
    a = ap.parse_args()

    cat = catalog()
    name = f"{NAMESPACE}.{a.table}"
    drop_and_purge(cat, name)
    spec = PartitionSpec(PartitionField(3, 1000, DayTransform(), "d_day")) if a.partition == "day" else PartitionSpec()
    tbl = cat.create_table(name, schema=SCHEMA, partition_spec=spec, properties={"format-version": "2"})
    con = duck(a.memory_limit)
    q = f"polaris.{name}"
    t0 = time.time()
    if a.partition == "day":
        # #609: one INSERT spanning all days OOMs / explodes into tiny files; slice by date range
        n = 0
        for lo in range(0, 365, 30):
            con.execute(f"INSERT INTO {q} SELECT * FROM (SELECT {ROW} FROM range(0,{a.rows}) t(i)) WHERE d >= DATE '2024-01-01' + {lo} AND d < DATE '2024-01-01' + {lo + 30}")
    else:
        for lo in range(0, a.rows, a.insert_chunk):
            hi = min(lo + a.insert_chunk, a.rows)
            con.execute(f"INSERT INTO {q} SELECT {ROW} FROM range({lo},{hi}) t(i)")
    print(f"seeded {a.rows} rows in {time.time()-t0:.0f}s", flush=True)

    # merge batch = 5% changed + 3% unchanged + 2% new keys (the #598 shape scaled 1:50)
    nk = a.rows
    changed, same, new = int(nk * 0.01), int(nk * 0.006), int(nk * 0.004)
    next_new = nk
    for m in range(a.merges):
        t1 = time.time()
        step = max(1, nk // changed) if a.spread == "all" else 1
        base_id = 0 if a.spread == "all" else m * changed * 3
        con.execute(f"""MERGE INTO {q} t USING (
            SELECT {base_id} + i * {step} + {m} AS id, (hash(i + {m}) % 1000)::INT AS cat, DATE '2024-01-01' + ((hash({base_id} + i * {step} + {m} + 7) % 365))::INT AS d,
                   999000.0 + {m} AS amount, 'm{m}' AS payload FROM range(0,{changed}) t(i)
            UNION ALL
            SELECT {ROW} FROM range({next_new},{next_new + new}) t(i)
        ) b ON t.id = b.id AND t.d = b.d
        WHEN MATCHED AND t.amount IS DISTINCT FROM b.amount THEN UPDATE SET amount = b.amount, payload = b.payload
        WHEN NOT MATCHED THEN INSERT VALUES (b.id, b.cat, b.d, b.amount, b.payload)""")
        next_new += new
        tbl.refresh()
        print(f"merge {m+1}: {time.time()-t1:.0f}s {file_counts(tbl)}", flush=True)

    for k in range(a.small_appends):
        lo = next_new + k * 20000
        batch = con.execute(f"SELECT {ROW} FROM range({lo},{lo+20000}) t(i)").fetch_arrow_table()
        # SELECT yields nullable fields; the table schema is required
        batch = batch.cast(pa.schema([pa.field(f.name, f.type, nullable=False) for f in batch.schema]))
        if a.partition == "day":
            break  # PyIceberg 0.12 append to day() needs pyiceberg-core; skip, the seed already spans all days
        tbl.append(batch)
    tbl.refresh()
    print("final", file_counts(tbl), f"VmHWM={vmhwm_mb():.0f}MB", flush=True)


if __name__ == "__main__":
    main()
