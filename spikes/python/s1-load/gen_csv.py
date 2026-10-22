# /// script
# requires-python = ">=3.10"
# dependencies = ["polars==2.0.0"]
# ///
"""Generate a deterministic CSV of about --gb gigabytes. `id` runs start..start+N-1, so duplicates and missing
rows are easy to detect. Writes <out>.meta.json with the exact row count and byte size."""

import argparse
import datetime as dt
import json
import os

import polars as pl

BATCH = 2_000_000
STATUSES = ["new", "paid", "shipped", "returned", "cancelled"]


def batch(start: int, n: int) -> pl.DataFrame:
    i = pl.int_range(start, start + n, dtype=pl.Int64)
    return pl.select(id=i).with_columns(
        customer_id=(pl.col("id") * 2654435761) % 1_000_003,
        amount=((pl.col("id") * 7919) % 10_000_000) / 100,
        status=pl.lit(pl.Series(STATUSES)).get(pl.col("id") % 5),
        event_date=pl.lit(dt.date(2020, 1, 1)) + pl.duration(days=pl.col("id") % 2000),
        event_ts=pl.lit(dt.datetime(2020, 1, 1)) + pl.duration(microseconds=(pl.col("id") * 104_729_113) % 63_000_000_000_000),
        note=pl.lit("note-") + pl.col("id").hash(7).cast(pl.String) + pl.lit("-") + pl.col("id").hash(11).cast(pl.String),
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("out")
    p.add_argument("--gb", type=float, required=True)
    p.add_argument("--start", type=int, default=0, help="first id, to make files with disjoint ids")
    a = p.parse_args()
    target = int(a.gb * 1e9)
    rows = 0
    with open(a.out, "wb") as f:
        while f.tell() < target:
            batch(a.start + rows, BATCH).write_csv(f, include_header=rows == 0, datetime_format="%Y-%m-%dT%H:%M:%S%.6f")
            rows += BATCH
            print(f"{rows:>12,} rows  {f.tell() / 1e9:6.2f} GB", flush=True)
    meta = {"rows": rows, "bytes": os.path.getsize(a.out)}
    with open(a.out + ".meta.json", "w") as f:
        json.dump(meta, f)
    print(json.dumps(meta))


if __name__ == "__main__":
    main()
