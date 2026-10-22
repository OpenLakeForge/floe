"""Seeded benchmark data generator.

Writes `<out>/<rows>/in/orders.csv` plus a `config.yml` for the benchmark contract:
mixed column types, ~1% cast errors (qty, always one in row 0: see #603), ~1% nulls in a not_null column (amount),
~1% duplicate single keys (customer_id), ~1% duplicate composite keys (region, order_no),
and one PII column (email, HMAC-SHA256).

Randomness comes from Polars' seeded hash of the row index, so a given seed and the pinned
Polars version always produce the same file.

Usage: python gen_data.py --rows 1000000 --out data [--seed 599]
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

CONFIG = """\
version: "0.3"
domains:
  - name: bench
entities:
  - name: orders
    domain: bench
    source:
      format: csv
      path: "./in/orders.csv"
      options:
        header: true
        separator: ","
    sink:
      accepted:
        format: parquet
        path: "./out/accepted/orders"
      rejected:
        format: csv
        path: "./out/rejected/orders"
    policy:
      severity: reject
    pii:
      columns:
        - name: email
          strategy: hash
          key: "bench-hmac-key"
    schema:
      unique_keys:
        - [customer_id]
        - [region, order_no]
      columns:
        - name: customer_id
          type: int64
          nullable: false
        - name: email
          type: string
        - name: region
          type: string
        - name: order_no
          type: int64
        - name: amount
          type: number
          nullable: false
        - name: qty
          type: int64
        - name: created
          type: date
        - name: active
          type: boolean
        - name: note
          type: string
report:
  path: "./report"
"""


def generate(rows: int, seed: int) -> pl.DataFrame:
    i = pl.int_range(rows, dtype=pl.Int64)

    def rnd(k: int) -> pl.Expr:  # uniform in [0, 10000)
        return (i.hash(seed + k) % 10_000).cast(pl.Int64)

    one_pct = lambda k: rnd(k) < 100  # noqa: E731 - local shorthand
    regions = pl.lit(["eu", "us", "apac", "latam"])
    region = regions.list.get(rnd(2) % 4)
    df = pl.select(
        i.alias("i"),
        # ~1% of rows reuse an earlier customer_id.
        pl.when(one_pct(1) & (i > 0))
        .then(i.hash(seed + 9) % i.clip(lower_bound=1) + 1)
        .otherwise(i + 1)
        .cast(pl.Int64)
        .alias("customer_id"),
        region.alias("region"),
        one_pct(3).alias("dup_composite"),
        pl.when(one_pct(4)).then(None).otherwise((rnd(5) / 100).cast(pl.String)).alias("amount"),
        # Row 0 always carries a qty cast error: Floe v0.7.0 rejects the whole file when the
        # first bad value comes after Polars' ~100-row inference window (#603).
        pl.when(one_pct(6) | (i == 0)).then(pl.format("x{}", rnd(6))).otherwise((rnd(6) % 50).cast(pl.String)).alias("qty"),
        (pl.date(2015, 1, 1) + pl.duration(days=rnd(7) % 3650)).dt.strftime("%Y-%m-%d").alias("created"),
        pl.when(rnd(8) % 2 == 0).then(pl.lit("true")).otherwise(pl.lit("false")).alias("active"),
        pl.format("user{}@example.com", i).alias("email"),
        pl.format("note-{}", rnd(10)).alias("note"),
    )
    # ~1% of rows copy the previous row's (region, order_no): a composite duplicate.
    df = df.with_columns(
        pl.when(pl.col("dup_composite") & (pl.col("i") > 0)).then(pl.col("region").shift(1)).otherwise(pl.col("region")).alias("region"),
        pl.when(pl.col("dup_composite") & (pl.col("i") > 0)).then(pl.col("i") - 1).otherwise(pl.col("i")).alias("order_no"),
    )
    return df.select(
        pl.col("customer_id").cast(pl.String),
        "email",
        "region",
        pl.col("order_no").cast(pl.String),
        "amount",
        "qty",
        "created",
        "active",
        "note",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--out", type=Path, default=Path("data"))
    parser.add_argument("--seed", type=int, default=599)
    args = parser.parse_args()
    root = args.out / str(args.rows)
    (root / "in").mkdir(parents=True, exist_ok=True)
    generate(args.rows, args.seed).write_csv(root / "in" / "orders.csv")
    (root / "config.yml").write_text(CONFIG)
    print(root)


if __name__ == "__main__":
    main()
