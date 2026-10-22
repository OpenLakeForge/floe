"""PII `strategy: hash` for the spike: SHA-256 (no key) or HMAC-SHA256 (key), lowercase hex.

Rust reference: crates/floe-core/src/run/entity/pii.rs `hash_column`: the column is cast to
string, the key is the UTF-8 bytes of the resolved key, nulls stay null.

Two implementations:
  hashlib: a per-value Python loop over `hmac.digest` (stdlib, single-threaded, GIL-bound).
  duckdb:  HMAC composed from DuckDB's vectorised `sha256` over blobs (RFC 2104):
           H((K ^ opad) || H((K ^ ipad) || m)), run multi-threaded inside DuckDB.

Benchmark: python pii_hash.py --rows 1000000
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import os
import time

import duckdb
import polars as pl

METHOD = "duckdb"  # used by floe_py.py; chosen from the benchmark in README.md


def hash_column(values: pl.Series, key: str | None, method: str | None = None) -> pl.Series:
    return (HASHERS[method or METHOD])(values, key)


def hash_hashlib(values: pl.Series, key: str | None) -> pl.Series:
    if key is None:
        out = [None if v is None else hashlib.sha256(v.encode()).hexdigest() for v in values]
    else:
        k = key.encode()
        out = [None if v is None else hmac.digest(k, v.encode(), "sha256").hex() for v in values]
    return pl.Series(values.name, out, dtype=pl.String)


def _pads(key: bytes) -> tuple[bytes, bytes]:
    if len(key) > 64:  # RFC 2104: keys longer than the block size are hashed first
        key = hashlib.sha256(key).digest()
    key = key.ljust(64, b"\0")
    return bytes(b ^ 0x36 for b in key), bytes(b ^ 0x5C for b in key)


def hash_duckdb(values: pl.Series, key: str | None) -> pl.Series:
    frame = values.to_frame("v")  # noqa: F841 - read by DuckDB's replacement scan
    # Same thread budget as Polars, so the Rust/Python comparison stays like for like.
    con = duckdb.connect(config={"threads": int(os.environ.get("POLARS_MAX_THREADS", os.cpu_count() or 1))})
    if key is None:
        sql = "SELECT sha256(v) AS h FROM frame"
        params: list = []
    else:
        ipad, opad = _pads(key.encode())
        sql = "SELECT sha256($2 || unhex(sha256($1 || encode(v)))) AS h FROM frame"
        params = [ipad, opad]
    out = con.sql(sql, params=params).pl()["h"]
    con.close()
    return out.rename(values.name)


HASHERS = {"hashlib": hash_hashlib, "duckdb": hash_duckdb}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1_000_000)
    args = parser.parse_args()
    values = pl.select(pl.format("user{}@example.com", pl.int_range(args.rows))).to_series().rename("email")
    values = values.scatter(list(range(0, args.rows, 97)), None)  # some nulls
    key = "bench-hmac-key"
    ref = None
    for name, fn in HASHERS.items():
        for k in (key, None):
            t = time.perf_counter()
            out = fn(values, k)
            dt = time.perf_counter() - t
            label = "hmac" if k else "sha256"
            if k and ref is None:
                ref = out
            same = "" if not k else f" identical_to_hashlib={out.equals(ref, null_equal=True)}"
            print(f"{name:8s} {label:6s} rows={args.rows} seconds={dt:.3f} rows_per_s={args.rows / dt:,.0f}{same}")


if __name__ == "__main__":
    main()
