"""Unique keys already in an Iceberg table, for append (Rust `seed_unique_tracker_for_append`)."""

from collections.abc import Mapping

import polars as pl
from pyiceberg.table import Table

from floe.checks.unique import (
    UniqueTracker,
    _float_bits,  # pyright: ignore[reportPrivateUsage] -- the tracker's own float key
)
from floe.iceberg.catalog import run_error


def seed_from_table(
    tracker: UniqueTracker,
    table: Table,
    batch: pl.DataFrame,
    stored: Mapping[str, str],
) -> None:
    """Seed `tracker` with the keys of `table` that `batch` also holds; `stored` maps runtime to table names.

    A target key the batch lacks is never hit, so the flags are Rust's while memory follows the
    batch: the table is streamed, key columns only. The join matches -0.0 to 0.0 and NaN to any
    NaN; the tracker then compares float bits, so those extra keys are never hit.
    A key with a column the table lacks (`add_columns` adds it on write) is skipped: historical
    rows hold NULL there, which never duplicates.
    """
    columns = set(table.schema().column_names)
    for key in tracker.keys:
        names = [stored.get(c, c) for c in key]
        if not columns.issuperset(names):
            continue
        # positional names (k0, b0, ...): no stored column name can collide with a temporary
        ids = [f"k{i}" for i in range(len(key))]
        distinct = (
            batch.select(pl.col(c).alias(k) for c, k in zip(key, ids, strict=True))
            .drop_nulls()
            .unique()
        )
        if not distinct.height:  # all-NULL keys are never checked; nothing to look up
            continue
        # distinct by float bits, as the tracker keys them: `unique` on a float merges -0.0 with 0.0
        floats = [k for c, k in zip(key, ids, strict=True) if batch.schema[c].is_float()]  # fmt: skip
        bits = {f"b{k}": pl.col(k).map_batches(_float_bits, return_dtype=pl.UInt64, is_elementwise=True) for k in floats}  # fmt: skip
        subset = [k for k in ids if k not in floats] + list(bits)
        with run_error("iceberg unique seed scan failed"):
            scan = pl.scan_iceberg(table).select(pl.col(n).alias(k) for n, k in zip(names, ids, strict=True))  # fmt: skip
            # the table's dtypes can differ (unsigned widened, datetimes in us); a key the table type
            # cannot hold casts to NULL, which never matches
            lookup = distinct.cast(scan.collect_schema(), strict=False).lazy()
            hits = (
                scan.join(lookup, on=ids, how="semi")
                .with_columns(**bits)
                # distinct keys only: memory follows the batch, not the history
                .unique(subset)
                .drop(*bits)
                .collect(engine="streaming")
            )
        back = pl.Schema({k: batch.schema[c] for c, k in zip(key, ids, strict=True)})
        tracker.seed(hits.cast(back).rename(dict(zip(ids, key, strict=True))))
