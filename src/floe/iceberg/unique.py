"""Unique keys already in an Iceberg table, for append (Rust `seed_unique_tracker_for_append`)."""

from collections.abc import Mapping

import polars as pl
from pyiceberg.table import Table

from floe.checks.unique import UniqueTracker
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
    """
    for key in tracker.keys:
        names = [stored.get(c, c) for c in key]
        distinct = (
            batch.select(pl.col(c).alias(n) for c, n in zip(key, names, strict=True))
            .drop_nulls()
            .unique()
        )
        if not distinct.height:  # all-NULL keys are never checked; nothing to look up
            continue
        with run_error("iceberg unique seed scan failed"):
            hits = (
                pl.scan_iceberg(table)
                .select(names)
                .join(distinct.lazy(), on=names, how="semi")
                .collect(engine="streaming")
            )
        tracker.seed(hits.rename(dict(zip(names, key, strict=True))))
