"""Unique keys already in an Iceberg table, for append (Rust `seed_unique_tracker_for_append`)."""

from collections.abc import Mapping

import polars as pl
from pyiceberg.table import Table

from floe.checks.unique import UniqueTracker, seed_from_scans
from floe.iceberg.catalog import run_error


def seed_from_table(
    tracker: UniqueTracker,
    table: Table,
    batch: pl.DataFrame,
    stored: Mapping[str, str],
) -> None:
    """Seed `tracker` with the keys of `table` that `batch` also holds, in one scan of the table.

    `batch` holds the key columns of every row the run will check, so the table is scanned once per
    entity, not once per chunk. `stored` maps runtime to table names.
    A key with a column the table lacks (`add_columns` adds it on write) is skipped: historical
    rows hold NULL there, which never duplicates.
    """
    columns = set(table.schema().column_names)
    keys = [k for k in tracker.keys if columns.issuperset(stored.get(c, c) for c in k)]
    if not keys:
        return
    with run_error("iceberg unique seed scan failed"):
        seed_from_scans(tracker, keys, [pl.scan_iceberg(table)], batch, stored)
