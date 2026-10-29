"""Unique keys already in an Iceberg table, for append (Rust `seed_unique_tracker_for_append`)."""

from collections.abc import Mapping, Sequence
from pathlib import Path

import polars as pl
import polars.selectors as cs
from pyiceberg.table import Table

from floe.iceberg.catalog import run_error


def cache_table_keys(
    table: Table, keys: Sequence[Sequence[str]], stored: Mapping[str, str], path: Path
) -> bool:
    """Write the key columns of `table` to `path` in one streamed scan; `stored` maps runtime to table names.

    A key with a column the table lacks (`add_columns` adds it on write) is left out: the tracker
    skips it, as historical rows hold NULL there. False when no key is left, and nothing is written.
    """
    present = set(table.schema().column_names)
    names = {n for k in keys if present.issuperset(ns := [stored.get(c, c) for c in k]) for n in ns}  # fmt: skip
    if not names:
        return False
    with run_error("iceberg unique seed scan failed"):
        pl.scan_iceberg(table).select(cs.by_name(sorted(names))).sink_parquet(path)
    return True
