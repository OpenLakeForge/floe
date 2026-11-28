"""Unique keys already in an Iceberg table, for append."""

import os
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
    # #1272: Polars >= 2.1 warns on every scan unless told how to plan it; Floe ships no
    # `polars_iceberg` plugin, so PyIceberg planning (as before 2.1) is chosen. A user's value wins.
    os.environ.setdefault("POLARS_ICEBERG_PLANNER", "pyiceberg")
    with run_error("iceberg unique seed scan failed"):
        # #1494: the native reader ignores the table's FileIO; Polars translates its `s3.*` etc.
        lf = pl.scan_iceberg(table, storage_options=dict(table.io.properties))
        lf.select(cs.by_name(sorted(names))).sink_parquet(path)
    return True
