"""Unique keys already in a Delta table, for append."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
# ^ deltalake and pyarrow ship no type stubs

from collections.abc import Mapping, Sequence
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable
from deltalake.exceptions import DeltaError

from floe.errors import FloeRunError


def cache_table_keys(
    table: DeltaTable,
    keys: Sequence[Sequence[str]],
    stored: Mapping[str, str],
    path: Path,
) -> bool:
    """Write the key columns of `table` to `path` in one streamed scan; `stored` maps runtime to table names.

    `DeltaTable.scan` reads column-mapped tables and deletion vectors, which `pl.scan_delta` and the pyarrow dataset
    cannot (delta-sink.md Q5). A key with a column the table lacks is left out, as in the Iceberg seed.
    False when no key is left, and nothing is written.
    """
    present = {f.name for f in table.schema().fields}
    names = {n for k in keys if present.issuperset(ns := [stored.get(c, c) for c in k]) for n in ns}  # fmt: skip
    if not names:
        return False
    try:
        reader = pa.RecordBatchReader.from_stream(table.scan(columns=sorted(names)))
        with pq.ParquetWriter(path, reader.schema) as out:
            for batch in reader:
                out.write_batch(batch)
    except (DeltaError, OSError, pa.ArrowException) as err:
        raise FloeRunError(f"delta unique seed scan failed: {err}", code="run_catalog_error") from err  # fmt: skip
    return True
