"""SCD1 merge into Iceberg: the batch is staged as Parquet, then one DuckDB `MERGE INTO` (#598).

Semantics of `io/write/strategy/merge/scd1.rs`: match on the merge key, update every non-key,
non-ignored column, insert unmatched rows with every column. Rust updates every matched row; here
the update is guarded by a null-safe change predicate, so rerunning a batch writes nothing. The
resulting rows are the same.
"""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs

import tempfile
from collections.abc import Collection, Sequence
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.manifest import DataFile, DataFileContent, ManifestEntryStatus
from pyiceberg.table import Table

from floe.iceberg.catalog import DUCKDB_CATALOG, run_error
from floe.iceberg.write import IcebergWriteResult, write_result


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def scd1_sql(
    target: str, columns: Sequence[str], key: Sequence[str], ignore: Collection[str]
) -> str:
    """The source is `read_parquet($1)`, aliased `s`; the target is aliased `t`."""
    update = [c for c in columns if c not in key and c not in ignore]
    on = " AND ".join(f"t.{_q(k)} = s.{_q(k)}" for k in key)
    matched = ""
    if update:
        changed = " OR ".join(f"t.{_q(c)} IS DISTINCT FROM s.{_q(c)}" for c in update)
        assign = ", ".join(f"{_q(c)} = s.{_q(c)}" for c in update)
        matched = f"WHEN MATCHED AND ({changed}) THEN UPDATE SET {assign} "
    return (
        f"MERGE INTO {target} t USING read_parquet($1) s ON {on} {matched}"
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(map(_q, columns))}) "
        f"VALUES ({', '.join(f's.{_q(c)}' for c in columns)})"
    )


def _added_data_files(table: Table, since: int | None) -> list[DataFile]:
    """Data files added by the snapshots after `since` (a DuckDB MERGE commits two)."""
    files: list[DataFile] = []
    snapshot = table.current_snapshot()
    while snapshot is not None and snapshot.snapshot_id != since:
        files += [
            entry.data_file
            for manifest in snapshot.manifests(table.io)
            if manifest.added_snapshot_id == snapshot.snapshot_id
            for entry in manifest.fetch_manifest_entry(table.io)
            if entry.status == ManifestEntryStatus.ADDED
            and entry.data_file.content == DataFileContent.DATA
        ]
        parent = snapshot.parent_snapshot_id
        snapshot = None if parent is None else table.snapshot_by_id(parent)
    return files


def merge_scd1(
    con: duckdb.DuckDBPyConnection,
    table: Table,
    reader: pa.RecordBatchReader,
    merge_key: Sequence[str],
    ignore_columns: Collection[str] = (),
) -> IcebergWriteResult:
    """`con` comes from `open_duckdb` on the table's catalog; the table already has the batch's columns."""
    before = table.current_snapshot()
    target = ".".join(map(_q, (DUCKDB_CATALOG, *table.name())))
    sql = scd1_sql(target, reader.schema.names, merge_key, ignore_columns)
    with tempfile.TemporaryDirectory(prefix="floe-merge-") as staging:
        source = Path(staging) / "batch.parquet"
        with (
            run_error("iceberg merge staging failed"),
            pq.ParquetWriter(source, reader.schema) as writer,
        ):
            for batch in reader:
                writer.write_batch(batch)
        with run_error("duckdb merge failed"):
            con.execute(sql, [str(source)])
    with run_error("rest catalog load_table failed"):
        table.refresh()
        added = _added_data_files(table, None if before is None else before.snapshot_id)
    return write_result(table, added)
