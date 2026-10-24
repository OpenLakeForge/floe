"""SCD1 merge into Iceberg: the batch is staged as Parquet, then one DuckDB `MERGE INTO` (#598).

Semantics of `io/write/strategy/merge/scd1.rs`: match on the merge key, update every non-key,
non-ignored column, insert unmatched rows with every column. Rust updates every matched row; here
the update is guarded by a null-safe change predicate, so rerunning a batch writes nothing. The
resulting rows are the same.
"""

# ponytail: DuckDB only as the Iceberg MERGE writer (merge-on-read, bounded memory); switch to Polars/PyIceberg and drop duckdb per #827 once upstream supports it.
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs

import tempfile
from collections.abc import Collection, Sequence
from dataclasses import dataclass
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


@dataclass(frozen=True)
class MergeResult:
    write: IcebergWriteResult
    inserted_count: int
    # matched rows that changed; Rust counts every matched row (D2)
    updated_count: int


def scd1_sql(
    target: str, columns: Sequence[str], key: Sequence[str], ignore: Collection[str]
) -> tuple[str, str]:
    """(count, merge) statements: the source is `read_parquet($1)`, aliased `s`; the target is `t`.

    The count returns (inserted, updated) as the merge will apply them. `MERGE ... RETURNING`
    can't be aggregated in SQL, and returning every row would not stay bounded.
    """
    update = [c for c in columns if c not in key and c not in ignore]
    on = " AND ".join(f"t.{_q(k)} = s.{_q(k)}" for k in key)
    changed = " OR ".join(f"t.{_q(c)} IS DISTINCT FROM s.{_q(c)}" for c in update)
    matched = ""
    if update:
        assign = ", ".join(f"{_q(c)} = s.{_q(c)}" for c in update)
        matched = f"WHEN MATCHED AND ({changed}) THEN UPDATE SET {assign} "
    # a matched target key is never NULL, since NULL = x never holds
    unmatched = f"t.{_q(key[0])} IS NULL"
    count = (
        f"SELECT count(*) FILTER ({unmatched}), "
        f"count(*) FILTER (NOT {unmatched} AND ({changed or 'false'})) "
        f"FROM read_parquet($1) s LEFT JOIN {target} t ON {on}"
    )
    merge = (
        f"MERGE INTO {target} t USING read_parquet($1) s ON {on} {matched}"
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(map(_q, columns))}) "
        f"VALUES ({', '.join(f's.{_q(c)}' for c in columns)})"
    )
    return count, merge


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
) -> MergeResult:
    """`con` comes from `open_duckdb` on the table's catalog; the table already has the batch's columns."""
    before = table.current_snapshot()
    target = ".".join(map(_q, (DUCKDB_CATALOG, *table.name())))
    count, merge = scd1_sql(target, reader.schema.names, merge_key, ignore_columns)
    with tempfile.TemporaryDirectory(prefix="floe-merge-") as staging:
        source = Path(staging) / "batch.parquet"
        with (
            run_error("iceberg merge staging failed"),
            pq.ParquetWriter(source, reader.schema) as writer,
        ):
            for batch in reader:
                writer.write_batch(batch)
        with run_error("duckdb merge failed"):
            counts = con.execute(count, [str(source)]).fetchone()
            assert counts is not None  # an aggregate always returns one row
            con.execute(merge, [str(source)])
    with run_error("rest catalog load_table failed"):
        table.refresh()
        added = _added_data_files(table, None if before is None else before.snapshot_id)
    return MergeResult(write_result(table, added), counts[0], counts[1])
