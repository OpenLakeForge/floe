"""SCD1 and SCD2 merges into Iceberg: the batch is staged as Parquet, then DuckDB `MERGE INTO` (#598).

SCD1 follows `io/write/strategy/merge/scd1.rs`: match on the merge key, update every non-key,
non-ignored column, insert unmatched rows with every column. Rust updates every matched row; here
the update is guarded by a null-safe change predicate, so rerunning a batch writes nothing. The
resulting rows are the same. SCD2 follows `merge/scd2.rs` (see `scd2_sql`).

A bucket-partitioned target gets one MERGE per bucket in one transaction, so one catalog commit:
a single MERGE runs out of memory there (#609).
"""

# ponytail: DuckDB only as the Iceberg MERGE writer (merge-on-read, bounded memory); switch to Polars/PyIceberg and drop duckdb per #827 once upstream supports it.
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs

import datetime as dt
import tempfile
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.manifest import DataFile, DataFileContent, ManifestEntryStatus
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import BucketTransform

from floe.config.validate import SCD2_DEFAULTS
from floe.errors import FloeRunError
from floe.iceberg.catalog import DUCKDB_CATALOG, run_error
from floe.iceberg.write import IcebergWriteResult, write_result
from floe.state import (
    _backoff,  # pyright: ignore[reportPrivateUsage] -- one jittered backoff for every CAS retry
)

# (is_current, valid_from, valid_to)
SCD2_COLUMNS = tuple(SCD2_DEFAULTS.values())
MERGE_ATTEMPTS = 5


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


@dataclass(frozen=True)
class MergeResult:
    write: IcebergWriteResult
    inserted_count: int
    # SCD1: matched rows that changed; Rust counts every matched row (D2). SCD2: closed rows.
    updated_count: int
    closed_count: int | None = None
    unchanged_count: int | None = None


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


def scd2_sql(
    target: str,
    columns: Sequence[str],
    key: Sequence[str],
    compare: Sequence[str],
    system: Sequence[str],
    now: dt.datetime,
) -> tuple[str, str, str]:
    """(count, changed, merge), as `scd1_sql`; the count returns (inserted, closed, unchanged).

    Rust closes changed current rows, then inserts new versions, in two Delta merges. Here it is
    one MERGE: each changed row is in the source twice, once with its key (it matches the current
    version and closes it) and once with a NULL key (it never matches, so it is inserted).
    `changed` copies the changed rows of `$1` to `$2` first, so the MERGE's source never reads
    the target the MERGE is writing.
    """
    is_current, valid_from, valid_to = map(_q, system)
    on = " AND ".join(f"t.{_q(k)} = s.{_q(k)}" for k in key) + f" AND t.{is_current}"
    changed = " OR ".join(f"t.{_q(c)} IS DISTINCT FROM s.{_q(c)}" for c in compare)
    changed = f"({changed or 'false'})"
    # the join keeps only current target rows, whose flag is never NULL
    unmatched = f"t.{is_current} IS NULL"
    count = (
        f"SELECT count(*) FILTER ({unmatched} OR {changed}), "
        f"count(*) FILTER (NOT {unmatched} AND {changed}), "
        f"count(*) FILTER (NOT {unmatched} AND NOT {changed}) "
        f"FROM read_parquet($1) s LEFT JOIN {target} t ON {on}"
    )
    keys = [f"__floe_key_{i}" for i in range(len(key))]
    values = ", ".join(f"s.{_q(c)}" for c in columns)
    copy = (
        f"COPY (SELECT {values} FROM read_parquet($1) s JOIN {target} t ON {on} "
        f"WHERE {changed}) TO $2 (FORMAT parquet)"
    )
    source = (
        f"SELECT {', '.join(f's.{_q(k)} AS {m}' for k, m in zip(key, keys))}, {values} "
        f"FROM read_parquet($1) s UNION ALL "
        f"SELECT {', '.join(f'NULL AS {m}' for m in keys)}, {values} FROM read_parquet($2) s"
    )
    merge_on = " AND ".join(f"t.{_q(k)} = s.{m}" for k, m in zip(key, keys))
    ts = f"TIMESTAMP '{now.isoformat(sep=' ')}'"
    insert = ", ".join([*map(_q, columns), is_current, valid_from, valid_to])
    merge = (
        f"MERGE INTO {target} t USING ({source}) s ON {merge_on} AND t.{is_current} "
        f"WHEN MATCHED AND {changed} THEN UPDATE SET {is_current} = false, {valid_to} = {ts} "
        f"WHEN NOT MATCHED THEN INSERT ({insert}) VALUES ({values}, true, {ts}, NULL)"
    )
    return count, copy, merge


def stage_batch(
    directory: Path, reader: pa.RecordBatchReader, spec: PartitionSpec, schema: Schema
) -> list[str]:
    """The batch as one Parquet file, or one per bucket when `spec` has a bucket field."""
    bucket = next(
        (f for f in spec.fields if isinstance(f.transform, BucketTransform)), None
    )
    writers: dict[object, pq.ParquetWriter] = {}

    def write(value: object, batch: pa.RecordBatch) -> None:
        if value not in writers:
            path = directory / f"batch-{len(writers)}.parquet"
            writers[value] = pq.ParquetWriter(path, reader.schema)
        writers[value].write_batch(batch)

    try:
        for batch in reader:
            if bucket is None:
                write(None, batch)
                continue
            column = schema.find_column_name(bucket.source_id)
            source_type = schema.find_type(bucket.source_id)
            buckets = bucket.transform.pyarrow_transform(source_type)(
                batch.column(column)
            )
            for value in buckets.unique().to_pylist():
                mask = (
                    buckets.is_null() if value is None else pc.equal(buckets, value)  # pyright: ignore[reportAttributeAccessIssue] -- pyarrow.compute builds its functions at import
                )
                write(value, batch.filter(mask))
        if not writers:  # an empty batch still needs a file to read
            write(None, pa.RecordBatch.from_pylist([], reader.schema))
    finally:
        for writer in writers.values():
            writer.close()
    return [str(directory / f"batch-{i}.parquet") for i in range(len(writers))]


def run_merge(
    con: duckdb.DuckDBPyConnection,
    count: str,
    merge: str,
    files: list[str],
    changed: str | None = None,
) -> tuple[int, ...]:
    """The counts, then one MERGE per staged file, all in one transaction (one catalog commit).

    With `changed` (SCD2), each file's changed rows are copied next to it before any MERGE.
    """
    counts = con.execute(count, [files]).fetchone()
    assert counts is not None  # an aggregate always returns one row
    # nothing to insert, update or close: no MERGE, so no snapshot
    if counts[0] or counts[1]:
        params = [
            [f] if changed is None else [f, f"{f}.changed.parquet"] for f in files
        ]
        if changed is not None:
            for p in params:
                con.execute(changed, p)
        con.begin()
        try:
            for p in params:
                con.execute(merge, p)
        except BaseException:
            con.rollback()
            raise
        con.commit()  # a failed commit has already ended the transaction: no rollback
    return counts


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


def _is_conflict(e: FloeRunError) -> bool:
    """The catalog's 409 on commit: DuckDB raises it from `commit()` and does not retry it."""
    return isinstance(e.__cause__, duckdb.TransactionException) and "Conflict_409" in str(e)  # fmt: skip


def _merge(
    con: duckdb.DuckDBPyConnection,
    table: Table,
    reader: pa.RecordBatchReader,
    count: str,
    merge: str,
    changed: str | None = None,
) -> tuple[IcebergWriteResult, tuple[int, ...]]:
    """On a commit conflict the whole MERGE runs again on the fresh table (#922): the counts and
    the changed rows are recomputed, so a key both writers inserted becomes an update.
    """
    with tempfile.TemporaryDirectory(prefix="floe-merge-") as staging:
        with run_error("iceberg merge staging failed"):
            files = stage_batch(Path(staging), reader, table.spec(), table.schema())
        for attempt in range(MERGE_ATTEMPTS):
            if attempt:
                _backoff(attempt - 1)
                with run_error("rest catalog load_table failed"):
                    table.refresh()
            before = table.current_snapshot()
            try:
                with run_error("duckdb merge failed"):
                    counts = run_merge(con, count, merge, files, changed)
            except FloeRunError as e:
                if not _is_conflict(e):
                    raise
                continue
            break
        else:
            raise FloeRunError(f"duckdb merge failed: conflicted {MERGE_ATTEMPTS} times in a row")  # fmt: skip
    with run_error("rest catalog load_table failed"):
        table.refresh()
        added = _added_data_files(table, None if before is None else before.snapshot_id)
    return write_result(table, added), counts


def _target(table: Table) -> str:
    return ".".join(map(_q, (DUCKDB_CATALOG, *table.name())))


def merge_scd1(
    con: duckdb.DuckDBPyConnection,
    table: Table,
    reader: pa.RecordBatchReader,
    merge_key: Sequence[str],
    ignore_columns: Collection[str] = (),
) -> MergeResult:
    """`con` comes from `open_duckdb` on the table's catalog; the table already has the batch's columns."""
    count, merge = scd1_sql(
        _target(table), reader.schema.names, merge_key, ignore_columns
    )
    write, counts = _merge(con, table, reader, count, merge)
    return MergeResult(write, counts[0], counts[1])


def merge_scd2(
    con: duckdb.DuckDBPyConnection,
    table: Table,
    reader: pa.RecordBatchReader,
    merge_key: Sequence[str],
    compare_columns: Sequence[str] | None = None,
    ignore_columns: Collection[str] = (),
    system_columns: Sequence[str] = SCD2_COLUMNS,
) -> MergeResult:
    """As `merge_scd1`; the table also has the system columns (is_current, valid_from, valid_to).

    Compare columns default to every non-key, non-ignored batch column, as in Rust.
    """
    columns = reader.schema.names
    if compare_columns is None:
        compare_columns = [
            c for c in columns if c not in merge_key and c not in ignore_columns
        ]
    # Rust stamps both versions with the UTC time of the merge, as a naive timestamp
    now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
    count, changed, merge = scd2_sql(
        _target(table), columns, merge_key, compare_columns, system_columns, now
    )
    write, (inserted, closed, unchanged) = _merge(
        con, table, reader, count, merge, changed
    )
    return MergeResult(write, inserted, closed, closed, unchanged)
