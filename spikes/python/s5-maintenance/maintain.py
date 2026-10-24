"""Floe-built table maintenance on PyIceberg 0.12 + DuckDB 1.5.6 (spike, not production).

    expire_snapshots   native  (ExpireSnapshots.by_ids / older_than) + Floe-built retain-last-N
    orphans            Floe-built (FileIO has no list; boto3 here) with a safety delay
    compact            Floe-built: DuckDB reads (deletes applied) -> PyIceberg writes -> RewriteFiles commit
"""

import itertools
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import pyarrow as pa
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestFile,
    ManifestWriterV2,
)
from pyiceberg.table import Table
from pyiceberg.table.snapshots import Operation, Summary
from pyiceberg.table.update.snapshot import CommitWindow, _OverwriteFiles
from pyiceberg.table.update.validate import _validate_data_files_exist, _validate_no_new_deletes_for_data_files

from common import file_counts, key_of, list_objects


# --------------------------------------------------------------------------------------------
# 1. Snapshot expiry
# --------------------------------------------------------------------------------------------
def expire_snapshots(tbl: Table, *, retain_last: int | None = None, older_than: datetime | None = None) -> int:
    """Keep the newest `retain_last` ancestors of main, and/or everything newer than `older_than`.

    Native: ExpireSnapshots.by_ids / older_than (RemoveSnapshotsUpdate, one catalog commit).
    Floe-built: the retain-last-N selection (PyIceberg has no retain_last). Metadata only: no file is
    deleted by expiry, unreferenced files are reclaimed by `remove_orphans`.
    """
    tbl.refresh()
    snaps = sorted(tbl.metadata.snapshots, key=lambda s: s.timestamp_ms, reverse=True)
    keep = set()
    if retain_last is not None:
        keep |= {s.snapshot_id for s in snaps[:retain_last]}
    if older_than is not None:
        cutoff = int(older_than.timestamp() * 1000)
        keep |= {s.snapshot_id for s in snaps if s.timestamp_ms >= cutoff}
    doomed = [s.snapshot_id for s in snaps if s.snapshot_id not in keep]
    if not doomed:
        return 0
    tbl.maintenance.expire_snapshots().by_ids(doomed).commit()  # protected refs are skipped by PyIceberg
    return len(doomed)


# --------------------------------------------------------------------------------------------
# 2. Orphan removal
# --------------------------------------------------------------------------------------------
def reachable_keys(tbl: Table) -> set[str]:
    """Every object key the table's retained metadata still references."""
    tbl.refresh()
    keys = {key_of(tbl.metadata_location)}
    keys |= {key_of(e.metadata_file) for e in tbl.metadata.metadata_log}
    for s in tbl.metadata.snapshots:
        keys.add(key_of(s.manifest_list))
    af = tbl.inspect.all_files()
    keys |= {key_of(p) for p in af.column("file_path").to_pylist()}
    am = tbl.inspect.all_manifests()
    keys |= {key_of(p) for p in am.column("path").to_pylist()}
    for st in tbl.metadata.statistics:
        keys.add(key_of(st.statistics_path))
    for st in tbl.metadata.partition_statistics:
        keys.add(key_of(st.statistics_path))
    return keys


def find_orphans(tbl: Table, safety: timedelta) -> tuple[list[dict], int]:
    """Objects under the table location that no retained snapshot references and older than `safety`."""
    keep = reachable_keys(tbl)
    cutoff = datetime.now(timezone.utc) - safety
    orphans, too_young = [], 0
    for o in list_objects(key_of(tbl.location()).rstrip("/") + "/"):
        if o["Key"] in keep:
            continue
        if o["LastModified"] > cutoff:
            too_young += 1
            continue
        orphans.append(o)
    return orphans, too_young


def remove_orphans(tbl: Table, safety: timedelta, dry_run: bool = False) -> dict:
    orphans, young = find_orphans(tbl, safety)
    if not dry_run:
        for o in orphans:
            tbl.io.delete(f"s3://warehouse/{o['Key']}")
    return {"removed": 0 if dry_run else len(orphans), "orphans": len(orphans),
            "orphan_bytes": sum(o["Size"] for o in orphans), "skipped_within_safety_delay": young}


# --------------------------------------------------------------------------------------------
# 3. Compaction (Floe-built RewriteFiles; PyIceberg 0.12 has no rewrite/replace commit)
# --------------------------------------------------------------------------------------------
class _DeleteManifestWriter(ManifestWriterV2):
    """PyIceberg 0.12 ManifestWriterV2 can only write content=data manifests."""

    def content(self) -> ManifestContent:
        return ManifestContent.DELETES

    @property
    def _meta(self) -> dict[str, str]:
        return {**super()._meta, "content": "deletes"}


class RewriteFiles(_OverwriteFiles):
    """REPLACE snapshot: add compacted data files, remove the rewritten data files AND the position
    delete files that only applied to them. Stock `_OverwriteFiles` can do the data half only:
    `_deleted_entries` filters on content == DATA, and `_manifests` writes every deleted entry into a
    content=data manifest.
    """

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._removed_delete_files: set[DataFile] = set()
        self._isolation_operation = Operation.DELETE

    def remove_delete_file(self, f: DataFile) -> "RewriteFiles":
        self._removed_delete_files.add(f)
        return self

    # -- manifests -------------------------------------------------------------------------
    def _existing_manifests(self) -> list[ManifestFile]:
        data_manifests = []
        snap = self._transaction.table_metadata.snapshot_by_name(name=self._target_branch)
        delete_manifests: list[ManifestFile] = []
        for m in snap.manifests(io=self._io) if snap else []:
            (delete_manifests if m.content == ManifestContent.DELETES else data_manifests).append(m)

        out: list[ManifestFile] = []
        # data manifests: same logic as _OverwriteFiles, restricted to data manifests
        for m in data_manifests:
            keep, found = [], []
            for e in m.fetch_manifest_entry(io=self._io, discard_deleted=True):
                (found if e.data_file in self._deleted_data_files else keep).append(e)
            if not found:
                out.append(m)
            elif keep:
                with self.new_manifest_writer(self.spec(m.partition_spec_id)) as w:
                    for e in keep:
                        w.add_entry(self._as_existing(e))
                out.append(w.to_manifest_file())
        # delete manifests: drop removed delete files, record them as DELETED in a delete manifest
        by_spec: dict[int, list[ManifestEntry]] = {}
        for m in delete_manifests:
            keep, found = [], []
            for e in m.fetch_manifest_entry(io=self._io, discard_deleted=True):
                (found if e.data_file in self._removed_delete_files else keep).append(e)
            if not found:
                out.append(m)
                continue
            for e in found:
                by_spec.setdefault(m.partition_spec_id, []).append(ManifestEntry.from_args(
                    status=ManifestEntryStatus.DELETED, snapshot_id=self._snapshot_id,
                    sequence_number=e.sequence_number, file_sequence_number=e.file_sequence_number, data_file=e.data_file))
            if keep:
                by_spec.setdefault(m.partition_spec_id, []).extend(self._as_existing(e) for e in keep)
        for spec_id, entries in by_spec.items():
            with self._delete_writer(spec_id) as w:
                for e in entries:
                    w.add_entry(e)
            out.append(w.to_manifest_file())
        return out

    @staticmethod
    def _as_existing(e: ManifestEntry) -> ManifestEntry:
        return ManifestEntry.from_args(status=ManifestEntryStatus.EXISTING, snapshot_id=e.snapshot_id,
                                       sequence_number=e.sequence_number, file_sequence_number=e.file_sequence_number,
                                       data_file=e.data_file)

    def _delete_writer(self, spec_id: int) -> _DeleteManifestWriter:
        return _DeleteManifestWriter(self.spec(spec_id), self.schema(), self.new_manifest_output(), self._snapshot_id, self._compression)

    # -- summary: PyIceberg 0.12 refuses Operation.REPLACE ----------------------------------------
    def _summary(self, snapshot_properties=None):
        snapshot_properties = snapshot_properties or {}
        self._deleted_data_files |= self._removed_delete_files
        self._operation = Operation.OVERWRITE
        try:
            s = super()._summary(snapshot_properties)
        finally:
            self._deleted_data_files -= self._removed_delete_files
            self._operation = Operation.REPLACE
        return Summary(operation=Operation.REPLACE, **s.additional_properties)

    # -- concurrency: Java RewriteFiles semantics --------------------------------------------------
    def _validate_concurrency(self) -> None:
        """A rewrite conflicts only if a concurrent commit removed a rewritten file or added deletes
        against one. Concurrent appends are fine (stock `_OverwriteFiles` at serializable isolation
        rejects them: `_validate_added_data_files(filter=None)`)."""
        if self._commit_window is None or self._commit_window.is_empty() or self._commit_window.head is None:
            return
        table = self._transaction._table
        head, base = self._commit_window.head, self._commit_window.base
        _validate_data_files_exist(table, head, self._deleted_data_files, base)
        _validate_no_new_deletes_for_data_files(table, head, None, self._deleted_data_files, base)

    def _commit(self):
        if self._commit_window is None:
            self._commit_window = CommitWindow.resolve(self._transaction.table_metadata, self._starting_snapshot_id, self._target_branch)
            self._validate_concurrency()
        return super()._commit()


@dataclass
class CompactResult:
    base_snapshot: int
    new_snapshot: int | None = None
    rewritten_data_files: int = 0
    removed_delete_files: int = 0
    added_data_files: int = 0
    rows_written: int = 0
    timings: dict = field(default_factory=dict)


def plan(tbl: Table, snapshot_id: int, row_filter="true"):
    """Files to rewrite = every data file overlapping `row_filter` (use a whole-partition filter),
    plus the position-delete files attached to them."""
    tasks = list(tbl.scan(row_filter=row_filter, snapshot_id=snapshot_id).plan_files())
    data = {t.file for t in tasks}
    deletes = {d for t in tasks for d in t.delete_files}
    return data, deletes


def _num(c: str, typ: str) -> str:
    return f"({c} - DATE '1970-01-01')::DOUBLE" if typ == "date" else f"{c}::DOUBLE"


def select_sql(src: str, kind: str | None, cols: list[str], types: dict[str, str], stats: dict[str, tuple[float, float]] | None,
               colnames: list[str], where: str = "", lead: str | None = None) -> str:
    """SELECT with the rewrite order. 'linear' = lexicographic ORDER BY; 'zorder' = Morton code (bit
    interleave of min-max scaled columns, 31 bits each for 2 cols, 21 for 3). `lead` = partition column,
    sorted first so each output file holds a single partition."""
    lead_sql = [lead] if lead else []
    out = ", ".join(colnames)
    if kind == "zorder":
        n = len(cols)
        bits = {2: 31, 3: 21}[n]
        mx = (1 << bits) - 1
        scaled = ", ".join(
            f"LEAST({mx}, GREATEST(0, ((({_num(c, types[c])} - {stats[c][0]}) / {max(stats[c][1] - stats[c][0], 1e-9)}) * {mx})::BIGINT))::BIGINT AS _z{i}"
            for i, c in enumerate(cols))
        z = " | ".join(f"((((_z{i} >> {b}) & 1)) << {b * n + i})" for b in range(bits) for i in range(n))
        return (f"SELECT {out} FROM (SELECT *, {z} AS _zk FROM (SELECT *, {scaled} FROM {src} {where})) "
                f"ORDER BY {', '.join(lead_sql + ['_zk'])}")
    keys = lead_sql + (cols if kind == "linear" else [])
    return f"SELECT {out} FROM {src} {where}" + (f" ORDER BY {', '.join(keys)}" if keys else "")


def compact(cat, name: str, con, *, sort: str | None = None, sort_cols: list[str] | None = None, row_filter="true",
            inject=None, partition_col: str | None = None, batch_rows=1_000_000, rewrite_cls=None) -> CompactResult:
    """One-commit compaction. `inject(phase)` is called between read/write and commit (concurrency tests)."""
    tbl = cat.load_table(name)
    base = tbl.current_snapshot().snapshot_id
    res = CompactResult(base_snapshot=base)
    t0 = time.time()
    data_files, delete_files = plan(tbl, base, row_filter)
    res.rewritten_data_files, res.removed_delete_files = len(data_files), len(delete_files)
    res.timings["plan_s"] = round(time.time() - t0, 2)

    # delete files may only go if every data file they apply to is rewritten: the filter must be
    # partition-aligned (or the whole table), so a partition's deletes only touch rewritten files.
    q = f"polaris.{name}"
    stats = None
    types = {f.name: str(f.field_type) for f in tbl.schema().fields}
    if sort == "zorder":
        sel = ", ".join(f"min({_num(c, types[c])}), max({_num(c, types[c])})" for c in sort_cols)
        row = con.execute(f"SELECT {sel} FROM {q} AT (VERSION => {base})").fetchone()
        stats = {c: (row[2 * i], row[2 * i + 1]) for i, c in enumerate(sort_cols)}
    where = "" if row_filter == "true" else f"WHERE {row_filter}"
    colnames = [f.name for f in tbl.schema().fields]
    cur = con.execute(select_sql(f"{q} AT (VERSION => {base})", sort, sort_cols or [], types, stats, colnames, where, partition_col))
    schema = tbl.schema().as_arrow()

    t1 = time.time()
    new_files: list[DataFile] = []
    wid = uuid.uuid4()
    counter = itertools.count(0)
    from pyiceberg.io.pyarrow import _dataframe_to_data_files

    def batches():
        for rb in cur.fetch_record_batch(batch_rows):
            yield pa.RecordBatch.from_arrays(rb.columns, schema=schema)

    if tbl.spec().is_unpartitioned():
        reader = pa.RecordBatchReader.from_batches(schema, batches())
        new_files = list(_dataframe_to_data_files(tbl.metadata, reader, tbl.io, wid, counter))
    else:
        # PyIceberg 0.12: streaming writes are unpartitioned-only. Flush one partition value at a time.
        pc_idx = schema.get_field_index(partition_col)
        buf, cur_key = [], None
        def flush():
            nonlocal buf
            if buf:
                new_files.extend(_dataframe_to_data_files(tbl.metadata, pa.Table.from_batches(buf, schema=schema), tbl.io, wid, counter))
                buf = []
        for rb in batches():
            # ORDER BY partition_col first, so a batch holds at most a few distinct partition values
            keys = set(rb.column(pc_idx).to_pylist())
            if len(keys) == 1 and (cur_key is None or keys == {cur_key}):
                cur_key = next(iter(keys)); buf.append(rb)
            else:
                flush(); cur_key = None
                new_files.extend(_dataframe_to_data_files(tbl.metadata, pa.Table.from_batches([rb], schema=schema), tbl.io, wid, counter))
            if sum(b.nbytes for b in buf) > 256 << 20:
                flush()
        flush()
    res.timings["read_write_s"] = round(time.time() - t1, 2)
    res.added_data_files = len(new_files)
    res.rows_written = sum(f.record_count for f in new_files)

    if inject:
        inject("before_commit")

    t2 = time.time()
    tbl = cat.load_table(name)  # head may have moved: producer reads existing manifests from head
    with tbl.transaction() as tx:
        rf = (rewrite_cls or RewriteFiles)(operation=Operation.REPLACE, transaction=tx, io=tbl.io, branch="main",
                          snapshot_properties={"floe.maintenance": "compact"})
        rf._starting_snapshot_id = base
        for f in data_files:
            rf.delete_data_file(f)
        for f in delete_files:
            rf.remove_delete_file(f)
        for f in new_files:
            rf.append_data_file(f)
        rf.commit()
        if inject:
            inject("in_transaction")  # staged, catalog commit not sent yet: a CAS conflict from here is retried
    res.timings["commit_s"] = round(time.time() - t2, 2)
    tbl.refresh()
    res.new_snapshot = tbl.current_snapshot().snapshot_id
    return res


class StockValidationRewrite(RewriteFiles):
    """Same producer, PyIceberg's stock `_OverwriteFiles` validation (serializable): shows why Floe needs its own."""

    _validate_concurrency = _OverwriteFiles._validate_concurrency
