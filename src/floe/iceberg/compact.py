"""Experimental compaction (#952): rewrite every data file, fold the position deletes, one REPLACE commit.

PyIceberg 0.12 has no `RewriteFiles`, so this subclasses its private `_OverwriteFiles` and `ManifestWriterV2`
(spike #618 S5). `tests/unit/iceberg/test_compact.py` pins the PyIceberg version they were checked against.
"""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false, reportPrivateUsage=false
# ^ pyarrow ships no type stubs; the private PyIceberg internals are the point of this module

from collections.abc import Mapping

import duckdb
import pyarrow as pa
from pyiceberg.io.pyarrow import _dataframe_to_data_files
from pyiceberg.manifest import (
    DataFileContent,
    ManifestContent,
    ManifestEntry,
    ManifestFile,
    ManifestWriter,
    ManifestWriterV2,
)
from pyiceberg.table import Table
from pyiceberg.table.snapshots import Operation, Summary
from pyiceberg.table.update import UpdatesAndRequirements
from pyiceberg.table.update.snapshot import CommitWindow, _OverwriteFiles
from pyiceberg.table.update.validate import (
    _validate_data_files_exist,
    _validate_no_new_deletes_for_data_files,
)

from floe.errors import FloeRunError
from floe.iceberg.catalog import run_error
from floe.iceberg.merge import _target
from floe.iceberg.write import CHUNK_ROWS


class _DeleteManifestWriter(ManifestWriterV2):
    """PyIceberg 0.12's `ManifestWriterV2` writes `content=data` manifests only."""

    def content(self) -> ManifestContent:
        return ManifestContent.DELETES

    @property
    def _meta(self) -> dict[str, str]:
        return {**super()._meta, "content": "deletes"}


class _RewriteFiles(_OverwriteFiles):
    """Java `RewriteFiles`: `_deleted_data_files` holds the rewritten data files and their delete files."""

    def _deleted_entries(self) -> list[ManifestEntry]:
        return []  # marked DELETED in place by `_existing_manifests`

    def _existing_manifests(self) -> list[ManifestFile]:
        """The head's manifests; one that holds a removed file is rewritten with that entry DELETED."""
        out: list[ManifestFile] = []
        head = self._transaction.table_metadata.snapshot_by_name(self._target_branch)
        for manifest in head.manifests(self._io) if head else []:
            entries = manifest.fetch_manifest_entry(self._io, discard_deleted=True)
            if not any(e.data_file in self._deleted_data_files for e in entries):
                out.append(manifest)
                continue
            spec = self.spec(manifest.partition_spec_id)
            writer: ManifestWriter = (
                _DeleteManifestWriter(spec, self.schema(), self.new_manifest_output(), self._snapshot_id, self._compression)
                if manifest.content == ManifestContent.DELETES
                else self.new_manifest_writer(spec)
            )  # fmt: skip
            with writer:
                for e in entries:
                    writer.delete(e) if e.data_file in self._deleted_data_files else writer.existing(e)  # fmt: skip
            out.append(writer.to_manifest_file())
        return out

    def _summary(self, snapshot_properties: Mapping[str, str] = {}) -> Summary:
        # built as an OVERWRITE: PyIceberg 0.12's `update_snapshot_summaries` rejects REPLACE
        summary = super()._summary(dict(snapshot_properties))
        return Summary(Operation.REPLACE, **summary.additional_properties)

    def _validate_concurrency(self) -> None:
        """Only a removed rewritten file, or a new delete against one, conflicts; concurrent appends don't."""
        window = self._commit_window
        if window is None or window.is_empty() or window.head is None:
            return
        table, data = self._transaction._table, {f for f in self._deleted_data_files if f.content == DataFileContent.DATA}  # fmt: skip
        _validate_data_files_exist(
            table, window.head, self._deleted_data_files, window.base
        )
        _validate_no_new_deletes_for_data_files(
            table, window.head, None, data, window.base
        )

    def _commit(self) -> UpdatesAndRequirements:
        # first attempt: validate the commits since the snapshot the rewrite read
        if self._commit_window is None:
            self._commit_window = CommitWindow.resolve(self._transaction.table_metadata, self._starting_snapshot_id, self._target_branch)  # fmt: skip
            self._validate_concurrency()
        return super()._commit()


def compact(table: Table, con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Rewrite the whole table at its current snapshot through DuckDB (deletes applied), then commit once.

    Concurrent appends are kept; a concurrent MERGE on the same files makes the compaction fail.
    """
    base = table.current_snapshot()
    if base is None:
        return {"rewritten_data_files": 0, "removed_delete_files": 0, "added_data_files": 0}  # fmt: skip
    if not table.spec().is_unpartitioned() or table.format_version != 2:
        # ponytail: PyIceberg 0.12 streams unpartitioned v2 writes only; partitioned needs an ordered per-partition flush
        raise FloeRunError(f"iceberg compaction supports unpartitioned format v2 tables only ({'.'.join(table.name())})")  # fmt: skip
    with run_error("iceberg compaction plan failed"):
        tasks = list(table.scan(snapshot_id=base.snapshot_id).plan_files())
    data = {t.file for t in tasks}
    deletes = {d for t in tasks for d in t.delete_files}
    if any(d.content == DataFileContent.EQUALITY_DELETES for d in deletes):
        raise FloeRunError("iceberg compaction does not support equality deletes")
    source = f"{_target(table)} AT (VERSION => {base.snapshot_id})"
    schema = table.schema().as_arrow()
    with run_error("iceberg compaction rewrite failed"):
        cursor = con.execute(f"SELECT * FROM {source}")
        batches = (b.cast(schema) for b in cursor.to_arrow_reader(CHUNK_ROWS))
        new = list(_dataframe_to_data_files(table.metadata, pa.RecordBatchReader.from_batches(schema, batches), table.io))  # fmt: skip
        live = con.execute(f"SELECT count(*) FROM {source}").fetchone()
    if live is None or live[0] != sum(f.record_count for f in new):
        raise FloeRunError(f"iceberg compaction wrote {sum(f.record_count for f in new)} rows, expected {live and live[0]}; nothing was committed")  # fmt: skip
    with run_error("iceberg compaction commit failed"):
        table.refresh()  # the head may have moved: its manifests are the ones carried over
        tx = table.transaction()
        rewrite = _RewriteFiles(Operation.OVERWRITE, tx, table.io, snapshot_properties={"floe.maintenance": "compact"})  # fmt: skip
        rewrite._starting_snapshot_id = base.snapshot_id
        for f in data | deletes:
            rewrite.delete_data_file(f)
        for f in new:
            rewrite.append_data_file(f)
        rewrite.commit()
        tx.commit_transaction()
    return {"rewritten_data_files": len(data), "removed_delete_files": len(deletes), "added_data_files": len(new)}  # fmt: skip
