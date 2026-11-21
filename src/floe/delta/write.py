"""Overwrite and append of an Arrow stream to a Delta table in one `write_deltalake` call, one commit."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportMissingTypeStubs=false
# ^ pyarrow and deltalake ship no usable type stubs

from pathlib import PurePosixPath

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError, DeltaError, TableNotFoundError

from floe.delta.guard import committed, stamp
from floe.errors import FloeRunError
from floe.write.outputs import AcceptedWrite

_SMALL_FILE_BYTES = 16 * 1024 * 1024
_REPORTED_FILES = 50
# what delta-rs says when a table needs a feature it cannot write (`Unsupported table features required: [...]`)
_UNSUPPORTED = ("Unsupported table features", "is not supported", "catalog-managed")


def _plain(kind: pa.DataType) -> pa.DataType:
    """The form Delta stores: `string` for Polars' `large_string`, microseconds for a timestamp (Floe casts to `ms`); a zone stays, so naive vs zoned is still a change."""
    if pa.types.is_large_string(kind):
        return pa.string()
    return pa.timestamp("us", kind.tz) if pa.types.is_timestamp(kind) else kind


def _files(table: DeltaTable) -> dict[str, int]:
    actions = pa.table(table.get_add_actions(flatten=True))
    return dict(zip(actions["path"].to_pylist(), actions["size_bytes"].to_pylist(), strict=True))  # fmt: skip


def _refuse_drift(who: str, table: DeltaTable, schema: pa.Schema) -> None:
    """delta-rs casts a column to the table's type silently (`"3"` into an int64 column), so the columns must match first."""
    have = {f.name: _plain(f.type) for f in pa.schema(table.schema().to_arrow())}
    want = {f.name: _plain(f.type) for f in schema}
    if have.keys() != want.keys():
        missing, extra = sorted(have.keys() - want.keys()), sorted(want.keys() - have.keys())  # fmt: skip
        raise FloeRunError(f"{who} delta schema evolution failed: columns differ from the table (missing: {', '.join(missing) or 'none'}; new: {', '.join(extra) or 'none'})", code="run_schema_evolution")  # fmt: skip
    for name, kind in want.items():
        if have[name] != kind:
            raise FloeRunError(f"{who} delta schema evolution failed: column {name} is {have[name]} in the table, {kind} in the batch", code="run_schema_evolution")  # fmt: skip


def named(who: str, uri: str, err: DeltaError | OSError) -> FloeRunError:
    text = str(err)
    if any(s in text for s in _UNSUPPORTED):
        return FloeRunError(f"{who} cannot write the delta table at {uri}: it uses a table feature delta-rs cannot write ({text}); disable the feature or write another table", code="run_not_supported")  # fmt: skip
    return FloeRunError(f"{who} delta write failed: {text}", code="run_commit_failed")


def write_table(who: str, uri: str, options: dict[str, str], reader: pa.RecordBatchReader, mode: str, run_id: str, input_hash: str | None) -> tuple[AcceptedWrite, int, bool]:  # fmt: skip
    """Write `reader` as one commit; returns the files it added, the version of that commit and whether it was already there.

    Overwrite removes every live file in the same commit. A table Floe creates keeps delta-rs's minimal protocol; an
    existing table is never altered. The commit carries `run_id` and, with an `input_hash`, the retry guard's `txn`
    (#1308); a commit conflict that finds that `txn` in the table means a racing run wrote this batch first (#920).

    ponytail: `write_deltalake` is not memory-bounded: peak memory is about twice the entity's compressed size
    (measured in context/decisions/delta-sink.md). Upgrade path: chunked data files plus `DeltaTable.create_write_transaction`.
    """
    try:
        try:
            table = DeltaTable(uri, storage_options=options)
        except TableNotFoundError:
            table = None
        if table is not None:
            _refuse_drift(who, table, reader.schema)
        before = _files(table) if table is not None else {}
        base = table.version() if table is not None else -1
        if (
            table is not None
            and input_hash is not None
            and committed(table, input_hash)
        ):  # a run that committed since the guard looked
            return AcceptedWrite(0, [], 0, 0.0, 0), base, True
        try:
            write_deltalake(uri if table is None else table, reader, mode="overwrite" if mode == "overwrite" else "append", storage_options=options, commit_properties=stamp(run_id, input_hash))  # fmt: skip
        except CommitFailedError as err:
            if input_hash is None or not committed(raced := DeltaTable(uri, storage_options=options), input_hash):  # fmt: skip
                raise named(who, uri, err) from err
            return AcceptedWrite(0, [], 0, 0.0, 0), raced.version(), True
        after = DeltaTable(uri, storage_options=options)
        added = {p: s for p, s in _files(after).items() if p not in before}
        # the commit's own version: the newest of the commits since `base` that carries this run's id
        version = next(h["version"] for h in after.history(limit=after.version() - base) if h.get("floe.run_id") == run_id)  # fmt: skip
    except (DeltaError, OSError) as err:
        raise named(who, uri, err) from err
    sizes = list(added.values())
    return (
        AcceptedWrite(
            len(added),
            [PurePosixPath(p).name for p in added][:_REPORTED_FILES],
            sum(sizes),
            sum(sizes) / len(sizes) / (1024 * 1024) if sizes else None,
            sum(s < _SMALL_FILE_BYTES for s in sizes),
        ),
        version,
        False,
    )
