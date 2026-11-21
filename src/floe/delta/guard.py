"""The retry guard of a Delta append (#1340): the `txn` action `floe:<input_hash>` stamped on the commit, and its lookup."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
# ^ deltalake ships no usable type stubs

import re
import time
from datetime import datetime, timedelta

from deltalake import CommitProperties, DeltaTable, Transaction

_RETENTION = "delta.setTransactionRetentionDuration"
# every unit delta-rs 1.6.6 parses (`parse_interval`, crates/core/src/table/config.rs), in microseconds
_MICROS = {"nanosecond": 1e-3, "microsecond": 1, "millisecond": 1e3, "second": 1e6, "minute": 60e6, "hour": 3600e6, "day": 86400e6, "week": 604800e6}  # fmt: skip


def stamp(run_id: str, input_hash: str | None) -> CommitProperties:
    """`floe.run_id` always; with a hash also `floe.input_hash` and the `txn` (always with `last_updated`, or delta-rs never expires it)."""
    meta = {"floe.run_id": run_id}
    if input_hash is None:
        return CommitProperties(custom_metadata=meta)
    meta["floe.input_hash"] = input_hash
    return CommitProperties(custom_metadata=meta, app_transactions=[Transaction(f"floe:{input_hash}", 1, int(time.time() * 1000))])  # fmt: skip


def _retention(table: DeltaTable) -> timedelta | None:
    """The table's `delta.setTransactionRetentionDuration` (`interval 2 days`); None when unset, zero when set but unreadable (any `txn` may be gone)."""
    raw = table.metadata().configuration.get(_RETENTION)
    if raw is None:
        return None
    found = re.fullmatch(r"interval\s+(\d+)\s+([a-z]+?)s?", raw.strip())
    try:
        return timedelta(microseconds=int(found[1]) * _MICROS[found[2]]) if found and found[2] in _MICROS else timedelta(0)  # fmt: skip
    except OverflowError:
        return timedelta(0)


def committed(
    table: DeltaTable, input_hash: str, since: datetime | None = None
) -> bool | None:
    """Whether the table holds the `txn` of `input_hash`; it is read from the table state, not the history.

    None when `since`, the claim's `acquired_at`, is older than the table's transaction retention, or the retention
    is set but unreadable: the `txn` may have been expired, so the answer is unknown.
    """
    if table.transaction_version(f"floe:{input_hash}") is not None:
        return True
    keep = _retention(table)
    return None if since is not None and keep is not None and datetime.now(since.tzinfo) - since > keep else False  # fmt: skip
