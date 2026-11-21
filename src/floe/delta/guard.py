"""The retry guard of a Delta append (#1308): the `txn` action `floe:<input_hash>` stamped on the commit, and its lookup."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
# ^ deltalake ships no usable type stubs

import re
import time
from datetime import datetime, timedelta

from deltalake import CommitProperties, DeltaTable, Transaction

_RETENTION = "delta.setTransactionRetentionDuration"
_UNITS = {"week": "weeks", "day": "days", "hour": "hours", "minute": "minutes", "second": "seconds", "millisecond": "milliseconds"}  # fmt: skip


def stamp(run_id: str, input_hash: str | None) -> CommitProperties:
    """`floe.run_id` always; with a hash also `floe.input_hash` and the `txn` (always with `last_updated`, or delta-rs never expires it)."""
    meta = {"floe.run_id": run_id}
    if input_hash is None:
        return CommitProperties(custom_metadata=meta)
    meta["floe.input_hash"] = input_hash
    return CommitProperties(custom_metadata=meta, app_transactions=[Transaction(f"floe:{input_hash}", 1, int(time.time() * 1000))])  # fmt: skip


def _retention(table: DeltaTable) -> timedelta | None:
    """The table's `delta.setTransactionRetentionDuration` (`interval 2 days`), None when unset or not an interval we know."""
    found = re.fullmatch(r"(?:interval\s+)?(\d+)\s+(\w+?)s?", table.metadata().configuration.get(_RETENTION, "").strip().lower())  # fmt: skip
    return (
        timedelta(**{_UNITS[found[2]]: int(found[1])})
        if found and found[2] in _UNITS
        else None
    )


def committed(
    table: DeltaTable, input_hash: str, since: datetime | None = None
) -> bool | None:
    """Whether the table holds the `txn` of `input_hash` (#1308); it is read from the table state, not the history.

    None when `since`, the claim's `acquired_at`, is older than the table's transaction retention: the `txn` may have
    been expired, so the answer is unknown.
    """
    if table.transaction_version(f"floe:{input_hash}") is not None:
        return True
    keep = _retention(table)
    return (
        None
        if since is not None
        and keep is not None
        and datetime.now(since.tzinfo) - since > keep
        else False
    )
