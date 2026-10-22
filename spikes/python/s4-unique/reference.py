"""Brute-force reference for Floe's unique check on append, in plain Python.

Mirrors the Rust contract:
- `crates/floe-core/src/io/unique_seed/mod.rs` (`seed_unique_tracker_for_append`): on append, the
  tracker is seeded with the key columns of every row already in the target.
- `crates/floe-core/src/checks/unique.rs` (`UniqueTracker::seed_from_df`, `apply_sparse_*`,
  `composite_key_from_row`, `unique_key`):
  * a row with a NULL in any key column is skipped: never flagged, never added to `seen`;
  * seeded keys go into `seen` and `seeded_keys`; target NULL keys are not seeded;
  * incoming rows are visited in order: a key already in `seen` is flagged (counted as
    `target` if it is a seeded key, else `batch`), otherwise it is added to `seen`. So the first
    occurrence of a new key passes and every later one is flagged, while every occurrence of a
    key already in the target is flagged;
  * floats compare by bit pattern (`f64::to_bits`): -0.0 != 0.0, and NaN == NaN for the same bits.

Holds every key in a Python set, so it only runs at small scale. That is the point of #600.
"""

import struct


def _key_value(v):
    if isinstance(v, float):
        return ("f64", struct.pack("<d", v))
    return v


def composite_key(row, columns):
    key = []
    for c in columns:
        v = row[c]
        if v is None:
            return None
        key.append(_key_value(v))
    return tuple(key)


def unique_check(history_rows, incoming_rows, columns):
    """Returns {incoming row index: 'target' | 'batch'} for every flagged row."""
    seeded = set()
    for row in history_rows:
        key = composite_key(row, columns)
        if key is not None:
            seeded.add(key)
    seen = set(seeded)
    flagged = {}
    for idx, row in enumerate(incoming_rows):
        key = composite_key(row, columns)
        if key is None:
            continue
        if key in seen:
            flagged[idx] = "target" if key in seeded else "batch"
        else:
            seen.add(key)
    return flagged
