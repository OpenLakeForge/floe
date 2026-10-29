"""Unique keys within a file and across the files of one run (Rust: `checks/unique.rs`)."""
# pyarrow ships no type stubs, so strict pyright cannot see its types.
# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false, reportUnknownArgumentType=false

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

import polars as pl
import pyarrow as pa

from floe.checks.rows import RowCheck

SAMPLE_LIMIT = 5
_SEEN = "__floe_seen"
_DEBUG = "__floe_debug"
_COUNT = "__floe_count"
_BITS = "_bits"  # suffix of the bit-pattern column that keys a float column


@dataclass(frozen=True)
class UniqueSample:
    values: dict[str, str]
    count: int


@dataclass(frozen=True)
class UniqueResult:
    """Rust's `UniqueConstraintResult`; every duplicate is a batch duplicate (no target history)."""

    columns: list[str]
    duplicates_count: int
    samples: list[UniqueSample]
    target_duplicates_count: int = 0  # duplicates of keys seeded from the existing sink


def resolve_unique_keys(
    unique_keys: Sequence[Sequence[str]] | None,
    unique_columns: Sequence[str],
    primary_key: Sequence[str] | None,
) -> list[list[str]]:
    """Rust's `resolve_schema_unique_keys`: `unique_keys`, else the `unique: true` columns, then the primary key.

    Names are trimmed; empty and repeated keys are dropped.
    """
    keys = unique_keys if unique_keys is not None else [[c] for c in unique_columns]
    resolved: list[list[str]] = []
    for key in [*keys, *([primary_key] if primary_key else [])]:
        names = [c.strip() for c in key]
        if names and names not in resolved:
            resolved.append(names)
    return resolved


# `str` Debug; ponytail: non-ASCII non-printable characters (Rust `\\u{..}`) are left raw
_STR_ESCAPES = {
    "\\": "\\\\",
    '"': '\\"',
    "\n": "\\n",
    "\r": "\\r",
    "\t": "\\t",
    "\0": "\\0",
    **{chr(i): f"\\u{{{i:x}}}" for i in [*range(1, 32), 127] if chr(i) not in "\n\r\t"},
}


def _float_bits(s: pl.Series) -> pl.Series:
    """Rust's `to_bits()`: Polars cannot reinterpret a float, so view the Arrow buffer."""
    return pl.Series(s.cast(pl.Float64).to_arrow().view(pa.uint64()))


def _debug(name: str, dtype: pl.DataType) -> pl.Expr:
    """Rust's `{:?}` of the `UniqueKey` variant: the sample tie-break sorts on these strings."""
    c = pl.col(name)
    if dtype == pl.String:
        escaped = c
        for raw, esc in _STR_ESCAPES.items():
            escaped = escaped.str.replace_all(raw, esc, literal=True)
        return pl.format('String("{}")', escaped)
    if dtype == pl.Boolean:
        return pl.format("Bool({})", c.cast(pl.String))
    if dtype.is_signed_integer():
        return pl.format("I64({})", c.cast(pl.String))
    if dtype.is_unsigned_integer():
        return pl.format("U64({})", c.cast(pl.String))
    if dtype.is_float():
        return pl.format("F64({})", pl.col(name + _BITS).cast(pl.String))
    if isinstance(dtype, pl.Datetime):  # chrono prints the fraction only when not zero
        return pl.format('Other("{}")', c.dt.to_string("%Y-%m-%d %H:%M:%S%.f"))
    # ponytail: date keys use `Other(display)`, unverified against Rust; ties may order differently
    return pl.format('Other("{}")', c.cast(pl.String))


def _text(value: object) -> str:
    """Rust's `to_string` of a key value."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        if not math.isfinite(value):
            return "NaN" if math.isnan(value) else str(value).replace("infinity", "inf")
        return format(Decimal(repr(value)).normalize(), "f")
    if isinstance(value, datetime):
        if not value.microsecond:
            spec = "seconds"
        else:
            spec = "milliseconds" if value.microsecond % 1000 == 0 else "microseconds"
        return value.replace(tzinfo=None).isoformat(sep=" ", timespec=spec)
    return str(value)


def seed_from_scans(
    tracker: "UniqueTracker",
    keys: Sequence[Sequence[str]],
    scans: Iterable[pl.LazyFrame],
    batch: pl.DataFrame,
    stored: Mapping[str, str],
) -> None:
    """Seed `tracker` with the keys of `scans` (the key columns of the history) that `batch` also holds.

    `stored` maps runtime to stored names. A target key the batch lacks is never hit, so the flags are
    Rust's while memory follows the batch: each scan is streamed, key columns only. The join matches
    -0.0 to 0.0 and NaN to any NaN; the tracker then compares float bits, so those extra keys are
    never hit.
    """
    # positional names (k0, b0, ...): no stored column name can collide with a temporary
    lookups: list[tuple[list[str], list[str], pl.DataFrame]] = []
    for key in keys:
        ids = [f"k{i}" for i in range(len(key))]
        distinct = (
            batch.select(pl.col(c).alias(k) for c, k in zip(key, ids, strict=True))
            .drop_nulls()
            .unique()
        )
        if distinct.height:  # all-NULL keys are never checked; nothing to look up
            lookups.append((list(key), ids, distinct))
    for scan in scans:
        for key, ids, distinct in lookups:
            names = [stored.get(c, c) for c in key]
            lazy = scan.select(pl.col(n).alias(k) for n, k in zip(names, ids, strict=True))  # fmt: skip
            # distinct by float bits, as the tracker keys them: `unique` on a float merges -0.0 with 0.0
            floats = [k for c, k in zip(key, ids, strict=True) if batch.schema[c].is_float()]  # fmt: skip
            bits = {f"b{k}": pl.col(k).map_batches(_float_bits, return_dtype=pl.UInt64, is_elementwise=True) for k in floats}  # fmt: skip
            subset = [k for k in ids if k not in floats] + list(bits)
            # the history's dtypes can differ (unsigned widened, datetimes in us); a key the history
            # cannot hold casts to NULL, which never matches
            lookup = distinct.cast(lazy.collect_schema(), strict=False).lazy()
            hits = (
                lazy.join(lookup, on=ids, how="semi")
                .with_columns(**bits)
                # distinct keys only: memory follows the batch, not the history
                .unique(subset)
                .drop(*bits)
                .collect(engine="streaming")
            )
            back = pl.Schema(
                {k: batch.schema[c] for c, k in zip(key, ids, strict=True)}
            )
            tracker.seed(hits.cast(back).rename(dict(zip(ids, key, strict=True))))


@dataclass
class _Constraint:
    columns: list[str]
    seen: pl.DataFrame
    dup_counts: list[pl.DataFrame] = field(default_factory=list[pl.DataFrame])
    duplicates: int = 0
    target: int = 0
    seeded: pl.DataFrame = field(default_factory=pl.DataFrame)

    @property
    def ids(self) -> list[str]:
        """Positional names, so key columns named like our temporaries or regexes stay literal."""
        return [f"k{i}" for i in range(len(self.columns))]


class UniqueTracker:
    """Keeps the keys seen so far, so a key repeated in a later file is a duplicate too."""

    def __init__(self, keys: Sequence[Sequence[str]]) -> None:
        self._constraints = [_Constraint(list(k), pl.DataFrame()) for k in keys]

    @property
    def keys(self) -> list[list[str]]:
        return [list(c.columns) for c in self._constraints]

    def check(self, df: pl.DataFrame) -> list[RowCheck]:
        """One `RowCheck` per constraint for the rows of the next file, in constraint order.

        Pass every row, rejected ones included. A row with a NULL in any key column is
        skipped: never a duplicate, never remembered.
        """
        return [self._apply(c, df) for c in self._constraints]

    def snapshot(self) -> list[tuple[pl.DataFrame, list[pl.DataFrame], int, int]]:
        """The state `restore` goes back to: a file that is rejected as unreadable leaves no keys."""
        return [
            (c.seen, list(c.dup_counts), c.duplicates, c.target)
            for c in self._constraints
        ]

    def restore(
        self, snapshot: list[tuple[pl.DataFrame, list[pl.DataFrame], int, int]]
    ) -> None:
        for c, (seen, counts, dups, target) in zip(
            self._constraints, snapshot, strict=True
        ):
            c.seen, c.dup_counts, c.duplicates, c.target = (
                seen,
                list(counts),
                dups,
                target,
            )

    def seed(self, df: pl.DataFrame) -> None:
        """Remember the keys of rows already in the sink (append mode); a later hit is a target duplicate."""
        for c in self._constraints:
            if all(n in df.columns for n in c.columns):
                keys = self._keys(c, df)
                valid = keys.select(pl.all_horizontal(pl.all().is_not_null()))[c.ids[0]]
                fresh = keys.filter(valid).unique()
                c.seen = pl.concat([c.seen, fresh]).unique() if c.seen.width else fresh
                # Parquet seeds part by part
                c.seeded = (
                    pl.concat([c.seeded, fresh]).unique() if c.seeded.width else fresh
                )

    @staticmethod
    def _keys(c: _Constraint, df: pl.DataFrame) -> pl.DataFrame:
        keys = pl.DataFrame(
            [df[n].alias(k) for n, k in zip(c.columns, c.ids, strict=True)]
        )
        # Rust compares `to_bits()`: -0.0 differs from 0.0, NaN payloads differ; key on the bits too
        return keys.with_columns(
            pl.col(k).map_batches(_float_bits, return_dtype=pl.UInt64).alias(k + _BITS)
            for k in c.ids
            if keys.schema[k].is_float()
        )

    def _apply(self, c: _Constraint, df: pl.DataFrame) -> RowCheck:
        keys = self._keys(c, df)
        valid = keys.select(pl.all_horizontal(pl.all().is_not_null()))[c.ids[0]]
        dup = pl.Series([False] * df.height)
        if (
            valid.any()
        ):  # skips all-null (Null dtype) frames, which cannot join the seen keys
            first = keys.select(pl.struct(keys.columns).is_first_distinct())[c.ids[0]]
            dup = valid & ~first
            if c.seen.width:
                hit = keys.join(
                    c.seen.with_columns(pl.lit(True).alias(_SEEN)),
                    on=keys.columns,
                    how="left",
                    maintain_order="left",
                )[_SEEN]
                dup = dup | (valid & hit.fill_null(False))
            fresh = keys.filter(valid & ~dup)
            c.seen = pl.concat([c.seen, fresh]) if c.seen.width else fresh
            dups = keys.filter(dup)
            c.duplicates += dups.height
            if c.seeded.width:
                c.target += dups.join(c.seeded, on=keys.columns, how="semi").height
            if dups.height:
                c.dup_counts.append(dups.group_by(keys.columns).len(_COUNT))
        single = len(c.columns) == 1
        name = c.columns[0] if single else f"[{','.join(c.columns)}]"
        message = "duplicate value" if single else "duplicate key"
        return RowCheck("unique", name, message, dup)

    def results(self) -> list[UniqueResult]:
        """Per constraint, the top `SAMPLE_LIMIT` keys by count, ties by Rust's Debug string."""
        return [self._result(c) for c in self._constraints]

    def _result(self, c: _Constraint) -> UniqueResult:
        if not c.dup_counts:
            return UniqueResult(c.columns, 0, [])
        parts = pl.concat(c.dup_counts)
        counts = parts.group_by(parts.columns[:-1]).agg(pl.col(_COUNT).sum())
        debug = pl.format(
            "CompositeKey([{}])",
            pl.concat_str([_debug(k, counts.schema[k]) for k in c.ids], separator=", "),
        )
        top = (
            counts.with_columns(debug.alias(_DEBUG))
            .sort([_COUNT, _DEBUG], descending=[True, False])
            .head(SAMPLE_LIMIT)
        )
        samples = [  # at most SAMPLE_LIMIT rows
            UniqueSample(
                {n: _text(row[k]) for n, k in zip(c.columns, c.ids, strict=True)},
                row[_COUNT],
            )
            for row in top.iter_rows(named=True)
        ]
        return UniqueResult(c.columns, c.duplicates, samples, c.target)
