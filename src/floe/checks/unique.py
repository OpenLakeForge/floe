"""Unique keys within a file and across the files of one run (Rust: `checks/unique.rs`)."""

import math
import struct
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

import polars as pl

from floe.checks.rows import RowCheck

SAMPLE_LIMIT = 5
_SEEN = "__floe_seen"
_DEBUG = "__floe_debug"
_COUNT = "__floe_count"


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
    # ponytail: loops over the distinct duplicated float keys; vectorise if that set gets large
    return pl.Series(
        [struct.unpack("<Q", struct.pack("<d", x))[0] for x in s], dtype=pl.UInt64
    )


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
        bits = c.map_batches(_float_bits, return_dtype=pl.UInt64)
        return pl.format("F64({})", bits.cast(pl.String))
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


@dataclass
class _Constraint:
    columns: list[str]
    seen: pl.DataFrame
    dup_counts: list[pl.DataFrame] = field(default_factory=list[pl.DataFrame])
    duplicates: int = 0

    @property
    def ids(self) -> list[str]:
        """Positional names, so key columns named like our temporaries or regexes stay literal."""
        return [f"k{i}" for i in range(len(self.columns))]


class UniqueTracker:
    """Keeps the keys seen so far, so a key repeated in a later file is a duplicate too."""

    def __init__(self, keys: Sequence[Sequence[str]]) -> None:
        self._constraints = [_Constraint(list(k), pl.DataFrame()) for k in keys]

    def check(self, df: pl.DataFrame) -> list[RowCheck]:
        """One `RowCheck` per constraint for the rows of the next file, in constraint order.

        Pass every row, rejected ones included. A row with a NULL in any key column is
        skipped: never a duplicate, never remembered.
        """
        return [self._apply(c, df) for c in self._constraints]

    def _apply(self, c: _Constraint, df: pl.DataFrame) -> RowCheck:
        keys = pl.DataFrame(
            [df[n].alias(k) for n, k in zip(c.columns, c.ids, strict=True)]
        )
        valid = keys.select(pl.all_horizontal(pl.all().is_not_null()))[c.ids[0]]
        dup = pl.Series([False] * df.height)
        if (
            valid.any()
        ):  # skips all-null (Null dtype) frames, which cannot join the seen keys
            # ponytail: floats compare with Polars equality (-0.0 == 0.0), Rust compares bit patterns
            first = keys.select(pl.struct(c.ids).is_first_distinct())[c.ids[0]]
            dup = valid & ~first
            if c.seen.width:
                hit = keys.join(
                    c.seen.with_columns(pl.lit(True).alias(_SEEN)),
                    on=c.ids,
                    how="left",
                    maintain_order="left",
                )[_SEEN]
                dup = dup | (valid & hit.fill_null(False))
            fresh = keys.filter(valid & ~dup)
            c.seen = pl.concat([c.seen, fresh]) if c.seen.width else fresh
            dups = keys.filter(dup)
            c.duplicates += dups.height
            if dups.height:
                c.dup_counts.append(dups.group_by(c.ids).len(_COUNT))
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
        counts = pl.concat(c.dup_counts).group_by(c.ids).agg(pl.col(_COUNT).sum())
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
        return UniqueResult(c.columns, c.duplicates, samples)
