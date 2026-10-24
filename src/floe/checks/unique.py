"""Unique keys within a file and across the files of one run (Rust: `checks/unique.rs`)."""

from collections.abc import Sequence
from dataclasses import dataclass, field

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


def _debug(name: str, dtype: pl.DataType) -> pl.Expr:
    """Rust's `{:?}` of the `UniqueKey` variant: the sample tie-break sorts on these strings."""
    c = pl.col(name)
    if dtype == pl.String:
        escaped = c.str.replace_all("\\", "\\\\", literal=True)
        for raw, esc in (('"', '\\"'), ("\n", "\\n"), ("\r", "\\r"), ("\t", "\\t")):
            escaped = escaped.str.replace_all(raw, esc, literal=True)
        return pl.format('String("{}")', escaped)
    if dtype == pl.Boolean:
        return pl.format("Bool({})", c.cast(pl.String))
    if dtype.is_signed_integer():
        return pl.format("I64({})", c.cast(pl.String))
    if dtype.is_unsigned_integer():
        return pl.format("U64({})", c.cast(pl.String))
    if dtype.is_float():
        bits = c.cast(pl.Float64).reinterpret(signed=False)
        return pl.format("F64({})", bits.cast(pl.String))
    # ponytail: date/datetime keys use `Other(display)`, unverified against Rust; ties may order differently
    return pl.format('Other("{}")', c.cast(pl.String))


def _text(value: object) -> str:
    """Rust's `to_string` of a key value."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return f"{value:.0f}" if value.is_integer() else str(value)
    return str(value)


@dataclass
class _Constraint:
    columns: list[str]
    seen: pl.DataFrame
    dup_counts: list[pl.DataFrame] = field(default_factory=list[pl.DataFrame])
    duplicates: int = 0


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
        keys = df.select(c.columns)
        # ponytail: floats compare with Polars equality (-0.0 == 0.0), Rust compares bit patterns
        flags = keys.select(
            pl.all_horizontal(pl.all().is_not_null()).alias("valid"),
            pl.struct(c.columns).is_first_distinct().alias("first"),
        )
        valid = flags["valid"]
        dup = valid & ~flags["first"]
        if c.seen.width:
            hit = keys.join(
                c.seen.with_columns(pl.lit(True).alias(_SEEN)),
                on=c.columns,
                how="left",
                maintain_order="left",
            )[_SEEN]
            dup = dup | (valid & hit.fill_null(False))
        fresh = keys.filter(valid & ~dup)
        c.seen = pl.concat([c.seen, fresh]) if c.seen.width else fresh
        dups = keys.filter(dup)
        c.duplicates += dups.height
        if dups.height:
            c.dup_counts.append(dups.group_by(c.columns).len(_COUNT))
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
        counts = pl.concat(c.dup_counts).group_by(c.columns).agg(pl.col(_COUNT).sum())
        debug = pl.format(
            "CompositeKey([{}])",
            pl.concat_str(
                [_debug(n, counts.schema[n]) for n in c.columns], separator=", "
            ),
        )
        top = (
            counts.with_columns(debug.alias(_DEBUG))
            .sort([_COUNT, _DEBUG], descending=[True, False])
            .head(SAMPLE_LIMIT)
        )
        samples = [  # at most SAMPLE_LIMIT rows
            UniqueSample({n: _text(row[n]) for n in c.columns}, row[_COUNT])
            for row in top.iter_rows(named=True)
        ]
        return UniqueResult(c.columns, c.duplicates, samples)
