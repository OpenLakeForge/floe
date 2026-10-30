"""Column-name normalization (Rust: `checks/normalize.rs`). ASCII only, as Rust."""

from collections.abc import Sequence
from string import ascii_lowercase, ascii_uppercase

import polars as pl

from floe.config.types import ColumnConfig, SchemaConfig
from floe.errors import FloeConfigError

_STRATEGIES = {"snakecase", "lower", "camelcase", "none"}
_LOWER = str.maketrans(ascii_uppercase, ascii_lowercase)


def _canonical(value: str) -> str:
    return value.lower().replace("-", "").replace("_", "")


def resolve_strategy(schema: SchemaConfig) -> str | None:
    """The canonical strategy when `normalize_columns.enabled`, else None."""
    config = schema.normalize_columns
    if config is None or not config.enabled:
        return None
    raw = config.strategy or "snake_case"
    if (strategy := _canonical(raw)) not in _STRATEGIES:
        raise FloeConfigError(f"unsupported normalize_columns.strategy: {raw}")
    return strategy


def _words(value: str) -> list[str]:
    words: list[str] = []
    current = ""
    for i, ch in enumerate(value):
        if not (ch.isascii() and ch.isalnum()):
            if current:
                words.append(current)
            current = ""
            continue
        prev = value[i - 1] if i else ""
        nxt = value[i + 1] if i + 1 < len(value) else ""
        prev_lower = prev.isascii() and prev.islower()
        prev_digit = prev.isascii() and prev.isdigit()
        prev_upper = prev.isascii() and prev.isupper()
        next_lower = nxt.isascii() and nxt.islower()
        if (
            current
            and ch.isupper()
            and (prev_lower or prev_digit or (prev_upper and next_lower))
        ):
            words.append(current)
            current = ""
        current += ch.translate(_LOWER)
    return [*words, current] if current else words


def normalize_name(value: str, strategy: str | None) -> str:
    match None if strategy is None else _canonical(strategy):
        case "snakecase":
            return "_".join(_words(value))
        case "lower":
            return value.translate(_LOWER)
        case "camelcase":
            return "".join(
                w if i == 0 else w[:1].upper() + w[1:]
                for i, w in enumerate(_words(value))
            )
        case _:
            return value


def source_names(columns: Sequence[ColumnConfig], strategy: str | None) -> list[str]:
    """Normalised `source or name` of each column; two columns on one name are an error."""
    seen: dict[str, str] = {}
    for column in columns:
        source = column.source or column.name
        name = normalize_name(source, strategy)
        if name in seen:
            raise FloeConfigError(
                f"column source collision: {seen[name]} and {source} -> {name}"
            )
        seen[name] = source
    return list(seen)


def output_names(columns: Sequence[ColumnConfig], strategy: str | None) -> list[str]:
    """Output name of each column: an explicit `source` keeps `name`, otherwise it is normalised."""
    return [c.name if c.source else normalize_name(c.name, strategy) for c in columns]


def output_column_mapping(
    columns: Sequence[ColumnConfig], strategy: str | None
) -> dict[str, str]:
    """Normalised source name -> output name, for the columns the rename changes."""
    targets: dict[str, str] = {}
    mapping: dict[str, str] = {}
    for column, target in zip(columns, output_names(columns, strategy), strict=True):
        if target in targets:
            raise FloeConfigError(
                f"output column name collision: {targets[target]} and {column.name} -> {target}"
            )
        targets[target] = column.name
        source = normalize_name(column.source or column.name, strategy)
        if source != target:
            mapping[source] = target
    return mapping


def normalize_columns(df: pl.DataFrame, strategy: str | None) -> pl.DataFrame:
    """Rename the frame's columns; two columns on one name are an error."""
    names = {c: normalize_name(c, strategy) for c in df.columns}
    seen: dict[str, str] = {}
    for old, new in names.items():
        if new in seen:
            raise FloeConfigError(
                f"normalized input column collision: {seen[new]} and {old} -> {new}"
            )
        seen[new] = old
    renamed = df.clone()
    renamed.columns = list(names.values())  # a rename is a query plan, this is not
    return renamed
