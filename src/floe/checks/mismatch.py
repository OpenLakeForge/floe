"""Schema mismatch: missing/extra columns and their policies (Rust: `checks/mismatch.rs`)."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

from floe.checks.normalize import normalize_name, resolve_strategy, source_names
from floe.config.types import EntityConfig
from floe.errors import FloeConfigError, FloeIoError
from floe.read.json import json_records

MAX_MISMATCH_COLUMNS = 50
# nested-path markers: `/` and `@` are XML path syntax, but ordinary characters in a JSON key
_NESTED = {"json": (".", "["), "xml": (".", "[", "/", "@")}


@dataclass(frozen=True)
class MismatchOutcome:
    """`report` is the `mismatch` object of the file report; the rest drives the run."""

    report: dict[str, Any]
    rejected: bool
    aborted: bool
    warnings: int
    errors: int
    missing: list[str]
    extra: list[str]
    fill_missing: bool
    ignore_extra: bool


def declared_names(entity: EntityConfig) -> list[str]:
    """The declared names input columns are compared with (normalised)."""
    schema = entity.schema_
    strategy = resolve_strategy(schema)
    if entity.source.format not in ("json", "xml"):
        return source_names(schema.columns, strategy)
    # Nested selectors are not file columns; only the top-level ones are compared.
    names = [
        normalize_name(c.source or c.name, strategy)
        for c in schema.columns
        if not any(t in (c.source or c.name) for t in _NESTED[entity.source.format])
    ]
    if len(set(names)) != len(names):
        dup = next(n for n in names if names.count(n) > 1)
        raise FloeConfigError(f"duplicate top-level column selector: {dup}")
    return names


def plan_mismatch(
    entity: EntityConfig, declared: list[str], input_names: list[str]
) -> MismatchOutcome:
    strategy = resolve_strategy(entity.schema_)
    inputs = [normalize_name(n, strategy) for n in input_names]
    missing = sorted(set(declared) - set(inputs))
    extra = sorted(set(inputs) - set(declared))
    policy = entity.schema_.mismatch
    on_missing = (policy and policy.missing_columns) or "fill_nulls"
    on_extra = (policy and policy.extra_columns) or "ignore"
    severity = entity.policy.severity

    def requested() -> bool:
        return (on_missing == "reject_file" and bool(missing)) or (
            on_extra == "reject_file" and bool(extra)
        )

    warning = None
    if requested() and severity == "warn":
        warning = (
            f"entity.name={entity.name} schema mismatch requested reject_file "
            "but policy.severity=warn; continuing"
        )
        on_missing, on_extra = "fill_nulls", "ignore"
    rejected = requested() and severity == "reject"
    aborted = requested() and severity == "abort"
    fill = not (rejected or aborted) and on_missing == "fill_nulls" and bool(missing)
    ignore = not (rejected or aborted) and on_extra == "ignore" and bool(extra)
    action = (
        "rejected_file" if rejected
        else "aborted" if aborted
        else "filled_nulls" if fill
        else "ignored_extras" if ignore
        else "none"
    )  # fmt: skip
    report: dict[str, Any] = {
        "declared_columns_count": len(declared),
        "input_columns_count": len(inputs),
        "missing_columns": missing[:MAX_MISMATCH_COLUMNS],
        "extra_columns": extra[:MAX_MISMATCH_COLUMNS],
        "mismatch_action": action,
    }
    if rejected or aborted:
        report["error"] = {
            "rule": "schema_mismatch",
            "message": f"entity.name={entity.name} schema mismatch: "
            f"missing={len(missing)} extra={len(extra)}",
        }
    if warning:
        report["warning"] = warning
    return MismatchOutcome(
        report, rejected, aborted, int(bool(warning)), int(rejected or aborted),
        missing, extra, fill, ignore,
    )  # fmt: skip


def apply_mismatch(df: pl.DataFrame, plan: MismatchOutcome) -> pl.DataFrame:
    """Add the missing columns as nulls and drop the extra ones, as the plan says."""
    if plan.fill_missing:
        # a column the reader already produced (an XML `a:id` selector the probe names `id`) stays
        df = df.with_columns(
            pl.lit(None, pl.String).alias(n)
            for n in plan.missing
            if n not in df.columns
        )
    if plan.ignore_extra:
        df = df.drop(plan.extra, strict=False)
    return df


def json_input_columns(path: Path, mode: str = "array") -> list[str] | None:
    """Sorted top-level keys of every record, read exactly as `read_json` reads them.

    D40: the union over all records, so a key only a later record carries is still an input
    column (Rust 0.7 probed the first object only). `None` for a file without records (D41), which
    differs from records that carry no keys (`[]`): those are still checked for missing columns.
    """
    if mode not in ("array", "ndjson"):
        raise FloeIoError(
            f"unsupported source.options.json_mode={mode} (allowed: array, ndjson)"
        )
    # ponytail: the file is parsed twice (probe, then read); fold the probe into the read if it costs
    keys: set[str] | None = None
    for _, record in json_records(path, mode):
        keys = (keys or set[str]()) | {str(k) for k in record}
    return None if keys is None else sorted(keys)
