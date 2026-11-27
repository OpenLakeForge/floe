from typing import NamedTuple

from floe.checks.normalize import resolve_strategy
from floe.config.types import EntityConfig


class Override(NamedTuple):
    path: str
    setting: str
    effect: str


def contract_overrides(entity: EntityConfig) -> list[Override]:
    found: list[Override] = []
    if entity.source.cast_mode == "coerce":
        found.append(
            Override(
                "source.cast_mode",
                "coerce",
                "values that fail the cast become NULL and pass on nullable columns",
            )
        )
    mismatch = entity.schema_.mismatch
    if mismatch and mismatch.missing_columns == "fill_nulls":
        found.append(
            Override(
                "schema.mismatch.missing_columns",
                "fill_nulls",
                "files missing a contracted column are accepted, the column is filled with NULL",
            )
        )
    strategy = resolve_strategy(entity.schema_)
    if strategy not in (None, "none"):
        normalize = entity.schema_.normalize_columns
        strategy = (normalize and normalize.strategy) or "snake_case"
        found.append(
            Override(
                "schema.normalize_columns",
                strategy,
                "incoming column names are renamed before schema.columns[].source is matched",
            )
        )
    policy = entity.policy
    unique = (policy.rule_severity or {}).get("unique", policy.severity)
    if entity.sink.write_mode in ("merge_scd1", "merge_scd2") and unique == "warn":
        found.append(
            Override(
                "sink.write_mode",
                entity.sink.write_mode,
                "duplicate primary keys are rejected even though unique is warn",
            )
        )
    return found
