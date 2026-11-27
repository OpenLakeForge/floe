from pathlib import Path

from floe.config.parse import parse_config
from floe.contract.effective import contract_overrides

ROOT = 'version: "0.3"\ninclude:\n  domains: ["domains/*/_domain.yml"]\n'
SINK = "sink: {accepted: {format: parquet, path: out}}"
SOURCE = "source: {format: csv, path: in}"
ENTITY = "name: orders\npolicy: {severity: warn}\nschema: {columns: [{name: id, type: string}]}\n"


def settings(
    tmp_path: Path, defaults: str, entity: str = ENTITY
) -> list[tuple[str, str]]:
    (tmp_path / "config.yml").write_text(ROOT)
    domain = tmp_path / "domains" / "sales"
    domain.mkdir(parents=True)
    body = "".join(f"  {line}\n" for line in defaults.splitlines())
    (domain / "_domain.yml").write_text(f"name: sales\ndefaults:\n{body}")
    (domain / "orders.yml").write_text(entity)
    entity_config = parse_config(tmp_path / "config.yml").entities[0]
    return [(o.path, o.setting) for o in contract_overrides(entity_config)]


def test_coerce_from_domain_defaults_reported(tmp_path: Path) -> None:
    defaults = f"source: {{format: csv, path: in, cast_mode: coerce}}\n{SINK}"
    assert settings(tmp_path, defaults) == [("source.cast_mode", "coerce")]


def test_strict_entity_reports_nothing(tmp_path: Path) -> None:
    assert settings(tmp_path, f"{SOURCE}\n{SINK}") == []


def test_all_overrides_in_fixed_order(tmp_path: Path) -> None:
    defaults = (
        "source: {format: csv, path: in, cast_mode: coerce}\n"
        "sink: {write_mode: merge_scd1, accepted: {format: iceberg, path: out}}"
    )
    entity = (
        "name: orders\npolicy: {severity: warn}\n"
        "schema:\n  primary_key: [id]\n  normalize_columns: {enabled: true}\n"
        "  mismatch: {missing_columns: fill_nulls}\n  columns: [{name: id, type: string}]\n"
    )
    assert settings(tmp_path, defaults, entity) == [
        ("source.cast_mode", "coerce"),
        ("schema.mismatch.missing_columns", "fill_nulls"),
        ("schema.normalize_columns", "snake_case"),
        ("sink.write_mode", "merge_scd1"),
    ]


def test_merge_with_unique_reject_reports_nothing(tmp_path: Path) -> None:
    defaults = f"{SOURCE}\nsink: {{write_mode: merge_scd1, accepted: {{format: iceberg, path: out}}}}"
    entity = ENTITY.replace("severity: warn", "severity: reject")
    assert settings(tmp_path, defaults, entity) == []
