"""Expected messages come from the Rust 0.7 binary (`floe validate`), except the D7 ones (#584 S6, S7)."""

import textwrap
from pathlib import Path
from typing import cast

import pytest

from floe import _cli
from floe.config.parse import parse_config
from floe.config.project import RootConfig, config_version, validate_domains
from floe.config.yaml import merge_yaml
from floe.errors import FloeConfigError

REST = "sink: {accepted: {format: parquet, path: o}}, policy: {severity: warn}, schema: {columns: [{name: id, type: string}]}"
ENTITY = f"{{name: orders, source: {{format: csv, path: in/orders.csv}}, {REST}}}"


def load(tmp_path: Path, text: str) -> RootConfig:
    path = tmp_path / "config.yml"
    path.write_text(textwrap.dedent(text))
    return parse_config(path)


def error(tmp_path: Path, text: str) -> str:
    with pytest.raises(FloeConfigError) as err:
        load(tmp_path, text)
    return str(err.value)


@pytest.mark.parametrize(
    ("raw", "version"),
    [("0.3", (0, 3)), ("+1.02", (1, 2)), ("18446744073709551615.0", (2**64 - 1, 0))],
)
def test_config_version(raw: str, version: tuple[int, int]) -> None:
    assert config_version(raw) == version


@pytest.mark.parametrize(
    "raw", ["", "1", "1.2.3", "a.1", "1.-1", "1. 2", "1.٣", "18446744073709551616.0"]
)
def test_config_version_invalid(raw: str) -> None:
    with pytest.raises(FloeConfigError) as err:
        config_version(raw)
    assert (
        str(err.value)
        == f'root.version={raw} is invalid; expected numeric major.minor format like "0.1"'
    )


def test_root_fields(tmp_path: Path) -> None:
    (tmp_path / "e.yml").write_text("K: v\n")
    config = load(
        tmp_path,
        f"""
        version: "0.3"
        metadata: {{project: p, tags: [a, b]}}
        env: {{file: e.yml, vars: {{A: b}}}}
        filesystems: {{definitions: []}}
        domains: [{{name: sales}}]
        entities: [{ENTITY}]
        """,
    )
    assert config.metadata is not None
    assert (config.metadata.project, config.metadata.owner, config.metadata.tags) == (
        "p",
        None,
        ["a", "b"],
    )
    assert config.env is not None
    assert (config.env.file, config.env.vars) == ("e.yml", {"A": "b"})
    assert config.storages is not None
    assert config.storages.definitions == []
    assert [d.name for d in config.domains] == ["sales"]
    assert config.config_files == [tmp_path / "config.yml"]


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("entities: []", "missing required field root.version"),
        ('version: "0.3"', "missing required field root.entities"),
        ('version: "0.3"\ncolour: 1\nentities: []', "unknown field root.colour"),
        (
            'version: "0.3"\nstorages: {}\nfilesystems: {}\nentities: []',
            "root.storages and root.filesystems are mutually exclusive",
        ),
        ('version: "0.3"\nenv: {files: x}\nentities: []', "unknown field env.files"),
        (
            'version: "0.3"\nenv: {vars: {a: 1}}\nentities: []',
            "expected string at env.vars",
        ),
        (
            'version: "0.3"\nmetadata: {tags: x}\nentities: []',
            "expected array at metadata.tags",
        ),
        ('version: "0.3"\ndomains: x\nentities: []', "expected array at domains"),
        (
            'version: "0.3"\ndomains: [{name: a, bogus: 1}]\nentities: []',
            "unknown field domains.bogus",
        ),
        (
            'version: "0.3"\ndomains: [{}]\nentities: []',
            "missing required field domains.name",
        ),
        (
            'version: "0.2"\ndomains: [{name: a}]\nentities: []',
            "missing required field domains.incoming_dir",
        ),
        (
            'version: "0.2"\ndomains: [{name: a, incoming_dir: d, defaults: {}}]\nentities: []',
            'domains.defaults requires root.version >= "0.3"',
        ),
        (
            'version: "0.3"\ndomains: [{name: a, defaults: 1}]\nentities: []',
            "expected map at domains.defaults",
        ),
    ],
)
def test_root_errors(tmp_path: Path, text: str, message: str) -> None:
    assert error(tmp_path, text) == message


SOURCES = "sources: [{name: lake, format: csv, path: 'in/{{resource}}.csv', options: {header: true}}]"


@pytest.mark.parametrize(
    ("text", "message"),
    [
        (
            'version: "0.2"\nsources: []\nentities: []',
            'root.sources requires root.version >= "0.3"',
        ),
        ('version: "0.3"\nsources: x\nentities: []', "expected array at sources"),
        ('version: "0.3"\nsources: [x]\nentities: []', "expected map at sources"),
        (
            'version: "0.3"\nsources: [{format: csv}]\nentities: []',
            "missing required field sources.name",
        ),
        (
            'version: "0.3"\nsources: [{name: a, bogus: 1}]\nentities: []',
            "unknown field sources.bogus",
        ),
        (
            'version: "0.3"\nsources: [{name: a}, {name: a}]\nentities: []',
            "duplicate source name a",
        ),
        (
            f'version: "0.3"\n{SOURCES}\nentities: [{{name: orders, source: {{ref: river}}}}]',
            "entities[0] (entity.name=orders): source.ref=river is not a declared source (available: lake)",
        ),
        (
            f'version: "0.3"\n{SOURCES}\nentities: [{{name: orders, source: {{ref: 1}}}}]',
            "entities[0] (entity.name=orders): expected string at source.ref",
        ),
        (
            f'version: "0.3"\n{SOURCES}\nentities: [{{name: orders, source: {{ref: lake, resource: 1}}}}]',
            "entities[0] (entity.name=orders): expected string at source.resource",
        ),
        (
            f'version: "0.3"\n{SOURCES}\nentities: [{{source: {{ref: river}}}}]',
            "entities[0]: source.ref=river is not a declared source (available: lake)",
        ),
    ],
)
def test_source_errors(tmp_path: Path, text: str, message: str) -> None:
    assert error(tmp_path, text) == message


def test_source_ref_overlays_the_named_source_and_substitutes_the_resource(
    tmp_path: Path,
) -> None:
    config = load(
        tmp_path,
        f"""
        version: "0.3"
        {SOURCES}
        entities:
        - {{name: orders, source: {{ref: lake, options: {{separator: "|"}}}}, {REST}}}
        - {{name: x, source: {{ref: lake, resource: items, path: "{{{{ resource }}}}/{{{{entity.name}}}}"}}, {REST}}}
        """,
    )
    assert [
        (e.source.path, e.source.options.header, e.source.options.separator)
        for e in config.entities
    ] == [("in/orders.csv", True, "|"), ("items/x", True, ";")]
    assert [(e.source.source_ref, e.source.resource) for e in config.entities] == [
        ("lake", "orders"),
        ("lake", "items"),
    ]


def test_domain_defaults_are_merged_under_the_entity(tmp_path: Path) -> None:
    config = load(
        tmp_path,
        """
        version: "0.3"
        domains:
        - name: finance
        - name: sales
          defaults: {policy: {severity: warn}, sink: {accepted: {format: parquet, path: o}}, source: {format: csv, path: p}}
        entities:
        - {name: a, domain: sales, policy: {severity: reject}, schema: {columns: [{name: id, type: string}]}}
        - {name: b, domain: finance, policy: {severity: abort}, sink: {accepted: {format: csv, path: x}}, source: {format: csv, path: p}, schema: {columns: [{name: id, type: string}]}}
        - {name: c, domain: sales, schema: {columns: [{name: id, type: string}]}}
        """,
    )
    a, b, c = config.entities
    assert [(e.policy.severity, e.sink.accepted.format) for e in (a, b, c)] == [
        ("reject", "parquet"),
        ("abort", "csv"),
        ("warn", "parquet"),
    ]
    assert [e.id for e in config.entities] == ["sales.a", "finance.b", "sales.c"]
    assert config.domains[1].defaults == {
        "policy": {"severity": "warn"},
        "sink": {"accepted": {"format": "parquet", "path": "o"}},
        "source": {"format": "csv", "path": "p"},
    }


def test_merge_yaml_neither_modifies_its_inputs_nor_keeps_a_replaced_key_in_place() -> (
    None
):
    base: dict[object, object] = {"a": {"x": 1}, "b": 2}
    overlay: dict[object, object] = {"a": {"y": 3}, "c": [1]}
    merged = merge_yaml(base, overlay)
    assert merged == {"a": {"x": 1, "y": 3}, "b": 2, "c": [1]}
    assert base == {"a": {"x": 1}, "b": 2}
    assert overlay == {"a": {"y": 3}, "c": [1]}
    moved = cast("dict[str, int]", merge_yaml({"a": 1, "b": 2}, {"a": 3}))
    assert list(moved) == ["b", "a"]
    assert merge_yaml({"a": 1}, [1]) == [1]


@pytest.mark.parametrize("name", ["sales.eu", "a/b", "a\\b", ""])
def test_d7_domain_name_is_checked_even_when_unused(tmp_path: Path, name: str) -> None:
    config = load(
        tmp_path, f"version: '0.3'\ndomains: [{{name: '{name}'}}]\nentities: [{ENTITY}]"
    )
    with pytest.raises(FloeConfigError) as err:
        validate_domains(config)
    assert (
        str(err.value)
        == f"domain.name={name} must be non-empty without '.', '/' or '\\' when root.version >= \"0.3\""
    )


def test_d7_domain_name_rule_applies_from_0_3_only(tmp_path: Path) -> None:
    validate_domains(
        load(
            tmp_path,
            f'version: "0.2"\ndomains: [{{name: a.b, incoming_dir: d}}]\nentities: [{ENTITY}]',
        )
    )


@pytest.mark.parametrize(
    ("defaults", "field"),
    [
        ("{name: other}", "name"),
        ("{schema: {columns: []}}", "schema.columns"),
        ("{schema: {primary_key: [a]}}", "schema.primary_key"),
    ],
)
def test_d7_domain_defaults_reject_entity_only_fields(
    tmp_path: Path, defaults: str, field: str
) -> None:
    config = load(
        tmp_path,
        f'version: "0.3"\ndomains: [{{name: a, defaults: {defaults}}}]\nentities: [{ENTITY}]',
    )
    with pytest.raises(FloeConfigError) as err:
        validate_domains(config)
    assert str(err.value) == f"unknown field domains.defaults.{field}"


def test_d7_accepts_domain_defaults_with_other_schema_keys(tmp_path: Path) -> None:
    validate_domains(
        load(
            tmp_path,
            f'version: "0.3"\ndomains: [{{name: a, defaults: {{schema: {{normalize_columns: {{strategy: none}}}}}}}}]\nentities: [{ENTITY}]',
        )
    )


def test_cli_validate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / "p.yml").write_text(
        "apiVersion: floe/v1\nkind: EnvironmentProfile\nmetadata: {name: p}\nvariables: {A: b}\n"
    )
    (tmp_path / "ok.yml").write_text(f'version: "0.3"\nentities: [{ENTITY}]')
    (tmp_path / "bad.yml").write_text('version: "0.3"\ncolour: 1\nentities: []')
    for config, code, stderr in [
        ("ok.yml", 0, ""),
        ("bad.yml", 1, "Error: unknown field root.colour\n"),
        ("missing.yml", 1, "Error: No such file or directory (os error 2)\n"),
    ]:
        monkeypatch.setattr(
            "sys.argv",
            [
                "floe",
                "validate",
                "-c",
                str(tmp_path / config),
                "--profile",
                str(tmp_path / "p.yml"),
            ],
        )
        assert _cli.main() == code
        assert capsys.readouterr().err == stderr
