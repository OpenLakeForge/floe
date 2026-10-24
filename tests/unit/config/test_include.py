"""Expected messages come from the Rust 0.7 binary (`floe validate`)."""

import sys
from pathlib import Path

import pytest

from floe.config.parse import parse_config
from floe.errors import FloeError

ENTITY = "source: {format: csv, path: in}\nsink: {}\npolicy: {severity: warn}\nschema: {columns: [{name: id, type: string}]}\n"
ROOT = 'version: "0.3"\ninclude:\n  domains: ["domains/*/_domain.yml"]\n'


def write(root: Path, name: str, text: str) -> None:
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def error(root: Path) -> str:
    with pytest.raises(FloeError) as err:
        parse_config(root / "config.yml")
    return str(err.value).replace(str(root), "<ROOT>")


def test_domains_and_entities_are_spliced_in_sorted_order(tmp_path: Path) -> None:
    write(tmp_path, "config.yml", ROOT)
    for domain in ("sales", "finance"):
        write(tmp_path, f"domains/{domain}/_domain.yml", f"name: {domain}\n")
        write(tmp_path, f"domains/{domain}/b.yml", f"name: b\n{ENTITY}")
        write(tmp_path, f"domains/{domain}/a.yml", f"name: a\n{ENTITY}")
        write(tmp_path, f"domains/{domain}/notes.txt", "ignored")
    config = parse_config(tmp_path / "config.yml")
    assert [d.name for d in config.domains] == ["finance", "sales"]
    assert [e.id for e in config.entities] == [
        "finance.a",
        "finance.b",
        "sales.a",
        "sales.b",
    ]
    assert len(config.config_files) == 1 + 2 * 3


@pytest.mark.skipif(sys.platform == "win32", reason="Windows cannot create `con.yml`")
def test_d4_entity_name_comes_from_the_yaml_not_the_file_name(tmp_path: Path) -> None:
    write(tmp_path, "config.yml", ROOT)
    write(tmp_path, "domains/sales/_domain.yml", "name: sales\n")
    write(tmp_path, "domains/sales/con.yml", f"name: 'sales:orders'\n{ENTITY}")
    write(tmp_path, "domains/sales/aux.yml", f"name: nul\n{ENTITY}")
    config = parse_config(tmp_path / "config.yml")
    assert sorted(e.name for e in config.entities) == ["nul", "sales:orders"]


def test_d4_names_that_windows_reserves_are_plain_names(tmp_path: Path) -> None:
    write(tmp_path, "config.yml", ROOT)
    write(tmp_path, "domains/sales/_domain.yml", "name: sales\n")
    write(tmp_path, "domains/sales/orders.yml", f"name: con\n{ENTITY}")
    assert [e.name for e in parse_config(tmp_path / "config.yml").entities] == ["con"]


def test_included_sources(tmp_path: Path) -> None:
    write(
        tmp_path,
        "config.yml",
        'version: "0.3"\ninclude: {sources: s.yml}\nentities: []\n',
    )
    write(
        tmp_path,
        "s.yml",
        "sources: [{name: lake, format: csv, path: 'in/{{resource}}'}]\n",
    )
    config = parse_config(tmp_path / "config.yml")
    assert config.config_files == [tmp_path / "config.yml", tmp_path / "s.yml"]


@pytest.mark.parametrize(
    ("files", "message"),
    [
        (
            {"config.yml": 'include: {domains: []}\nentities: []\nversion: "0.2"'},
            'root.include requires root.version >= "0.3"',
        ),
        (
            {"config.yml": 'version: "0.3"\ninclude: {entities: []}'},
            "unknown field include.entities",
        ),
        (
            {"config.yml": 'version: "0.3"\ninclude: {}'},
            "missing required field include.domains",
        ),
        (
            {"config.yml": 'version: "0.3"\ninclude: {sources: s.yml}\nsources: []'},
            "root.sources and include.sources are mutually exclusive",
        ),
        (
            {
                "config.yml": 'version: "0.3"\ninclude: {sources: s.yml}',
                "s.yml": "x: 1",
            },
            "<ROOT>/s.yml: unknown field sources file.x",
        ),
        (
            {"config.yml": 'version: "0.3"\ninclude: {sources: s.yml}'},
            "<ROOT>/s.yml: No such file or directory (os error 2)",
        ),
        (
            {"config.yml": 'version: "0.3"\ninclude: {domains: [nowhere/*.yml]}'},
            'include.domains pattern "nowhere/*.yml" matched no file (includes resolve against local files only; a remote project cannot include)',
        ),
        (
            {"config.yml": 'version: "0.3"\ninclude: {domains: ["a/***/b"]}'},
            "Pattern syntax error near position {pos}: wildcards are either regular `*` or recursive `**`",
        ),
        (
            {"config.yml": ROOT, "domains/s/_domain.yml": ""},
            "<ROOT>/domains/s/_domain.yml: YAML is empty",
        ),
        (
            {"config.yml": ROOT, "domains/s/_domain.yml": "x: 1"},
            "<ROOT>/domains/s/_domain.yml: missing required field domains.name",
        ),
        (
            {
                "config.yml": ROOT,
                "domains/s/_domain.yml": "name: s",
                "domains/s/e.yml": "- 1",
            },
            "<ROOT>/domains/s/e.yml: expected map at entity",
        ),
        (
            {
                "config.yml": ROOT,
                "domains/s/_domain.yml": "name: s",
                "domains/s/e.yml": "name: e\ndomain: f",
            },
            '<ROOT>/domains/s/e.yml has domain "f" but its _domain.yml names "s"',
        ),
        (
            {"config.yml": ROOT + "entities: x", "domains/s/_domain.yml": "name: s"},
            "expected array at entities",
        ),
        (
            {
                "config.yml": ROOT,
                "domains/s/_domain.yml": "name: s",
                "domains/s/e.yml": "name: e\npolicy: {severity: loud}\n"
                + ENTITY.split("policy")[0],
            },
            "<ROOT>/domains/s/e.yml: entities[0] (entity.name=e): policy.severity=loud is unsupported (allowed: warn, reject, abort)",
        ),
    ],
)
def test_include_errors(tmp_path: Path, files: dict[str, str], message: str) -> None:
    for name, text in files.items():
        write(tmp_path, name, text)
    base = len(str(tmp_path)) + len("/a/") + 2
    assert error(tmp_path) == message.replace("{pos}", str(base))
