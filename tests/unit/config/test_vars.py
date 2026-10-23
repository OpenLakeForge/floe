"""Expected values and messages come from the Rust 0.7 binary (`floe validate`) and `vars/resolve.rs`."""

from pathlib import Path

import pytest

from floe.config.vars import (
    build_env_vars,
    extract_config_env_vars,
    replace_placeholders,
    resolve_vars,
)
from floe.errors import FloeConfigError, FloeIoError


def test_replace_placeholders_substitutes_trimmed_keys() -> None:
    assert (
        replace_placeholders("a/{{ X }}/{{Y}}/b", {"X": "1", "Y": "2"}, "report.path")
        == "a/1/2/b"
    )
    assert (
        replace_placeholders("no placeholders", {}, "report.path") == "no placeholders"
    )


@pytest.mark.parametrize(
    ("value", "entity", "message"),
    [
        ("{{ NOPE }}", None, "report.path references unknown variable NOPE"),
        ("{{ A", None, "report.path missing closing '}'"),
        ("x/{{ A }", None, "report.path missing closing '}'"),
        ("{{   }}", None, "report.path empty placeholder"),
        (
            "{{NOPE}}",
            "e1",
            "entity.name=e1 report.path references unknown variable NOPE",
        ),
        ("x/{{ A }", "e1", "entity.name=e1 report.path missing closing '}'"),
        ("{{}}", "e1", "entity.name=e1 report.path empty placeholder"),
    ],
)
def test_replace_placeholders_errors(
    value: str, entity: str | None, message: str
) -> None:
    with pytest.raises(FloeConfigError) as err:
        replace_placeholders(value, {}, "report.path", entity)
    assert str(err.value) == message


def test_build_env_vars_without_env_returns_profile_vars(tmp_path: Path) -> None:
    assert build_env_vars(tmp_path, None, {}, {"P": "1"}) == {"P": "1"}


def test_build_env_vars_precedence_is_profile_then_file_then_inline(
    tmp_path: Path,
) -> None:
    (tmp_path / "e.yml").write_text("A: file\nB: file\n")
    got = build_env_vars(
        tmp_path, "e.yml", {"B": "inline"}, {"A": "profile", "C": "profile"}
    )
    assert got == {"A": "file", "B": "inline", "C": "profile"}


def test_build_env_vars_ignores_documents_after_the_first(tmp_path: Path) -> None:
    (tmp_path / "e.yml").write_text("A: 1st\n---\nA: 2nd\n")
    assert build_env_vars(tmp_path, "e.yml", {}, {}) == {"A": "1st"}


@pytest.mark.parametrize(
    ("content", "message"),
    [
        ("- a\n", "expected map at env.file"),
        ("A: 1\n", "expected string at env.file"),
        ("A: true\n", "expected string at env.file"),
        ("1: a\n", "expected string at env.file"),
    ],
)
def test_env_file_errors(tmp_path: Path, content: str, message: str) -> None:
    (tmp_path / "e.yml").write_text(content)
    with pytest.raises(FloeConfigError) as err:
        build_env_vars(tmp_path, "e.yml", {}, {})
    assert str(err.value) == message


def test_env_file_empty(tmp_path: Path) -> None:
    (tmp_path / "e.yml").write_text("")
    with pytest.raises(FloeConfigError) as err:
        build_env_vars(tmp_path, "e.yml", {}, {})
    assert str(err.value) == f"env file {tmp_path / 'e.yml'} is empty"


def test_env_file_missing(tmp_path: Path) -> None:
    with pytest.raises(FloeIoError) as err:
        build_env_vars(tmp_path, "nofile.yml", {}, {})
    assert str(err.value) == "No such file or directory (os error 2)"


def test_env_file_path_is_relative_to_config_dir_and_normalised(tmp_path: Path) -> None:
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "e.yml").write_text("A: x\n")
    assert build_env_vars(tmp_path, "./sub/../sub/e.yml", {}, {}) == {"A": "x"}
    assert build_env_vars(
        Path("/elsewhere"), str(tmp_path / "sub" / "e.yml"), {}, {}
    ) == {"A": "x"}


def test_resolve_vars_precedence_is_config_then_cli_then_profile() -> None:
    got = resolve_vars(
        {"KEY": "profile", "EXTRA": "extra"},
        {"KEY": "cli", "ONLY_CLI": "c"},
        {"KEY": "config"},
    )
    assert got == {"KEY": "config", "EXTRA": "extra", "ONLY_CLI": "c"}


def test_resolve_vars_expands_nested_and_cross_source_references() -> None:
    got = resolve_vars(
        {"OUT": "${ROOT}/${ENV}/out", "ENV": "prod"},
        {},
        {"ROOT": "/data", "PAD": "a${ ENV }b${ENV}"},
    )
    assert got == {
        "OUT": "/data/prod/out",
        "ENV": "prod",
        "ROOT": "/data",
        "PAD": "aprodbprod",
    }


def test_resolve_vars_empty() -> None:
    assert resolve_vars({}, {}, {}) == {}


@pytest.mark.parametrize(
    ("profile", "message"),
    [
        ({"A": "${B}"}, 'variable "${B}" is referenced but not defined'),
        (
            {"A": "${B}", "B": "${A}"},
            "circular variable reference detected: A -> B -> A",
        ),
        ({"A": "${A}"}, "circular variable reference detected: A -> A"),
        ({"A": "${B", "B": "x"}, 'variable "A": unclosed placeholder in value "${B"'),
        ({"A": "${}"}, 'variable "A": empty placeholder ${}'),
        ({"A": "x ${  } y"}, 'variable "A": empty placeholder ${}'),
    ],
)
def test_resolve_vars_errors(profile: dict[str, str], message: str) -> None:
    with pytest.raises(FloeConfigError) as err:
        resolve_vars(profile, {}, {})
    assert str(err.value) == message


def test_resolve_vars_depth_limit() -> None:
    chain = {f"V{i}": f"${{V{i + 1}}}" for i in range(40)} | {"V40": "end"}
    with pytest.raises(FloeConfigError) as err:
        resolve_vars(chain, {}, {})
    assert str(err.value) == (
        "variable expansion exceeded maximum depth (32); "
        'check for deeply nested or circular references near "${V33}"'
    )


def test_extract_config_env_vars_merges_file_under_inline(tmp_path: Path) -> None:
    (tmp_path / "e.yml").write_text("A: file\nB: file\n")
    cfg = tmp_path / "c.yml"
    cfg.write_text("env:\n  file: e.yml\n  vars:\n    B: inline\n")
    assert extract_config_env_vars(cfg) == {"A": "file", "B": "inline"}


def test_extract_config_env_vars_skips_non_string_file_entries(tmp_path: Path) -> None:
    (tmp_path / "e.yml").write_text("A: ok\nB: 1\nC: true\n")
    cfg = tmp_path / "c.yml"
    cfg.write_text("env:\n  file: e.yml\n")
    assert extract_config_env_vars(cfg) == {"A": "ok"}


def test_extract_config_env_vars_drops_an_invalid_inline_map_entirely(
    tmp_path: Path,
) -> None:
    cfg = tmp_path / "c.yml"
    cfg.write_text("env:\n  vars:\n    A: ok\n    B: 1\n")
    assert extract_config_env_vars(cfg) == {}


@pytest.mark.parametrize(
    "content",
    [
        "",
        "- a\n",
        "env: 1\n",
        "env:\n  file: 5\n",
        "env:\n  file: missing.yml\n",
        "a: [1\n",
        "env:\n  vars: x\n",
    ],
)
def test_extract_config_env_vars_never_raises(tmp_path: Path, content: str) -> None:
    cfg = tmp_path / "c.yml"
    cfg.write_text(content)
    assert extract_config_env_vars(cfg) == {}


def test_extract_config_env_vars_missing_config(tmp_path: Path) -> None:
    assert extract_config_env_vars(tmp_path / "nope.yml") == {}
