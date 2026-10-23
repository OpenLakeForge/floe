"""Expected values and messages come from the Rust 0.7 binary (`floe validate`) and yaml-rust2 0.11."""

import math
from pathlib import Path

import pytest

from floe.config.yaml import load_single_doc, load_yaml, yaml_hash, yaml_string
from floe.errors import FloeConfigError, FloeError, FloeIoError


def write(tmp_path: Path, content: str | bytes) -> Path:
    path = tmp_path / "c.yml"
    if isinstance(content, str):
        content = content.encode()
    path.write_bytes(content)
    return path


@pytest.mark.parametrize("content", ["", "# only a comment\n"])
def test_empty_file_is_an_error(tmp_path: Path, content: str) -> None:
    with pytest.raises(FloeConfigError) as err:
        load_single_doc(write(tmp_path, content))
    assert str(err.value) == "YAML is empty"


def test_multiple_documents_are_an_error(tmp_path: Path) -> None:
    with pytest.raises(FloeConfigError) as err:
        load_single_doc(write(tmp_path, 'version: "0.3"\n---\nversion: "0.3"\n'))
    assert str(err.value) == "YAML contains multiple documents; expected one"


def test_load_yaml_returns_every_document(tmp_path: Path) -> None:
    assert load_yaml(write(tmp_path, "a: 1\n---\nb: 2\n")) == [{"a": 1}, {"b": 2}]


@pytest.mark.parametrize("content", ["---\n", "hello\n", "- 1\n- 2\n"])
def test_non_map_root_loads_but_yaml_hash_rejects_it(
    tmp_path: Path, content: str
) -> None:
    doc = load_single_doc(write(tmp_path, content))
    with pytest.raises(FloeConfigError) as err:
        yaml_hash(doc, "root")
    assert str(err.value) == "expected map at root"


def test_yaml_hash_and_string_accept_their_type() -> None:
    assert yaml_hash({"a": 1}, "root") == {"a": 1}
    assert yaml_string("x", "root") == "x"


@pytest.mark.parametrize("value", [1, True, None, 1.5, ["a"], {"a": "b"}])
def test_yaml_string_rejects_non_strings(value: object) -> None:
    with pytest.raises(FloeConfigError) as err:
        yaml_string(value, "env.file")
    assert str(err.value) == "expected string at env.file"


@pytest.mark.parametrize(
    ("content", "message"),
    [
        (
            'version: "0.3"\nversion: "0.3"\n',
            'String("version"): duplicated key in mapping at byte 24 line 2 column 10',
        ),
        (
            'version: "0.3"\nversion:   abc  \nz: 1\n',
            'String("version"): duplicated key in mapping at byte 26 line 2 column 12',
        ),
        (
            "a: 1\nb: 2\na: [1]\n",
            'String("a"): duplicated key in mapping at byte 15 line 3 column 6',
        ),
        (
            "a: 1\nb: 2\na:\n  x: 1\n",
            'String("a"): duplicated key in mapping at byte 20 line 5 column 1',
        ),
        (
            "a:\n  b: 1\n  b: 2\n",
            'String("b"): duplicated key in mapping at byte 15 line 3 column 6',
        ),
        (
            'a: 1\n"a": 2\n',
            'String("a"): duplicated key in mapping at byte 10 line 2 column 6',
        ),
        (
            "1: 1\n1: 2\n",
            "Integer(1): duplicated key in mapping at byte 8 line 2 column 4",
        ),
        (
            "true: 1\ntrue: 2\n",
            "Boolean(true): duplicated key in mapping at byte 14 line 2 column 7",
        ),
        (
            "x: 1.5\n1.5: 2\n1.5: 3\n",
            'Real("1.5"): duplicated key in mapping at byte 19 line 3 column 6',
        ),
        ("~: 1\n~: 2\n", "Null: duplicated key in mapping at byte 8 line 2 column 4"),
    ],
)
def test_duplicate_key_message(tmp_path: Path, content: str, message: str) -> None:
    with pytest.raises(FloeError) as err:
        load_yaml(write(tmp_path, content))
    assert str(err.value) == message


def test_syntax_error_message(tmp_path: Path) -> None:
    with pytest.raises(FloeError) as err:
        load_yaml(write(tmp_path, "a: [1, 2\n"))
    assert (
        str(err.value)
        == "while parsing a flow sequence, expected ',' or ']' at byte 9 line 2 column 1"
    )


def test_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FloeIoError) as err:
        load_yaml(tmp_path / "nope.yml")
    assert str(err.value) == "No such file or directory (os error 2)"


def test_invalid_utf8(tmp_path: Path) -> None:
    with pytest.raises(FloeIoError) as err:
        load_yaml(write(tmp_path, b"\xff\xfe"))
    assert str(err.value) == "stream did not contain valid UTF-8"


@pytest.mark.parametrize(
    ("scalar", "expected"),
    [
        ("1", 1),
        ("-7", -7),
        ("+5", 5),
        ("007", 7),
        ("0x1F", 31),
        ("0x+1F", 31),
        ("++5", 5),
        ("+-5", -5),
        ("+0x1F", "+0x1F"),
        ("0x8000000000000000", "0x8000000000000000"),
        ("0o17", 15),
        ("9223372036854775807", 2**63 - 1),
        ("1.0", 1.0),
        ("1e3", 1000.0),
        (".5", 0.5),
        ("9223372036854775808", 9223372036854775808.0),
        (".inf", float("inf")),
        ("-.INF", float("-inf")),
        ("true", True),
        ("True", True),
        ("FALSE", False),
        ("null", None),
        ("~", None),
        ("", None),
        ("yes", "yes"),
        ("no", "no"),
        ("on", "on"),
        ("Null", "Null"),
        ("tRue", "tRue"),
        ("1_000", "1_000"),
        ("0b101", "0b101"),
        ("-0x1F", "-0x1F"),
        ("1.2.3", "1.2.3"),
        ("inf", "inf"),
        ("nan", "nan"),
        ("2024-01-02", "2024-01-02"),
        ("12:30", "12:30"),
        ('"1"', "1"),
        ("'true'", "true"),
    ],
)
def test_plain_scalar_rules(tmp_path: Path, scalar: str, expected: object) -> None:
    value = yaml_hash(load_single_doc(write(tmp_path, f"k: {scalar}\n")), "root")["k"]
    assert value == expected
    assert type(value) is type(expected)


def test_nan_scalar(tmp_path: Path) -> None:
    value = yaml_hash(load_single_doc(write(tmp_path, "k: .nan\n")), "root")["k"]
    assert isinstance(value, float) and math.isnan(value)


def test_merge_key_is_an_ordinary_key(tmp_path: Path) -> None:
    doc = load_single_doc(
        write(tmp_path, "base: &b\n  x: 1\nchild:\n  <<: *b\n  y: 2\n")
    )
    assert doc == {"base": {"x": 1}, "child": {"<<": {"x": 1}, "y": 2}}


def test_scalar_keys_keep_their_type(tmp_path: Path) -> None:
    assert load_single_doc(write(tmp_path, "1: a\nyes: b\n")) == {1: "a", "yes": "b"}
