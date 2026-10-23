"""Messages come from `config/yaml_decode.rs` and the field readers in `config/parse.rs`."""

from collections.abc import Callable

import pytest

from floe.config.yaml import (
    get_array,
    get_string,
    get_value,
    opt_bool,
    opt_string,
    opt_u64,
    opt_vec_string,
    opt_vec_vec_string,
    validate_known_keys,
    yaml_array,
)
from floe.errors import FloeConfigError

Reader = Callable[[dict[object, object], str, str], object]


def test_yaml_array() -> None:
    assert yaml_array([1], "x") == [1]
    with pytest.raises(FloeConfigError, match="^expected array at x$"):
        yaml_array({}, "x")


def test_validate_known_keys() -> None:
    validate_known_keys({"a": 1}, "ctx", ("a", "b"))
    with pytest.raises(FloeConfigError, match=r"^unknown field ctx\.c$"):
        validate_known_keys({"a": 1, "c": 2}, "ctx", ("a", "b"))
    with pytest.raises(FloeConfigError, match="^expected string key at ctx$"):
        validate_known_keys({1: 1}, "ctx", ("a",))


def test_get_value_distinguishes_null_from_absent() -> None:
    assert get_value({"k": None}, "k", "c") is None
    with pytest.raises(FloeConfigError, match=r"^missing required field c\.k$"):
        get_value({}, "k", "c")


@pytest.mark.parametrize(
    ("reader", "good", "bad", "message"),
    [
        (get_string, "s", 1, "expected string at c.k"),
        (get_array, [], 1, "expected array at c.k"),
        (opt_string, "s", 1, "expected string at c.k"),
        (opt_bool, True, "yes", "expected boolean at c.k"),
        (opt_bool, False, 1, "expected boolean at c.k"),
        (opt_u64, 0, 1.5, "expected integer at c.k"),
        (opt_u64, 3, True, "expected integer at c.k"),
        (opt_u64, 3, -1, "expected positive integer at c.k"),
        (opt_vec_string, ["a"], "a", "expected array at c.k"),
        (opt_vec_string, ["a"], ["a", 1], "expected string at c.k[1]"),
        (opt_vec_vec_string, [["a"]], "a", "expected array at c.k"),
        (opt_vec_vec_string, [["a"]], [["a"], "b"], "expected array at c.k[1]"),
        (opt_vec_vec_string, [["a"]], [["a", 1]], "expected string at c.k[0][1]"),
    ],
)
def test_readers(reader: Reader, good: object, bad: object, message: str) -> None:
    assert reader({"k": good}, "k", "c") == good
    with pytest.raises(FloeConfigError) as err:
        reader({"k": bad}, "k", "c")
    assert str(err.value) == message


@pytest.mark.parametrize(
    "reader", [opt_string, opt_bool, opt_u64, opt_vec_string, opt_vec_vec_string]
)
def test_optional_readers_treat_absent_and_null_as_none(reader: Reader) -> None:
    assert reader({}, "k", "c") is None
    assert reader({"k": None}, "k", "c") is None
