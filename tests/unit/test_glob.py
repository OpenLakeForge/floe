"""Ported from the `#[cfg(test)]` module of glob 0.3.4 `src/lib.rs`, plus regressions."""

import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from floe.errors import FloeError
from floe.glob import MatchOptions, Pattern, PatternError, glob


def m(pattern: str, s: str, **opts: bool) -> bool:
    return Pattern(pattern).matches_with(s, MatchOptions(**opts))


def pos(pattern: str) -> int:
    with pytest.raises(PatternError) as e:
        Pattern(pattern)
    return e.value.pos


def test_pattern_error_text() -> None:
    with pytest.raises(PatternError) as e:
        Pattern("a/**b")
    assert str(e.value) == (
        "Pattern syntax error near position 4: "
        "recursive wildcards must form a single path component"
    )
    assert e.value.msg == "recursive wildcards must form a single path component"
    with pytest.raises(
        PatternError,
        match=r"position 2: wildcards are either regular `\*` or recursive `\*\*`$",
    ):
        Pattern("***")
    with pytest.raises(PatternError, match="position 3: invalid range pattern$"):
        Pattern("abc[def")


def test_wildcard_errors() -> None:
    assert pos("a/**b") == 4
    assert pos("a/bc**") == 3
    assert pos("a/*****") == 4
    assert pos("a/b**c**d") == 2
    assert pos("a**b") == 0


def test_unclosed_bracket_errors() -> None:
    for p in [
        "abc[def",
        "abc[!def",
        "abc[",
        "abc[!",
        "abc[d",
        "abc[!d",
        "abc[]",
        "abc[!]",
    ]:
        assert pos(p) == 3


def test_glob_errors() -> None:
    with pytest.raises(PatternError) as e:
        glob("a/**b")
    assert e.value.pos == 4
    with pytest.raises(PatternError) as e:
        glob("abc[def")
    assert e.value.pos == 3


def test_wildcards() -> None:
    assert m("a*b", "a_b")
    assert m("a*b*c", "abc")
    assert not m("a*b*c", "abcd")
    assert m("a*b*c", "a_b_c")
    assert m("a*b*c", "a___b___c")
    assert m("abc*abc*abc", "abcabcabcabcabcabcabc")
    assert not m("abc*abc*abc", "abcabcabcabcabcabcabca")
    assert m("a*a*a*a*a*a*a*a*a", "a" * 31)
    assert m("a*b[xyz]c*d", "abxcdbxcddd")


def test_recursive_wildcards() -> None:
    for p in ["some/**/needle.txt", "some/**/**/needle.txt"]:
        assert m(p, "some/needle.txt")
        assert m(p, "some/one/needle.txt")
        assert m(p, "some/one/two/needle.txt")
        assert m(p, "some/other/needle.txt")
        assert not m(p, "some/other/notthis.txt")
    assert Pattern("**").is_recursive
    for s in ["abcde", "", ".asdf", "/x/.asdf"]:
        assert m("**", s)
    for s in ["one/two/test", "one/test", "test"]:
        assert m("**/test", s)
    for s in ["/one/two/test", "/one/test", "/test"]:
        assert m("/**/test", s)
    assert not m("/**/test", "/one/notthis")
    assert not m("/**/test", "/notthis")
    assert m("**/.*", ".abc")
    assert m("**/.*", "abc/.abc")
    assert not m("**/.*", "ab.c")
    assert not m("**/.*", "abc/ab.c")


def test_range_pattern() -> None:
    assert all(m("a[0-9]b", f"a{i}b") for i in range(10))
    assert not m("a[0-9]b", "a_b")
    assert not any(m("a[!0-9]b", f"a{i}b") for i in range(10))
    assert m("a[!0-9]b", "a_b")
    lower, upper = "abcdefghijklmnopqrstuvwxyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    for p in ["[a-z123]", "[1a-z23]", "[123a-z]"]:
        assert all(m(p, c) for c in lower)
        assert all(m(p, c, case_sensitive=False) for c in upper)
        assert all(m(p, c) for c in "123")
    for p in ["[abc-]", "[-abc]", "[a-c-]"]:
        assert all(m(p, c) for c in "abc-")
        assert not m(p, "d")
    assert not m("[2-1]", "1")
    assert not m("[2-1]", "2")
    assert m("[-]", "-")
    assert not m("[!-]", "-")


def test_pattern_matches() -> None:
    p = "*hello.txt"
    for s in [
        "hello.txt",
        "gareth_says_hello.txt",
        "some/path/to/hello.txt",
        "some\\path\\to\\hello.txt",
        "/an/absolute/path/to/hello.txt",
    ]:
        assert m(p, s)
    assert not m(p, "hello.txt-and-then-some")
    assert not m(p, "goodbye.txt")
    p = "*some/path/to/hello.txt"
    assert m(p, "some/path/to/hello.txt")
    assert m(p, "a/bigger/some/path/to/hello.txt")
    assert not m(p, "some/path/to/hello.txt-and-then-some")
    assert not m(p, "some/other/path/to/hello.txt")


def test_pattern_escape() -> None:
    s = "_[_]_?_*_!_"
    assert Pattern.escape(s) == "_[[]_[]]_[?]_[*]_!_"
    assert m(Pattern.escape(s), s)


def test_case_insensitive() -> None:
    for s in ["aBcDeFg", "abcdefg", "ABCDEFG", "AbCdEfG"]:
        assert m("aBcDeFg", s, case_sensitive=False)
    assert m("[a]", "a", case_sensitive=False)
    assert m("[a]", "A", case_sensitive=False)
    assert not m("[a]", "A")
    assert not m("[!a]", "a", case_sensitive=False)
    assert not m("[!a]", "A", case_sensitive=False)
    assert m("[!a]", "A")


def test_require_literal_separator() -> None:
    for p in ["abc?def", "abc*def", "abc[/]def"]:
        assert not m(p, "abc/def", require_literal_separator=True)
        assert m(p, "abc/def")
    assert m("abc/def", "abc/def", require_literal_separator=True)
    assert m("abc/def", "abc/def")


def test_require_literal_leading_dot() -> None:
    def f(p: str, s: str, dot: bool) -> bool:
        return m(p, s, require_literal_leading_dot=dot)

    cases = [
        ("*.txt", ".hello.txt", False),
        (".*.*", ".hello.txt", True),
        ("aaa/bbb/*", "aaa/bbb/.ccc", False),
        ("aaa/bbb/*", "aaa/bbb/c.c.c.", True),
        ("aaa/bbb/.*", "aaa/bbb/.ccc", True),
        ("aaa/?bbb", "aaa/.bbb", False),
        ("aaa/[.]bbb", "aaa/.bbb", False),
        ("**/*", ".bbb", False),
    ]
    for p, s, expected_when_required in cases:
        assert f(p, s, False)
        assert f(p, s, True) is expected_when_required


def test_matches_path_and_join() -> None:
    assert m("a/b", "a/b")
    Pattern(os.path.join("one", "**/*.rs"))


def test_default_options_are_the_crates() -> None:
    assert MatchOptions() == MatchOptions(True, False, False)


# walker ---------------------------------------------------------------------------


@pytest.fixture
def tree(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    for f in ["a/1.csv", "a/b/2.csv", "a/b/c/3.csv", "d/4.csv", "z.csv", ".h/5.csv"]:
        (tmp_path / f).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / f).write_text("x")
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_absolute_pattern() -> None:
    assert glob("/")
    assert glob("//")
    assert glob("/*")


def test_walker_order_and_relative_paths(tree: Path) -> None:
    assert glob("*") == [".h", "a", "d", "z.csv"]
    assert glob("*/") == [".h", "a", "d"]
    assert glob("*/*.csv") == [".h/5.csv", "a/1.csv", "d/4.csv"]
    assert glob("./a/*") == ["a/1.csv", "a/b"]
    assert glob("**/*.csv") == [
        ".h/5.csv", "a/1.csv", "a/b/2.csv", "a/b/c/3.csv", "d/4.csv", "z.csv",
    ]  # fmt: skip
    assert glob("a/**") == ["a/b", "a/b/c"]  # trailing ** yields directories only
    assert glob("a/b/2.csv") == ["a/b/2.csv"]
    assert glob("missing/*") == []
    assert glob("a/.") == ["a/."]
    assert glob("a/.*") == ["a/..", "a/."]  # crate order


def test_walker_absolute_and_options(tree: Path) -> None:
    root = Pattern.escape(str(tree))
    assert glob(f"{root}/d/*") == [f"{tree}/d/4.csv"]
    assert glob("*", MatchOptions(require_literal_leading_dot=True)) == [
        "a",
        "d",
        "z.csv",
    ]
    assert glob("*.CSV", MatchOptions(case_sensitive=False)) == ["z.csv"]


# regressions: each fails with Python's glob/fnmatch (see PR) ----------------------

unix_only = pytest.mark.skipif(
    os.name == "nt" or (hasattr(os, "geteuid") and os.geteuid() == 0),
    reason="needs chmod 000 to deny access",
)


@pytest.fixture
def locked(tree: Path) -> Iterator[Path]:
    (tree / "a").chmod(0o000)
    yield tree / "a"
    (tree / "a").chmod(0o755)


def test_double_star_twice_matches_each_file_once(tree: Path) -> None:
    assert glob("**/**") == sorted(set(glob("**/**")))
    assert glob("**/**/*.csv") == glob("**/*.csv")


@unix_only
def test_unreadable_dir_under_double_star_raises(locked: Path) -> None:
    with pytest.raises(
        FloeError,
        match=r"^attempting to read `a` resulted in an error: Permission denied \(os error 13\)$",
    ):
        glob("**/*.csv")


@unix_only
def test_unreadable_sibling_under_star_raises(locked: Path) -> None:
    with pytest.raises(
        FloeError, match="^attempting to read `a` resulted in an error: "
    ):
        glob("*/*.csv")


def test_negated_class_with_dash_and_underscore(tree: Path) -> None:
    assert m("[!_-]", "a")
    assert not m("[!_-]", "_")
    assert not m("[!_-]", "-")
    (tree / "_").write_text("x")
    assert "_" not in glob("[!_-]")
