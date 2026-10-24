"""Ported from `crates/floe-core/tests/unit/io/storage/paths.rs`."""

from pathlib import Path

import pytest

from floe.errors import FloeConfigError
from floe.storage.paths import (
    archive_relative_path_for_run,
    normalize_local_path,
    parse_s3_uri,
)


def test_normalize_local_path_removes_dot_segments_lexically() -> None:
    assert normalize_local_path(Path("/tmp/floe/./in/../out/file.csv")) == Path(
        "/tmp/floe/out/file.csv"
    )
    assert normalize_local_path(Path("a/./b/../c")) == Path("a/c")


def test_normalize_local_path_keeps_leading_parent_dirs() -> None:
    assert normalize_local_path(Path("../a/../../b")) == Path("../../b")
    assert normalize_local_path(Path("/../a")) == Path("/a")
    assert normalize_local_path(Path("a/..")) == Path(".")
    assert normalize_local_path(Path("/a/..")) == Path("/")


def test_normalize_local_path_preserves_symlink_sensitive_parent_segments(
    tmp_path: Path,
) -> None:
    (tmp_path / "target").mkdir()
    (tmp_path / "link").symlink_to(tmp_path / "target")
    original = tmp_path / "link" / ".." / "data.csv"
    assert normalize_local_path(original) == original
    assert "/link/../data.csv" in str(normalize_local_path(original))


def test_parse_s3_uri() -> None:
    assert parse_s3_uri("s3://b/k/x.csv") == ("b", "k/x.csv")
    assert parse_s3_uri("s3://b") == ("b", "")


@pytest.mark.parametrize(
    ("uri", "message"),
    [
        ("gs://b/k", "expected s3 uri, got gs://b/k"),
        ("s3:///k", "missing bucket in s3 uri: s3:///k"),
    ],
)
def test_parse_s3_uri_errors(uri: str, message: str) -> None:
    with pytest.raises(FloeConfigError) as err:
        parse_s3_uri(uri)
    assert str(err.value) == message


def test_archive_filename_avoids_collisions_across_runs_and_sources() -> None:
    a = archive_relative_path_for_run(
        "o", "input.csv", "run-1", "local:///src/a/input.csv"
    )
    b = archive_relative_path_for_run(
        "o", "input.csv", "run-2", "local:///src/a/input.csv"
    )
    c = archive_relative_path_for_run(
        "o", "input.csv", "run-1", "local:///src/b/input.csv"
    )
    assert len({a, b, c}) == 3
    assert a.startswith("o/input__run-run-1__src-")
    assert a.endswith(".csv")


def test_archive_relative_path_sanitizes_run_and_keeps_entity() -> None:
    rel = archive_relative_path_for_run(
        "orders",
        "nested/input.csv",
        "2026/02/24 run",
        "s3://bucket/data/nested/input.csv",
    )
    assert rel.startswith("orders/input__run-2026_02_24_run__src-")
    assert rel.endswith(".csv")
    assert archive_relative_path_for_run("", "input.csv", "r", "u").startswith(
        "input__run-r"
    )


def test_archive_filename_bounds_long_names_and_run_ids() -> None:
    rel = archive_relative_path_for_run(
        "", "a" * 300 + ".csv", "run-1", "local:///tmp/input.csv"
    )
    assert len(rel) <= 255
    assert "__run-run-1__src-" in rel
    assert rel.endswith(".csv")
    long_run = archive_relative_path_for_run("", "x.csv", "r" * 100, "u")
    assert f"__run-{'r' * 48}_" in long_run


def test_archive_fnv1a_hash_matches_rust() -> None:
    # FNV-1a 64 of "a" is af63dc4c8601ec8c
    assert (
        archive_relative_path_for_run("", "x.csv", "r", "a")
        == "x__run-r__src-af63dc4c8601ec8c.csv"
    )
