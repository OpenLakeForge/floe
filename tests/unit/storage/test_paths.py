"""Ported from `crates/floe-core/tests/unit/io/storage/paths.rs`."""

from pathlib import Path

import pytest

from floe.errors import FloeConfigError
from floe.storage.paths import normalize_local_path, parse_s3_uri


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
