"""Ported from `crates/floe-core/tests/unit/io/storage/` (s3 option cases); no network."""

from pathlib import Path

import pytest

from floe.errors import FloeRunError
from floe.storage.inputs import InputFile, localize_input
from floe.storage.s3 import S3Client, resolve_path_style_access


def test_path_style_config_beats_env_and_env_accepts_1_or_true(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AWS_S3_FORCE_PATH_STYLE", raising=False)
    assert resolve_path_style_access(None) is False
    for value, want in [("1", True), (" TRUE ", True), ("yes", False), ("", False)]:
        monkeypatch.setenv("AWS_S3_FORCE_PATH_STYLE", value)
        assert resolve_path_style_access(None) is want
    assert resolve_path_style_access(False) is False
    monkeypatch.setenv("AWS_S3_FORCE_PATH_STYLE", "false")
    assert resolve_path_style_access(True) is True


def test_resolve_uri_and_client_options(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "k")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "s")
    client = S3Client("b", "eu-west-1", "http://localhost:9000", True)
    assert client.resolve_uri("/a/b.csv") == "s3://b/a/b.csv"
    assert client.resolve_uri("") == "s3://b"


def test_localize_requires_client_and_temp_dir(tmp_path: Path) -> None:
    file = InputFile("s3://b/k.csv", "k.csv", "k", None, None)
    with pytest.raises(FloeRunError, match="storage client required to download"):
        localize_input(file, None, tmp_path)


def test_localize_local_uri_is_not_copied(tmp_path: Path) -> None:
    file = InputFile("local:///data/k.csv", "k.csv", "k", None, None)
    local = localize_input(file, None, None)
    assert local.local_path == Path("/data/k.csv")
    assert not local.is_ephemeral
