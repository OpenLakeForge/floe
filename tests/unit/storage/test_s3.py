"""Ported from `crates/floe-core/tests/unit/io/storage/` (s3 option cases); no network."""

from pathlib import Path

import pytest
from botocore.stub import Stubber

from floe.errors import FloeRunError, FloeStorageError
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


def test_list_keeps_leading_slash_in_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "k")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "s")
    client = S3Client("b", "us-east-1")
    with Stubber(client._client) as stub:  # pyright: ignore[reportPrivateUsage]
        stub.add_response(  # pyright: ignore[reportUnknownMemberType]
            "list_objects_v2",
            {"Contents": [{"Key": "/in/a.csv", "Size": 1}]},
            {"Bucket": "b", "Prefix": "/in/"},
        )
        assert [r.uri for r in client.list("/in/")] == ["s3://b//in/a.csv"]


def test_d30_list_passes_start_after(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "k")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "s")
    client = S3Client("b", "us-east-1")
    with Stubber(client._client) as stub:  # pyright: ignore[reportPrivateUsage]
        stub.add_response(  # pyright: ignore[reportUnknownMemberType]
            "list_objects_v2",
            {"Contents": [{"Key": "in/2026/10/23/a/x.csv", "Size": 1}]},
            {"Bucket": "b", "Prefix": "in/", "StartAfter": "in/2026/10/23"},
        )
        assert len(client.list("in/", "in/2026/10/23")) == 1


def test_conditional_delete_uses_if_match(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "k")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "s")
    client = S3Client("b", "us-east-1")
    params = {"Bucket": "b", "Key": "s/state.json", "IfMatch": '"e1"'}
    with Stubber(client._client) as stub:  # pyright: ignore[reportPrivateUsage]
        stub.add_response("delete_object", {}, params)  # pyright: ignore[reportUnknownMemberType]
        stub.add_client_error(  # pyright: ignore[reportUnknownMemberType]
            "delete_object",
            "PreconditionFailed",
            http_status_code=412,
            expected_params=params,
        )
        stub.add_client_error(  # pyright: ignore[reportUnknownMemberType]
            "delete_object",
            "AccessDenied",
            http_status_code=403,
            expected_params=params,
        )
        assert client.delete_conditional("s3://b/s/state.json", '"e1"')
        assert not client.delete_conditional("s3://b/s/state.json", '"e1"')
        with pytest.raises(FloeStorageError, match="s3 delete object failed"):
            client.delete_conditional("s3://b/s/state.json", '"e1"')
        assert client.delete_conditional("s3://b/s/state.json", None)  # Rust: no call
        stub.assert_no_pending_responses()


def test_endpoint_ignores_a_trailing_slash() -> None:
    """D34 (#931) compares endpoints to tell two stores apart: `http://minio:9000/` is the same store."""
    assert S3Client("b", "us-east-1", "http://minio:9000/").endpoint == S3Client("b", "us-east-1", "http://minio:9000").endpoint  # fmt: skip
