"""s3:// reads go through boto3 (s3fs cannot be installed next to floe-python, #1021)."""

from __future__ import annotations

import io
import json
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

from floe_dagster.assets import _load_json_document, _read_json_text
from floe_dagster.manifest import load_manifest


def _install_fake_boto3(monkeypatch, body: str | bytes, calls: dict) -> None:
    data = body.encode("utf-8") if isinstance(body, str) else body

    class _Body:
        def read(self) -> bytes:
            return data

    class _Client:
        def get_object(self, **kwargs):
            calls["get_object"] = kwargs
            return {"Body": _Body()}

    def client(service, **kwargs):
        calls["client"] = (service, kwargs)
        return _Client()

    fake = types.ModuleType("boto3")
    fake.client = client  # type: ignore[attr-defined]  # fake module in a test
    monkeypatch.setitem(sys.modules, "boto3", fake)
    monkeypatch.setitem(sys.modules, "fsspec", None)  # an fsspec import would raise


def _clear_aws_env(monkeypatch) -> None:
    for name in (
        "AWS_ENDPOINT_URL_S3",
        "AWS_ENDPOINT_URL",
        "AWS_S3_FORCE_PATH_STYLE",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
    ):
        monkeypatch.delenv(name, raising=False)


def test_s3_report_is_read_with_boto3_without_fsspec(monkeypatch) -> None:
    _clear_aws_env(monkeypatch)
    calls: dict = {}
    _install_fake_boto3(monkeypatch, json.dumps({"entities": []}), calls)
    result = _load_json_document(
        "s3://my-bucket/floe/reports/run.json", "s3://my-bucket/config.yml", label="test"
    )
    assert result == {"entities": []}
    assert calls["get_object"] == {"Bucket": "my-bucket", "Key": "floe/reports/run.json"}
    service, kwargs = calls["client"]
    assert service == "s3"
    assert "endpoint_url" not in kwargs
    assert kwargs["region_name"] is None


def test_s3_boto3_honors_endpoint_region_and_path_style(monkeypatch) -> None:
    _clear_aws_env(monkeypatch)
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://fallback:9000")
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://seaweedfs:8333")
    monkeypatch.setenv("AWS_S3_FORCE_PATH_STYLE", "true")
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    calls: dict = {}
    _install_fake_boto3(monkeypatch, "{}", calls)
    _read_json_text("s3://bucket/report.json", "s3://bucket/config.yml")
    _, kwargs = calls["client"]
    assert kwargs["endpoint_url"] == "http://seaweedfs:8333"
    assert kwargs["region_name"] == "eu-west-1"
    assert kwargs["config"].s3 == {"addressing_style": "path"}


def test_s3_without_boto3_falls_back_to_fsspec(monkeypatch) -> None:
    _clear_aws_env(monkeypatch)  # boto3 is blocked by the autouse fixture in conftest.py
    mock_fsspec = MagicMock()
    mock_fsspec.open.return_value.__enter__.return_value = io.StringIO("{}")
    monkeypatch.setitem(sys.modules, "fsspec", mock_fsspec)
    _read_json_text("s3://bucket/report.json", "s3://bucket/config.yml")
    mock_fsspec.open.assert_called_once_with("s3://bucket/report.json", "r", encoding="utf-8")


def test_non_s3_remote_ignores_boto3(monkeypatch) -> None:
    calls: dict = {}
    _install_fake_boto3(monkeypatch, "{}", calls)
    mock_fsspec = MagicMock()
    mock_fsspec.open.return_value.__enter__.return_value = io.StringIO("{}")
    monkeypatch.setitem(sys.modules, "fsspec", mock_fsspec)
    _read_json_text("gs://bucket/report.json", "gs://bucket/config.yml")
    assert "client" not in calls
    mock_fsspec.open.assert_called_once()


def test_s3_manifest_is_read_with_boto3(monkeypatch) -> None:
    _clear_aws_env(monkeypatch)
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    calls: dict = {}
    _install_fake_boto3(monkeypatch, fixture.read_bytes(), calls)
    assert load_manifest("s3://bucket/test/manifest.json").manifest_id
    assert calls["get_object"] == {"Bucket": "bucket", "Key": "test/manifest.json"}
