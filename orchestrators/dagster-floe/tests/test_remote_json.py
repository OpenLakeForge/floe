from __future__ import annotations

import io
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from floe_dagster.assets import _load_json_document, _read_json_text


# ── local plain path ──────────────────────────────────────────────────────────

def test_load_json_document_local_path(tmp_path: Path) -> None:
    payload = {"entities": [{"name": "orders"}]}
    doc = tmp_path / "report.json"
    doc.write_text(json.dumps(payload), encoding="utf-8")
    result = _load_json_document(str(doc), str(tmp_path / "config.yml"), label="test")
    assert result == payload


def test_load_json_document_local_relative_path(tmp_path: Path) -> None:
    payload = {"status": "ok"}
    doc = tmp_path / "report.json"
    doc.write_text(json.dumps(payload), encoding="utf-8")
    config_uri = str(tmp_path / "config.yml")
    result = _load_json_document("report.json", config_uri, label="test")
    assert result == payload


# ── local:// URI ──────────────────────────────────────────────────────────────

def test_load_json_document_local_uri(tmp_path: Path) -> None:
    payload = {"files": []}
    doc = tmp_path / "entity.json"
    doc.write_text(json.dumps(payload), encoding="utf-8")
    ref = "local://" + str(doc).replace("\\", "/")
    result = _load_json_document(ref, "/any/config.yml", label="test")
    assert result == payload


# ── file:// URI ───────────────────────────────────────────────────────────────

def test_load_json_document_file_uri(tmp_path: Path) -> None:
    payload = {"rows": 42}
    doc = tmp_path / "summary.json"
    doc.write_text(json.dumps(payload), encoding="utf-8")
    ref = doc.as_uri()  # produces file:///... on all platforms
    result = _load_json_document(ref, "/any/config.yml", label="test")
    assert result == payload


# ── remote URIs via fsspec ────────────────────────────────────────────────────

def _make_fsspec_mock(payload_text: str) -> MagicMock:
    mock_fsspec = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=io.StringIO(payload_text))
    ctx.__exit__ = MagicMock(return_value=False)
    mock_fsspec.open.return_value = ctx
    return mock_fsspec


def test_load_json_document_s3_uri_calls_fsspec(monkeypatch) -> None:
    for name in ("AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL", "AWS_S3_FORCE_PATH_STYLE"):
        monkeypatch.delenv(name, raising=False)
    payload = {"entities": []}
    mock_fsspec = _make_fsspec_mock(json.dumps(payload))
    with patch.dict(sys.modules, {"fsspec": mock_fsspec}):
        result = _load_json_document(
            "s3://my-bucket/floe/reports/run.json",
            "s3://my-bucket/floe/config.yml",
            label="test",
        )
    mock_fsspec.open.assert_called_once_with(
        "s3://my-bucket/floe/reports/run.json", "r", encoding="utf-8"
    )
    assert result == payload


def test_load_json_document_s3_uri_honors_endpoint_env(monkeypatch) -> None:
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://fallback:9000")
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://seaweedfs:8333")
    monkeypatch.setenv("AWS_S3_FORCE_PATH_STYLE", "true")
    mock_fsspec = _make_fsspec_mock("{}")
    with patch.dict(sys.modules, {"fsspec": mock_fsspec}):
        _read_json_text("s3://bucket/report.json", "s3://bucket/config.yml")
    mock_fsspec.open.assert_called_once_with(
        "s3://bucket/report.json",
        "r",
        encoding="utf-8",
        client_kwargs={"endpoint_url": "http://seaweedfs:8333"},
        config_kwargs={"s3": {"addressing_style": "path"}},
    )


def test_load_json_document_s3_uri_endpoint_fallback_without_path_style(monkeypatch) -> None:
    monkeypatch.delenv("AWS_ENDPOINT_URL_S3", raising=False)
    monkeypatch.delenv("AWS_S3_FORCE_PATH_STYLE", raising=False)
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://minio:9000")
    mock_fsspec = _make_fsspec_mock("{}")
    with patch.dict(sys.modules, {"fsspec": mock_fsspec}):
        _read_json_text("s3://bucket/report.json", "s3://bucket/config.yml")
    mock_fsspec.open.assert_called_once_with(
        "s3://bucket/report.json",
        "r",
        encoding="utf-8",
        client_kwargs={"endpoint_url": "http://minio:9000"},
    )


def test_load_json_document_gs_uri_calls_fsspec() -> None:
    payload = {"status": "success"}
    mock_fsspec = _make_fsspec_mock(json.dumps(payload))
    with patch.dict(sys.modules, {"fsspec": mock_fsspec}):
        result = _load_json_document(
            "gs://bucket/report.json",
            "gs://bucket/config.yml",
            label="test",
        )
    mock_fsspec.open.assert_called_once()
    assert result == payload


def test_load_json_document_abfs_uri_calls_fsspec() -> None:
    payload = {"rows": 100}
    mock_fsspec = _make_fsspec_mock(json.dumps(payload))
    with patch.dict(sys.modules, {"fsspec": mock_fsspec}):
        result = _load_json_document(
            "abfs://container@account.dfs.core.windows.net/report.json",
            "abfs://container@account.dfs.core.windows.net/config.yml",
            label="test",
        )
    mock_fsspec.open.assert_called_once()
    assert result == payload


# ── missing fsspec → helpful ImportError ─────────────────────────────────────

def test_load_json_document_remote_missing_fsspec_raises_import_error() -> None:
    original = sys.modules.pop("fsspec", None)
    try:
        with patch.dict(sys.modules, {"fsspec": None}):
            with pytest.raises(ImportError, match="pip install fsspec"):
                _read_json_text("s3://bucket/report.json", "s3://bucket/config.yml")
    finally:
        if original is not None:
            sys.modules["fsspec"] = original


# ── non-dict JSON root raises ValueError ─────────────────────────────────────

def test_load_json_document_non_dict_root_raises(tmp_path: Path) -> None:
    doc = tmp_path / "bad.json"
    doc.write_text("[1, 2, 3]", encoding="utf-8")
    with pytest.raises(ValueError, match="must be an object"):
        _load_json_document(str(doc), str(tmp_path / "config.yml"), label="entity report")
