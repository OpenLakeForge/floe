"""`floe manifest generate`: determinism, the revision hash, secret redaction and refusals."""

import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import pytest

from floe._cli import main

CONFIG = """\
version: "0.3"
domains: [{name: d}]
entities:
  - name: a
    domain: d
    source: {format: csv, path: ./in/a.csv}
    sink: {accepted: {format: parquet, path: ./out/a}}
    policy: {severity: warn}
    schema:
      columns:
        - {name: email, type: string}
    pii:
      columns:
        - {name: email, strategy: hash, key: literal-secret}
report: {path: ./report}
"""


def generate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *extra: str, config: str = CONFIG
) -> int:
    (tmp_path / "config.yml").write_text(config)
    argv = ["floe", "manifest", "generate", "-c", str(tmp_path / "config.yml")]
    monkeypatch.setattr(sys, "argv", [*argv, *extra])
    return main()


def test_deterministic_manifest_is_stable_and_revision_is_the_content_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    out = tmp_path / "m" / "manifest.json"
    args = ("-o", str(out), "--deterministic", "--manifest-name", "sales.prod")
    assert generate(tmp_path, monkeypatch, *args) == 0
    first = out.read_text()
    assert generate(tmp_path, monkeypatch, *args) == 0
    assert out.read_text() == first
    manifest: dict[str, Any] = json.loads(first)
    assert manifest["generated_at_ts_ms"] == 0
    assert manifest["manifest_name"] == "sales.prod"
    body = {
        k: v
        for k, v in manifest.items()
        if k not in ("generated_at_ts_ms", "manifest_revision")
    }
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode()).hexdigest()
    assert manifest["manifest_revision"] == f"sha256:{digest}"


def test_literal_pii_hmac_key_is_redacted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    assert generate(tmp_path, monkeypatch, "-o", "-") == 0
    assert "literal-secret" not in capsys.readouterr().out


@pytest.mark.parametrize("output", ["s3://bucket/manifest.json", "gs://b/m.json"])
def test_remote_output_is_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    output: str,
) -> None:
    assert generate(tmp_path, monkeypatch, "-o", output) == 1
    assert "remote manifest output is not supported yet" in capsys.readouterr().err
