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


def manifest_of(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, config: str
) -> dict[str, Any]:
    out = tmp_path / "manifest.json"
    assert generate(tmp_path, monkeypatch, "-o", str(out), config=config) == 0
    return json.loads(out.read_text())


def test_s3_paths_resolve_under_bucket_and_prefix_and_registries_are_embedded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = CONFIG.replace(
        "domains:",
        "storages:\n  default: lake\n  definitions:\n"
        "    - {name: lake, type: s3, bucket: b, region: r, prefix: base}\n"
        "domains:",
    )
    manifest = manifest_of(tmp_path, monkeypatch, config)
    assert manifest["report_base_uri"] == "s3://b/base/./report"
    source = manifest["entities"][0]["source"]
    assert (source["storage"], source["uri"]) == ("lake", "s3://b/base/./in/a.csv")
    (definition,) = manifest["storages"]["definitions"]
    assert definition["fs_type"] == "s3"


def test_config_files_are_relative_to_the_root_and_hashed_into_the_checksum(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "domains" / "d").mkdir(parents=True)
    (tmp_path / "domains" / "d" / "_domain.yml").write_text("name: d\n")
    (tmp_path / "domains" / "d" / "a.yml").write_text(
        "domain: d\nname: a\nsource: {format: csv, path: in/a.csv}\n"
        "sink: {accepted: {format: parquet, path: out/a}}\n"
        "policy: {severity: warn}\nschema: {columns: [{name: id, type: string}]}\n"
    )
    config = "version: '0.3'\ninclude:\n  domains: ['domains/*/_domain.yml']\n"
    manifest = manifest_of(tmp_path, monkeypatch, config + "report: {path: ./report}\n")
    files = manifest["config_files"]
    assert [f["path"] for f in files] == [
        "config.yml",
        "domains/d/_domain.yml",
        "domains/d/a.yml",
    ]
    lines = "".join(f"{f['path']}\0{f['sha256']}\n" for f in files)
    digest = hashlib.sha256(lines.encode()).hexdigest()
    assert manifest["config_checksum"] == f"sha256:{digest}"
