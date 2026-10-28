"""`-p s3://...` and `profile_path="s3://..."`: the profile is read from S3 as an `s3://` manifest is."""

import hashlib
import json
import sys
from pathlib import Path

import pytest

import floe
from floe._cli import main
from floe.storage.s3 import S3Client
from floe.storage.types import StoredObject

URI = "s3://cfg/profiles/prod.yml"
PROFILE = (
    "apiVersion: floe/v1\nkind: EnvironmentProfile\nmetadata: {name: prod}\n"
    "variables: {ZONE: bronze}\n"
)
CONFIG = """\
version: "0.3"
domains: [{name: d}]
entities:
  - name: a
    domain: d
    source: {format: csv, path: ./in/a.csv, options: {header: true, separator: ","}}
    sink: {accepted: {format: parquet, path: "./out/{{ZONE}}/a"}}
    policy: {severity: warn}
    schema:
      columns:
        - {name: id, type: int64}
report: {path: ./report}
"""


@pytest.fixture
def profile_on_s3(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    reads: list[str] = []

    def read(_self: S3Client, uri: str) -> StoredObject | None:
        reads.append(uri)
        return StoredObject(PROFILE.encode(), "etag") if uri == URI else None

    monkeypatch.setattr(S3Client, "read", read)
    return reads


@pytest.fixture
def config(tmp_path: Path) -> Path:
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "a.csv").write_text("id\n1\n")
    (tmp_path / "config.yml").write_text(CONFIG)
    return tmp_path / "config.yml"


def cli(monkeypatch: pytest.MonkeyPatch, *args: str) -> int:
    monkeypatch.setattr(sys, "argv", ["floe", *args])
    return main()


def test_run_reads_the_profile_from_s3(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    profile_on_s3: list[str],
    config: Path,
) -> None:
    assert cli(monkeypatch, "run", "-c", str(config), "-p", URI) == 0
    assert profile_on_s3 == [URI]
    assert (tmp_path / "out" / "bronze" / "a").exists()


def test_validate_reads_the_profile_from_s3_and_keeps_it_in_the_next_command(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    profile_on_s3: list[str],
    config: Path,
) -> None:
    assert cli(monkeypatch, "validate", "-p", URI) == 0
    assert "Profile valid: prod\n" in capsys.readouterr().out
    assert cli(monkeypatch, "validate", "-c", str(config), "-p", URI) == 0
    assert capsys.readouterr().out.endswith(f" -p {URI}\n")
    assert profile_on_s3 == [URI, URI]


def test_a_missing_s3_profile_is_an_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    profile_on_s3: list[str],
    config: Path,
) -> None:
    missing = "s3://cfg/profiles/absent.yml"
    assert cli(monkeypatch, "run", "-c", str(config), "-p", missing) == 1
    assert capsys.readouterr().err == f"Error: profile not found: {missing}\n"


def test_the_python_api_reads_the_profile_from_s3(
    tmp_path: Path, profile_on_s3: list[str], config: Path
) -> None:
    floe.validate(str(config), profile_path=URI)
    floe.run(str(config), profile_path=URI)
    assert (tmp_path / "out" / "bronze" / "a").exists()
    assert profile_on_s3 == [URI, URI]
    with pytest.raises(floe.FloeConfigError, match="^failed to load profile "):
        floe.validate(str(config), profile_path="s3://cfg/profiles/absent.yml")


def test_manifest_generate_records_an_s3_profile_as_typed(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    profile_on_s3: list[str],
    config: Path,
) -> None:
    assert cli(monkeypatch, "manifest", "generate", "-c", str(config), "-p", URI, "-o", "-") == 0  # fmt: skip
    manifest = json.loads(capsys.readouterr().out)
    assert manifest["profile_uri"] == URI
    assert manifest["profile_checksum"] == f"sha256:{hashlib.sha256(PROFILE.encode()).hexdigest()}"  # fmt: skip
