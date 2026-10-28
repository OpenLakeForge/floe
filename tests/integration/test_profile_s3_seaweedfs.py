"""`-p s3://...` and `profile_path="s3://..."` against the S3-compatible service of the #602 harness (#1025).

Skipped unless `FLOE_S3_ENDPOINT` is set, e.g. `FLOE_S3_ENDPOINT=http://localhost:8333` (SeaweedFS).
"""

import hashlib
import json
import os
import sys
import uuid
from pathlib import Path

import pytest

import floe
from floe._cli import main
from floe.config.profile import parse_profile
from floe.errors import FloeIoError
from floe.storage.s3 import S3Client

pytestmark = [
    pytest.mark.s3,
    pytest.mark.skipif(
        not os.environ.get("FLOE_S3_ENDPOINT"), reason="FLOE_S3_ENDPOINT not set"
    ),
]
BUCKET = os.environ.get("FLOE_S3_BUCKET", "warehouse")
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
def profile_uri(monkeypatch: pytest.MonkeyPatch) -> str:
    """A profile uploaded to the service; Floe reaches it through the environment, as a run pod does."""
    endpoint = os.environ["FLOE_S3_ENDPOINT"]
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "spike")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "spike-s3-secret")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", endpoint)
    monkeypatch.setenv("AWS_S3_FORCE_PATH_STYLE", "true")
    s3 = S3Client(BUCKET, "us-east-1", endpoint, True)
    uri = s3.resolve_uri(f"it-{uuid.uuid4().hex}/profiles/prod.yml")
    s3.write(uri, PROFILE.encode())
    return uri


@pytest.fixture
def config(tmp_path: Path) -> Path:
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "a.csv").write_text("id\n1\n")
    (tmp_path / "config.yml").write_text(CONFIG)
    return tmp_path / "config.yml"


def cli(monkeypatch: pytest.MonkeyPatch, *args: str) -> int:
    monkeypatch.setattr(sys, "argv", ["floe", *args])
    return main()


def test_parse_profile_reads_s3(profile_uri: str) -> None:
    profile = parse_profile(profile_uri)
    assert (profile.name, profile.variables) == ("prod", {"ZONE": "bronze"})
    with pytest.raises(FloeIoError, match="^profile not found: "):
        parse_profile(profile_uri.replace("prod.yml", "absent.yml"))


def test_run_reads_the_profile_from_s3(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, profile_uri: str, config: Path
) -> None:
    assert cli(monkeypatch, "run", "-c", str(config), "-p", profile_uri) == 0
    assert (tmp_path / "out" / "bronze" / "a").exists()


def test_validate_keeps_the_s3_profile_in_the_next_command(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    profile_uri: str,
    config: Path,
) -> None:
    assert cli(monkeypatch, "validate", "-p", profile_uri) == 0
    assert "Profile valid: prod\n" in capsys.readouterr().out
    assert cli(monkeypatch, "validate", "-c", str(config), "-p", profile_uri) == 0
    assert capsys.readouterr().out.endswith(f" -p {profile_uri}\n")


def test_a_missing_s3_profile_is_an_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    profile_uri: str,
    config: Path,
) -> None:
    missing = profile_uri.replace("prod.yml", "absent.yml")
    assert cli(monkeypatch, "run", "-c", str(config), "-p", missing) == 1
    assert capsys.readouterr().err == f"Error: profile not found: {missing}\n"


def test_the_python_api_reads_the_profile_from_s3(
    tmp_path: Path, profile_uri: str, config: Path
) -> None:
    floe.validate(str(config), profile_path=profile_uri)
    floe.run(str(config), profile_path=profile_uri)
    assert (tmp_path / "out" / "bronze" / "a").exists()
    with pytest.raises(floe.FloeConfigError, match="^failed to load profile "):
        floe.validate(
            str(config), profile_path=profile_uri.replace("prod.yml", "absent.yml")
        )


def test_manifest_generate_records_an_s3_profile_as_typed(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    profile_uri: str,
    config: Path,
) -> None:
    assert cli(monkeypatch, "manifest", "generate", "-c", str(config), "-p", profile_uri, "-o", "-") == 0  # fmt: skip
    manifest = json.loads(capsys.readouterr().out)
    assert manifest["profile_uri"] == profile_uri
    assert manifest["profile_checksum"] == f"sha256:{hashlib.sha256(PROFILE.encode()).hexdigest()}"  # fmt: skip
