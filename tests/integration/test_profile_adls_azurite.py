"""`-p abfss://...`, `manifest generate -o abfss://...` and `run --manifest abfss://...` on Azurite (#1389).

Skipped unless `FLOE_AZURITE_ENDPOINT` is set, e.g. `http://localhost:10000/devstoreaccount1`. A bare `abfss://` URI has no
storage definition, so the endpoint comes from obstore's `AZURE_STORAGE_ENDPOINT`; `AZURE_STORAGE_ACCOUNT_KEY` comes from
the workflow environment.
"""

import json
import os
import sys
import uuid
from pathlib import Path

import pytest

from floe._cli import main
from floe.storage.resolve import read_object, write_object

pytestmark = [
    pytest.mark.adls,
    pytest.mark.skipif(
        not os.environ.get("FLOE_AZURITE_ENDPOINT"),
        reason="FLOE_AZURITE_ENDPOINT not set",
    ),
]
CONTAINER = os.environ.get("FLOE_AZURITE_CONTAINER", "warehouse")
ROOT = f"abfss://{CONTAINER}@devstoreaccount1.dfs.core.windows.net"
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


@pytest.fixture(autouse=True)
def azurite(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AZURE_STORAGE_ENDPOINT", os.environ["FLOE_AZURITE_ENDPOINT"])
    monkeypatch.setenv("AZURE_ALLOW_HTTP", "true")


def cli(monkeypatch: pytest.MonkeyPatch, *args: str) -> int:
    monkeypatch.setattr(sys, "argv", ["floe", *args])
    return main()


def test_profile_and_manifest_round_trip_through_azurite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = f"{ROOT}/it-{uuid.uuid4().hex}"
    write_object(f"{base}/prod.yml", PROFILE.encode())
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "a.csv").write_text("id\n1\n")
    config = tmp_path / "config.yml"
    config.write_text(CONFIG)
    profile, manifest = f"{base}/prod.yml", f"{base}/m.json"
    assert cli(monkeypatch, "validate", "-c", str(config), "-p", profile) == 0
    argv = ("manifest", "generate", "-c", str(config), "-p", profile, "-o", manifest)
    assert cli(monkeypatch, *argv) == 0
    written = json.loads(read_object(manifest, "manifest"))
    assert written["profile_uri"] == profile
    assert written["execution"]["base_args"][:3] == ["run", "--manifest", manifest]
    monkeypatch.setenv("FLOE_WORK_ROOT", str(tmp_path))
    assert cli(monkeypatch, "run", "--manifest", manifest) == 0
    assert (tmp_path / "out" / "bronze" / "a").exists()
