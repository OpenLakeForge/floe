"""`floe manifest generate`: determinism, the revision hash, secret redaction and refusals."""

import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

import pytest
from botocore.stub import ANY, Stubber

from floe import __version__
from floe._cli import main
from floe.manifest import detect_runtime
from floe.storage.s3 import S3Client

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


@pytest.mark.parametrize("output", ["gs://b/m.json", "abfs://c@a/m.json"])
def test_non_s3_remote_output_is_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    output: str,
) -> None:
    assert generate(tmp_path, monkeypatch, "-o", output) == 1
    assert "remote manifest output is not supported yet" in capsys.readouterr().err


def test_s3_output_is_put_through_the_s3_client_and_baked_into_base_args(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "k")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "s")
    uri = "s3://b/dev/manifests/sales.json"
    client = S3Client("b", "us-east-1")
    sent: dict[str, Any] = {}
    client._client.meta.events.register(  # pyright: ignore[reportPrivateUsage]
        "provide-client-params.s3.PutObject",
        lambda params, **_: sent.update(params),  # pyright: ignore[reportUnknownLambdaType, reportUnknownArgumentType]  # botocore hooks are untyped
    )
    monkeypatch.setattr("floe._cli.S3Client", lambda _bucket: client)  # pyright: ignore[reportUnknownLambdaType, reportUnknownArgumentType]  # one stubbed client
    with Stubber(client._client) as stub:  # pyright: ignore[reportPrivateUsage]
        stub.add_response(  # pyright: ignore[reportUnknownMemberType]
            "put_object",
            {},
            {"Bucket": "b", "Key": "dev/manifests/sales.json", "Body": ANY},
        )
        assert generate(tmp_path, monkeypatch, "-o", uri) == 0
        stub.assert_no_pending_responses()
    assert capsys.readouterr().out == f"Manifest written: {uri}\n"
    manifest = json.loads(sent["Body"])
    assert manifest["execution"]["base_args"][:3] == ["run", "--manifest", uri]


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


ENTITY = """\
  - name: {name}
    domain: {domain}
    source: {{format: csv, path: ./in/{name}.csv}}
    sink: {{accepted: {{format: parquet, path: ./out/{domain}/{name}}}}}
    policy: {{severity: warn}}
    schema: {{columns: [{{name: id, type: string}}]}}
"""
TWO_DOMAINS = (
    'version: "0.3"\ndomains: [{name: d1}, {name: d2}]\nentities:\n'
    + "".join(
        ENTITY.format(name=n, domain=d)
        for n, d in (("a", "d1"), ("a", "d2"), ("b", "d1"))
    )
    + "report: {path: ./report}\n"
)


def names(manifest: dict[str, Any]) -> list[str]:
    return [e["name"] for e in manifest["entities"]]


def test_entities_select_by_id_then_name_and_fail_like_rust(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    out = tmp_path / "m.json"
    args = ("-o", str(out), "--entities")
    assert generate(tmp_path, monkeypatch, *args, "d1.a,b", config=TWO_DOMAINS) == 0
    assert names(json.loads(out.read_text())) == ["d1.a", "d1.b"]
    assert generate(tmp_path, monkeypatch, *args, "a", config=TWO_DOMAINS) == 1
    assert "entity a is ambiguous; use one of: d1.a, d2.a" in capsys.readouterr().err
    assert generate(tmp_path, monkeypatch, *args, "x,b,y", config=TWO_DOMAINS) == 1
    assert "entities not found: x, y" in capsys.readouterr().err


def test_default_domain_and_resolved_uri_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    extra = ("--default-domain", "dd", "--manifest-path-mode", "resolved-uri")
    out = tmp_path / "m.json"
    config = CONFIG.replace("    domain: d\n", "")
    assert generate(tmp_path, monkeypatch, "-o", str(out), *extra, config=config) == 0
    entity = json.loads(out.read_text())["entities"][0]
    assert (entity["domain"], entity["asset_key"]) == ("dd", ["dd", "a"])
    assert entity["source"]["path"] == f"{tmp_path}/in/a.csv"


def test_image_runtime_rebases_the_config_uri_under_the_work_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FLOE_WORK_ROOT", "/data/")
    out = tmp_path / "m.json"
    argv = ("-o", str(out), "--runtime", "image")
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.yml").write_text(CONFIG)
    monkeypatch.setattr(
        sys, "argv", ["floe", "manifest", "generate", "-c", "./config.yml", *argv]
    )
    assert main() == 0
    manifest = json.loads(out.read_text())
    assert manifest["runtime_env"] == "image"
    assert manifest["work_root"] == "/data/"
    assert manifest["config_uri"] == "local:///data/config.yml"


def test_floe_runtime_overrides_container_detection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FLOE_RUNTIME", " Image ")
    assert detect_runtime() == "image"
    monkeypatch.setenv("FLOE_RUNTIME", "cli")
    assert detect_runtime() == "cli"


def test_spec_version_is_the_floe_major_minor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """D22: `spec_version` is the Floe major.minor, and `floe.manifest.v1` is gone."""
    assert generate(tmp_path, monkeypatch, "-o", "-") == 0
    manifest: dict[str, Any] = json.loads(capsys.readouterr().out)
    assert manifest["spec_version"] == ".".join(__version__.split(".")[:2])
    root = Path(__file__).resolve().parents[2]
    schema = json.loads(
        (root / "orchestrators/schemas/floe.manifest.schema.json").read_text()
    )
    assert set(manifest) <= set(schema["properties"])
    assert set(schema["required"]) <= set(manifest)
    pattern = schema["properties"]["spec_version"]["pattern"]
    assert re.fullmatch(pattern, manifest["spec_version"])


TAIL = (
    "sink: {accepted: {format: parquet, path: ./out}}, policy: {severity: warn}, "
    "schema: {columns: [{name: id, type: string}]}"
)
D20_PROJECT = f"""\
version: "0.3"
metadata: {{project: sales, owner: data-team, tags: [bronze]}}
sources:
  - {{name: crm, format: csv, path: "./in/crm/{{{{resource}}}}.csv"}}
lineage: {{url: "http://ol", namespace: floe, api_key: "${{OL_API_KEY}}"}}
domains: [{{name: d}}]
entities:
  - {{name: orders, domain: d, source: {{ref: crm}}, {TAIL}}}
  - {{name: clients, domain: d, source: {{ref: crm, resource: customers}}, {TAIL}}}
  - {{name: inline, domain: d, source: {{format: csv, path: ./in/x.csv}}, {TAIL}}}
report: {{path: ./report}}
"""


def test_d20_manifest_holds_the_sources_index_metadata_and_no_host_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("FLOE_WORK_ROOT", raising=False)
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.yml").write_text(D20_PROJECT)
    argv = ["floe", "manifest", "generate", "-c", "config.yml", "-o", "-"]
    monkeypatch.setattr(sys, "argv", [*argv, "--runtime", "image"])
    assert main() == 0
    text = capsys.readouterr().out
    manifest: dict[str, Any] = json.loads(text)
    assert str(tmp_path) not in text
    assert manifest["report_base_uri"] == "local:///work/report"
    assert manifest["lineage"]["api_key"] == "${OL_API_KEY}"
    assert manifest["metadata"] == {
        "project": "sales",
        "owner": "data-team",
        "tags": ["bronze"],
    }
    assert manifest["sources"] == [
        {
            "name": "crm",
            "format": "csv",
            "uri": "local:///work/in/crm",
            "entities": ["d.clients", "d.orders"],
        }
    ]
    uris = {e["name"]: e["source"]["uri"] for e in manifest["entities"]}
    assert uris["d.orders"] == "local:///work/in/crm/orders.csv"
    assert uris["d.inline"] == "local:///work/in/x.csv"
    root = Path(__file__).resolve().parents[2]
    schema = json.loads(
        (root / "orchestrators/schemas/floe.manifest.schema.json").read_text()
    )
    assert set(manifest) <= set(schema["properties"])
