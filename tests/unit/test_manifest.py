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
        - {name: email, strategy: hash, key: "${PII_KEY}"}
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


def test_literal_pii_hash_key_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """#1006: dropping the key would make a manifest run hash with unsalted SHA-256."""
    config = CONFIG.replace('"${PII_KEY}"', "literal-secret")
    assert generate(tmp_path, monkeypatch, "-o", "-", config=config) == 1
    out, err = capsys.readouterr()
    assert "literal-secret" not in out + err
    assert (
        "entity.name=a pii.columns[name=email].key: a manifest never carries secrets"
        in err
    )
    assert "${ENV_VAR}" in err


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


def test_d20_sources_uri_is_null_when_consumers_share_no_location(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = D20_PROJECT.replace(
        "domains:",
        "storages:\n  default: fs\n  definitions:\n"
        "    - {name: fs, type: local}\n"
        "    - {name: lake, type: s3, bucket: b, region: r}\n"
        "domains:",
        1,
    ).replace("{ref: crm, resource: customers}", "{ref: crm, storage: lake}")
    (crm,) = manifest_of(tmp_path, monkeypatch, config)["sources"]
    assert (crm["name"], crm["uri"]) == ("crm", None)


PROFILE = """\
apiVersion: floe/v1
kind: EnvironmentProfile
metadata: {name: prod}
storages:
  default: lake
  definitions:
    - {name: lake, type: s3, bucket: prod-bucket, region: r}
"""


def test_profile_storages_shape_the_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """#872: `-p` applies the profile as `floe run -p` does, and records it like Rust 0.7."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "prod.yml").write_text(PROFILE)
    args = ("-o", "-", "--runtime", "image", "-p", "prod.yml")
    assert generate(tmp_path, monkeypatch, *args) == 0
    manifest: dict[str, Any] = json.loads(capsys.readouterr().out)
    source = manifest["entities"][0]["source"]
    assert (source["storage"], source["uri"]) == ("lake", "s3://prod-bucket/./in/a.csv")
    assert manifest["profile_uri"] == "local:///work/prod.yml"
    checksum = hashlib.sha256(PROFILE.encode()).hexdigest()
    assert manifest["profile_checksum"] == f"sha256:{checksum}"
    assert generate(tmp_path, monkeypatch, "-o", "-") == 0
    assert "profile_uri" not in json.loads(capsys.readouterr().out)


_NULLS = dict.fromkeys(
    [
        "command",
        "args",
        "timeout_seconds",
        "ttl_seconds_after_finished",
        "poll_interval_seconds",
        "secrets",
        "image",
        "namespace",
        "service_account",
        "resources",
        "env",
        "workspace_url",
        "existing_cluster_id",
        "config_uri",
        "python_file_uri",
        "job_name",
        "auth",
        "env_parameters",
    ]
)
# Recorded from the Rust 0.7.0 release binary: `floe manifest generate -c config.yml -p <profile>`.
K8S = """\
apiVersion: floe/v1
kind: EnvironmentProfile
metadata: {name: prod}
execution:
  runner:
    type: kubernetes_job
    image: registry/floe:0.8
    namespace: floe-prod
    command: floe
    args: [run, -c, /config/config.yml]
    timeout_seconds: 3600
    env: {Z_LAST: z, A_FIRST: a}
    resources: {memory_mb: 512}
    secrets:
      - {key: password, name: DB_PASSWORD, secret_name: floe-db}
  orchestration: {strategy: parallel, max_concurrent_entities: 4}
"""
RUST_K8S = {
    "orchestration": {"max_concurrent_entities": 4, "strategy": "parallel"},
    "runner": _NULLS
    | {
        "type": "kubernetes_job",
        "command": "floe",
        "args": ["run", "-c", "/config/config.yml"],
        "timeout_seconds": 3600,
        "secrets": [
            {"name": "DB_PASSWORD", "secret_name": "floe-db", "key": "password"}
        ],
        "image": "registry/floe:0.8",
        "namespace": "floe-prod",
        "resources": {"cpu": None, "memory_mb": 512},
        "env": {"A_FIRST": "a", "Z_LAST": "z"},
    },
}
DBX = """\
apiVersion: floe/v1
kind: EnvironmentProfile
metadata: {name: prod}
execution:
  runner:
    type: databricks_job
    workspace_url: https://adb.example.net
    existing_cluster_id: c-1
    config_uri: dbfs:/floe/prod.yml
    python_file_uri: dbfs:/floe/entry.py
    auth: {service_principal_oauth_ref: sp}
    env_parameters: {B: b, A: a}
    image: ignored
  orchestration: {}
"""
RUST_DBX = {
    "orchestration": {},
    "runner": _NULLS
    | {
        "type": "databricks_job",
        "workspace_url": "https://adb.example.net",
        "existing_cluster_id": "c-1",
        "config_uri": "dbfs:/floe/prod.yml",
        "python_file_uri": "dbfs:/floe/entry.py",
        "job_name": "floe-{domain}-{env}",
        "auth": {"service_principal_oauth_ref": "sp"},
        "env_parameters": {"A": "a", "B": "b"},
    },
}


@pytest.mark.parametrize(("profile", "rust"), [(K8S, RUST_K8S), (DBX, RUST_DBX)])
def test_profile_execution_shapes_the_runner_and_orchestration(
    profile: str,
    rust: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """#891: `-p` compiles the profile's `execution` block as Rust 0.7 does."""
    (tmp_path / "prod.yml").write_text(profile)
    argv = ("-o", "-", "-p", str(tmp_path / "prod.yml"))
    assert generate(tmp_path, monkeypatch, *argv) == 0
    manifest: dict[str, Any] = json.loads(capsys.readouterr().out)
    assert manifest["execution"]["orchestration"] == rust["orchestration"]
    runner = {"type": rust["runner"]["type"]} | rust["runner"]  # `type` comes first
    runners = {"default": "default", "definitions": {"default": runner}}
    assert json.dumps(manifest["runners"]) == json.dumps(runners)  # key order too


def test_profile_runner_command_may_be_a_list(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The manifest and the connectors take a list; the profile took only a string."""
    profile = K8S.replace("    command: floe\n", "    command: [olf-floe-job, --]\n")
    (tmp_path / "prod.yml").write_text(profile)
    assert generate(tmp_path, monkeypatch, "-o", "-", "-p", str(tmp_path / "prod.yml")) == 0  # fmt: skip
    runner = json.loads(capsys.readouterr().out)["runners"]["definitions"]["default"]
    assert runner["command"] == ["olf-floe-job", "--"]


def test_580_domain_scope_fields_ride_the_k8s_runner_only_when_set(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    profile = K8S.replace(
        "    resources: {memory_mb: 512}\n",
        "    resources: {memory_mb: 512, memory_limit_mb: 1024}\n"
        "    job_scope: domain\n"
        "    domain_resources: {sales: {cpu: '2', cpu_limit: '4', memory_mb: 4096}}\n",
    )
    (tmp_path / "prod.yml").write_text(profile)
    assert (
        generate(tmp_path, monkeypatch, "-o", "-", "-p", str(tmp_path / "prod.yml"))
        == 0
    )
    runner = json.loads(capsys.readouterr().out)["runners"]["definitions"]["default"]
    assert runner["job_scope"] == "domain"
    assert runner["resources"] == {
        "cpu": None,
        "memory_mb": 512,
        "memory_limit_mb": 1024,
    }
    assert runner["domain_resources"] == {
        "sales": {"cpu": "2", "memory_mb": 4096, "cpu_limit": "4"}
    }


def test_580_one_config_compiles_per_environment_domain_resources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def runner_for(env: str, memory_mb: int) -> dict[str, Any]:
        profile = K8S.replace("name: prod", f"name: {env}").replace(
            "    command",
            f"    job_scope: domain\n    domain_resources: {{sales: {{memory_mb: {memory_mb}}}}}\n    command",
        )
        (tmp_path / f"{env}.yml").write_text(profile)
        assert (
            generate(
                tmp_path, monkeypatch, "-o", "-", "-p", str(tmp_path / f"{env}.yml")
            )
            == 0
        )
        return json.loads(capsys.readouterr().out)["runners"]["definitions"]["default"]

    assert runner_for("dev", 1024)["domain_resources"] == {
        "sales": {"cpu": None, "memory_mb": 1024}
    }
    assert runner_for("prod", 8192)["domain_resources"] == {
        "sales": {"cpu": None, "memory_mb": 8192}
    }


@pytest.mark.parametrize(
    ("setting", "where"),
    [
        ("resources: {memory_limit_mb: 0}", "profile.execution.runner.resources"),
        (
            "domain_resources: {sales: {memory_limit_mb: 0}}",
            "profile.execution.runner.domain_resources.sales",
        ),
    ],
)
def test_580_a_zero_memory_limit_is_rejected(
    setting: str,
    where: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    profile = K8S.replace("    resources: {memory_mb: 512}\n", f"    {setting}\n")
    (tmp_path / "prod.yml").write_text(profile)
    assert (
        generate(tmp_path, monkeypatch, "-o", "-", "-p", str(tmp_path / "prod.yml"))
        != 0
    )
    assert f"{where}.memory_limit_mb: must be >= 1" in capsys.readouterr().err


def test_580_a_zero_per_domain_memory_request_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    profile = K8S.replace(
        "    resources: {memory_mb: 512}\n",
        "    domain_resources: {sales: {memory_mb: 0}}\n",
    )
    (tmp_path / "prod.yml").write_text(profile)
    assert (
        generate(tmp_path, monkeypatch, "-o", "-", "-p", str(tmp_path / "prod.yml"))
        != 0
    )
    assert "domain_resources.sales.memory_mb: must be >= 1" in capsys.readouterr().err


def test_580_job_scope_must_be_entity_or_domain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / "prod.yml").write_text(
        K8S.replace("    command", "    job_scope: all\n    command")
    )
    assert (
        generate(tmp_path, monkeypatch, "-o", "-", "-p", str(tmp_path / "prod.yml"))
        != 0
    )
    assert (
        'job_scope: expected "entity" or "domain", got "all"' in capsys.readouterr().err
    )


K8S_POD_OPTIONS: dict[str, Any] = {
    "labels": {"olf.io/flow": "ingest"},
    "annotations": {"team": "data"},
    "image_pull_policy": "IfNotPresent",
    "security_context": {
        "runAsNonRoot": True,
        "runAsUser": 1000,
        "capabilities": {"drop": ["ALL"]},
    },
    "node_selector": {"pool": "batch"},
    "tolerations": [{"key": "batch", "operator": "Exists", "effect": "NoSchedule"}],
    "backoff_limit": 2,
    "env_from": [{"secretRef": {"name": "floe-env"}}],
}


def k8s_runner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    setting: str,
) -> dict[str, Any]:
    (tmp_path / "prod.yml").write_text(
        K8S.replace("    command", f"    {setting}\n    command")
    )
    argv = ("-o", "-", "-p", str(tmp_path / "prod.yml"))
    assert generate(tmp_path, monkeypatch, *argv) == 0
    return json.loads(capsys.readouterr().out)["runners"]["definitions"]["default"]


def test_1023_pod_options_ride_the_k8s_runner_only_when_set(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    unset = k8s_runner(tmp_path, monkeypatch, capsys, "poll_interval_seconds: 5")
    assert not K8S_POD_OPTIONS.keys() & unset.keys()
    # a YAML flow mapping is valid JSON
    settings = "\n    ".join(
        f"{k}: {json.dumps(v)}" for k, v in K8S_POD_OPTIONS.items()
    )
    runner = k8s_runner(tmp_path, monkeypatch, capsys, settings)
    assert {k: runner[k] for k in K8S_POD_OPTIONS} == K8S_POD_OPTIONS


@pytest.mark.parametrize(
    ("setting", "message"),
    [
        ("labels: x", "expected map at profile.execution.runner.labels"),
        ("labels: {a: 1}", "expected string at profile.execution.runner.labels"),
        ("annotations: [a]", "expected map at profile.execution.runner.annotations"),
        (
            "image_pull_policy: [Always]",
            "expected string at profile.execution.runner.image_pull_policy",
        ),
        (
            "security_context: x",
            "expected map at profile.execution.runner.security_context",
        ),
        (
            "security_context: {1: a}",
            "expected string key at profile.execution.runner.security_context",
        ),
        (
            "node_selector: {a: 1}",
            "expected string at profile.execution.runner.node_selector",
        ),
        (
            "tolerations: {a: b}",
            "expected array at profile.execution.runner.tolerations",
        ),
        ("tolerations: [x]", "expected map at profile.execution.runner.tolerations[]"),
        ("env_from: [[a]]", "expected map at profile.execution.runner.env_from[]"),
        (
            "backoff_limit: -1",
            "profile.execution.runner.backoff_limit must be a non-negative integer",
        ),
        (
            "backoff_limit: two",
            "profile.execution.runner.backoff_limit must be a non-negative integer",
        ),
    ],
)
def test_1023_a_malformed_pod_option_is_rejected(
    setting: str,
    message: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    (tmp_path / "prod.yml").write_text(
        K8S.replace("    command", f"    {setting}\n    command")
    )
    argv = ("-o", "-", "-p", str(tmp_path / "prod.yml"))
    assert generate(tmp_path, monkeypatch, *argv) != 0
    assert message in capsys.readouterr().err


def test_d28_the_entity_source_carries_its_partitioning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = D20_PROJECT.replace(
        "source: {format: csv, path: ./in/x.csv}",
        'source: {format: csv, path: ./in, partitioning: {pattern: "{yyyy}/{MM}/{dd}", date_column: day}}',
    )
    entities = {
        e["name"]: e for e in manifest_of(tmp_path, monkeypatch, config)["entities"]
    }
    assert entities["d.inline"]["source"]["partitioning"] == {
        "batch_column": None,
        "date_column": "day",
        "lookback_days": 1,  # D30
        "pattern": "{yyyy}/{MM}/{dd}",
    }
    assert "partitioning" not in entities["d.orders"]["source"]
    root = Path(__file__).resolve().parents[2]
    schema = json.loads(
        (root / "orchestrators/schemas/floe.manifest.schema.json").read_text()
    )
    source = schema["properties"]["entities"]["items"]["properties"]["source"]
    assert set(entities["d.inline"]["source"]) <= set(source["properties"])


def test_build_manifest_returns_what_manifest_generate_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """#986: the public API and `manifest generate -o -` build the same manifest."""
    import floe

    (tmp_path / "prod.yml").write_text(K8S)
    monkeypatch.chdir(tmp_path)  # `-c`/`-p` as typed, relative, land in the manifest
    argv = ("-o", "-", "-p", "prod.yml", "--deterministic", "--entities", "a")
    (tmp_path / "config.yml").write_text(CONFIG)
    monkeypatch.setattr(
        sys, "argv", ["floe", "manifest", "generate", "-c", "config.yml", *argv]
    )
    assert main() == 0
    generated = json.loads(capsys.readouterr().out)
    assert floe.build_manifest("config.yml", "prod.yml", entities=["a"]) == generated
