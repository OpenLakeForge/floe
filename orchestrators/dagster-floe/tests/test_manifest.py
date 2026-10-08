import json
from pathlib import Path

import pytest

from floe_dagster.manifest import (
    DagsterManifest,
    load_manifest,
    render_execution_args,
    resolve_config_uri,
    resolve_entity_runner,
)


def test_manifest_asset_keys_and_groups():
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    manifest = load_manifest(fixture)

    assert [e.asset_key for e in manifest.entities] == [["hr", "employees"], ["sales", "orders"]]
    assert [e.group_name for e in manifest.entities] == ["hr", "sales"]


def test_render_execution_args_replaces_placeholders():
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    manifest = load_manifest(fixture)
    args = render_execution_args(
        manifest.execution,
        config_uri="/tmp/config.yml",
        entity_name="employees",
        run_id="run-42",
    )
    assert args == [
        "run",
        "-c",
        "/tmp/config.yml",
        "--log-format",
        "json",
        "--run-id",
        "run-42",
        "--entities",
        "employees",
    ]


def test_resolve_config_uri_with_relative_path():
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    data = json.loads(fixture.read_text(encoding="utf-8"))
    manifest = load_manifest(fixture)
    resolved = resolve_config_uri(str(fixture), manifest.config_uri)
    assert resolved == str((fixture.parent / data["config_uri"]).resolve())


def test_resolve_entity_runner_prefers_entity_override():
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    manifest = load_manifest(fixture)

    employees_runner = resolve_entity_runner(manifest, manifest.entities[0])
    orders_runner = resolve_entity_runner(manifest, manifest.entities[1])

    assert employees_runner.runner_type == "local_process"
    assert orders_runner.runner_type == "kubernetes_job"


def test_manifest_rejects_non_json_log_format():
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    payload["execution"]["log_format"] = "text"

    with pytest.raises(ValueError, match="execution.log_format"):
        DagsterManifest.from_dict(payload)


def test_manifest_schema_rejects_unknown_top_level_key(tmp_path: Path):
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    payload["unexpected_top_level"] = True
    manifest_path = tmp_path / "manifest.invalid.json"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="manifest schema validation failed"):
        load_manifest(manifest_path)


def test_manifest_schema_accepts_runtime_env_and_work_root(tmp_path: Path):
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    payload["runtime_env"] = "image"
    payload["work_root"] = "/work"
    manifest_path = tmp_path / "manifest.runtime_env.json"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    manifest = load_manifest(manifest_path)
    assert manifest.manifest_id == payload["manifest_id"]


def test_manifest_schema_rejects_missing_entity_source(tmp_path: Path):
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    del payload["entities"][0]["source"]
    manifest_path = tmp_path / "manifest.invalid.json"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="manifest schema validation failed"):
        load_manifest(manifest_path)


def test_manifest_schema_accepts_extended_k8_runner_fields(tmp_path: Path):
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    payload["runners"]["definitions"]["k8s"].update(
        {
            "command": ["floe"],
            "args": ["run", "-c", "{config_uri}", "--entities", "{entity_name}"],
            "timeout_seconds": 120,
            "ttl_seconds_after_finished": 60,
            "poll_interval_seconds": 5,
            "secrets": [{"name": "API_TOKEN", "secret_name": "floe-secrets", "key": "token"}],
        }
    )
    manifest_path = tmp_path / "manifest.k8.extended.json"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    manifest = load_manifest(manifest_path)
    k8s_runner = manifest.runners.definitions["k8s"]
    assert k8s_runner.command == ["floe"]
    assert k8s_runner.poll_interval_seconds == 5
    assert k8s_runner.secrets is not None and k8s_runner.secrets[0]["name"] == "API_TOKEN"


def test_entity_source_uri_parsed_from_source_object():
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    manifest = load_manifest(fixture)

    assert manifest.entities[0].source_uri == "./in/hr/employees.csv"
    assert manifest.entities[1].source_uri == "./in/sales/orders.csv"


def test_entity_policy_severity_parsed():
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    manifest = load_manifest(fixture)

    # employees uses "reject", orders uses "warn"
    assert manifest.entities[0].policy_severity == "reject"
    assert manifest.entities[1].policy_severity == "warn"


def test_entity_policy_severity_defaults_to_none_when_absent():
    # policy_severity is required and non-nullable in the manifest schema (Rust
    # ManifestEntity.policy_severity is a plain String, never absent or null for a
    # real manifest) - but the dataclass layer still tolerates an absent/null value
    # on its own, so exercise it directly via from_dict, bypassing schema validation.
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    del payload["entities"][0]["policy_severity"]

    manifest = DagsterManifest.from_dict(payload)
    assert manifest.entities[0].policy_severity is None


def test_manifest_schema_accepts_databricks_runner_fields(tmp_path: Path):
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    payload["runners"] = {
        "default": "dbx",
        "definitions": {
            "dbx": {
                "type": "databricks_job",
                "workspace_url": "https://adb-1234.5.azuredatabricks.net",
                "existing_cluster_id": "1111-222222-abc123",
                "config_uri": "dbfs:/floe/configs/prod.yml",
                "python_file_uri": "dbfs:/floe/bin/floe_entry.py",
                "job_name": "floe-sales-prod",
                "command": "floe",
                "args": ["run", "-c", "dbfs:/floe/configs/prod.yml"],
                "poll_interval_seconds": 20,
                "timeout_seconds": 1800,
                "auth": {
                    "service_principal_oauth_ref": "env://DATABRICKS_TOKEN"
                },
                "env_parameters": {"FLOE_ENV": "prod"},
            }
        },
    }
    manifest_path = tmp_path / "manifest.dbx.json"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    manifest = load_manifest(manifest_path)
    runner = manifest.runners.definitions["dbx"]
    assert runner.runner_type == "databricks_job"
    assert runner.command == ["floe"]
    assert runner.workspace_url == "https://adb-1234.5.azuredatabricks.net"


def test_resolve_config_uri_with_remote_manifest_relative_path():
    result = resolve_config_uri("s3://bucket/manifests/prod.json", "../configs/prod.yml")
    assert result == "s3://bucket/configs/prod.yml"


def test_resolve_config_uri_with_remote_manifest_same_dir():
    result = resolve_config_uri("s3://bucket/manifests/prod.json", "prod.yml")
    assert result == "s3://bucket/manifests/prod.yml"


def test_load_manifest_remote_uri_raises_if_fsspec_missing(monkeypatch) -> None:
    import sys

    monkeypatch.setitem(sys.modules, "fsspec", None)
    with pytest.raises(ImportError, match="fsspec"):
        load_manifest("s3://bucket/manifest.json")


def test_load_manifest_remote_uri_uses_fsspec(monkeypatch) -> None:
    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    manifest_text = fixture.read_text(encoding="utf-8")

    class _FakeFile:
        def __init__(self, text):
            self._text = text

        def read(self):
            return self._text

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

    import types

    fake_fsspec = types.ModuleType("fsspec")
    fake_fsspec.open = lambda uri, *args, **kwargs: _FakeFile(manifest_text)
    monkeypatch.setitem(__import__("sys").modules, "fsspec", fake_fsspec)

    result = load_manifest("s3://bucket/test/manifest.json")
    assert result.manifest_id


def test_load_manifest_remote_uri_honors_endpoint_env(monkeypatch) -> None:
    import io
    import types

    fixture = Path(__file__).parent / "fixtures" / "manifest.json"
    calls = []

    def fake_open(uri, *args, **kwargs):
        calls.append(kwargs)
        return io.StringIO(fixture.read_text(encoding="utf-8"))

    fake_fsspec = types.ModuleType("fsspec")
    fake_fsspec.open = fake_open
    monkeypatch.setitem(__import__("sys").modules, "fsspec", fake_fsspec)
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://seaweedfs:8333")
    monkeypatch.setenv("AWS_S3_FORCE_PATH_STYLE", "1")

    load_manifest("s3://bucket/test/manifest.json")
    assert calls == [
        {
            "encoding": "utf-8",
            "client_kwargs": {"endpoint_url": "http://seaweedfs:8333"},
            "config_kwargs": {"s3": {"addressing_style": "path"}},
        }
    ]


# ---------------------------------------------------------------------------
# ManifestOrchestration parsing
# ---------------------------------------------------------------------------

from floe_dagster.manifest import ManifestOrchestration


def _base_execution_dict(**kwargs):
    base = {
        "entrypoint": "floe",
        "base_args": ["run", "--manifest", "{manifest_uri}", "--log-format", "json"],
        "per_entity_args": ["--entities", "{entity_name}"],
        "log_format": "json",
        "result_contract": {
            "run_finished_event": True,
            "summary_uri_field": "summary_uri",
            "exit_codes": {"0": "success_or_rejected"},
        },
        "defaults": {"env": {}, "workdir": None},
    }
    base.update(kwargs)
    return base


def test_manifest_orchestration_absent_when_not_in_execution():
    from floe_dagster.manifest import ManifestExecution
    data = _base_execution_dict()
    execution = ManifestExecution.from_dict(data)
    assert execution.orchestration is None


def test_manifest_orchestration_parsed_strategy_sequential():
    from floe_dagster.manifest import ManifestExecution
    data = _base_execution_dict(orchestration={"strategy": "sequential"})
    execution = ManifestExecution.from_dict(data)
    assert execution.orchestration is not None
    assert execution.orchestration.strategy == "sequential"
    assert execution.orchestration.max_concurrent_entities is None


def test_manifest_orchestration_parsed_max_concurrent():
    from floe_dagster.manifest import ManifestExecution
    data = _base_execution_dict(orchestration={"max_concurrent_entities": 2})
    execution = ManifestExecution.from_dict(data)
    assert execution.orchestration is not None
    assert execution.orchestration.max_concurrent_entities == 2
    assert execution.orchestration.strategy is None


def test_manifest_orchestration_parsed_both_fields():
    from floe_dagster.manifest import ManifestExecution
    data = _base_execution_dict(
        orchestration={"max_concurrent_entities": 1, "strategy": "sequential"}
    )
    execution = ManifestExecution.from_dict(data)
    assert execution.orchestration is not None
    assert execution.orchestration.max_concurrent_entities == 1
    assert execution.orchestration.strategy == "sequential"


def test_manifest_orchestration_null_value_treated_as_absent():
    from floe_dagster.manifest import ManifestExecution
    data = _base_execution_dict(orchestration=None)
    execution = ManifestExecution.from_dict(data)
    assert execution.orchestration is None
