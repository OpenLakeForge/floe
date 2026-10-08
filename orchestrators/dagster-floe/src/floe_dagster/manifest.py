from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from importlib import resources
import json
import os
from pathlib import Path
from typing import Any
import posixpath
from urllib.parse import unquote, urlparse, urlunparse

from jsonschema import Draft202012Validator

MANIFEST_SCHEMA = "floe.manifest.v1"


@dataclass(frozen=True)
class ManifestEntity:
    name: str
    domain: str | None
    group_name: str
    source_format: str
    accepted_sink_uri: str
    rejected_sink_uri: str | None
    asset_key: list[str]
    runner: str | None
    source_uri: str | None = None
    policy_severity: str | None = None
    write_mode: str | None = None
    incremental_mode: str | None = None

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "ManifestEntity":
        asset_key = _required_string_list(data, "asset_key")
        if not asset_key:
            raise ValueError("entities[].asset_key must contain at least one part")
        source = data.get("source") or {}
        source_uri = _optional_str(source, "uri") if isinstance(source, dict) else None
        return ManifestEntity(
            name=_required_str(data, "name"),
            domain=_optional_str(data, "domain"),
            group_name=_required_str(data, "group_name"),
            source_format=_required_str(data, "source_format"),
            accepted_sink_uri=_required_str(data, "accepted_sink_uri"),
            rejected_sink_uri=_optional_str(data, "rejected_sink_uri"),
            asset_key=asset_key,
            runner=_optional_str(data, "runner"),
            source_uri=source_uri,
            policy_severity=_optional_str(data, "policy_severity"),
            write_mode=_optional_str(data, "write_mode"),
            incremental_mode=_optional_str(data, "incremental_mode"),
        )


@dataclass(frozen=True)
class ManifestOrchestration:
    max_concurrent_entities: int | None
    strategy: str | None

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "ManifestOrchestration":
        return ManifestOrchestration(
            max_concurrent_entities=_optional_int(data, "max_concurrent_entities"),
            strategy=_optional_str(data, "strategy"),
        )


@dataclass(frozen=True)
class ManifestExecutionResultContract:
    run_finished_event: bool
    summary_uri_field: str
    exit_codes: dict[str, str]

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "ManifestExecutionResultContract":
        run_finished_event = data.get("run_finished_event")
        if not isinstance(run_finished_event, bool):
            raise ValueError("execution.result_contract.run_finished_event must be boolean")
        return ManifestExecutionResultContract(
            run_finished_event=run_finished_event,
            summary_uri_field=_required_str(data, "summary_uri_field"),
            exit_codes=_required_string_map(data, "exit_codes"),
        )


@dataclass(frozen=True)
class ManifestExecutionDefaults:
    env: dict[str, str]
    workdir: str | None

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "ManifestExecutionDefaults":
        return ManifestExecutionDefaults(
            env=_required_string_map(data, "env"),
            workdir=_optional_str(data, "workdir"),
        )


@dataclass(frozen=True)
class ManifestExecution:
    entrypoint: str
    base_args: list[str]
    per_entity_args: list[str]
    log_format: str
    result_contract: ManifestExecutionResultContract
    defaults: ManifestExecutionDefaults
    orchestration: ManifestOrchestration | None = None

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "ManifestExecution":
        log_format = _required_str(data, "log_format")
        if log_format != "json":
            raise ValueError("execution.log_format must be 'json' for dagster-floe")
        orchestration_raw = data.get("orchestration")
        orchestration = (
            ManifestOrchestration.from_dict(orchestration_raw)
            if isinstance(orchestration_raw, dict)
            else None
        )
        return ManifestExecution(
            entrypoint=_required_str(data, "entrypoint"),
            base_args=_required_string_list(data, "base_args"),
            per_entity_args=_required_string_list(data, "per_entity_args"),
            log_format=log_format,
            result_contract=ManifestExecutionResultContract.from_dict(
                _required_object(data, "result_contract")
            ),
            defaults=ManifestExecutionDefaults.from_dict(_required_object(data, "defaults")),
            orchestration=orchestration,
        )


@dataclass(frozen=True)
class ManifestRunnerDefinition:
    runner_type: str
    image: str | None
    namespace: str | None
    service_account: str | None
    env: dict[str, str] | None
    command: list[str] | None
    args: list[str] | None
    timeout_seconds: int | None
    ttl_seconds_after_finished: int | None
    poll_interval_seconds: int | None
    secrets: list[dict[str, Any]] | None
    workspace_url: str | None = None
    existing_cluster_id: str | None = None
    config_uri: str | None = None
    python_file_uri: str | None = None
    job_name: str | None = None
    auth: dict[str, str] | None = None
    env_parameters: dict[str, str] | None = None

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "ManifestRunnerDefinition":
        env = _optional_string_map(data, "env")
        command = _optional_command(data, "command")
        args = _optional_string_list(data, "args")
        timeout_seconds = _optional_int(data, "timeout_seconds")
        ttl_seconds_after_finished = _optional_int(data, "ttl_seconds_after_finished")
        poll_interval_seconds = _optional_int(data, "poll_interval_seconds")
        secrets = _optional_object_list(data, "secrets")
        auth = _optional_string_map(data, "auth")
        env_parameters = _optional_string_map(data, "env_parameters")
        return ManifestRunnerDefinition(
            runner_type=_required_str(data, "type"),
            image=_optional_str(data, "image"),
            namespace=_optional_str(data, "namespace"),
            service_account=_optional_str(data, "service_account"),
            env=env,
            command=command,
            args=args,
            timeout_seconds=timeout_seconds,
            ttl_seconds_after_finished=ttl_seconds_after_finished,
            poll_interval_seconds=poll_interval_seconds,
            secrets=secrets,
            workspace_url=_optional_str(data, "workspace_url"),
            existing_cluster_id=_optional_str(data, "existing_cluster_id"),
            config_uri=_optional_str(data, "config_uri"),
            python_file_uri=_optional_str(data, "python_file_uri"),
            job_name=_optional_str(data, "job_name"),
            auth=auth,
            env_parameters=env_parameters,
        )


@dataclass(frozen=True)
class ManifestRunners:
    default: str
    definitions: dict[str, ManifestRunnerDefinition]

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "ManifestRunners":
        default = _required_str(data, "default")
        definitions_raw = _required_object(data, "definitions")
        definitions = {
            key: ManifestRunnerDefinition.from_dict(value)
            for key, value in _required_object_map(definitions_raw, "definitions").items()
        }
        if default not in definitions:
            raise ValueError(f"runners.default '{default}' not found in runners.definitions")
        return ManifestRunners(default=default, definitions=definitions)


@dataclass(frozen=True)
class DagsterManifest:
    schema: str
    generated_at_ts_ms: int
    floe_version: str
    spec_version: str
    manifest_id: str
    config_uri: str
    config_checksum: str | None
    report_base_uri: str
    execution: ManifestExecution
    runners: ManifestRunners
    entities: list[ManifestEntity]
    storages: dict[str, Any] | None = None
    catalogs: dict[str, Any] | None = None
    lineage: dict[str, Any] | None = None
    manifest_name: str | None = None
    manifest_revision: str | None = None
    profile_uri: str | None = None
    profile_checksum: str | None = None

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "DagsterManifest":
        schema = _required_str(data, "schema")
        if schema != MANIFEST_SCHEMA:
            raise ValueError(f"unexpected manifest schema: {schema}")
        generated_at = data.get("generated_at_ts_ms")
        if not isinstance(generated_at, int) or generated_at < 0:
            raise ValueError("generated_at_ts_ms must be a non-negative integer")
        entities_raw = data.get("entities")
        if not isinstance(entities_raw, list):
            raise ValueError("entities must be a list")
        entities = [ManifestEntity.from_dict(item) for item in entities_raw]
        storages = data.get("storages")
        catalogs = data.get("catalogs")
        lineage = data.get("lineage")
        return DagsterManifest(
            schema=schema,
            generated_at_ts_ms=generated_at,
            floe_version=_required_str(data, "floe_version"),
            spec_version=_required_str(data, "spec_version"),
            manifest_id=_required_str(data, "manifest_id"),
            config_uri=_required_str(data, "config_uri"),
            config_checksum=_optional_str(data, "config_checksum"),
            report_base_uri=_required_str(data, "report_base_uri"),
            execution=ManifestExecution.from_dict(_required_object(data, "execution")),
            runners=ManifestRunners.from_dict(_required_object(data, "runners")),
            entities=entities,
            storages=storages if isinstance(storages, dict) else None,
            catalogs=catalogs if isinstance(catalogs, dict) else None,
            lineage=lineage if isinstance(lineage, dict) else None,
            manifest_name=_optional_str(data, "manifest_name"),
            manifest_revision=_optional_str(data, "manifest_revision"),
            profile_uri=_optional_str(data, "profile_uri"),
            profile_checksum=_optional_str(data, "profile_checksum"),
        )


def _is_remote_uri(path: str) -> bool:
    return path.startswith(("s3://", "gs://", "abfs://"))


def _fsspec_storage_options(uri: str) -> dict[str, Any]:
    """S3-compatible endpoint/path-style options, matching floe-core's env handling."""
    if not uri.startswith("s3://"):
        return {}
    options: dict[str, Any] = {}
    endpoint = os.environ.get("AWS_ENDPOINT_URL_S3") or os.environ.get("AWS_ENDPOINT_URL")
    if endpoint:
        options["client_kwargs"] = {"endpoint_url": endpoint}
    if os.environ.get("AWS_S3_FORCE_PATH_STYLE", "").strip().lower() in ("1", "true"):
        options["config_kwargs"] = {"s3": {"addressing_style": "path"}}
    return options


def _read_remote_text(uri: str) -> str:
    try:
        import fsspec
    except ImportError as exc:
        raise ImportError(
            "reading remote manifest URIs requires fsspec; "
            "install it with: pip install fsspec"
        ) from exc
    with fsspec.open(uri, "r", encoding="utf-8", **_fsspec_storage_options(uri)) as f:
        return f.read()


def load_manifest(path: str | Path) -> DagsterManifest:
    path_str = str(path)
    text = _read_remote_text(path_str) if _is_remote_uri(path_str) else Path(path).read_text(encoding="utf-8")
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError("manifest file must contain a JSON object")
    _validate_manifest_payload(payload)
    return DagsterManifest.from_dict(payload)


def resolve_config_uri(manifest_path: str | Path, config_uri: str) -> str:
    if config_uri.startswith("local://"):
        return _local_uri_to_path(config_uri, manifest_path)
    if "://" in config_uri:
        return config_uri

    config_path = Path(config_uri)
    if config_path.is_absolute():
        return str(config_path)

    manifest_str = str(manifest_path)
    if _is_remote_uri(manifest_str):
        parsed = urlparse(manifest_str)
        new_path = posixpath.normpath(posixpath.join(posixpath.dirname(parsed.path), config_uri))
        return urlunparse(parsed._replace(path=new_path, params="", query="", fragment=""))

    base = Path(manifest_path).resolve().parent
    return str((base / config_path).resolve())


def render_execution_args(
    execution: ManifestExecution,
    config_uri: str,
    entity_name: str | None,
    run_id: str | None = None,
    manifest_uri: str | None = None,
) -> list[str]:
    args: list[str] = []
    args.extend(
        _render_token(
            token,
            config_uri=config_uri,
            entity_name=None,
            run_id=run_id,
            manifest_uri=manifest_uri,
        )
        for token in execution.base_args
    )
    if entity_name is not None:
        args.extend(
            _render_token(
                token,
                config_uri=config_uri,
                entity_name=entity_name,
                run_id=run_id,
                manifest_uri=manifest_uri,
            )
            for token in execution.per_entity_args
        )
    return args


def resolve_entity_runner(
    manifest: DagsterManifest, entity: ManifestEntity
) -> ManifestRunnerDefinition:
    runner_name = entity.runner or manifest.runners.default
    definition = manifest.runners.definitions.get(runner_name)
    if definition is None:
        raise ValueError(f"runner '{runner_name}' not found in runners.definitions")
    return definition


def _render_token(
    token: str,
    *,
    config_uri: str,
    entity_name: str | None,
    run_id: str | None,
    manifest_uri: str | None = None,
) -> str:
    rendered = token.replace("{config_uri}", config_uri)
    if manifest_uri is not None:
        rendered = rendered.replace("{manifest_uri}", manifest_uri)
    if entity_name is not None:
        rendered = rendered.replace("{entity_name}", entity_name)
    if run_id is not None:
        rendered = rendered.replace("{run_id}", run_id)
    return rendered


def _local_uri_to_path(local_uri: str, manifest_path: str | Path | None = None) -> str:
    raw_path = unquote(local_uri[len("local://") :])
    local_path = Path(raw_path)
    if local_path.is_absolute():
        return str(local_path)
    if manifest_path is not None:
        base = Path(manifest_path).resolve().parent
        return str((base / local_path).resolve())
    return str(local_path.resolve())


def _required_str(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{key} must be a non-empty string")
    return value


def _optional_str(data: dict[str, Any], key: str) -> str | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{key} must be a string when provided")
    return value


def _required_object(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"{key} must be an object")
    return value


def _required_object_map(data: dict[str, Any], key: str) -> dict[str, dict[str, Any]]:
    if not isinstance(data, dict):
        raise ValueError(f"{key} must be an object")
    if not all(isinstance(name, str) and isinstance(value, dict) for name, value in data.items()):
        raise ValueError(f"{key} must be a map<string, object>")
    return data


def _required_string_list(data: dict[str, Any], key: str) -> list[str]:
    value = data.get(key)
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{key} must be an array of strings")
    return value


def _required_string_map(data: dict[str, Any], key: str) -> dict[str, str]:
    value = data.get(key)
    if not isinstance(value, dict) or not all(
        isinstance(item_key, str) and isinstance(item_value, str)
        for item_key, item_value in value.items()
    ):
        raise ValueError(f"{key} must be a map<string,string>")
    return value


def _optional_string_map(data: dict[str, Any], key: str) -> dict[str, str] | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, dict) or not all(
        isinstance(item_key, str) and isinstance(item_value, str)
        for item_key, item_value in value.items()
    ):
        raise ValueError(f"{key} must be a map<string,string> when provided")
    return value


def _optional_string_list(data: dict[str, Any], key: str) -> list[str] | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{key} must be an array of strings when provided")
    return value


def _optional_command(data: dict[str, Any], key: str) -> list[str] | None:
    value = data.get(key)
    if value is None:
        return None
    if isinstance(value, str):
        return [value]
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return value
    raise ValueError(f"{key} must be string|list[str] when provided")


def _optional_int(data: dict[str, Any], key: str) -> int | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, int):
        raise ValueError(f"{key} must be an integer when provided")
    return value


def _optional_object_list(data: dict[str, Any], key: str) -> list[dict[str, Any]] | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ValueError(f"{key} must be a list of objects when provided")
    return value


def _validate_manifest_payload(payload: dict[str, Any]) -> None:
    validator = _manifest_validator()
    errors = sorted(validator.iter_errors(payload), key=lambda error: list(error.path))
    if not errors:
        return
    first = errors[0]
    path = ".".join(str(part) for part in first.absolute_path) or "$"
    raise ValueError(f"manifest schema validation failed at {path}: {first.message}")


@lru_cache(maxsize=1)
def _manifest_validator() -> Draft202012Validator:
    schema_path = resources.files("floe_dagster").joinpath(
        "schemas/floe.manifest.v1.json"
    )
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    return Draft202012Validator(schema)
