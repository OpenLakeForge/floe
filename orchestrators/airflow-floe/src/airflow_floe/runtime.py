"""Runtime helpers for Airflow-Floe DAGs."""

from __future__ import annotations

from dataclasses import dataclass, replace
import json
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

try:
    from airflow.sdk import Asset
except Exception:  # pragma: no cover - fallback for local unit tests without Airflow
    @dataclass
    class Asset:  # type: ignore[override]
        name: str
        uri: str
        group: str | None = None

from .manifest import AirflowManifest, ManifestEntity, load_manifest


@dataclass(frozen=True)
class DagManifestContext:
    manifest_path: str
    manifest: AirflowManifest | None
    config_path: str
    assets_by_entity: dict[str, Asset]
    entities_by_name: dict[str, ManifestEntity]
    entity_names: list[str]


def build_dag_manifest_context(
    manifest_path: str,
    config_override: str | None = None,
    domains: list[str] | None = None,
) -> DagManifestContext:
    manifest = load_manifest(manifest_path)
    if domains is not None:
        unknown = sorted(set(domains) - {e.domain for e in manifest.entities})
        if unknown:
            raise ValueError(f"unknown domain(s) {unknown} in {manifest_path}")
        manifest = replace(
            manifest, entities=[e for e in manifest.entities if e.domain in domains]
        )
    config_path = config_override or resolve_config_path(manifest_path, manifest.config_uri)
    assets_by_entity = build_entity_assets(manifest, manifest_path)
    entities_by_name = {entity.name: entity for entity in manifest.entities}
    entity_names = [entity.name for entity in manifest.entities]

    return DagManifestContext(
        manifest_path=manifest_path,
        manifest=manifest,
        config_path=config_path,
        assets_by_entity=assets_by_entity,
        entities_by_name=entities_by_name,
        entity_names=entity_names,
    )


def build_dag_manifest_context_or_empty(
    manifest_path: str,
    *,
    config_override: str | None = None,
    default_config_path: str | None = None,
) -> DagManifestContext:
    try:
        return build_dag_manifest_context(
            manifest_path=manifest_path,
            config_override=config_override,
        )
    except Exception:
        fallback_config = config_override or default_config_path
        if fallback_config is None:
            raise ValueError(
                "manifest loading failed and no fallback config path was provided"
            )
        return DagManifestContext(
            manifest_path=manifest_path,
            manifest=None,
            config_path=_normalize_fallback_config_path(fallback_config),
            assets_by_entity={},
            entities_by_name={},
            entity_names=[],
        )


def _normalize_fallback_config_path(config_value: str) -> str:
    if config_value.startswith("local://"):
        return _local_uri_to_path(config_value)
    if "://" in config_value:
        # Keep URI-style config overrides untouched (s3://, gs://, abfs://, ...).
        return config_value
    return str(Path(config_value).resolve())


def resolve_config_path(manifest_path: str, config_uri: str) -> str:
    if config_uri.startswith("local://"):
        return _local_uri_to_path(config_uri, manifest_path)

    if "://" in config_uri:
        # Keep remote config URI untouched.
        return config_uri

    config_path = Path(config_uri)
    if config_path.is_absolute():
        return str(config_path)

    base = Path(manifest_path).resolve().parent
    return str((base / config_path).resolve())


def _local_uri_to_path(local_uri: str, manifest_path: str | None = None) -> str:
    raw_path = unquote(local_uri[len("local://") :])
    local_path = Path(raw_path)
    if local_path.is_absolute():
        return str(local_path)

    if manifest_path is not None:
        base = Path(manifest_path).resolve().parent
        return str((base / local_path).resolve())

    return str(local_path.resolve())


def _resolve_target_uri(manifest_path: str, target_uri: str) -> str:
    if "://" in target_uri:
        return target_uri

    target_path = Path(target_uri)
    if not target_path.is_absolute():
        base = Path(manifest_path).resolve().parent
        target_path = (base / target_path).resolve()

    return target_path.as_uri()


def build_entity_assets(manifest: AirflowManifest, manifest_path: str) -> dict[str, Asset]:
    assets: dict[str, Asset] = {}
    for entity in manifest.entities:
        name = "floe." + ".".join(entity.asset_key)
        uri = _resolve_target_uri(manifest_path, entity.accepted_sink_uri)
        assets[entity.name] = Asset(name=name, uri=uri, group=entity.group_name)
    return assets


def parse_run_finished(stdout: str) -> dict[str, Any]:
    run_finished: dict[str, Any] | None = None
    for line in stdout.splitlines():
        if not line.strip():
            continue
        event = json.loads(line)
        if event.get("schema") != "floe.log.v1":
            continue
        if event.get("event") == "run_finished":
            run_finished = event

    if run_finished is None:
        raise ValueError("run_finished event not found")
    return run_finished


def _resolve_summary_path(summary_uri: str, config_path: str) -> Path | None:
    if summary_uri.startswith("file://"):
        parsed = urlparse(summary_uri)
        return Path(unquote(parsed.path))

    if summary_uri.startswith("local://"):
        # Floe local report targets are emitted as local://<path>.
        raw_path = unquote(summary_uri[len("local://") :])
        summary_path = Path(raw_path)
        if not summary_path.is_absolute():
            base = Path(config_path).resolve().parent
            summary_path = (base / summary_path).resolve()
        return summary_path

    if "://" in summary_uri:
        # Remote summary loading is out of scope for MVP.
        return None

    summary_path = Path(summary_uri)
    if not summary_path.is_absolute():
        base = Path(config_path).resolve().parent
        summary_path = (base / summary_path).resolve()
    return summary_path


def load_run_summary(summary_uri: str | None, config_path: str) -> dict[str, Any] | None:
    if not summary_uri:
        return None

    summary_path = _resolve_summary_path(summary_uri, config_path)
    if summary_path is None:
        return None

    if not summary_path.exists():
        return None

    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return None

    return payload


def summary_entities_by_name(summary_payload: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(summary_payload, dict):
        return {}

    entities = summary_payload.get("entities")
    if not isinstance(entities, list):
        return {}

    indexed: dict[str, dict[str, Any]] = {}
    for item in entities:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        if isinstance(name, str) and name:
            indexed[name] = item
    return indexed


def build_asset_event_extra(
    entity: ManifestEntity,
    run_finished: dict[str, Any],
    summary_entity: dict[str, Any] | None,
    config_path: str | None = None,
) -> dict[str, Any]:
    summary_entity = summary_entity or {}
    results = summary_entity.get("results") if isinstance(summary_entity, dict) else None
    if not isinstance(results, dict):
        results = {}
    entity_report_file = _resolve_entity_report_file(
        entity_name=entity.name,
        summary_entity=summary_entity,
        summary_uri=run_finished.get("summary_uri"),
        config_path=config_path,
    )

    return {
        "entity": entity.name,
        "domain": entity.domain,
        "run_id": run_finished.get("run_id"),
        "status": summary_entity.get("status", run_finished.get("status")),
        "summary_uri": run_finished.get("summary_uri"),
        "entity_report_file": entity_report_file,
        "files_total": results.get("files_total"),
        "rows_total": results.get("rows_total"),
        "accepted_total": results.get("accepted_total", run_finished.get("accepted")),
        "rejected_total": results.get("rejected_total", run_finished.get("rejected")),
        "warnings_total": results.get("warnings_total", run_finished.get("warnings")),
        "errors_total": results.get("errors_total", run_finished.get("errors")),
    }


def _resolve_entity_report_file(
    *,
    entity_name: str,
    summary_entity: dict[str, Any],
    summary_uri: Any,
    config_path: str | None,
) -> str | None:
    explicit = summary_entity.get("report_file")
    if isinstance(explicit, str) and explicit:
        return explicit

    if not isinstance(summary_uri, str) or not summary_uri:
        return None
    if not config_path:
        return None

    summary_path = _resolve_summary_path(summary_uri, config_path)
    if summary_path is None:
        return None

    return str(summary_path.parent / entity_name / "run.json")
