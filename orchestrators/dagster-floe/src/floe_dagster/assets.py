from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from dagster import (
    AssetKey,
    AssetOut,
    Definitions,
    Failure,
    MetadataValue,
    Output,
    SourceAsset,
    multi_asset,
)

from .k8s_status import STATUS_SUCCEEDED

from .asset_checks import build_asset_check_results, build_asset_check_specs
from .events import last_run_finished, parse_json_event_lines, parse_run_finished, render_log_event
from .manifest import (
    DagsterManifest,
    ManifestExecution,
    ManifestEntity,
    ManifestRunnerDefinition,
    _fsspec_storage_options,
    load_manifest,
    resolve_config_uri,
    resolve_entity_runner,
)
from .runner import Runner


def load_floe_assets(
    manifest_path: str,
    runner: Runner,
    entities: list[str] | None = None,
    register_source_assets: bool = True,
    manifest_uri: str | None = None,
    domains: list[str] | None = None,
) -> Definitions:
    """Build a Dagster Definitions for the given manifest.

    register_source_assets: if False, skip SourceAsset registration for source keys.
        Set to False when another Dagster pipeline in the same workspace already
        materialises those keys to avoid asset key conflicts.

    manifest_uri: optional runtime URI passed as {manifest_uri} into the execution
        args for kubernetes_job and databricks_job runners. When omitted, manifest_path
        is used. Set this when the Dagster code server loads the manifest from a local
        path but the runner pod must reference it via object storage (e.g. s3://).
    """
    assets_defs, source_assets, _selected_entities = build_floe_asset_defs(
        manifest_path=manifest_path,
        runner=runner,
        entities=entities,
        register_source_assets=register_source_assets,
        manifest_uri=manifest_uri,
        domains=domains,
    )
    return Definitions(assets=[*assets_defs, *source_assets])


def build_floe_asset_defs(
    manifest_path: str,
    runner: Runner,
    entities: list[str] | None = None,
    register_source_assets: bool = True,
    manifest_uri: str | None = None,
    domains: list[str] | None = None,
) -> tuple[list[Any], list[SourceAsset], list[ManifestEntity]]:
    manifest = load_manifest(manifest_path)
    config_uri = resolve_config_uri(manifest_path, manifest.config_uri)
    entity_items = selected_manifest_entities(manifest, entities)
    if domains is not None:
        unknown = sorted(set(domains) - {e.domain for e in manifest.entities})
        if unknown:
            raise ValueError(f"unknown domain(s) {unknown} in {manifest_path}")
        entity_items = [e for e in entity_items if e.domain in domains]

    # Qualified op names (sales__accounts) can equal a legal domain-less name; fail
    # with both entities named instead of Dagster's later duplicate-node error.
    op_owners: dict[str, str] = {}
    for entity in entity_items:
        op_name = _op_name(entity)
        other = op_owners.setdefault(op_name, entity.name)
        if other != entity.name:
            raise ValueError(
                f"entities {other!r} and {entity.name!r} map to the same Dagster op name "
                f"{op_name!r} in {manifest_path}; rename one of them"
            )

    assets_defs = []
    for entity in entity_items:
        runner_definition = resolve_entity_runner(manifest, entity)
        assets_defs.append(
            _make_entity_multi_asset(
                entity=entity,
                config_uri=config_uri,
                manifest_path=manifest_path,
                manifest_uri=manifest_uri,
                runner=runner,
                execution=manifest.execution,
                runner_definition=runner_definition,
            )
        )

    source_assets = _build_source_assets(entity_items) if register_source_assets else []
    entity_key_set = {tuple(e.asset_key) for e in entity_items}
    for sa in source_assets:
        sk = tuple(sa.key.path)
        if sk in entity_key_set:
            collision = "/".join(sa.key.path)
            raise ValueError(
                f"duplicate asset key {collision!r}: generated source key collides with "
                f"an existing entity key in {manifest_path}"
            )
    return assets_defs, source_assets, entity_items


def selected_manifest_entities(
    manifest: DagsterManifest, entities: list[str] | None = None
) -> list[ManifestEntity]:
    if not entities:
        return list(manifest.entities)
    selected = set(entities)
    return [item for item in manifest.entities if item.name in selected]


def _op_name(entity: ManifestEntity) -> str:
    # Domain-less entities keep their historical op name; qualified ones need the
    # domain so `sales.accounts` and `finance.accounts` don't collide.
    if entity.domain in (None, "default"):
        return entity.asset_key[-1]
    return re.sub(r"[^A-Za-z0-9_]", "_", "__".join(entity.asset_key))


def _has_rejected_asset(entity: ManifestEntity) -> bool:
    if entity.policy_severity is not None:
        return entity.policy_severity in ("reject", "abort")
    # Backward compat: old manifests without policy_severity
    return entity.rejected_sink_uri is not None


def _safe_int(val: Any, default: int = 0) -> int:
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def _count_files_with_rejections(report: dict[str, Any]) -> int:
    files = report.get("files")
    if not isinstance(files, list):
        return 0
    return sum(
        1 for f in files
        if isinstance(f, dict) and _safe_int(f.get("rejected_count")) > 0
    )


def _dominant_rejection_reason(report: dict[str, Any]) -> str | None:
    files = report.get("files")
    if not isinstance(files, list):
        return None
    counts: dict[str, int] = {}
    for f in files:
        if not isinstance(f, dict):
            continue
        validation = f.get("validation")
        rules = validation.get("rules") if isinstance(validation, dict) else None
        if not isinstance(rules, list):
            continue
        for rule in rules:
            if not isinstance(rule, dict):
                continue
            name = rule.get("rule") or ""
            if not isinstance(name, str) or not name:
                continue
            violations = _safe_int(rule.get("violations"))
            if violations > 0:
                counts[name] = counts.get(name, 0) + violations
    if not counts:
        return None
    return max(counts, key=lambda k: counts[k])


def _make_entity_multi_asset(
    *,
    entity: ManifestEntity,
    config_uri: str,
    manifest_path: str,
    manifest_uri: str | None = None,
    runner: Runner,
    execution: ManifestExecution,
    runner_definition: ManifestRunnerDefinition,
):
    accepted_key = list(entity.asset_key)
    source_dep_key = list(entity.asset_key[:-1]) + [entity.asset_key[-1] + "_source"]
    has_rejected = _has_rejected_asset(entity)
    rejected_key = list(entity.asset_key[:-1]) + [entity.asset_key[-1] + "_rejected"]

    asset_name = _op_name(entity)
    group_name = entity.group_name
    entity_name = entity.name

    outs: dict[str, AssetOut] = {
        "accepted": AssetOut(key=AssetKey(accepted_key), is_required=True),
    }
    if has_rejected:
        outs["rejected"] = AssetOut(key=AssetKey(rejected_key), is_required=False)

    @multi_asset(
        name=asset_name,
        outs=outs,
        deps=[AssetKey(source_dep_key)],
        group_name=group_name,
        check_specs=build_asset_check_specs(accepted_key),
        can_subset=False,
        compute_kind="floe",
    )
    def _multi_asset(context):
        run = getattr(context, "run", None)
        run_id = getattr(run, "run_id", None) if run is not None else None
        if run_id is None:
            run_id = getattr(context, "run_id", None)
        dagster_job_name = getattr(run, "job_name", None) if run is not None else None
        result = runner.run_floe_entity(
            config_uri=config_uri,
            manifest_uri=manifest_uri or manifest_path,
            run_id=run_id,
            entity=entity_name,
            log_format=execution.log_format,
            execution=execution,
            runner_definition=runner_definition,
            dagster_job_name=dagster_job_name,
        )

        try:
            events = parse_json_event_lines(result.stdout)
            _log_floe_output(context, result.stdout, events)
            if result.stderr.strip():
                _log_floe_stderr(context, result.stderr)
            finished_event = last_run_finished(events)
            finished = parse_run_finished(
                finished_event,
                summary_uri_field=execution.result_contract.summary_uri_field,
            )
        except ValueError as exc:
            _log_floe_output(context, result.stdout, [])
            if result.stderr.strip():
                _log_floe_stderr(context, result.stderr)
            context.log.warning(f"failed to parse Floe run events: {exc}")
            synthetic_status = result.status or ("success" if result.exit_code == 0 else "failed")
            synthetic_exit_code = result.exit_code
            from .events import FloeRunFinished

            finished = FloeRunFinished(
                run_id=(result.backend_metadata or {}).get("backend_run_id") or (run_id or "unknown"),
                status=synthetic_status,
                exit_code=synthetic_exit_code,
                summary_uri=None,
            )

        summary_uri = finished.summary_uri
        entity_stats: dict[str, Any] = {}
        entity_report_json: dict[str, Any] | None = None
        entity_report_uri: str | None = None

        # Prefer entity_report_uris from the event (richer contract); fall back to
        # loading the run summary and extracting entity_report_file from it.
        entity_report_uri = finished.entity_report_uris.get(entity_name)
        if summary_uri:
            try:
                summary_json = _load_summary_json(summary_uri, config_uri)
                entity_stats = _extract_entity_stats(summary_json, entity_name)
                if not entity_report_uri:
                    entity_report_uri = _as_optional_str(entity_stats.get("entity_report_file"))
                if entity_report_uri:
                    entity_report_json = _load_entity_report_json(entity_report_uri, config_uri)
            except Exception as exc:  # noqa: BLE001
                context.log.warning(f"failed to load run summary/entity report: {exc}")

        metadata: dict[str, Any] = {
            "run_id": finished.run_id,
            "status": finished.status,
            "exit_code": finished.exit_code,
        }
        if summary_uri:
            metadata["summary_uri"] = summary_uri

        metadata.update(entity_stats)
        if entity_report_uri:
            metadata["entity_report_uri"] = entity_report_uri
        if result.failure_reason:
            metadata["failure_reason"] = result.failure_reason
        if result.status:
            metadata["backend_status"] = result.status
        if result.backend_metadata:
            metadata["backend_metadata"] = result.backend_metadata

        check_results = build_asset_check_results(
            asset_key=accepted_key,
            entity_name=entity_name,
            finished=finished,
            entity_report=entity_report_json,
            entity_stats=entity_stats,
            summary_uri=summary_uri,
            entity_report_uri=entity_report_uri,
        )
        for check_result in check_results:
            yield check_result

        if (
            result.exit_code != 0
            or finished.exit_code != 0
            or ((result.status is not None) and (result.status != STATUS_SUCCEEDED))
        ):
            raise Failure(
                description=f"floe run failed for entity {entity_name}",
                metadata=metadata,
            )

        yield Output(value=None, output_name="accepted", metadata=metadata)
        if has_rejected:
            rows = entity_stats.get("rows") or 0
            rejected_count = entity_stats.get("rejected") or 0
            rejection_rate = rejected_count / rows if rows > 0 else 0.0
            rejected_metadata: dict[str, Any] = {
                "run_id": finished.run_id,
                "rejected": MetadataValue.int(rejected_count),
                "status": finished.status,
                "rejection_rate": MetadataValue.float(rejection_rate),
            }
            if entity.rejected_sink_uri:
                rejected_metadata["dagster/uri"] = MetadataValue.url(entity.rejected_sink_uri)
            if entity_report_json:
                files_with_rejections = _count_files_with_rejections(entity_report_json)
                dominant = _dominant_rejection_reason(entity_report_json)
                rejected_metadata["files_with_rejections"] = MetadataValue.int(files_with_rejections)
                if dominant:
                    rejected_metadata["dominant_rejection_reason"] = dominant
            yield Output(value=None, output_name="rejected", metadata=rejected_metadata)

    return _multi_asset


def _log_floe_stderr(context: Any, stderr: str) -> None:
    for line in stderr.splitlines():
        stripped = line.strip()
        if stripped:
            context.log.info(f"floe stderr | {stripped}")


def _log_floe_output(context: Any, stdout: str, events: list[dict[str, Any]]) -> None:
    rendered_by_raw = {
        json.dumps(event, sort_keys=True, separators=(",", ":")): render_log_event(event)
        for event in events
    }
    for line in stdout.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        rendered = None
        if stripped.startswith("{"):
            try:
                event = json.loads(stripped)
            except json.JSONDecodeError:
                event = None
            if isinstance(event, dict):
                rendered = rendered_by_raw.get(
                    json.dumps(event, sort_keys=True, separators=(",", ":")),
                    render_log_event(event),
                )
        context.log.info(rendered or f"floe stdout | {stripped}")


def _load_summary_json(summary_uri: str, config_uri: str) -> dict[str, Any]:
    return _load_json_document(summary_uri, config_uri, label="summary")


def _load_entity_report_json(entity_report_uri: str, config_uri: str) -> dict[str, Any]:
    return _load_json_document(entity_report_uri, config_uri, label="entity report")


def _load_json_document(ref: str, config_uri: str, *, label: str) -> dict[str, Any]:
    data = json.loads(_read_json_text(ref, config_uri))
    if not isinstance(data, dict):
        raise ValueError(f"{label} JSON must be an object")
    return data


def _read_json_text(ref: str, config_uri: str) -> str:
    if ref.startswith("local://"):
        raw_path = unquote(ref[len("local://"):])
        path = Path(raw_path)
        if not path.is_absolute() and "://" not in config_uri:
            path = (Path(config_uri).resolve().parent / path).resolve()
        return path.read_text(encoding="utf-8")
    if ref.startswith("file://"):
        parsed = urlparse(ref)
        return Path(unquote(parsed.path)).read_text(encoding="utf-8")
    if "://" in ref:
        return _fsspec_read_text(ref)
    path = Path(ref)
    if not path.is_absolute() and "://" not in config_uri:
        path = (Path(config_uri).resolve().parent / path).resolve()
    return path.read_text(encoding="utf-8")


def _fsspec_read_text(uri: str) -> str:
    try:
        import fsspec
    except ImportError as exc:
        raise ImportError(
            "reading remote report URIs requires fsspec; "
            "install it with: pip install fsspec"
        ) from exc
    with fsspec.open(uri, "r", encoding="utf-8", **_fsspec_storage_options(uri)) as f:
        return f.read()


def _extract_entity_stats(summary_json: dict[str, Any], entity_name: str) -> dict[str, Any]:
    entities = summary_json.get("entities") or []
    if not isinstance(entities, list):
        return {}
    for item in entities:
        if not isinstance(item, dict):
            continue
        if item.get("name") != entity_name:
            continue
        results = item.get("results") or {}
        if not isinstance(results, dict):
            results = {}
        return {
            "files": results.get("files_total"),
            "rows": results.get("rows_total"),
            "accepted": results.get("accepted_total"),
            "rejected": results.get("rejected_total"),
            "warnings": results.get("warnings_total"),
            "errors": results.get("errors_total"),
            "entity_status": item.get("status"),
            "entity_report_file": item.get("report_file"),
        }
    return {}


def _as_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def _build_source_assets(entity_items: list[ManifestEntity]) -> list[SourceAsset]:
    source_assets = []
    for entity in entity_items:
        source_key = list(entity.asset_key[:-1]) + [entity.asset_key[-1] + "_source"]
        metadata: dict[str, Any] = {"format": entity.source_format}
        if entity.source_uri:
            metadata["dagster/uri"] = MetadataValue.url(entity.source_uri)
        source_assets.append(
            SourceAsset(
                key=AssetKey(source_key),
                group_name=entity.group_name,
                description=f"Raw source files for '{entity.name}' (format: {entity.source_format})",
                metadata=metadata,
            )
        )
    return source_assets
