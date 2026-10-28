from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from collections.abc import Callable
from dataclasses import replace
from typing import Any
from urllib.parse import unquote, urlparse

from dagster import (
    AssetCheckKey,
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
from .events import FloeRunFinished, last_run_finished, parse_json_event_lines, parse_run_finished, render_log_event
from .manifest import (
    DagsterManifest,
    ManifestExecution,
    ManifestEntity,
    ManifestRunnerDefinition,
    _boto3_read_text,
    _fsspec_storage_options,
    entity_table,
    load_manifest_source,
    resolve_entity_runner,
)
from .runner import RunResult, Runner

# Advisory Dagster tag (#924): entities writing one Iceberg table share its value, so a
# `tag_concurrency_limits` entry on this key keeps their runs apart.
TABLE_TAG = "floe/table"

SourceKeyFn = Callable[[ManifestEntity], AssetKey | None]


def load_floe_assets(
    manifest_path: str | None = None,
    runner: Runner | None = None,
    entities: list[str] | None = None,
    register_source_assets: bool = True,
    manifest_uri: str | None = None,
    domains: list[str] | None = None,
    config_path: str | None = None,
    profile_path: str | None = None,
    manifest_name: str | None = None,
    source_key_fn: SourceKeyFn | None = None,
) -> Definitions:
    """Build a Dagster Definitions for the given manifest.

    config_path/profile_path: in place of manifest_path, build the manifest in memory from the
        Floe config (needs the `floe` extra); runs then take `floe run -c <config> [-p <profile>]`.
    manifest_name: config mode only; the manifest's stable name. Without it the name of a domain-scope op
        derives from the config's path and checksum, so every config edit renames the op.

    register_source_assets: if False, skip SourceAsset registration for source keys.
        Set to False when another Dagster pipeline in the same workspace already
        materialises those keys to avoid asset key conflicts.

    manifest_uri: optional runtime URI passed as {manifest_uri} into the execution
        args for kubernetes_job and databricks_job runners. When omitted, manifest_path
        is used. Set this when the Dagster code server loads the manifest from a local
        path but the runner pod must reference it via object storage (e.g. s3://).

    source_key_fn: optional `(entity) -> AssetKey | None` choosing each entity's upstream source
        key; None keeps the default. The key is wired like the default one (a dep of the entity's
        asset), and no SourceAsset is registered for it: the caller owns that asset.
    """
    assets_defs, source_assets, _selected_entities = build_floe_asset_defs(
        manifest_path=manifest_path,
        runner=runner,
        entities=entities,
        register_source_assets=register_source_assets,
        manifest_uri=manifest_uri,
        domains=domains,
        config_path=config_path,
        profile_path=profile_path,
        manifest_name=manifest_name,
        source_key_fn=source_key_fn,
    )
    return Definitions(assets=[*assets_defs, *source_assets])


def build_floe_asset_defs(
    manifest_path: str | None = None,
    runner: Runner | None = None,
    entities: list[str] | None = None,
    register_source_assets: bool = True,
    manifest_uri: str | None = None,
    domains: list[str] | None = None,
    config_path: str | None = None,
    profile_path: str | None = None,
    manifest_name: str | None = None,
    source_key_fn: SourceKeyFn | None = None,
) -> tuple[list[Any], list[SourceAsset], list[ManifestEntity]]:
    if runner is None:
        raise TypeError("runner is required")
    manifest, config_uri = load_manifest_source(manifest_path, config_path, profile_path, manifest_name)
    # in config mode it names the source in errors; no `{manifest_uri}` token is left to render
    manifest_path = manifest_path or config_path
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

    scope = _source_scope(manifest)
    chosen = {e.name: key for e in entity_items if source_key_fn and (key := source_key_fn(e)) is not None}
    source_keys = {
        e.name: list(chosen[e.name].path) if e.name in chosen else _source_key(e, manifest.sources, scope)
        for e in entity_items
    }
    assets_defs = []
    domain_groups: dict[tuple[str, str], list[ManifestEntity]] = {}
    for entity in entity_items:
        runner_definition = resolve_entity_runner(manifest, entity)
        if runner_definition.runner_type == "kubernetes_job" and runner_definition.job_scope == "domain":
            # one Job per (domain, runner): an entity with its own runner gets its own Job
            group = (entity.domain or "default", entity.runner or manifest.runners.default)
            domain_groups.setdefault(group, []).append(entity)
            continue
        assets_defs.append(
            _make_entity_multi_asset(
                entity=entity,
                config_uri=config_uri,
                manifest_path=manifest_path,
                manifest_uri=manifest_uri,
                runner=runner,
                execution=manifest.execution,
                runner_definition=runner_definition,
                source_key=source_keys[entity.name],
                table=entity_table(manifest, entity),
            )
        )
    for (domain, runner_name), members in domain_groups.items():
        assets_defs.append(
            _make_domain_multi_asset(
                scope=_source_scope(manifest),  # the multi-manifest loader needs distinct op names
                domain=domain,
                runner_name=runner_name,
                entities=members,
                domain_names={e.name for e in manifest.entities if e.domain == domain},
                source_keys={e.name: source_keys[e.name] for e in members},
                config_uri=config_uri,
                manifest_path=manifest_path,
                manifest_uri=manifest_uri,
                runner=runner,
                execution=manifest.execution,
                runner_definition=manifest.runners.definitions[runner_name],
            )
        )

    chosen_keys = set(chosen.values())  # theirs, whichever entity would have generated them
    source_assets = (
        [
            sa
            for sa in _build_source_assets([e for e in entity_items if e.name not in chosen], manifest.sources, scope)
            if sa.key not in chosen_keys
        ]
        if register_source_assets
        else []
    )
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
    source_key: list[str],
    table: str | None = None,
):
    accepted_key = list(entity.asset_key)
    has_rejected = _has_rejected_asset(entity)
    rejected_key = _rejected_key(entity)

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
        deps=[AssetKey(source_key)],
        group_name=group_name,
        check_specs=build_asset_check_specs(accepted_key),
        can_subset=False,
        compute_kind="floe",
        op_tags={TABLE_TAG: table} if table else None,
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

        finished = _parse_finished(context, result, execution, run_id)
        summary = _load_run_summary(context, finished, config_uri)
        failed = (
            result.exit_code != 0
            or finished.exit_code != 0
            or ((result.status is not None) and (result.status != STATUS_SUCCEEDED))
        )
        failure = yield from _entity_events(
            context,
            entity=entity,
            config_uri=config_uri,
            finished=finished,
            result=result,
            summary=summary,
            failed=failed,
        )
        if failure is not None:
            raise failure

    return _multi_asset


def _parse_finished(context: Any, result: RunResult, execution: ManifestExecution, run_id: str | None):
    try:
        events = parse_json_event_lines(result.stdout)
        _log_floe_output(context, result.stdout, events)
        if result.stderr.strip():
            _log_floe_stderr(context, result.stderr)
        finished_event = last_run_finished(events)
        return parse_run_finished(
            finished_event,
            summary_uri_field=execution.result_contract.summary_uri_field,
        )
    except ValueError as exc:
        _log_floe_output(context, result.stdout, [])
        if result.stderr.strip():
            _log_floe_stderr(context, result.stderr)
        context.log.warning(f"failed to parse Floe run events: {exc}")
        synthetic_status = result.status or ("success" if result.exit_code == 0 else "failed")
        return FloeRunFinished(
            run_id=(result.backend_metadata or {}).get("backend_run_id") or (run_id or "unknown"),
            status=synthetic_status,
            exit_code=result.exit_code,
            summary_uri=None,
        )


def _load_run_summary(context: Any, finished: FloeRunFinished, config_uri: str) -> dict[str, Any] | None:
    if not finished.summary_uri:
        return None
    try:
        return _load_summary_json(finished.summary_uri, config_uri)
    except Exception as exc:  # noqa: BLE001
        context.log.warning(f"failed to load run summary/entity report: {exc}")
        return None


def _entity_events(
    context: Any,
    *,
    entity: ManifestEntity,
    config_uri: str,
    finished: FloeRunFinished,
    result: RunResult,
    summary: dict[str, Any] | None,
    failed: bool,
    out_prefix: str = "",
    accepted_selected: bool = True,
    rejected_selected: bool = True,
    check_status: str | None = None,
    selected_checks: set[AssetCheckKey] | None = None,
):
    """Yield one entity's checks and outputs from a finished run; return its `Failure` (not raised)."""
    accepted_key = list(entity.asset_key)
    entity_name = entity.name
    summary_uri = finished.summary_uri
    entity_stats: dict[str, Any] = {}
    entity_report_json: dict[str, Any] | None = None

    # Prefer entity_report_uris from the event (richer contract); fall back to the
    # entity_report_file in the run summary.
    entity_report_uri = finished.entity_report_uris.get(entity_name)
    if summary is not None:
        entity_stats = _extract_entity_stats(summary, entity_name)
        if not entity_report_uri:
            entity_report_uri = _as_optional_str(entity_stats.get("entity_report_file"))
        if entity_report_uri:
            try:
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
        finished=replace(finished, status=check_status) if check_status else finished,
        entity_report=entity_report_json,
        entity_stats=entity_stats,
        summary_uri=summary_uri,
        entity_report_uri=entity_report_uri,
    )
    for check_result in check_results:
        if selected_checks is None or AssetCheckKey(AssetKey(accepted_key), check_result.check_name) in selected_checks:
            yield check_result

    if failed:
        return Failure(description=f"floe run failed for entity {entity_name}", metadata=metadata)

    if not accepted_selected:
        return None
    yield Output(value=None, output_name=f"{out_prefix}accepted", metadata=metadata)
    if _has_rejected_asset(entity) and rejected_selected:
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
        yield Output(value=None, output_name=f"{out_prefix}rejected", metadata=rejected_metadata)
    return None


def _rejected_key(entity: ManifestEntity) -> list[str]:
    # A `rejected` segment, not a suffix: `orders_rejected` may be a real table next to `orders`.
    return [*entity.asset_key[:-1], "rejected", entity.asset_key[-1]]


# Each entity is reported from its own summary status: `rejected` and `success_with_warnings` entities
# are materialized, `aborted` / `failed` ones fail, and one absent from the summary never ran. Until
# #959 lets a policy abort fail only its entity, a run stops at the first abort, so the entities after
# it are "not run" (not failed); the asset code needs no change once #959 lands.
_ENTITY_FAILED = ("aborted", "failed")


def _domain_op_name(scope: str, domain: str, runner_name: str) -> str:
    # sanitizing alone maps `k8s-big` and `k8s_big` to one name; the digest keeps every group distinct
    digest = hashlib.sha1(f"{scope}\0{domain}\0{runner_name}".encode()).hexdigest()[:8]
    return re.sub(r"[^A-Za-z0-9_]", "_", f"floe_{scope}__{domain}__{runner_name}") + f"_{digest}"


def _make_domain_multi_asset(
    *,
    scope: str,
    domain: str,
    runner_name: str,
    entities: list[ManifestEntity],
    domain_names: set[str],
    source_keys: dict[str, list[str]],
    config_uri: str,
    manifest_path: str,
    manifest_uri: str | None,
    runner: Runner,
    execution: ManifestExecution,
    runner_definition: ManifestRunnerDefinition,
):
    """`job_scope: domain`: one subsettable multi-asset, so one Job runs the selected entities in order."""
    outs: dict[str, AssetOut] = {}
    check_specs = []
    for entity in entities:
        op = _op_name(entity)
        # optional: a subsettable asset yields only its selected outputs, and fails the step itself
        outs[f"{op}__accepted"] = AssetOut(
            key=AssetKey(entity.asset_key), group_name=entity.group_name, is_required=False
        )
        if _has_rejected_asset(entity):
            outs[f"{op}__rejected"] = AssetOut(
                key=AssetKey(_rejected_key(entity)), group_name=entity.group_name, is_required=False
            )
        check_specs.extend(build_asset_check_specs(list(entity.asset_key)))
    # each output keeps its own entity's source, so selecting one source's downstream runs one entity
    internal_deps = {
        name: {AssetKey(source_keys[entity.name])}
        for entity in entities
        for name in (f"{_op_name(entity)}__accepted", f"{_op_name(entity)}__rejected")
        if name in outs
    }

    # ponytail: no `floe/table` op tag here, an op spans several tables; the entities of one Job run
    # one after another, so only runs of different Jobs on one table can still overlap (#924).
    @multi_asset(
        name=_domain_op_name(scope, domain, runner_name),
        outs=outs,
        deps=[AssetKey(key) for key in {tuple(k) for k in source_keys.values()}],
        internal_asset_deps=internal_deps,
        check_specs=check_specs,
        can_subset=True,
        compute_kind="floe",
    )
    def _domain_asset(context):
        selected = context.selected_asset_keys
        selected_checks = set(context.selected_asset_check_keys)
        chosen = [  # manifest order; a checks-only selection still runs the entity
            entity
            for entity in entities
            if AssetKey(entity.asset_key) in selected
            or any(k.asset_key == AssetKey(entity.asset_key) for k in selected_checks)
        ]
        run = getattr(context, "run", None)
        run_id = getattr(run, "run_id", None) if run is not None else None
        # concurrent groups of one Dagster run must not share a Floe run id (it names the summary file)
        floe_run_id = f"{run_id}_{context.op_def.name}" if run_id else None
        names = [entity.name for entity in chosen]
        # the whole domain of the manifest: `floe run --domain` says it; a custom template stays as written
        whole_domain = bool(names) and set(names) == domain_names and execution.per_entity_args == [
            "--entities",
            "{entity_name}",
        ]
        result = runner.run_floe_entities(
            config_uri=config_uri,
            manifest_uri=manifest_uri or manifest_path,
            run_id=floe_run_id,
            domain=domain,
            entities=names,
            execution=replace(execution, per_entity_args=["--domain", domain]) if whole_domain else execution,
            runner_definition=runner_definition,
        )
        finished = _parse_finished(context, result, execution, run_id)
        summary = _load_run_summary(context, finished, config_uri)
        run_failed = (
            result.exit_code != 0
            or finished.exit_code != 0
            or ((result.status is not None) and (result.status != STATUS_SUCCEEDED))
        )
        failures: list[str] = []
        not_run: list[str] = []
        for entity in chosen:
            status = None
            if summary is None:
                failed = run_failed
            else:
                status = _extract_entity_stats(summary, entity.name).get("entity_status")
                if status is None:
                    not_run.append(entity.name)
                    continue
                failed = status in _ENTITY_FAILED
            failure = yield from _entity_events(
                context,
                entity=entity,
                config_uri=config_uri,
                finished=finished,
                result=result,
                summary=summary,
                failed=failed,
                out_prefix=f"{_op_name(entity)}__",
                accepted_selected=AssetKey(entity.asset_key) in selected,
                rejected_selected=AssetKey(_rejected_key(entity)) in selected,
                check_status=status,  # the entity's own outcome, not the run's (an aborted later entity)
                selected_checks=selected_checks,
            )
            if failure is not None:
                failures.append(entity.name)
        # entity outcomes are kept, but a failed backend (or any non-zero run) still fails the step
        if failures or not_run or run_failed:
            if not_run:
                context.log.warning(f"floe did not run entities {not_run}")
            raise Failure(
                description=f"floe run failed for entities {', '.join(failures) or 'none'}"
                + (f"; not run: {', '.join(not_run)}" if not_run else ""),
                metadata={"failed_entities": failures, "not_run_entities": not_run},
            )

    return _domain_asset



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
    text = _boto3_read_text(uri)
    if text is not None:
        return text
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


def _source_scope(manifest: DagsterManifest) -> str:
    """The project a named source belongs to, so two manifests can both name a source `crm`."""
    return manifest.manifest_name or manifest.manifest_id


def _source_key(
    entity: ManifestEntity, sources: dict[str, dict[str, Any]] | None, scope: str
) -> list[str]:
    """D20: a source named in the manifest `sources` index is one asset shared by its entities."""
    if sources and entity.source_ref in sources:
        return ["sources", scope, entity.source_ref]
    return list(entity.asset_key[:-1]) + [entity.asset_key[-1] + "_source"]


def _build_source_assets(
    entity_items: list[ManifestEntity],
    sources: dict[str, dict[str, Any]] | None = None,
    scope: str = "",
) -> list[SourceAsset]:
    source_assets = []
    named_seen: set[str] = set()
    for entity in entity_items:
        source_key = _source_key(entity, sources, scope)
        if sources and entity.source_ref in sources:
            if entity.source_ref not in named_seen:
                named_seen.add(entity.source_ref)
                named = sources[entity.source_ref]
                source_assets.append(
                    SourceAsset(
                        key=AssetKey(source_key),
                        group_name="sources",
                        description=f"Named source '{named['name']}' (format: {named['format']})",
                        metadata={"format": named["format"]}
                        | ({"dagster/uri": MetadataValue.url(named["uri"])} if named["uri"] else {}),
                    )
                )
            continue
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
