"""A run: entity selection, run context, sequential or parallel entities, dry run, full refresh, events (Rust: `run/mod.rs`, `run/context.rs`)."""

import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from floe.checks.rows import Formatter
from floe.config.project import RootConfig
from floe.config.types import EntityConfig
from floe.errors import FloeConfigError, FloeError
from floe.iceberg.catalog import clear_catalogs
from floe.report.summary import (
    build_summary,
    id_path,
    run_dir,
    run_domain,
    run_outcome,
    write_entity_report,
    write_reports,
)
from floe.run.entity import EntityRun, list_source, resolve_source, run_entity
from floe.run.events import Emitter, Observer
from floe.storage.inputs import ResolvedInputs
from floe.storage.paths import normalize_local_path
from floe.storage.resolve import clear_storage_clients, resolve_path, storage_client


@dataclass(frozen=True)
class RunOutcome:
    run_id: str
    summary: dict[str, Any]
    dry_run: dict[str, list[str]] | None  # entity id -> scanned files
    report_base_path: str
    run_dir: str  # D37: `<report>/<domain>/run_<id>` for a single-domain run
    entity_reports: list[dict[str, Any]]


def select_entities(root: RootConfig, selectors: Sequence[str]) -> list[EntityConfig]:
    """Config order; a selector is an entity id, else a name (Rust `select_entities`)."""
    if not selectors:
        return list(root.entities)
    picked: set[int] = set()
    missing: list[str] = []
    for sel in selectors:
        hits = [i for i, e in enumerate(root.entities) if e.id == sel] or [
            i for i, e in enumerate(root.entities) if e.name == sel
        ]
        if len(hits) > 1:
            ids = ", ".join(root.entities[i].id for i in hits)
            raise FloeConfigError(f"entity {sel} is ambiguous; use one of: {ids}")
        if hits:
            picked.add(hits[0])
        else:
            missing.append(sel)
    if missing:
        raise FloeConfigError(f"entities not found: {', '.join(missing)}")
    return [e for i, e in enumerate(root.entities) if i in picked]


def domain_entities(root: RootConfig, domains: Sequence[str]) -> list[str]:
    """Ids of the entities of these domains, as `select_entities` selectors."""
    missing = [d for d in dict.fromkeys(domains) if all(e.domain != d for e in root.entities)]  # fmt: skip
    if missing:
        raise FloeConfigError(f"domains not found: {', '.join(missing)}")
    return [e.id for e in root.entities if e.domain in domains]


def _rfc3339() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _refresh(entity: EntityConfig) -> EntityConfig:
    """Full refresh: an append sink overwrites; `merge_scd1` overwrites in `run_entity`, which keeps its merge checks."""
    mode = entity.sink.write_mode
    if mode == "merge_scd2":
        raise FloeConfigError(f"entity '{entity.name}': --full-refresh is not supported with write_mode '{mode}': its version history cannot be rebuilt from the inputs")  # fmt: skip
    if mode != "append":
        return entity
    return entity.model_copy(update={"sink": entity.sink.model_copy(update={"write_mode": "overwrite"})})  # fmt: skip


def _finished(emit: Emitter, name: str, report: dict[str, Any] | None) -> None:
    """`entity_finished`; a failed entity (no report) counts zeros."""
    keys = ("files", "files_skipped", "rows", "accepted", "rejected", "warnings", "errors")  # fmt: skip
    results: dict[str, int] = report["results"] if report else {}
    totals = {k: results.get(k if k == "files_skipped" else f"{k}_total", 0) for k in keys}  # fmt: skip
    status = "failed"
    if report:
        status = run_outcome([f["status"] for f in report["files"]], totals["warnings"])[0]  # fmt: skip
    emit("entity_finished", name=name, status=status, **totals)


def run(
    path: Path,
    root: RootConfig,
    *,
    run_id: str | None = None,
    entities: Sequence[str] = (),
    dry_run: bool = False,
    full_refresh: bool = False,
    observer: Observer | None = None,
    config_uri: str | None = None,
) -> RunOutcome:
    """Run the selected entities in config order, writing each entity's report as it finishes. A failing entity raises: no summary, as Rust."""
    clear_storage_clients()
    started, timer = _rfc3339(), time.monotonic()
    clear_catalogs()
    selected = select_entities(root, entities)
    if full_refresh:
        selected = [_refresh(e) for e in selected]
    # a symlinked manifest or config: paths are relative to its target
    config_dir = normalize_local_path(path.resolve().parent)
    # a remote manifest is reported as typed
    config = config_uri or str(normalize_local_path(path))
    report_uri, report_storage = resolve_path(root.storages, config_dir, root.report.storage, root.report.path, "report.storage")  # fmt: skip
    report_path = report_uri.removeprefix("local://")
    run_id = run_id or started.replace(":", "-")
    domain = run_domain(selected)
    run_path = run_dir(report_path, run_id, domain)

    def inputs(e: EntityConfig) -> ResolvedInputs:
        return list_source(
            e, config_dir, root.storages, *resolve_source(e, config_dir, root.storages), full_refresh
        )  # fmt: skip

    if dry_run:  # Rust: no events, no reports; the inputs are only listed
        scanned = {e.id: [f.source_uri for f in inputs(e).files] for e in selected}
        summary = build_summary(
            root.version, [], run_id=run_id, started_at=started, finished_at=_rfc3339(),
            duration_ms=0, config_path=config, report_path=report_path,
            metadata=root.metadata, domain=domain,
        )  # fmt: skip
        summary["report"]["report_file"] = "disabled (dry-run)"
        return RunOutcome(run_id, summary, scanned, report_path, run_path, [])
    emit = Emitter(observer, run_id)
    formatter = cast(
        Formatter, root.report.formatter or "json"
    )  # validated with the config

    def one(entity: EntityConfig) -> EntityRun:
        emit("entity_started", name=entity.id)
        try:
            return run_entity(
                entity, config_dir, root.version, formatter, root.storages, emit, full_refresh,
                root.catalogs, listed.get(entity.id),
            )  # fmt: skip
        except FloeError:
            _finished(emit, entity.id, None)
            raise

    emit("run_started", config=config, report_base=report_path)
    # Rust resolves every entity's inputs before the first entity_started (#864); run_entity reuses them
    listed: dict[str, ResolvedInputs] = {}
    for e in selected:
        # D24: a full refresh may rebuild from the archive alone, so run_entity decides
        archive = full_refresh and e.sink.archive and e.incremental_mode in ("none", "archive")  # fmt: skip
        if not archive:
            listed[e.id] = inputs(e)
    reports: list[tuple[str, dict[str, Any]]] = []
    client = storage_client(report_storage)
    # D36: a policy abort fails only its entity; a FloeError still stops the run
    for e in selected:
        report = one(e).report
        reports.append((id_path(e), report))
        write_entity_report(run_path, id_path(e), report, client)
        _finished(emit, e.id, report)
    summary = build_summary(
        root.version, reports,
        run_id=run_id, started_at=started, finished_at=_rfc3339(),
        duration_ms=int((time.monotonic() - timer) * 1000),
        config_path=config, report_path=report_path,
        metadata=root.metadata, domain=domain,
    )  # fmt: skip
    write_reports(summary, [], client)
    r, t = summary["run"], summary["results"]
    emit("run_finished", status=r["status"], exit_code=r["exit_code"], files=t["files_total"],
         files_skipped=t["files_skipped"], rows=t["rows_total"], accepted=t["accepted_total"], rejected=t["rejected_total"],
         warnings=t["warnings_total"], errors=t["errors_total"],
         summary_uri=summary["report"]["report_file"], report_base=report_path,
         entity_report_uris={e["name"]: e["report_file"] for e in summary["entities"]})  # fmt: skip
    return RunOutcome(
        run_id, summary, None, report_path, run_path, [r for _, r in reports]
    )
