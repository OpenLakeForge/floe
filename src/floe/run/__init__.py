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
from floe.report.summary import (
    build_summary,
    id_path,
    run_outcome,
    write_entity_report,
    write_reports,
)
from floe.run.entity import EntityRun, run_entity
from floe.run.events import Emitter, Observer
from floe.storage.inputs import resolve_inputs
from floe.storage.local import LocalClient
from floe.storage.paths import normalize_local_path
from floe.storage.resolve import require_local


@dataclass(frozen=True)
class RunOutcome:
    run_id: str
    summary: dict[str, Any]
    dry_run: dict[str, list[str]] | None  # entity id -> scanned files


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


def _rfc3339() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _refresh(entity: EntityConfig) -> EntityConfig:
    """Full refresh: an append sink overwrites; merge sinks cannot refresh."""
    mode = entity.sink.write_mode
    if mode in ("merge_scd1", "merge_scd2"):
        raise FloeConfigError(f"entity '{entity.name}': --full-refresh is not supported with write_mode '{mode}'")  # fmt: skip
    if mode != "append":
        return entity
    return entity.model_copy(update={"sink": entity.sink.model_copy(update={"write_mode": "overwrite"})})  # fmt: skip


def _finished(emit: Emitter, name: str, report: dict[str, Any] | None) -> None:
    """`entity_finished`; a failed entity (no report) counts zeros."""
    keys = ("files", "rows", "accepted", "rejected", "warnings", "errors")
    totals = {k: report["results"][f"{k}_total"] if report else 0 for k in keys}
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
) -> RunOutcome:
    """Run the selected entities in config order, writing each entity's report as it finishes. A failing entity raises: no summary, as Rust."""
    started, timer = _rfc3339(), time.monotonic()
    selected = select_entities(root, entities)
    if full_refresh:
        selected = [_refresh(e) for e in selected]
    config_dir = normalize_local_path(path.parent)
    require_local(root.storages, root.report.storage, "report.storage")
    run_id = run_id or started.replace(":", "-")
    report_path = str(normalize_local_path(config_dir / root.report.path))
    if dry_run:  # Rust: no events, no reports; the inputs are only listed
        client = LocalClient()
        scanned = {
            e.id: [f.source_uri for f in resolve_inputs(
                config_dir, e.source.path, e.source.format, e.name, e.source.storage or "local",
                client, recursive=bool(e.source.options.recursive),
                glob_override=e.source.options.glob).files]
            for e in selected
        }  # fmt: skip
        return RunOutcome(run_id, {}, scanned)
    emit = Emitter(observer, run_id)
    formatter = cast(
        Formatter, root.report.formatter or "json"
    )  # validated with the config

    def one(entity: EntityConfig) -> EntityRun:
        emit("entity_started", name=entity.id)
        try:
            return run_entity(
                entity, config_dir, root.version, formatter, root.storages, emit, full_refresh
            )  # fmt: skip
        except FloeError:
            _finished(emit, entity.id, None)
            raise

    emit("run_started", config=str(normalize_local_path(path)), report_base=report_path)
    reports: list[tuple[str, dict[str, Any]]] = []
    run_dir = f"{report_path.rstrip('/')}/run_{run_id}"
    for e in selected:  # stops at the first abort
        result = one(e)
        reports.append((id_path(e), result.report))
        write_entity_report(run_dir, id_path(e), result.report)
        _finished(emit, e.id, result.report)
        if result.aborted:
            break
    summary = build_summary(
        root.version, reports,
        run_id=run_id, started_at=started, finished_at=_rfc3339(),
        duration_ms=int((time.monotonic() - timer) * 1000),
        config_path=str(normalize_local_path(path)), report_path=report_path,
        metadata=root.metadata,
    )  # fmt: skip
    write_reports(summary, [])
    r, t = summary["run"], summary["results"]
    emit("run_finished", status=r["status"], exit_code=r["exit_code"], files=t["files_total"],
         rows=t["rows_total"], accepted=t["accepted_total"], rejected=t["rejected_total"],
         warnings=t["warnings_total"], errors=t["errors_total"],
         summary_uri=summary["report"]["report_file"])  # fmt: skip
    return RunOutcome(run_id, summary, None)
