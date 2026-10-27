"""The public `floe` API, matching `crates/floe-python/python/floe/_floe.pyi` (Rust: `floe-python/src/`)."""

import json
from collections.abc import Callable
from dataclasses import asdict, dataclass, field, replace
from html import escape
from importlib.metadata import version
from pathlib import Path
from typing import Any, cast

__version__ = version(
    "floe-python"
)  # before the imports below: `floe.report.summary` reads it

from floe import run as _run
from floe.config.parse import parse_config
from floe.config.profile import ProfileConfig, apply_profile, parse_profile
from floe.config.project import RootConfig as _Root
from floe.config.validate import validate_config
from floe.config.vars import extract_config_env_vars as _env_vars
from floe.config.vars import resolve_vars
from floe.errors import (
    FloeConfigError,
    FloeError,
    FloeIoError,
    FloeRunError,
    FloeStorageError,
)
from floe.lineage.observer import run_with_lineage
from floe.maintain import FLOOR_DAYS, FLOOR_SNAPSHOTS, ORPHAN_DELAY_DAYS
from floe.maintain import maintain as _maintain
from floe.manifest_run import load_manifest
from floe.report.summary import id_path
from floe.run.entity import state_uri
from floe.run.events import (
    EntityFinishedEvent,
    EntityStartedEvent,
    FileFinishedEvent,
    FileStartedEvent,
    LogEvent,
    RunEvent,
    RunFinishedEvent,
    RunStartedEvent,
    SchemaEvolutionAppliedEvent,
)
from floe.state import dump_state, read_state, reset
from floe.storage.paths import normalize_local_path
from floe.storage.types import StorageClient

_observer: Callable[[RunEvent], None] | None = None


def _debug(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)  # Rust `{:?}` quoting


@dataclass(frozen=True, repr=False)
class EntityConfig:
    name: str
    domain: str | None
    source_format: str
    source_path: str
    source_storage: str | None
    accepted_format: str
    accepted_path: str
    accepted_storage: str | None
    incremental_mode: str

    def __repr__(self) -> str:
        return f"EntityConfig(name={_debug(self.name)}, source_format={_debug(self.source_format)}, accepted_format={_debug(self.accepted_format)})"  # fmt: skip


@dataclass(frozen=True, repr=False)
class RootConfig:
    version: str
    entities: list[EntityConfig]

    @property
    def entity_names(self) -> list[str]:
        return [e.name for e in self.entities]

    def __repr__(self) -> str:
        return f"RootConfig(version={_debug(self.version)}, entities={len(self.entities)})"  # fmt: skip


@dataclass(frozen=True, repr=False)
class RunOutcome:
    run_id: str
    report_base_path: str | None
    dry_run: bool
    summary: dict[str, Any]
    entity_reports: list[dict[str, Any]] = field(default_factory=list[dict[str, Any]])
    dry_run_previews: list[dict[str, Any]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def __repr__(self) -> str:
        return f"RunOutcome(run_id={_debug(self.run_id)}, entities={len(self.entity_reports)}, dry_run={str(self.dry_run).lower()})"  # fmt: skip

    def _repr_html_(self) -> str:
        colors = {"success": "#2e7d32", "success_with_warnings": "#e65100"}
        status = self.summary.get("run", {}).get("status", "unknown")
        totals = self.summary.get("results", {})
        rows = "".join(
            f'<tr><td style="padding:4px 8px">{escape(e["name"])}</td>'
            f'<td style="padding:4px 8px;color:{colors.get(e["status"], "#c62828")};font-weight:bold">{e["status"]}</td>'
            f'<td style="padding:4px 8px;text-align:right">{e["results"]["accepted_total"]}</td>'
            f'<td style="padding:4px 8px;text-align:right">{e["results"]["rejected_total"]}</td></tr>'
            for e in self.summary.get("entities", [])
        )  # fmt: skip
        return (
            '<div style="font-family:monospace;border:1px solid #ddd;border-radius:4px;padding:12px;max-width:600px">'
            '<div style="margin-bottom:8px"><strong>RunOutcome</strong> '
            f'<code style="color:#555">{escape(self.run_id)}</code> '
            f'<span style="color:{colors.get(status, "#c62828")};font-weight:bold;margin-left:8px">{status}</span></div>'
            f'<div style="margin-bottom:8px;color:#555">{totals.get("rows_total", 0)} rows &nbsp;·&nbsp; '
            f'<span style="color:#2e7d32">{totals.get("accepted_total", 0)} accepted</span> &nbsp;·&nbsp; '
            f'<span style="color:#c62828">{totals.get("rejected_total", 0)} rejected</span></div>'
            '<table style="border-collapse:collapse;width:100%"><thead><tr style="border-bottom:1px solid #ddd">'
            '<th style="padding:4px 8px;text-align:left">Entity</th><th style="padding:4px 8px;text-align:left">Status</th>'
            '<th style="padding:4px 8px;text-align:right">Accepted</th><th style="padding:4px 8px;text-align:right">Rejected</th>'
            f"</tr></thead><tbody>{rows}</tbody></table></div>"
        )  # fmt: skip


def _profile(
    path: str | None, variables: dict[str, str] | None
) -> ProfileConfig | None:
    """Rust `load_optional_profile`: `profile_vars` extend the profile's, or stand alone."""
    if path is None:
        return None if variables is None else ProfileConfig("", {}, variables, None, None, None)  # fmt: skip
    try:
        profile = parse_profile(Path(path))
    except FloeError as e:
        raise FloeConfigError(f"failed to load profile {_debug(path)}: {e}") from e
    return ProfileConfig(profile.name, profile.runner, profile.variables | (variables or {}),
                         profile.catalogs, profile.storages, profile.lineage)  # fmt: skip


def _load(config_path: str, profile: ProfileConfig | None = None) -> tuple[Path, _Root]:
    path = Path(config_path).absolute()
    root = parse_config(path, profile.variables if profile else None)
    return path, apply_profile(root, profile) if profile else root


def validate(
    config_path: str,
    entities: list[str] | None = None,
    profile_vars: dict[str, str] | None = None,
    profile_path: str | None = None,
) -> None:
    _, root = _load(config_path, _profile(profile_path, profile_vars))
    validate_config(root)
    _run.select_entities(root, entities or [])


def run(
    config_path: str,
    entities: list[str] | None = None,
    dry_run: bool = False,
    run_id: str | None = None,
    full_refresh: bool = False,
    profile_vars: dict[str, str] | None = None,
    profile_path: str | None = None,
) -> RunOutcome:
    profile = _profile(profile_path, profile_vars)
    if profile:  # Rust `run_with_runtime`: profile vars resolved against the config's env vars first
        profile = replace(profile, variables=resolve_vars(profile.variables, {}, _env_vars(Path(config_path).absolute())))  # fmt: skip
    path, root = _load(config_path, profile)
    validate_config(root)
    out = _run.run(
        path, root, run_id=run_id, entities=entities or [], dry_run=dry_run,
        full_refresh=full_refresh, observer=_emit,
    )  # fmt: skip
    return _outcome(root, entities, out)


def maintain(
    config_path: str,
    entities: list[str] | None = None,
    older_than_days: int = FLOOR_DAYS,
    retain_last: int = FLOOR_SNAPSHOTS,
    orphan_delay_days: int = ORPHAN_DELAY_DAYS,
    dry_run: bool = False,
    profile_vars: dict[str, str] | None = None,
    profile_path: str | None = None,
) -> list[dict[str, Any]]:
    """`floe maintain`: expire Iceberg snapshots behind the retention floor (7 days, 10 snapshots), then remove orphan files."""
    path, root = _load(config_path, _profile(profile_path, profile_vars))
    return _maintain(path, root, entities or [], older_than_days, retain_last, orphan_delay_days, dry_run)  # fmt: skip


def run_manifest(
    manifest_path: str,
    entities: list[str] | None = None,
    dry_run: bool = False,
    run_id: str | None = None,
    full_refresh: bool = False,
) -> RunOutcome:
    """`floe run --manifest`, OpenLineage included; the manifest was validated when it was generated."""
    root, observed = load_manifest(Path(manifest_path))
    out = run_with_lineage(
        Path(manifest_path), root, observed, "", _emit, validate=False, run_id=run_id,
        entities=entities or [], dry_run=dry_run, full_refresh=full_refresh,
    )  # fmt: skip
    return _outcome(root, entities, out)


def _emit(event: dict[str, Any]) -> None:
    if _observer:
        _observer(cast(RunEvent, event))


def _outcome(
    root: _Root, entities: list[str] | None, out: _run.RunOutcome
) -> RunOutcome:
    base = out.summary["report"]["path"]
    if out.dry_run is None:
        return RunOutcome(out.run_id, base, False, out.summary, out.entity_reports)
    previews = [
        {
            "name": e.id, "input_path": e.source.path, "input_format": e.source.format,
            "accepted_path": e.sink.accepted.path, "accepted_format": e.sink.accepted.format,
            "rejected_path": e.sink.rejected and e.sink.rejected.path,
            "rejected_format": e.sink.rejected and e.sink.rejected.format,
            "archive_path": e.sink.archive.path if e.sink.archive else "",
            "archive_storage": e.sink.archive and e.sink.archive.storage,
            "report_file": f"{base.rstrip('/')}/run_{out.run_id}/{id_path(e)}/run.json",
            "scanned_files": out.dry_run[e.id],
        }
        for e in _run.select_entities(root, entities or [])
    ]  # fmt: skip
    return RunOutcome(out.run_id, base, True, out.summary, [], previews)


def load_config(config_path: str) -> RootConfig:
    _, root = _load(config_path)
    return RootConfig(root.version, [
        EntityConfig(e.name, e.domain, e.source.format, e.source.path, e.source.storage,
                     e.sink.accepted.format, e.sink.accepted.path, e.sink.accepted.storage,
                     e.incremental_mode)
        for e in root.entities
    ])  # fmt: skip


def extract_config_env_vars(config_path: str) -> dict[str, str]:
    return _env_vars(Path(config_path))


def _state(config_path: str, entity_name: str) -> tuple[str, str, str, StorageClient]:
    path, root = _load(config_path)
    [entity] = _run.select_entities(root, [entity_name])
    uri, client = state_uri(entity, normalize_local_path(path.parent), root.storages)
    return entity.name, entity.incremental_mode, uri, client


def inspect_entity_state(config_path: str, entity_name: str) -> dict[str, Any]:
    name, mode, uri, client = _state(config_path, entity_name)
    state = read_state(client, uri, name)
    return {
        "entity_name": name,
        "incremental_mode": mode,
        "path_uri": uri,
        "path_local": uri.removeprefix("local://")
        if uri.startswith("local://")
        else None,
        "state": json.loads(dump_state(state)) if state else None,
    }


def reset_entity_state(config_path: str, entity_name: str) -> bool:
    name, _, uri, client = _state(config_path, entity_name)
    return reset(client, uri, name)


def set_observer(callback: Callable[[RunEvent], None]) -> bool:
    global _observer
    if not callable(callback):
        raise TypeError("observer callback must be callable")
    _observer = callback
    return True


def clear_observer() -> None:
    global _observer
    _observer = None


__all__ = [
    "EntityConfig", "EntityFinishedEvent", "EntityStartedEvent", "FileFinishedEvent", "FileStartedEvent",
    "FloeConfigError", "FloeError", "FloeIoError", "FloeRunError", "FloeStorageError", "LogEvent", "RootConfig",
    "RunEvent", "RunFinishedEvent", "RunOutcome", "RunStartedEvent", "SchemaEvolutionAppliedEvent", "__version__",
    "clear_observer", "extract_config_env_vars", "inspect_entity_state", "load_config", "maintain", "reset_entity_state", "run",
    "run_manifest", "set_observer", "validate",
]  # fmt: skip
