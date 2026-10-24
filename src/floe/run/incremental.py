"""Incremental `file` mode around one entity run (Rust: `run/entity/incremental.rs`, `resolve_entity_state_path`)."""

import threading
from contextlib import suppress
from pathlib import Path
from typing import Any

from floe.config.types import EntityConfig
from floe.errors import FloeConfigError
from floe.report.summary import run_outcome
from floe.run.events import Emitter
from floe.state import (
    CLAIM_TTL_SECONDS,
    FileState,
    Loaded,
    claim_inputs,
    promote,
    release,
    renew,
)
from floe.storage.inputs import InputFile, suffixes_for_format
from floe.storage.local import LocalClient
from floe.storage.paths import normalize_local_path
from floe.storage.types import StorageClient

RENEW_INTERVAL_S = float(min(max(CLAIM_TTL_SECONDS // 3, 1), 300))
_SEPARATORS = "/\\"


def _parent(value: str) -> str | None:
    i = max(value.rfind(s) for s in _SEPARATORS)
    return None if i < 0 else value[: max(i, 1)]


def _source_root(raw: str, fmt: str, resolved: Path) -> str:
    """Rust `derive_source_root`: the directory the default state lives under."""
    trimmed = raw.rstrip(_SEPARATORS)
    if not trimmed:
        return ""
    cut = next((i for i, c in enumerate(trimmed) if c in "*?["), -1)
    if cut >= 0:
        prefix = trimmed[:cut]
        done = prefix if not prefix or prefix[-1] in _SEPARATORS else _parent(prefix) or prefix  # fmt: skip
        return done.rstrip(_SEPARATORS)
    if resolved.is_dir():
        return trimmed
    if resolved.is_file() or trimmed.lower().endswith(tuple(suffixes_for_format(fmt))):
        return (_parent(trimmed) or trimmed).rstrip(_SEPARATORS)
    return trimmed


def state_uri(entity: EntityConfig, config_dir: Path) -> str:
    """`state.path` if set, else `<source root>/.floe/state/<domain>/<name>/state.json`; local only."""
    custom = entity.state and entity.state.path
    if custom:
        if "://" in custom:
            raise FloeConfigError(f"entity.name={entity.name} entity.state.path must be a local path (got {custom})")  # fmt: skip
        return f"local://{normalize_local_path(config_dir / custom)}"
    src = entity.source
    root = _source_root(
        src.path, src.format, normalize_local_path(config_dir / src.path)
    )
    ident = f"{entity.domain}/{entity.name}" if entity.domain and entity.id != entity.name else entity.name  # fmt: skip
    tail = f".floe/state/{ident}/state.json"
    return f"local://{normalize_local_path(config_dir / (f'{root}/{tail}' if root and root != '.' else tail))}"  # fmt: skip


def skipped_report(uri: str, reason: str, warning: str | None = None) -> dict[str, Any]:
    mismatch: dict[str, Any] = {
        "declared_columns_count": 0,
        "input_columns_count": 0,
        "missing_columns": [],
        "extra_columns": [],
        "mismatch_action": "none",
    }
    if warning:
        mismatch["warning"] = warning
    return {
        "input_file": uri,
        "status": "skipped",
        "row_count": 0,
        "accepted_count": 0,
        "rejected_count": 0,
        "skip_reason": reason,
        "mismatch": mismatch,
        "output": {
            "accepted_path": None,
            "rejected_path": None,
            "errors_path": None,
            "archived_path": None,
        },
        "validation": {"errors": 0, "warnings": int(bool(warning)), "rules": []},
    }


def skipped_reports(
    entity: EntityConfig,
    already: list[tuple[InputFile, FileState]],
    active: list[str],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for f, rec in already:
        if (rec.size, rec.mtime) == (f.source_size, f.source_mtime):
            out.append(skipped_report(f.source_uri, "already_ingested"))
            continue
        msg = (
            f"entity.name={entity.name} incremental_mode=file skipping previously ingested file with changed metadata: {f.source_uri} "
            f"(recorded size={rec.size}, mtime={rec.mtime}; current size={f.source_size}, mtime={f.source_mtime})"
        )  # fmt: skip
        out.append(skipped_report(f.source_uri, "already_ingested_changed", msg))
    out += [
        skipped_report(
            u, "active_claim",
            f"entity.name={entity.name} incremental_mode=file skipping file claimed by another active run: {u}",
        )
        for u in active
    ]  # fmt: skip
    return out


class Incremental:
    """Claims the entity's inputs, renews the claims while it runs, then promotes or releases them."""

    def __init__(self, entity: EntityConfig, config_dir: Path, run_id: str) -> None:
        self.entity, self.config_dir, self.run_id = entity, config_dir, run_id
        self.client: StorageClient = LocalClient()
        self.uri = ""
        self.claimed: Loaded | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def claim(self, inputs: list[InputFile], emit: Emitter) -> tuple[list[InputFile], list[dict[str, Any]]]:  # fmt: skip
        """The inputs left to process and the reports of the skipped ones; `row` mode never skips (Rust 0.7)."""
        e = self.entity
        if e.incremental_mode != "file":
            return inputs, []
        self.uri = state_uri(e, self.config_dir)
        got = claim_inputs(self.client, self.uri, e.name, self.run_id, inputs)
        reports = skipped_reports(e, got.already_processed, got.active_claims)
        for r in reports:
            emit("file_finished", entity=e.name, input=r["input_file"], status="skipped",
                 skip_reason=r["skip_reason"], rows=0, accepted=0, rejected=0, elapsed_ms=0)  # fmt: skip
        if got.claimed:
            self.claimed = got.claimed
            self._thread = threading.Thread(target=self._beat, daemon=True)
            self._thread.start()
        return got.pending, reports

    def _beat(self) -> None:
        while not self._stop.wait(RENEW_INTERVAL_S):
            with suppress(Exception):  # Rust ignores renewal errors
                if self.claimed:
                    renew(self.client, self.uri, self.run_id, self.claimed)

    def _end(self) -> Loaded | None:
        self._stop.set()
        if self._thread:
            self._thread.join()
        claimed, self.claimed = self.claimed, None
        return claimed

    def release(self) -> None:
        if claimed := self._end():
            release(self.client, self.uri, self.run_id, claimed)

    def finish(self, report: dict[str, Any]) -> None:
        """Promote unless the entity failed or aborted (Rust commits on success, warnings and rejected)."""
        status, _ = run_outcome([f["status"] for f in report["files"]])
        if status in ("failed", "aborted"):
            return self.release()
        if claimed := self._end():
            promote(self.client, self.uri, self.run_id, claimed)
