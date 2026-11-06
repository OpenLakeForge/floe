"""The incremental `file` mode of an entity run: claim the inputs, renew the claims, promote or release them, and where the state lives."""

import threading
from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path
from typing import Any

from pyiceberg.table import Table

from floe.config.storage import StoragesConfig
from floe.config.types import EntityConfig
from floe.errors import FloeRunError
from floe.iceberg.sink import input_hash
from floe.iceberg.write import find_committed
from floe.report.summary import run_outcome
from floe.run.events import Emitter
from floe.state import (
    CLAIM_TTL_SECONDS,
    Claim,
    EntityState,
    FileState,
    Loaded,
    claim_all,
    claim_inputs,
    promote,
    release,
    renew,
)
from floe.storage.inputs import InputFile, suffixes_for_format
from floe.storage.local import LocalClient
from floe.storage.paths import normalize_local_path
from floe.storage.resolve import resolve_path, storage_client
from floe.storage.types import StorageClient

RENEW_INTERVAL_S = float(min(max(CLAIM_TTL_SECONDS // 3, 1), 300))
_SEPARATORS = "/\\"


def _parent(value: str) -> str | None:
    i = max(value.rfind(s) for s in _SEPARATORS)
    return None if i < 0 else value[: max(i, 1)]


def _source_root(raw: str, fmt: str, resolved: Path | None) -> str:
    """Rust `derive_source_root`: the directory the default state lives under."""
    trimmed = raw.rstrip(_SEPARATORS)
    if not trimmed:
        return ""
    cut = next((i for i, c in enumerate(trimmed) if c in "*?["), -1)
    if cut >= 0:
        prefix = trimmed[:cut]
        done = prefix if not prefix or prefix[-1] in _SEPARATORS else _parent(prefix) or prefix  # fmt: skip
        return done.rstrip(_SEPARATORS)
    if resolved and resolved.is_dir():  # a remote source has no local path to probe
        return trimmed
    if (resolved and resolved.is_file()) or trimmed.lower().endswith(
        tuple(suffixes_for_format(fmt))
    ):
        # #813: a bare filename lives in the config dir (Rust 0.7 used the file itself)
        return (_parent(trimmed) or ".").rstrip(_SEPARATORS)
    return trimmed


def state_uri(entity: EntityConfig, config_dir: Path, storages: StoragesConfig | None = None) -> tuple[str, StorageClient]:  # fmt: skip
    """`state.path` if set (a URI on the source storage, else local), else `<source root>/.floe/state/<domain>/<name>/state.json` on the source storage (Rust `resolve_entity_state_path`)."""
    src, field = entity.source, f"entity.name={entity.name} entity.state.path"
    custom = entity.state and entity.state.path
    if custom and "://" not in custom:
        return f"local://{normalize_local_path(config_dir / custom)}", LocalClient()
    if not custom:
        source_uri, _ = resolve_path(storages, config_dir, src.storage, src.path, field)
        local = source_uri.startswith("local://")
        root = _source_root(src.path, src.format, Path(source_uri.removeprefix("local://")) if local else None)  # fmt: skip
        ident = f"{entity.domain}/{entity.name}" if entity.domain and entity.id != entity.name else entity.name  # fmt: skip
        tail = f".floe/state/{ident}/state.json"
        custom = f"{root}/{tail}" if root and root != "." else tail
    uri, definition = resolve_path(storages, config_dir, src.storage, custom, field)
    return uri, storage_client(definition)


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


def _emit_skipped(emit: Emitter, entity: str, reports: list[dict[str, Any]]) -> None:
    for r in reports:
        emit("file_finished", entity=entity, input=r["input_file"], status="skipped",
             skip_reason=r["skip_reason"], rows=0, accepted=0, rejected=0, elapsed_ms=0)  # fmt: skip


class Incremental:
    """Claims the entity's inputs, renews the claims while it runs, then promotes or releases them."""

    def __init__(self, entity: EntityConfig, config_dir: Path, run_id: str, full_refresh: bool = False, storages: StoragesConfig | None = None) -> None:  # fmt: skip
        self.entity, self.config_dir, self.run_id = entity, config_dir, run_id
        self.storages = storages
        self.full_refresh = full_refresh
        self.client: StorageClient = LocalClient()
        self.uri = ""
        self.claimed: Loaded | None = None
        self.expired: dict[str, Claim] = {}  # the dead runs' claims this run took over
        # the file reports, set once the sink step is done (#915)
        self.committed: list[dict[str, Any]] | None = None
        self.advance: Callable[[EntityState, list[str]], None] | None = None  # D30
        self.dated: Callable[[str], date | None] | None = None  # #949: an input's partition date  # fmt: skip
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def claim(self, inputs: list[InputFile], emit: Emitter) -> tuple[list[InputFile], list[dict[str, Any]]]:  # fmt: skip
        """The inputs left to process and the reports of the skipped ones; `row` mode and a full refresh never skip (Rust 0.7)."""
        e = self.entity
        if e.incremental_mode != "file":
            return inputs, []
        self.uri, self.client = state_uri(e, self.config_dir, self.storages)
        if self.full_refresh:
            # #949: `expired` holds the claims it replaced, which `release` restores if the refresh wrote nothing
            claimed, self.expired = claim_all(self.client, self.uri, e.name, self.run_id, inputs)  # fmt: skip
            self._hold(claimed, emit)
            return inputs, []
        got = claim_inputs(self.client, self.uri, e.name, self.run_id, inputs, e.sink.write_mode.startswith("merge"), self.dated)  # fmt: skip
        reports = skipped_reports(e, got.already_processed, got.active_claims)
        _emit_skipped(emit, e.name, reports)
        self.expired = got.expired
        if got.claimed:
            self._hold(got.claimed, emit)
        return got.pending, reports

    def skip_committed(self, table: Table | None, pending: list[InputFile], emit: Emitter) -> list[dict[str, Any]]:  # fmt: skip
        """#545 retry guard (append): the reports of the pending inputs a dead run already committed, before any is read.

        `file` mode re-derives each dead run's batch from its expired claims (by `run_id`, with their size and mtime),
        so a retry that also holds new inputs still matches; it looks back to that run's claim. Other modes match the
        whole set.
        """
        e = self.entity
        if table is None or not pending or self.full_refresh:
            return []
        if e.incremental_mode != "file":
            done = pending if find_committed(table, input_hash(e, pending)) else []
            return self._skipped(done, emit)
        ours = {f.source_uri for f in pending}
        runs: dict[str, dict[str, Claim]] = {}
        for u, c in self.expired.items():
            runs.setdefault(c.run_id, {})[u] = c
        committed: set[str] = set()
        for claims in runs.values():
            if ours.isdisjoint(claims):
                continue  # new inputs, or a reload after `floe state reset`
            since = min(datetime.fromisoformat(c.acquired_at) for c in claims.values())
            batch = [InputFile(u, "", "", c.size, c.mtime) for u, c in claims.items()]
            found = find_committed(table, input_hash(e, batch), since)
            # fail closed; `run_entity` releases, which puts the dead runs' claims back so the next run fails too
            if found is None:
                raise FloeRunError(
                    f"entity.name={e.name} cannot tell whether a run that died after claiming its inputs at {since} "
                    "committed them: the table's snapshots since then were expired (retention floor: 7 days and at least "
                    "10 snapshots); run `floe state reset` for the entity, then rerun"
                )  # fmt: skip
            if found:
                committed |= claims.keys() & ours
        # promote now, so a later failure of the other inputs can't release them
        if committed and self.claimed:
            promote(self.client, self.uri, self.run_id, self.claimed, only=committed, advance=self.advance)  # fmt: skip
        return self._skipped([f for f in pending if f.source_uri in committed], emit)

    def _skipped(self, files: list[InputFile], emit: Emitter) -> list[dict[str, Any]]:
        reports = [skipped_report(f.source_uri, "already_committed") for f in files]
        _emit_skipped(emit, self.entity.name, reports)
        return reports

    def _hold(self, claimed: Loaded, emit: Emitter) -> None:
        self.claimed = claimed
        self._thread = threading.Thread(target=self._beat, args=(emit,), daemon=True)
        self._thread.start()

    def _beat(self, emit: Emitter) -> None:
        while not self._stop.wait(RENEW_INTERVAL_S):
            try:  # a failed renewal never aborts the run: the next beat retries it
                if self.claimed:
                    renew(self.client, self.uri, self.run_id, self.claimed)
            except Exception as e:  # noqa: BLE001 -- any renewal failure is reported, never fatal
                emit("log", log_level="warn", code="heartbeat_failed", message=f"entity.name={self.entity.name} claim renewal failed: {e}", entity=self.entity.name, input=None)  # fmt: skip

    def _end(self) -> Loaded | None:
        self._stop.set()
        if self._thread:
            self._thread.join()
        claimed, self.claimed = self.claimed, None
        return claimed

    def release(self) -> None:
        """The dead runs' claims this run took over go back: the next run checks them again (#545).

        #949: a full refresh puts back the claims it replaced only if it wrote nothing, as its overwrite drops their rows
        and the retry guard would find their run's snapshot behind it.
        """
        if claimed := self._end():
            restore = (
                {} if self.full_refresh and self.committed is not None else self.expired
            )
            # D30: an older day's released file stays listed for a retry
            release(self.client, self.uri, self.run_id, claimed, restore, self.advance is not None and not self.full_refresh)  # fmt: skip

    def finish(self, files: list[dict[str, Any]]) -> None:
        """Promote unless the entity failed (Rust commits on success, warnings and rejected).

        #918: an abort still writes the files before it (Rust 0.7), so it promotes those and releases the rest;
        under `overwrite` it releases them all, as the retry rebuilds the output from every file.
        """
        status, _ = run_outcome([f["status"] for f in files])
        if status == "failed" or (status == "aborted" and self.entity.sink.write_mode == "overwrite"):  # fmt: skip
            return self.release()
        if claimed := self._end():
            only = {f["input_file"] for f in files if f["status"] != "aborted"} if status == "aborted" else None  # fmt: skip
            promote(self.client, self.uri, self.run_id, claimed, self.full_refresh, only, self.advance)  # fmt: skip
            if (
                only is not None
            ):  # D30: the aborted day's released files stay listed for a retry
                release(self.client, self.uri, self.run_id, claimed, self.expired, self.advance is not None and not self.full_refresh)  # fmt: skip
