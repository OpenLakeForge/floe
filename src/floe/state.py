"""Incremental file state, ported from `state/mod.rs` (CAS claim, promote, release, renew, reset)."""

import copy
import json
import random
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import NamedTuple

from floe.errors import FloeConfigError
from floe.storage.inputs import InputFile
from floe.storage.types import StorageClient

SCHEMA_V1 = "floe.state.file-ingest.v1"
SCHEMA_V2 = "floe.state.file-ingest.v2"
CAS_RETRIES = 8
_BASE_BACKOFF_MS = 50
_MAX_BACKOFF_MS = 2_000
CLAIM_TTL_SECONDS = 60 * 60


@dataclass
class FileState:
    processed_at: str
    size: int | None = None
    mtime: str | None = None


@dataclass
class Claim:
    run_id: str
    acquired_at: str
    expires_at: str
    size: int | None = None
    mtime: str | None = None


@dataclass
class EntityState:
    entity: str
    updated_at: str | None
    files: dict[str, FileState]
    claims: dict[str, Claim]


class Loaded(NamedTuple):
    state: EntityState
    version: str | None


class ClaimOutcome(NamedTuple):
    pending: list[InputFile]
    claimed: Loaded | None
    active_claims: list[str]
    already_processed: list[tuple[InputFile, FileState]]
    expired: dict[str, Claim]  # dropped claims of dead runs, by URI (#545 retry guard)


def _now(offset_s: int = 0) -> str:
    at = datetime.now(UTC) + timedelta(seconds=offset_s)
    return at.isoformat().replace("+00:00", "Z")


def dump_state(state: EntityState) -> bytes:
    doc = {
        "schema": SCHEMA_V2,
        "entity": state.entity,
        "updated_at": state.updated_at,
        "files": {k: asdict(v) for k, v in sorted(state.files.items())},
        "claims": {k: asdict(v) for k, v in sorted(state.claims.items())},
    }
    return json.dumps(doc, indent=2, ensure_ascii=False).encode()


def parse_state(entity: str, body: bytes) -> EntityState:
    try:
        doc = json.loads(body)
        schema: str = doc["schema"]
        state = EntityState(
            doc["entity"],
            doc.get("updated_at"),
            {k: FileState(**v) for k, v in doc.get("files", {}).items()},
            {k: Claim(**v) for k, v in doc.get("claims", {}).items()}
            if schema != SCHEMA_V1
            else {},
        )
    except (ValueError, KeyError, TypeError, AttributeError) as e:
        raise FloeConfigError(f"entity.name={entity} invalid state file: {e}") from e
    if schema not in (SCHEMA_V1, SCHEMA_V2):
        raise FloeConfigError(
            f"entity.name={entity} state schema mismatch: expected {SCHEMA_V1} or {SCHEMA_V2}, got {schema}"
        )
    if state.entity != entity:
        raise FloeConfigError(
            f"entity.name={entity} state entity mismatch: expected {entity}, got {state.entity}"
        )
    return state


def read_state(client: StorageClient, uri: str, entity: str) -> EntityState | None:
    stored = client.read(uri)
    return None if stored is None else parse_state(entity, stored.body)


def _load(client: StorageClient, uri: str, entity: str) -> Loaded:
    stored = client.read(uri)
    if stored is None:
        return Loaded(EntityState(entity, None, {}, {}), None)
    return Loaded(parse_state(entity, stored.body), stored.version)


def _backoff(attempt: int) -> None:
    window = min(_BASE_BACKOFF_MS * 2 ** min(attempt, 20), _MAX_BACKOFF_MS)
    time.sleep(random.uniform(0, window) / 1000)


def _is_live(claim: Claim, now: datetime) -> bool:
    try:
        return datetime.fromisoformat(claim.expires_at) > now
    except (ValueError, TypeError):
        return False


def _persist(client: StorageClient, uri: str, loaded: Loaded) -> str | None:
    return client.write_conditional(uri, loaded.version, dump_state(loaded.state))


def claim_inputs(
    client: StorageClient,
    uri: str,
    entity: str,
    run_id: str,
    inputs: list[InputFile],
) -> ClaimOutcome:
    if not inputs:
        return ClaimOutcome([], None, [], [], {})
    for attempt in range(CAS_RETRIES):
        if attempt:
            _backoff(attempt - 1)
        loaded = _load(client, uri, entity)
        state = loaded.state
        now = datetime.now(UTC)
        expired = {u: c for u, c in state.claims.items() if not _is_live(c, now)}
        state.claims = {u: c for u, c in state.claims.items() if u not in expired}
        pending: list[InputFile] = []
        active: list[str] = []
        processed: list[tuple[InputFile, FileState]] = []
        acquired_at, expires_at = _now(), _now(CLAIM_TTL_SECONDS)
        for f in inputs:
            if f.source_uri in state.files:
                processed.append((f, state.files[f.source_uri]))
            elif f.source_uri in state.claims:
                active.append(f.source_uri)
            else:
                state.claims[f.source_uri] = Claim(
                    run_id, acquired_at, expires_at, f.source_size, f.source_mtime
                )
                pending.append(f)
        if not pending:
            if not active:
                _persist(client, uri, loaded)
            return ClaimOutcome([], None, active, processed, expired)
        state.updated_at = acquired_at
        version = _persist(client, uri, loaded)
        if version is not None:
            return ClaimOutcome(
                pending, Loaded(state, version), active, processed, expired
            )
    raise FloeConfigError(
        f"entity.name={entity} incremental state update conflicted after {CAS_RETRIES} retries"
    )


def claim_all(
    client: StorageClient, uri: str, entity: str, run_id: str, inputs: list[InputFile]
) -> Loaded:
    """Full refresh: claim every input, dropping other claims (Rust `claim_all_entity_inputs`)."""
    for attempt in range(CAS_RETRIES):
        if attempt:
            _backoff(attempt - 1)
        loaded = _load(client, uri, entity)
        acquired_at, expires_at = _now(), _now(CLAIM_TTL_SECONDS)
        claims = {
            f.source_uri: Claim(run_id, acquired_at, expires_at, f.source_size, f.source_mtime)
            for f in inputs
        }  # fmt: skip
        fresh = Loaded(EntityState(entity, acquired_at, loaded.state.files, claims), loaded.version)  # fmt: skip
        version = _persist(client, uri, fresh)
        if version is not None:
            return Loaded(fresh.state, version)
    raise FloeConfigError(
        f"entity.name={entity} full-refresh state write conflicted after {CAS_RETRIES} retries"
    )


def _mutate(
    client: StorageClient,
    uri: str,
    claimed: Loaded,
    mutate: Callable[[EntityState, set[str]], None],
) -> None:
    entity = claimed.state.entity
    ours = set(claimed.state.claims)
    for attempt in range(CAS_RETRIES):
        if attempt:
            _backoff(attempt - 1)
        loaded = (
            Loaded(copy.deepcopy(claimed.state), claimed.version)
            if attempt == 0
            else _load(client, uri, entity)
        )
        mutate(loaded.state, ours)
        state = loaded.state
        if state.files or state.claims:
            done = _persist(client, uri, loaded) is not None
        else:  # Rust deletes an emptied state
            done = client.delete_conditional(uri, loaded.version)
        if done:
            return
    raise FloeConfigError(
        f"entity.name={entity} incremental state update conflicted after {CAS_RETRIES} retries"
    )


def promote(
    client: StorageClient,
    uri: str,
    run_id: str,
    claimed: Loaded,
    full_refresh: bool = False,
    only: set[str] | None = None,
) -> None:
    """A full refresh replaces `files` with this run's (Rust `promote_full_refresh_claimed_entity_state`); `only` limits the URIs."""

    def apply(state: EntityState, ours: set[str]) -> None:
        processed_at = _now()
        if full_refresh:
            state.files.clear()
        mine = ours if only is None else ours & only
        for u in [
            u for u, c in state.claims.items() if c.run_id == run_id and u in mine
        ]:
            c = state.claims.pop(u)
            state.files[u] = FileState(processed_at, c.size, c.mtime)
        state.updated_at = processed_at

    _mutate(client, uri, claimed, apply)


def release(client: StorageClient, uri: str, run_id: str, claimed: Loaded, restore: dict[str, Claim] | None = None) -> None:  # fmt: skip
    """`restore`: the dead runs' expired claims this run took over, put back as they were (#545)."""

    def apply(state: EntityState, ours: set[str]) -> None:
        state.claims = {
            u: c
            for u, c in state.claims.items()
            if not (c.run_id == run_id and u in ours)
        }
        for u, c in (restore or {}).items():
            state.claims.setdefault(u, c)
        state.updated_at = _now()

    _mutate(client, uri, claimed, apply)


def renew(client: StorageClient, uri: str, run_id: str, claimed: Loaded) -> None:
    def apply(state: EntityState, ours: set[str]) -> None:
        expires_at = _now(CLAIM_TTL_SECONDS)
        for u, c in state.claims.items():
            if c.run_id == run_id and u in ours:
                c.expires_at = expires_at
        state.updated_at = _now()

    _mutate(client, uri, claimed, apply)


def reset(client: StorageClient, uri: str, entity: str) -> bool:
    """Delete the state file, even a malformed one; False when there is none (Rust `reset_entity_state`)."""
    stored = client.read(uri)
    if stored is None:
        return False
    if not client.delete_conditional(uri, stored.version):
        raise FloeConfigError(
            f"entity.name={entity} remote state changed while resetting: {uri}"
        )
    return True
