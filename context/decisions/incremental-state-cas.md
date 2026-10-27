# Decision — CAS-based incremental state locking

## Context

Floe supports incremental ingestion: it tracks which source files have already been processed and skips them on subsequent runs. Originally this was a simple JSON file (`state.json`) recording processed file URIs. This worked for single-process sequential runs but failed in two scenarios:

1. **Concurrent runs** — two parallel Floe processes (e.g., triggered by an orchestrator backfill) could both read the same state, both decide a file needs processing, and process it twice.
2. **Crashed runs** — if a process died mid-run, files it was processing were absent from the `files` map with no indication of what happened.

## Decision

Use **compare-and-swap (CAS)** with a **time-bounded claims** model.

The state file gains a `claims` map alongside the existing `files` map. A claim is `{ run_id, acquired_at, expires_at, size, mtime }` written for each file being processed. Claims expire after 1 hour. All writes are conditional so only one concurrent writer wins: `If-Match` / `If-None-Match` with ETags on S3, and a content-hash version token with an atomic rename on local storage (D18). Losers retry with backoff (`CAS_RETRIES` in `src/floe/state.py`). A background thread renews `expires_at` every TTL/3 seconds while the entity runs. When the entity finishes, its claims are promoted to `files`; when it fails, they are released.

## Rationale

- **No lock server needed.** The storage layer (S3, or the local filesystem) is the arbiter via conditional write primitives. Same pattern as DynamoDB conditional writes, Kubernetes `resourceVersion`, and Git ref updates.
- **Crash-safe by expiry.** A crashed process leaves stale claims that disappear after 1 hour without any cleanup job.
- **Backward-compatible.** V1 state files (no `claims` field) are read as V2 with no claims.

## Trade-offs

The 1-hour TTL is a blunt instrument. If a run takes longer than 1 hour and the heartbeat thread fails, claims expire and the files may be reprocessed. The heartbeat mitigates this in practice but does not eliminate the theoretical risk.
