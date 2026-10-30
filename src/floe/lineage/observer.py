"""OpenLineage observer for `floe run` over plain HTTP (Rust: `lineage/mod.rs`). Lineage failures never fail the run."""

import json
import os
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable, Sequence
from contextlib import suppress
from http.client import HTTPConnection, HTTPMessage, HTTPResponse, HTTPSConnection
from pathlib import Path
from typing import IO, Any
from urllib.parse import urlsplit

from floe.config.project import LineageConfig, RootConfig
from floe.config.types import EntityConfig
from floe.config.validate import validate_config
from floe.errors import FloeError
from floe.lineage.events import Datasets, EventBuilder, event_type, resolve_api_key
from floe.run import RunOutcome, run

RETRY_DELAYS_S = (0.0, 0.1, 0.5)


def _origin(url: str) -> tuple[str, str | None, int | None]:
    u = urlsplit(url)
    return u.scheme, u.hostname, u.port or {"http": 80, "https": 443}.get(u.scheme)


class _Attempt:
    """One POST and its redirects. On the deadline the caller cancels it: the live socket is shut down and nothing is sent afterwards."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.cancelled = False
        self.sock: socket.socket | None = None

    def hooked(self, base: type[HTTPConnection]) -> type[HTTPConnection]:
        attempt = self

        class Conn(base):
            def connect(self) -> None:
                super().connect()
                with attempt.lock:
                    if attempt.cancelled:
                        self.close()
                        raise OSError("lineage attempt cancelled")
                    attempt.sock = self.sock

        return Conn

    def cancel(self) -> None:
        with self.lock:
            self.cancelled = True
            if self.sock:
                with suppress(OSError):
                    self.sock.shutdown(socket.SHUT_RDWR)


class _Handler(urllib.request.HTTPHandler, urllib.request.HTTPSHandler):
    """urllib's HTTP(S) handlers, but every connection reports its socket to the attempt."""

    def __init__(self, attempt: _Attempt) -> None:
        super().__init__()
        self.attempt = attempt

    def http_open(self, req: urllib.request.Request) -> HTTPResponse:
        return self.do_open(self.attempt.hooked(HTTPConnection), req)

    def https_open(self, req: urllib.request.Request) -> HTTPResponse:
        return self.do_open(self.attempt.hooked(HTTPSConnection), req)


class _Redirect(urllib.request.HTTPRedirectHandler):
    """Redirects as reqwest follows them: 307/308 keep the POST; `Authorization` never crosses origins."""

    def __init__(self, attempt: _Attempt) -> None:
        self.attempt = attempt

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: HTTPMessage,
        newurl: str,
    ) -> urllib.request.Request | None:
        if self.attempt.cancelled:
            return None
        if code in (307, 308) and req.get_method() == "POST":
            new = urllib.request.Request(
                newurl, req.data, dict(req.headers), method="POST"
            )
        else:
            new = super().redirect_request(req, fp, code, msg, headers, newurl)
        if new and _origin(newurl) != _origin(req.full_url):
            new.remove_header("Authorization")
        return new


def _warn(message: str) -> None:
    print(f"warn: lineage: {message}", file=sys.stderr)


class OpenLineageObserver:
    """A run observer (`floe.run.events.Observer`) posting OpenLineage `RunEvent`s."""

    def __init__(
        self, config: LineageConfig, entities: Sequence[EntityConfig], config_path: str
    ) -> None:
        self.config = config
        self.events = EventBuilder(config, entities, config_path)
        path = (config.endpoint or "").strip() or "api/v1/lineage"
        self.url = f"{config.url.rstrip('/')}/{path.lstrip('/')}"
        secret = config.api_key.get_secret_value() if config.api_key else None
        self.api_key = resolve_api_key(secret, os.environ.get("OPENLINEAGE_API_KEY"))
        self.disabled = bool(self.api_key and ("\r" in self.api_key or "\n" in self.api_key))  # fmt: skip
        if self.disabled:
            _warn("api_key contains a line break; events are not sent")
        self.entity_run_ids: dict[str, str] = {}
        self.run_uuid: str | None = None
        self.consecutive_failures = 0
        self.circuit_open = False
        self._lock = threading.Lock()  # entities may run in parallel threads

    def __call__(self, event: dict[str, Any]) -> None:
        kind, ts = event["event"], event["ts_ms"]
        job = self.events.run_job_name
        if kind == "run_started":
            with self._lock:
                self.consecutive_failures, self.circuit_open = 0, False
            self.run_uuid = str(uuid.uuid4())
            self._post("START", ts, self.run_uuid, job)
        elif kind == "entity_started":
            run_id = self.entity_run_ids[event["name"]] = str(uuid.uuid4())
            self._post("START", ts, run_id, event["name"])
        elif kind == "entity_finished":
            name = event["name"]
            run_id = self.entity_run_ids.get(name, f"{event['run_id']}.entity.{name}")
            inputs, outputs = self.events.datasets(name, event)
            self._post(event_type(event["status"]), ts, run_id, name, inputs, outputs)
        elif kind == "run_finished":
            run_id = self.run_uuid or str(uuid.uuid4())
            self._post(event_type(event["status"]), ts, run_id, job)

    def fail_run(self) -> None:
        """A run that raised: `START` if it never started, then `FAIL` (Rust: the CLI failure path)."""
        ts = time.time_ns() // 1_000_000
        if self.run_uuid is None:
            self({"event": "run_started", "ts_ms": ts})
        self({"event": "run_finished", "status": "failed", "ts_ms": ts})

    def _post(
        self,
        event_type: str,
        ts_ms: int,
        run_id: str,
        job: str,
        inputs: Datasets | None = None,
        outputs: Datasets | None = None,
    ) -> None:
        if self.circuit_open or self.disabled:
            return
        body = self.events.body(event_type, ts_ms, run_id, job, inputs, outputs)
        data = json.dumps(body).encode()
        for delay in RETRY_DELAYS_S:
            time.sleep(delay)
            retryable = self._attempt(data)
            if retryable is None:
                with self._lock:
                    self.consecutive_failures = 0
                return
            if not retryable:
                break
        _warn(f"POST {self.url} failed — event dropped")
        with self._lock:
            self.consecutive_failures += 1
            failures = self.consecutive_failures
            limit = 3 if self.config.max_failures is None else self.config.max_failures
            opening = failures >= limit and not self.circuit_open
            self.circuit_open |= opening
        if opening:
            _warn(f"disabled for this run after {failures} consecutive failures — check endpoint {self.url}")  # fmt: skip

    def _attempt(self, data: bytes) -> bool | None:
        """None on success, else whether the failure is retryable (transport, timeout, 5xx, 429).

        The timeout bounds the whole request, as reqwest's does; urllib's alone bounds each socket read.
        An expired attempt is cancelled, so it never sends later (a redirect, a stale event).
        """
        timeout = 5 if self.config.timeout_secs is None else self.config.timeout_secs
        attempt = _Attempt()
        result: list[bool | None] = []
        thread = threading.Thread(
            target=lambda: result.append(self._send(data, timeout, attempt)),
            daemon=True,
        )
        thread.start()
        thread.join(timeout)
        if thread.is_alive():
            attempt.cancel()
            return True  # an expired attempt failed, even if its cut-off read looks like a 200 (#886)
        return result[0] if result else True

    def _send(self, data: bytes, timeout: float, attempt: _Attempt) -> bool | None:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        request = urllib.request.Request(self.url, data, headers, method="POST")
        opener = urllib.request.build_opener(_Handler(attempt), _Redirect(attempt))
        try:
            with opener.open(request, timeout=timeout):
                return None
        except urllib.error.HTTPError as e:
            return e.code == 429 or e.code >= 500
        except Exception:  # noqa: BLE001 -- the text may carry the token; never let it reach a thread hook
            return True


def run_with_lineage(
    path: Path,
    root: RootConfig,
    observed: Sequence[EntityConfig],
    job: str,
    observer: Callable[[dict[str, Any]], None],
    *,
    validate: bool,
    run_id: str | None,
    entities: Sequence[str],
    dry_run: bool,
    full_refresh: bool,
    config_uri: str | None = None,
) -> RunOutcome:
    """`run`, posting OpenLineage events when the config has a `lineage` block (Rust: the CLI run path)."""
    lineage = OpenLineageObserver(root.lineage, observed, job) if root.lineage else None

    def observe(event: dict[str, Any]) -> None:
        try:
            observer(event)
        finally:  # a failing user observer never drops a lineage event
            if lineage:
                lineage(event)

    try:
        if validate:  # after the observer: lineage sees a config that fails validation
            validate_config(root)
        return run(
            path, root, run_id=run_id, entities=entities, dry_run=dry_run,
            full_refresh=full_refresh, observer=observe, config_uri=config_uri,
            workers=root.entity_workers,
        )  # fmt: skip
    except FloeError:
        if lineage:
            try:
                lineage.fail_run()
            except Exception as e:  # noqa: BLE001 -- lineage must not mask the run error
                _warn(str(e))
        raise
