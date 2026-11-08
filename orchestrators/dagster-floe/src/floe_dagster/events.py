from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class FloeRunFinished:
    run_id: str
    status: str
    exit_code: int
    summary_uri: str | None
    report_base: str | None = None
    entity_report_uris: dict[str, str] = field(default_factory=dict)


def parse_ndjson_events(stdout: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for line in stdout.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            value = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON log line: {stripped!r}") from exc
        if not isinstance(value, dict):
            raise ValueError("log line must be a JSON object")
        events.append(value)
    return events


def parse_json_event_lines(stdout: str) -> list[dict[str, Any]]:
    """Parse JSON object event lines from a mixed backend log stream.

    Kubernetes and Databricks backends can return the complete container stdout.
    Older Floe images may append human summary lines after the NDJSON events, so
    Dagster integrations should extract JSON object lines instead of treating the
    whole stream as strict NDJSON.
    """
    events: list[dict[str, Any]] = []
    for line in stdout.splitlines():
        stripped = line.strip()
        if not stripped or not stripped.startswith("{"):
            continue
        try:
            value = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            events.append(value)
    return events


def render_log_event(event: dict[str, Any]) -> str | None:
    """Render a Floe JSON event as a compact Dagster log line."""
    event_name = event.get("event")
    if not isinstance(event_name, str) or not event_name:
        return None

    if event_name == "run_started":
        return _format_parts(
            "floe run started",
            run_id=event.get("run_id"),
            config=event.get("config"),
            report_base=event.get("report_base"),
        )
    if event_name == "entity_started":
        return _format_parts(
            "floe entity started",
            run_id=event.get("run_id"),
            entity=event.get("name"),
        )
    if event_name == "file_started":
        return _format_parts(
            "floe file started",
            run_id=event.get("run_id"),
            entity=event.get("entity"),
            input=event.get("input"),
        )
    if event_name == "file_finished":
        return _format_parts(
            "floe file finished",
            run_id=event.get("run_id"),
            entity=event.get("entity"),
            input=event.get("input"),
            status=event.get("status"),
            rows=event.get("rows"),
            accepted=event.get("accepted"),
            rejected=event.get("rejected"),
            elapsed_ms=event.get("elapsed_ms"),
        )
    if event_name == "entity_finished":
        return _format_parts(
            "floe entity finished",
            run_id=event.get("run_id"),
            entity=event.get("name"),
            status=event.get("status"),
            files=event.get("files"),
            rows=event.get("rows"),
            accepted=event.get("accepted"),
            rejected=event.get("rejected"),
            warnings=event.get("warnings"),
            errors=event.get("errors"),
        )
    if event_name == "run_finished":
        return _format_parts(
            "floe run finished",
            run_id=event.get("run_id"),
            status=event.get("status"),
            exit_code=event.get("exit_code"),
            files=event.get("files"),
            rows=event.get("rows"),
            accepted=event.get("accepted"),
            rejected=event.get("rejected"),
            warnings=event.get("warnings"),
            errors=event.get("errors"),
            summary_uri=event.get("summary_uri"),
        )
    if event_name == "log":
        return _format_parts(
            "floe log",
            run_id=event.get("run_id"),
            level=event.get("log_level") or event.get("level"),
            code=event.get("code"),
            entity=event.get("entity"),
            input=event.get("input"),
            message=event.get("message"),
        )
    return _format_parts(f"floe {event_name}", run_id=event.get("run_id"))


def _format_parts(prefix: str, **items: Any) -> str:
    parts = [prefix]
    for key, value in items.items():
        if value is None:
            continue
        if isinstance(value, str) and value == "":
            continue
        parts.append(f"{key}={_format_value(value)}")
    return " ".join(parts)


def _format_value(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def last_run_finished(events: Iterable[dict[str, Any]]) -> dict[str, Any]:
    last: dict[str, Any] | None = None
    for event in events:
        if event.get("event") == "run_finished":
            last = event
    if last is None:
        raise ValueError("run_finished event not found in stdout stream")
    return last


def parse_run_finished(event: dict[str, Any], summary_uri_field: str = "summary_uri") -> FloeRunFinished:
    if event.get("event") != "run_finished":
        raise ValueError("expected run_finished event")
    return FloeRunFinished(
        run_id=str(event.get("run_id")),
        status=str(event.get("status")),
        exit_code=int(event.get("exit_code")),
        summary_uri=event.get(summary_uri_field),
        report_base=event.get("report_base"),
        entity_report_uris=event.get("entity_report_uris") or {},
    )
