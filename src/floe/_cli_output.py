"""`floe run` output: run events as log lines and the run summary (Rust: `crates/floe-cli/src/logging.rs`, `output.rs`)."""

import json
import time
from typing import Any

from floe.config.types import EntityConfig
from floe.errors import (
    FloeConfigError,
    FloeError,
    FloeIoError,
    FloeRunError,
    FloeStorageError,
)
from floe.report.summary import id_path
from floe.run import RunOutcome

LOG_SCHEMA = "floe.log.v1"
MAX_PREVIEW_FILES = 50
_LEVELS = {
    "warn": "warn",
    "rejected": "warn",
    "success_with_warnings": "warn",
    "error": "error",
    "aborted": "error",
    "failed": "error",
}
_CODES: dict[type[FloeError], str] = {
    FloeStorageError: "storage_error",
    FloeIoError: "io_error",
    FloeConfigError: "config_error",
    FloeRunError: "run_error",
}
_COUNTS = ("files", "files_skipped", "rows", "accepted", "rejected", "warnings")


def event_json(event: dict[str, Any]) -> str:
    kind = event["event"]
    status: str = event["log_level"] if kind == "log" else event.get("status", "")
    level = _LEVELS.get(status, "info")
    doc: dict[str, Any] = {"schema": LOG_SCHEMA, "level": level, **event}
    return json.dumps(doc, ensure_ascii=False, separators=(",", ":"))


def event_text(e: dict[str, Any]) -> str:
    kind = e["event"]
    if kind == "log":
        tags = "".join(f" {k}={e[k]}" for k in ("code", "entity", "input") if e[k] is not None)  # fmt: skip
        return f"log level={e['log_level']}{tags} {e['message']}"
    if kind == "run_started":
        return f"run_started run_id={e['run_id']} config={e['config']} report_base={e['report_base'] or 'disabled'}"  # fmt: skip
    if kind == "entity_started":
        return f"entity_started name={e['name']}"
    if kind == "file_started":
        return f"file_started entity={e['entity']} input={e['input']}"
    if kind == "file_finished":
        skip = f" skip_reason={e['skip_reason']}" if e.get("skip_reason") else ""
        return (
            f"file_finished entity={e['entity']} input={e['input']} status={e['status']} rows={e['rows']} "
            f"accepted={e['accepted']} rejected={e['rejected']} elapsed_ms={e['elapsed_ms']}{skip}"
        )  # fmt: skip
    if kind == "entity_finished":
        counts = "".join(f" {k}={e[k]}" for k in (*_COUNTS, "errors"))
        return f"entity_finished name={e['name']} status={e['status']}{counts}"
    if kind == "schema_evolution_applied":
        return f"schema_evolution_applied entity={e['entity']} mode={e['mode']} added_columns={','.join(e['added_columns'])}"  # fmt: skip
    return f"run_finished status={e['status']} exit_code={e['exit_code']} summary={e['summary_uri'] or 'disabled'}"  # fmt: skip


class Console:
    """The CLI's run observer: prints each event in `log_format` and keeps the file timings for the summary."""

    def __init__(self, log_format: str, run_id: str) -> None:
        self.log_format, self.run_id = log_format, run_id
        self.started = False
        self.timings: list[dict[str, int]] = []  # per entity: input -> elapsed_ms

    def __call__(self, event: dict[str, Any]) -> None:
        kind = event["event"]
        self.started |= kind == "run_started"
        if kind == "entity_started":
            self.timings.append({})
        elif kind == "file_finished" and self.timings:
            self.timings[-1][event["input"]] = event["elapsed_ms"]
        if self.log_format != "off":
            fmt = event_json if self.log_format == "json" else event_text
            print(fmt(event), flush=True)

    def failed(self, err: FloeError, config: str | None) -> None:
        """Rust's `emit_failed_run_events`, after a `run_started` for `config` when the run never started.

        No `config` means it could not be resolved: Rust sees a bare `std::io::Error` there, so the code is `error`.
        """
        base = {"run_id": self.run_id}
        ts = {"ts_ms": time.time_ns() // 1_000_000}
        if config is not None and not self.started:
            self({"event": "run_started", **base, "config": config, "report_base": None, **ts})  # fmt: skip
        code = next(
            (c for t, c in _CODES.items() if isinstance(err, t) and config), "error"
        )
        self({"event": "log", **base, "log_level": "error", "code": code, "message": str(err), "entity": None, "input": None, **ts})  # fmt: skip
        self({"event": "run_finished", **base, "status": "failed", "exit_code": 1, **dict.fromkeys(_COUNTS, 0),
              "errors": 1, "summary_uri": None, **ts})  # fmt: skip


def _short(path: str | None) -> str:
    return "-" if path is None else path.rstrip("/").rsplit("/", 1)[-1]


def _entity(
    report: dict[str, Any], timings: dict[str, int], verbose: bool
) -> list[str]:
    src, sink = report["source"], report["sink"]
    lines = [f"==> entity {report['entity']['name']} (severity={report['policy']['severity']}, format={src['format']})"]  # fmt: skip
    if verbose:
        lines += [f"  source: {src['format']} ({src['path']})", f"  sink accepted: {sink['accepted']['format']} ({sink['accepted']['path']})"]  # fmt: skip
        if rejected := sink.get("rejected"):
            lines.append(f"  sink rejected: {rejected['format']} ({rejected['path']})")
        if src.get("cast_mode"):
            lines.append(f"  cast_mode: {src['cast_mode']}")
        if files := src["resolved_inputs"]["files"]:
            lines += ["  inputs:", *(f"    {f}" for f in files)]
    for f in report["files"]:
        line = f"  {f['status'].upper()} {_short(f['input_file'])}"
        line += f" reason={f['skip_reason']}" if f.get("skip_reason") else ""
        line += f" rows={f['row_count']} accepted={f['accepted_count']} rejected={f['rejected_count']}"  # fmt: skip
        ms = timings.get(f["input_file"])
        line += "" if ms is None else f" elapsed_ms={ms}"
        out = f["output"]
        lines.append(f"{line} accepted_out={_short(out['accepted_path'])} rejected_out={_short(out['rejected_path'])}")  # fmt: skip
    return lines


def _preview(e: EntityConfig, files: list[str], report_file: str, verbose: bool) -> list[str]:  # fmt: skip
    rejected = e.sink.rejected
    lines = [
        f"==> entity {e.id} (format={e.source.format})",
        f"  Input: {e.source.path} ({e.source.format})",
        f"  Accepted Output: {e.sink.accepted.path} ({e.sink.accepted.format})",
        f"  Rejected Output: {rejected.path} ({rejected.format})"
        if rejected
        else "  Rejected Output: -",
    ]
    if e.sink.archive and e.sink.archive.path:
        lines.append(f"  Archive Path: {e.sink.archive.path} ({e.sink.archive.storage or 'default'})")  # fmt: skip
    shown = files if verbose else files[:MAX_PREVIEW_FILES]
    lines += [f"  Resolved Inputs: {len(files)}", "  Resolved Files:", *(f"    {f}" for f in shown)]  # fmt: skip
    if len(files) > len(shown):
        lines.append(f"    ... {len(files) - len(shown)} more")
    return lines + ([f"  report: {report_file}"] if verbose else [])


def run_output(
    outcome: RunOutcome,
    entities: list[EntityConfig],
    mode: str,
    timings: list[dict[str, int]],
) -> str:
    """Rust's `format_run_output`; `mode` is `default`, `quiet` or `verbose`."""
    run_dir = outcome.run_dir  # a URI on remote storage: join with `/`, never `Path`
    head = [f"run id: {outcome.run_id}", f"report base: {outcome.report_base_path}"]
    if outcome.dry_run is not None:
        by_id = {e.id: e for e in entities}
        blocks = [_preview(by_id[i], files, f"{run_dir}/{id_path(by_id[i])}/run.json", mode == "verbose")
                  for i, files in outcome.dry_run.items()]  # fmt: skip
        results = dict.fromkeys(("files_total", "rows_total", "accepted_total", "rejected_total", "files_skipped"), 0)  # fmt: skip
        status, exit_code = "success", 0
    else:
        blocks = [_entity(r, t, mode == "verbose") for r, t in zip(outcome.entity_reports, timings, strict=True)]  # fmt: skip
        results, run = outcome.summary["results"], outcome.summary["run"]
        status, exit_code = run["status"], run["exit_code"]
    lines = ["DRY RUN MODE - No actual execution performed"] if outcome.dry_run is not None else []  # fmt: skip
    if mode != "quiet":
        lines += [*head, ""]
        for i, block in enumerate(blocks):
            lines += [""] * (i > 0) + block
        lines.append("")
    else:
        lines += head
    skipped = f" skipped={results['files_skipped']}" if results["files_skipped"] else ""
    lines += [
        f"Totals: files={results['files_total']} rows={results['rows_total']} accepted={results['accepted_total']} rejected={results['rejected_total']}{skipped}",
        f"Overall: {status} (exit_code={exit_code})",
        f"Run summary: {run_dir}/run.summary.json",
    ]  # fmt: skip
    return "\n".join(lines)
