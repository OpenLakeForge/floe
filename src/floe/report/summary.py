"""Run `run.summary.json`, run status and exit code, report layout (Rust: `report/{mod,output}.rs`, `run/mod.rs`)."""

from collections.abc import Sequence
from typing import Any

from floe import __version__
from floe.config.project import ProjectMetadata
from floe.config.types import EntityConfig
from floe.report.entity import serialize
from floe.storage.types import StorageClient

_TOTALS = ("files_total", "files_skipped", "rows_total", "accepted_total",
           "rejected_total", "warnings_total", "errors_total")  # fmt: skip


def id_path(entity: EntityConfig) -> str:
    """Report sub-directory of an entity: `<domain>/<name>` when the id is qualified."""
    if entity.domain and entity.id != entity.name:
        return f"{entity.domain}/{entity.name}"
    return entity.id


def run_outcome(file_statuses: Sequence[str], warnings: int = 0) -> tuple[str, int]:
    """Rust's `compute_run_outcome` then `upgrade_status_for_warnings`: (status, exit code)."""
    if "failed" in file_statuses:
        return "failed", 1
    if "aborted" in file_statuses:
        return "aborted", 2
    if "rejected" in file_statuses:
        return "rejected", 0
    return ("success_with_warnings" if warnings else "success"), 0


def build_summary(
    version: str,
    reports: Sequence[tuple[str, dict[str, Any]]],
    *,
    run_id: str,
    started_at: str,
    finished_at: str,
    duration_ms: int,
    config_path: str,
    report_path: str,
    metadata: ProjectMetadata | None = None,
) -> dict[str, Any]:
    """`reports` are the entity `run.json` dicts in run order, each with its `id_path`."""
    run_dir = f"{report_path.rstrip('/')}/run_{run_id}"
    totals = {k: sum(r["results"][k] for _, r in reports) for k in _TOTALS}
    statuses = [f["status"] for _, r in reports for f in r["files"]]
    status, exit_code = run_outcome(statuses, totals["warnings_total"])
    entities = [
        {
            "name": r["entity"]["name"],
            "status": run_outcome(
                [f["status"] for f in r["files"]], r["results"]["warnings_total"]
            )[0],
            "results": r["results"],
            "report_file": f"{run_dir}/{path}/run.json",
        }
        for path, r in reports
    ]
    return {
        "spec_version": version,
        "tool": {"name": "floe", "version": __version__},
        "run": {
            "run_id": run_id,
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_ms": duration_ms,
            "status": status,
            "exit_code": exit_code,
        },
        "config": {"path": config_path, "version": version}
        | ({"metadata": metadata.model_dump(exclude_none=True)} if metadata else {}),
        "report": {"path": report_path, "report_file": f"{run_dir}/run.summary.json"},
        "results": totals,
        "entities": entities,
    }


def write_reports(
    summary: dict[str, Any],
    reports: Sequence[tuple[str, dict[str, Any]]],
    client: StorageClient,
) -> None:
    """Write `<report>/run_<id>/<id_path>/run.json` per entity, then `run.summary.json` (overwriting, as Rust)."""
    summary_file = summary["report"]["report_file"]
    run_dir = summary_file.removesuffix("/run.summary.json")
    for path, report in reports:
        write_entity_report(run_dir, path, report, client)
    client.write(summary_file, serialize(summary).encode())


def write_entity_report(
    run_dir: str, path: str, report: dict[str, Any], client: StorageClient
) -> None:
    """One entity's `run.json`, written as the entity finishes (Rust)."""
    client.write(f"{run_dir}/{path}/run.json", serialize(report).encode())
