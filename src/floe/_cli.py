import argparse
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from floe.checks.rows import Formatter
from floe.config.parse import parse_config
from floe.config.project import RootConfig, validate_domains
from floe.config.validate import validate_entities
from floe.config.vars import string_map
from floe.config.yaml import load_yaml, yaml_hash
from floe.errors import FloeError, FloeIoError
from floe.report.summary import build_summary, id_path, write_reports
from floe.run.entity import run_entity
from floe.storage.paths import normalize_local_path
from floe.storage.resolve import require_local


def _load(config: str, profile: str | None) -> tuple[Path, RootConfig]:
    try:
        path = Path(os.path.realpath(config, strict=True))
    except OSError as e:
        raise FloeIoError(f"{e.strerror} (os error {e.errno})") from e
    # ponytail: raw `variables:` only; validation and `${}` expansion land with #635
    variables = (
        string_map(
            yaml_hash(load_yaml(Path(profile))[0], "profile").get("variables", {}),
            "profile.variables",
        )
        if profile
        else {}
    )
    root = parse_config(path, variables)
    validate_domains(root)
    validate_entities(root, profile is not None)
    return path, root


def _rfc3339() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _run(config: str) -> int:
    started, timer = _rfc3339(), time.monotonic()
    path, root = _load(config, None)
    config_dir = normalize_local_path(path.parent)
    require_local(root.storages, root.report.storage, "report.storage")
    reports: list[tuple[str, dict[str, Any]]] = []
    formatter = cast(
        Formatter, root.report.formatter or "json"
    )  # validated with the config
    # config order; `--entities` and parallelism are #656/#657
    failure: FloeError | None = None
    for entity in root.entities:
        try:
            result = run_entity(
                entity, config_dir, root.version, formatter, root.storages
            )
        except FloeError as e:  # the entities already run keep their reports
            failure = e
            break
        reports.append((id_path(entity), result.report))
        if result.aborted:
            break
    summary = build_summary(
        root.version, reports,
        run_id=started.replace(":", "-"), started_at=started, finished_at=_rfc3339(),
        duration_ms=int((time.monotonic() - timer) * 1000),
        config_path=str(normalize_local_path(path)),
        report_path=str(normalize_local_path(config_dir / root.report.path)),
        metadata=root.metadata,
    )  # fmt: skip
    if failure:  # Rust: failed -> exit code 1
        summary["run"] |= {"status": "failed", "exit_code": 1}
    write_reports(summary, reports)
    if failure:
        raise failure
    run = summary["run"]
    print(f"run {run['run_id']}: {run['status']}")
    return run["exit_code"]


def main() -> int:
    parser = argparse.ArgumentParser(prog="floe")
    commands = parser.add_subparsers(dest="command", required=True)
    validate = commands.add_parser("validate")
    validate.add_argument("-c", "--config", required=True)
    validate.add_argument("-p", "--profile")  # ponytail: profiles land with #635
    commands.add_parser("run").add_argument("-c", "--config", required=True)
    try:
        args = parser.parse_args()
        if args.command == "run":
            return _run(args.config)
        _load(args.config, args.profile)
    except FloeError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    return 0
