import argparse
import os
import sys
from pathlib import Path

from floe.config.parse import parse_config
from floe.config.profile import apply_profile, parse_profile
from floe.config.project import RootConfig, validate_domains
from floe.config.validate import validate_entities
from floe.config.vars import extract_config_env_vars, resolve_vars
from floe.errors import FloeError, FloeIoError
from floe.lineage.observer import OpenLineageObserver
from floe.run import run


def _load(config: str, profile: str | None) -> tuple[Path, RootConfig]:
    try:
        path = Path(os.path.realpath(config, strict=True))
    except OSError as e:
        raise FloeIoError(f"{e.strerror} (os error {e.errno})") from e
    parsed = parse_profile(Path(profile)) if profile else None
    variables = (
        resolve_vars(parsed.variables, {}, extract_config_env_vars(path))
        if parsed
        else {}
    )
    root = parse_config(path, variables)
    if parsed:
        root = apply_profile(root, parsed)
    validate_domains(root)
    validate_entities(root)
    return path, root


def _run(config: str) -> int:
    path, root = _load(config, None)
    lineage = (
        OpenLineageObserver(root.lineage, root.entities, str(path))
        if root.lineage
        else None
    )
    try:
        outcome = run(
            path, root, observer=lineage
        )  # `--entities` and the other flags are #657
    except FloeError:
        if lineage:
            try:
                lineage.fail_run()
            except Exception as e:  # noqa: BLE001 -- lineage must not mask the run error
                print(f"warn: lineage: {e}", file=sys.stderr)
        raise
    run_info = outcome.summary["run"]
    print(f"run {run_info['run_id']}: {run_info['status']}")
    return run_info["exit_code"]


def main() -> int:
    parser = argparse.ArgumentParser(prog="floe")
    commands = parser.add_subparsers(dest="command", required=True)
    validate = commands.add_parser("validate")
    validate.add_argument("-c", "--config", required=True)
    validate.add_argument("-p", "--profile")
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
