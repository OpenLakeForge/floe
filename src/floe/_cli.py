import argparse
import os
import sys
from pathlib import Path

from floe.config.parse import parse_config
from floe.config.project import RootConfig, validate_domains
from floe.config.validate import validate_entities
from floe.config.vars import string_map
from floe.config.yaml import load_yaml, yaml_hash
from floe.errors import FloeError, FloeIoError
from floe.run import run


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


def _run(config: str) -> int:
    path, root = _load(config, None)
    outcome = run(path, root)  # `--entities` and the other flags are #657
    run_info = outcome.summary["run"]
    print(f"run {run_info['run_id']}: {run_info['status']}")
    return run_info["exit_code"]


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
