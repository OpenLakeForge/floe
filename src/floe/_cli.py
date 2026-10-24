import argparse
import os
import sys
from pathlib import Path

from floe.config.parse import parse_config
from floe.config.project import validate_domains
from floe.config.validate import validate_entities
from floe.config.vars import string_map
from floe.config.yaml import load_yaml, yaml_hash
from floe.errors import FloeError, FloeIoError


def _validate(config: str, profile: str | None) -> None:
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


def main() -> int:
    parser = argparse.ArgumentParser(prog="floe")
    validate = parser.add_subparsers(dest="command", required=True).add_parser(
        "validate"
    )
    validate.add_argument("-c", "--config", required=True)
    validate.add_argument("-p", "--profile")  # ponytail: profiles land with #635
    try:
        args = parser.parse_args()
        _validate(args.config, args.profile)
    except FloeError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    return 0
