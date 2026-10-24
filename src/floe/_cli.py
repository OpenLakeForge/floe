import argparse
import os
import sys
from pathlib import Path

from floe.config.parse import parse_config
from floe.config.project import validate_domains
from floe.errors import FloeError, FloeIoError


def _validate(config: str) -> None:
    try:
        path = Path(os.path.realpath(config, strict=True))
    except OSError as e:
        raise FloeIoError(f"{e.strerror} (os error {e.errno})") from e
    validate_domains(parse_config(path))


def main() -> int:
    parser = argparse.ArgumentParser(prog="floe")
    validate = parser.add_subparsers(dest="command", required=True).add_parser(
        "validate"
    )
    validate.add_argument("-c", "--config", required=True)
    validate.add_argument("-p", "--profile")  # ponytail: profiles land with #635
    try:
        _validate(parser.parse_args().config)
    except FloeError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    return 0
