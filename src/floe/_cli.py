import argparse
import json
import os
import sys
from pathlib import Path

from floe.config.parse import parse_config
from floe.config.profile import (
    API_VERSION,
    KIND,
    ProfileConfig,
    apply_profile,
    parse_profile,
)
from floe.config.project import RootConfig
from floe.config.validate import validate_config
from floe.config.vars import extract_config_env_vars, resolve_vars
from floe.errors import FloeConfigError, FloeError, FloeIoError
from floe.lineage.observer import OpenLineageObserver
from floe.manifest import build_manifest
from floe.run import run, select_entities


def _realpath(path: str) -> Path:
    try:
        return Path(os.path.realpath(path, strict=True))
    except OSError as e:
        raise FloeIoError(f"{e.strerror} (os error {e.errno})") from e


def _load(config: str, profile: ProfileConfig | None) -> tuple[Path, RootConfig]:
    path = _realpath(config)
    variables = (
        resolve_vars(profile.variables, {}, extract_config_env_vars(path))
        if profile
        else {}
    )
    root = parse_config(path, variables)
    if profile:
        root = apply_profile(root, profile)
    return path, root


def _validate(args: argparse.Namespace) -> int:
    profile = parse_profile(_realpath(args.profile)) if args.profile else None
    if args.config is None:
        if profile is None:
            raise FloeConfigError(
                "floe validate requires --config unless --profile is provided"
            )
        print(f"Profile valid: {profile.name}")
        print(f"Schema: {API_VERSION}/{KIND}")
        print(f"Catalogs: {len(profile.catalogs.definitions) if profile.catalogs else 0}")  # fmt: skip
        return 0
    path, root = _load(args.config, profile)
    validate_config(root)
    select_entities(root, args.entities)
    print(f"Config valid: {path}")
    print(f"Version: {root.version}")
    print(f"Report: {root.report.path}")
    print(f"Entities: {len(root.entities)}")
    for e in root.entities:
        print(f"Entity: {e.name}")
        print(f"  Source: {e.source.format} ({e.source.path})")
        print(f"  Sink accepted: {e.sink.accepted.format} ({e.sink.accepted.path})")
        if e.sink.rejected:
            print(f"  Sink rejected: {e.sink.rejected.format} ({e.sink.rejected.path})")  # fmt: skip
        print(f"  Severity: {e.policy.severity}")
    profile_arg = f" -p {os.path.realpath(args.profile)}" if args.profile else ""
    print(f"Next: floe run -c {path}{profile_arg}")
    return 0


def _run(args: argparse.Namespace) -> int:
    if args.manifest is not None:
        raise FloeConfigError("floe run --manifest is not supported yet")
    if args.config is None:
        raise FloeConfigError("one of --config or --manifest is required")
    profile = parse_profile(_realpath(args.profile)) if args.profile else None
    path, root = _load(
        args.config, profile
    )  # lineage observes a config that fails validation
    lineage = (
        OpenLineageObserver(root.lineage, root.entities, str(path))
        if root.lineage
        else None
    )
    try:
        validate_config(root)
        outcome = run(
            path, root, run_id=args.run_id, entities=args.entities,
            full_refresh=args.full_refresh, observer=lineage,
        )  # fmt: skip
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


def _manifest(args: argparse.Namespace) -> int:
    if "://" in args.output:  # ponytail: remote `-o` needs the storage clients
        raise FloeConfigError(f"remote manifest output is not supported yet: {args.output}")  # fmt: skip
    path, root = _load(args.config, None)
    _check_config(root)
    manifest = build_manifest(root, path, args.deterministic, args.manifest_name)
    text = json.dumps(manifest, indent=2, ensure_ascii=False)
    if args.output == "-":
        print(text)
        return 0
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(f".{out.name}.tmp")
    tmp.write_text(text)
    tmp.replace(out)
    print(f"Manifest written: {args.output}")
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="floe")
    commands = parser.add_subparsers(dest="command", required=True)
    validate = commands.add_parser("validate")
    run = commands.add_parser("run")
    run_source = run.add_mutually_exclusive_group()
    run_source.add_argument("-c", "--config")
    run_source.add_argument("--manifest")
    run.add_argument("--run-id")
    run.add_argument("-F", "--full-refresh", action="store_true")
    validate.add_argument("-c", "--config")
    for command in (validate, run):
        command.add_argument("--entities", action="extend", type=lambda s: s.split(","), default=[])  # fmt: skip
        command.add_argument("-p", "--profile")
    generate = (
        commands.add_parser("manifest")
        .add_subparsers(dest="manifest_command", required=True)
        .add_parser("generate")
    )
    generate.add_argument("-c", "--config", required=True)
    generate.add_argument("-o", "--output", required=True)
    generate.add_argument("--deterministic", action="store_true")
    generate.add_argument("--manifest-name")
    return parser


def main() -> int:
    args, extra = _parser().parse_known_args()
    if extra:  # clap's wording for an unknown flag
        print(f"error: unexpected argument '{extra[0]}' found\n\nFor more information, try '--help'.", file=sys.stderr)  # fmt: skip
        return 2
    try:
        if args.command == "manifest":
            return _manifest(args)
        return _run(args) if args.command == "run" else _validate(args)
    except FloeError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
