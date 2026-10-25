import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from floe._cli_output import Console, run_output
from floe.add_entity import add_entity
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
from floe.manifest import Options, build_manifest
from floe.run import run, select_entities
from floe.run.entity import state_uri
from floe.state import dump_state, read_state, reset
from floe.storage.paths import normalize_local_path, parse_s3_uri
from floe.storage.s3 import S3Client


def _realpath(path: str) -> Path:
    try:
        return Path(os.path.realpath(path, strict=True))
    except OSError as e:
        raise FloeIoError(f"{e.strerror} (os error {e.errno})") from e


def _load(path: Path, profile: ProfileConfig | None) -> RootConfig:
    variables = (
        resolve_vars(profile.variables, {}, extract_config_env_vars(path))
        if profile
        else {}
    )
    root = parse_config(path, variables)
    if profile:
        root = apply_profile(root, profile)
    return root


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
    path = _realpath(args.config)
    root = _load(path, profile)
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
    run_id = args.run_id or datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%S.%fZ")
    console = Console(args.log_format, run_id)
    profile = parse_profile(_realpath(args.profile)) if args.profile else None
    try:  # Rust logs a missing config without `run_started`
        path = _realpath(args.config)
    except FloeError as e:
        console.failed(e, None)
        raise
    try:
        root = _load(path, profile)
    except FloeError as e:
        console.failed(e, str(path))
        raise
    lineage = (
        OpenLineageObserver(root.lineage, root.entities, str(path))
        if root.lineage
        else None
    )

    def observe(event: dict[str, Any]) -> None:
        console(event)
        if lineage:
            lineage(event)

    try:
        # after the observer: lineage sees a config that fails validation
        validate_config(root)
        outcome = run(
            path, root, run_id=run_id, entities=args.entities, dry_run=args.dry_run,
            full_refresh=args.full_refresh, observer=observe,
        )  # fmt: skip
    except FloeError as e:
        console.failed(e, str(path))
        if lineage:
            try:
                lineage.fail_run()
            except Exception as e:  # noqa: BLE001 -- lineage must not mask the run error
                print(f"warn: lineage: {e}", file=sys.stderr)
        raise
    mode = "quiet" if args.quiet else "verbose" if args.verbose else "default"
    out = run_output(outcome, root.entities, mode, console.timings)
    print(out, file=sys.stderr if args.log_format == "json" else sys.stdout)
    return 0 if args.dry_run else outcome.summary["run"]["exit_code"]


def _manifest(args: argparse.Namespace) -> int:
    remote = "://" in args.output
    if remote and not args.output.startswith("s3://"):  # D1: gcs and adls are dropped
        raise FloeConfigError(f"remote manifest output is not supported yet: {args.output}")  # fmt: skip
    path = _realpath(args.config)
    profile = parse_profile(_realpath(args.profile)) if args.profile else None
    root = _load(path, profile)
    validate_config(root)
    options = Options(
        args.config,
        args.deterministic,
        args.manifest_name,
        args.default_domain,
        args.manifest_path_mode,
        args.runtime,
        args.output
        if remote
        else None,  # a remote `-o` is where the manifest is deployed
        args.profile,
        profile,
    )
    manifest = build_manifest(root, path, options, args.entities)
    text = json.dumps(manifest, indent=2, ensure_ascii=False)
    if args.output == "-":
        print(text)
        return 0
    if (
        remote
    ):  # one PutObject: readers see the old or the new manifest, never a partial one
        S3Client(parse_s3_uri(args.output)[0]).write(args.output, text.encode())
    else:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_name(f".{out.name}.tmp")
        tmp.write_text(text)
        tmp.replace(out)
    print(f"Manifest written: {args.output}")
    return 0


def _add_entity(args: argparse.Namespace) -> int:
    added = add_entity(
        args.config, args.input, args.format, args.name, args.domain,
        args.source_ref, args.resource, args.output, args.dry_run,
    )  # fmt: skip
    if added.rendered is not None:
        print(f"# add-entity dry run (no file written)\n# entity: {added.name}")
        for note in added.notes:
            print(f"# note: {note}")
        print(added.rendered)
    else:
        print(f"Entity added: {added.name} (format={added.format}, columns={added.columns})")  # fmt: skip
        print(f"Config written: {added.output}")
        for note in added.notes:
            print(f"Note: {note}")
    return 0


def _state(args: argparse.Namespace) -> int:
    if args.state_command == "reset" and not args.yes:
        raise FloeConfigError("state reset is destructive, rerun with --yes to confirm")
    profile = parse_profile(_realpath(args.profile)) if args.profile else None
    path = _realpath(args.config)
    root = _load(path, profile)
    [entity] = select_entities(root, [args.entity])
    uri, client = state_uri(entity, normalize_local_path(path.parent), root.storages)
    if args.state_command == "reset":
        removed = reset(client, uri, entity.name)
        print(f"Entity: {entity.name}\nState path: {uri}")
        print(f"State reset: {'removed state object' if removed else 'no state object found'}")  # fmt: skip
        return 0
    state = read_state(client, uri, entity.name)
    print(f"Entity: {entity.name}\nIncremental mode: {entity.incremental_mode}\nState path: {uri}")  # fmt: skip
    if state is None:
        print("State exists: no\nTracked files: 0\nActive claims: 0")
        return 0
    print(f"State exists: yes\nTracked files: {len(state.files)}\nActive claims: {len(state.claims)}")  # fmt: skip
    print(f"Updated at: {state.updated_at or '(unknown)'}\n")
    print(dump_state(state).decode())
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
    run.add_argument("--dry-run", action="store_true")
    run.add_argument("--log-format", choices=("off", "text", "json"), default="off")
    verbosity = run.add_mutually_exclusive_group()
    verbosity.add_argument("--quiet", action="store_true")
    verbosity.add_argument("--verbose", action="store_true")
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
    generate.add_argument("-p", "--profile")
    generate.add_argument("--deterministic", action="store_true")
    generate.add_argument("--manifest-name")
    generate.add_argument("--default-domain")
    generate.add_argument("--manifest-path-mode", default="default")
    generate.add_argument("--runtime", choices=("image", "cli"))
    generate.add_argument(
        "--entities", action="extend", default=[], type=lambda v: v.split(",")
    )
    add = commands.add_parser("add-entity")
    add.add_argument("-c", "--config", required=True)
    add.add_argument("--input", required=True)
    add.add_argument("--format", choices=("csv", "json", "parquet"))
    for flag in ("--name", "--domain", "--resource", "--output"):
        add.add_argument(flag)
    add.add_argument("--source", dest="source_ref")
    add.add_argument("--dry-run", action="store_true")
    state = commands.add_parser("state").add_subparsers(dest="state_command", required=True)  # fmt: skip
    for name in ("inspect", "reset"):
        command = state.add_parser(name)
        command.add_argument("-c", "--config", required=True)
        command.add_argument("--entity", required=True)
        command.add_argument("-p", "--profile")
        if name == "reset":
            command.add_argument("--yes", action="store_true")
    return parser


def main() -> int:
    args, extra = _parser().parse_known_args()
    if extra:  # clap's wording for an unknown flag
        print(f"error: unexpected argument '{extra[0]}' found\n\nFor more information, try '--help'.", file=sys.stderr)  # fmt: skip
        return 2
    try:
        if args.command == "manifest":
            return _manifest(args)
        if args.command == "add-entity":
            return _add_entity(args)
        if args.command == "state":
            return _state(args)
        return _run(args) if args.command == "run" else _validate(args)
    except FloeError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
