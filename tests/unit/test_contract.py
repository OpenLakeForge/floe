"""The 1.0 public contract, written out literally (#1368).

Every value below is a snapshot of what Floe 1.0 freezes: error codes, `floe.__all__`, the
parameter names of the public functions, and the CLI commands and their flags. Nothing is
generated from the code, so renaming or removing an item fails here. Adding one passes:
extend the expected values in this file in the same PR (a minor release that adds a code, a
public name, a parameter, a command or a flag also adds it here).

A failure means a breaking change. See the stability policy, docs/reference/stability.md:
frozen items wait for 2.0 and are deprecated first.
"""

import argparse
import inspect
from typing import cast, get_args

import pytest

import floe
from floe._cli import (
    _parser,  # pyright: ignore[reportPrivateUsage] -- the parser is what is under test
)
from floe.errors import ErrorCode

POLICY = "docs/reference/stability.md"

FROZEN_CODES = frozenset({
    "config_invalid_yaml", "config_unknown_key", "config_missing_key", "config_invalid_value",
    "config_not_supported", "config_unknown_reference", "config_duplicate", "config_conflict",
    "config_version",
    "io_read_error", "io_write_error", "io_not_found", "io_schema_mismatch", "io_invalid_option",
    "storage_not_found", "storage_access_denied", "storage_conflict", "storage_error",
    "run_state_conflict", "run_commit_failed", "run_schema_evolution", "run_catalog_error",
    "run_merge_key", "run_aborted", "run_not_supported", "run_invalid_input", "run_invalid_setup",
    "run_archive_failed",
})  # fmt: skip

FROZEN_ALL = frozenset({
    "EntityConfig", "EntityFinishedEvent", "EntityStartedEvent", "FileFinishedEvent",
    "FileStartedEvent", "FloeConfigError", "FloeError", "FloeIoError", "FloeRunError",
    "FloeStorageError", "LogEvent", "RootConfig", "RunEvent", "RunFinishedEvent", "RunOutcome",
    "RunStartedEvent", "SchemaEvolutionAppliedEvent", "__version__", "build_manifest",
    "clear_observer", "extract_config_env_vars", "inspect_entity_state", "load_config",
    "maintain", "reset_entity_state", "run", "run_manifest", "set_observer", "validate",
})  # fmt: skip

# Parameter names per public function. New optional parameters are allowed.
FROZEN_PARAMS: dict[str, tuple[str, ...]] = {
    "build_manifest": (
        "config_path", "profile_path", "deterministic", "manifest_name", "default_domain",
        "path_mode", "runtime", "manifest_uri", "entities",
    ),
    "clear_observer": (),
    "extract_config_env_vars": ("config_path",),
    "inspect_entity_state": ("config_path", "entity_name"),
    "load_config": ("config_path",),
    "maintain": (
        "config_path", "entities", "older_than_days", "retain_last", "profile_vars",
        "profile_path", "orphan_delay_days", "dry_run", "compact", "experimental",
    ),
    "reset_entity_state": ("config_path", "entity_name"),
    "run": (
        "config_path", "entities", "dry_run", "run_id", "full_refresh", "profile_vars",
        "profile_path",
    ),
    "run_manifest": ("manifest_path", "entities", "dry_run", "run_id", "full_refresh"),
    "set_observer": ("callback",),
    "validate": ("config_path", "entities", "profile_vars", "profile_path"),
}  # fmt: skip

# Option strings per command path (`-h/--help` aside). New flags and commands are allowed.
FROZEN_CLI: dict[str, frozenset[str]] = {
    "": frozenset({"--version"}),
    "validate": frozenset({"-c", "--config", "--entities", "-p", "--profile"}),
    "run": frozenset({
        "-c", "--config", "--manifest", "--domain", "--run-id", "-F", "--full-refresh",
        "--dry-run", "--log-format", "--quiet", "--verbose", "--entities", "-p", "--profile",
    }),
    "manifest generate": frozenset({
        "-c", "--config", "-o", "--output", "-p", "--profile", "--deterministic",
        "--manifest-name", "--default-domain", "--manifest-path-mode", "--runtime", "--entities",
    }),
    "add-entity": frozenset({
        "-c", "--config", "--input", "--format", "--name", "--domain", "--resource", "--output",
        "--source", "--dry-run",
    }),
    "state inspect": frozenset({"-c", "--config", "--entity", "-p", "--profile"}),
    "state reset": frozenset({"-c", "--config", "--entity", "-p", "--profile", "--yes"}),
    "state forget": frozenset({"-c", "--config", "--entity", "-p", "--profile", "--yes", "--file"}),
    "maintain": frozenset({
        "-c", "--config", "-p", "--profile", "--entities", "--older-than-days", "--retain-last",
        "--orphan-delay-days", "--dry-run", "--compact", "--experimental",
    }),
}  # fmt: skip

# The `--log-format` values connectors pass (`floe run --manifest ... --log-format json`).
FROZEN_LOG_FORMATS = ("off", "text", "json")


def _walk(
    parser: argparse.ArgumentParser, path: str = ""
) -> dict[str, argparse.ArgumentParser]:
    found = {path: parser}
    for action in parser._actions:  # pyright: ignore[reportPrivateUsage] -- argparse has no public action list
        if isinstance(action, argparse._SubParsersAction):  # pyright: ignore[reportPrivateUsage] -- same
            choices = cast("dict[str, argparse.ArgumentParser]", action.choices)  # pyright: ignore[reportUnknownMemberType] -- typeshed leaves choices unparametrised
            for name, sub in choices.items():
                found.update(_walk(sub, f"{path} {name}".strip()))
    return found


def _options(parser: argparse.ArgumentParser) -> set[str]:
    return {o for a in parser._actions for o in a.option_strings}  # pyright: ignore[reportPrivateUsage] -- see _walk


def test_frozen_error_codes_still_exist() -> None:
    missing = FROZEN_CODES - set(get_args(ErrorCode))
    assert not missing, (
        f"error codes removed or renamed (breaking, see {POLICY}): {sorted(missing)}"
    )


def test_frozen_names_are_still_exported() -> None:
    missing = FROZEN_ALL - set(floe.__all__)
    assert not missing, (
        f"floe.__all__ lost names (breaking, see {POLICY}): {sorted(missing)}"
    )
    unimportable = sorted(n for n in FROZEN_ALL if not hasattr(floe, n))
    assert not unimportable, (
        f"names in the contract are not attributes of floe: {unimportable}"
    )


def test_every_public_function_is_pinned() -> None:
    public = {n for n in floe.__all__ if inspect.isfunction(getattr(floe, n))}
    # A new public function must add its parameters to FROZEN_PARAMS in the same PR.
    assert public == set(FROZEN_PARAMS), (
        f"public functions and FROZEN_PARAMS differ ({POLICY})"
    )


@pytest.mark.parametrize("name", sorted(FROZEN_PARAMS))
def test_public_function_keeps_its_parameters(name: str) -> None:
    params = tuple(inspect.signature(getattr(floe, name)).parameters)
    missing = [p for p in FROZEN_PARAMS[name] if p not in params]
    assert not missing, (
        f"floe.{name} lost parameters (breaking, see {POLICY}): {missing}"
    )
    # Existing parameters keep their order, so positional callers keep working.
    kept = [p for p in params if p in FROZEN_PARAMS[name]]
    assert kept == list(FROZEN_PARAMS[name]), (
        f"floe.{name} reordered parameters ({POLICY})"
    )


@pytest.mark.parametrize("command", sorted(FROZEN_CLI))
def test_cli_command_keeps_its_flags(command: str) -> None:
    commands = _walk(_parser())
    assert command in commands, (
        f"CLI command `floe {command}` is gone (breaking, see {POLICY})"
    )
    missing = FROZEN_CLI[command] - _options(commands[command])
    assert not missing, (
        f"`floe {command}` lost flags (breaking, see {POLICY}): {sorted(missing)}"
    )


def test_cli_log_format_keeps_its_values() -> None:
    run = _walk(_parser())["run"]
    (action,) = (a for a in run._actions if "--log-format" in a.option_strings)  # pyright: ignore[reportPrivateUsage] -- see _walk
    missing = set(FROZEN_LOG_FORMATS) - set(action.choices or ())
    assert not missing, (
        f"`floe run --log-format` lost values (breaking, see {POLICY}): {sorted(missing)}"
    )
