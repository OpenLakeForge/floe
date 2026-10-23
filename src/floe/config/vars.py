"""Variable resolution, ported from `config/template.rs`, `config/parse.rs` (`extract_raw_env_vars`) and `vars/resolve.rs`."""

import json
import os
from collections.abc import Mapping
from pathlib import Path

from floe.config.yaml import load_yaml, yaml_hash, yaml_string
from floe.errors import FloeConfigError, FloeError

_MAX_DEPTH = 32


def _resolve_local_path(config_dir: Path, raw: str) -> Path:
    # ponytail: lexical `..` collapse only; Rust keeps `..` after a symlink. Fix with the storage port.
    return Path(os.path.normpath(config_dir / raw))


def _string_map(value: object, ctx: str) -> dict[str, str]:
    return {
        yaml_string(k, ctx): yaml_string(v, ctx)
        for k, v in yaml_hash(value, ctx).items()
    }


def replace_placeholders(
    value: str, variables: Mapping[str, str], field: str, entity: str | None = None
) -> str:
    """Substitute `{{ KEY }}` placeholders."""
    prefix = f"entity.name={entity} " if entity is not None else ""
    result: list[str] = []
    rest = value
    while (start := rest.find("{{")) != -1:
        result.append(rest[:start])
        rest = rest[start + 2 :]
        end = rest.find("}}")
        if end == -1:
            raise FloeConfigError(f"{prefix}{field} missing closing '}}'")
        key = rest[:end].strip()
        if not key:
            raise FloeConfigError(f"{prefix}{field} empty placeholder")
        if key not in variables:
            raise FloeConfigError(f"{prefix}{field} references unknown variable {key}")
        result.append(variables[key])
        rest = rest[end + 2 :]
    return "".join(result) + rest


def build_env_vars(
    config_dir: Path,
    env_file: str | None,
    env_vars: Mapping[str, str],
    profile_vars: Mapping[str, str],
) -> dict[str, str]:
    """Merge profile variables, then `env.file`, then inline `env.vars` (last wins)."""
    merged = dict(profile_vars)
    if env_file is not None:
        path = _resolve_local_path(config_dir, env_file)
        docs = load_yaml(path)
        if not docs:
            raise FloeConfigError(f"env file {path} is empty")
        merged |= _string_map(docs[0], "env.file")
    return merged | dict(env_vars)


def extract_config_env_vars(path: Path) -> dict[str, str]:
    """`env.file` and `env.vars` of a config, ignoring anything malformed (never raises)."""
    try:
        docs = load_yaml(path)
        root = yaml_hash(docs[0], "root") if docs else {}
        env = yaml_hash(root.get("env"), "env")
        variables: dict[str, str] = {}
        file = env.get("file")
        if isinstance(file, str):
            try:
                file_doc = load_yaml(_resolve_local_path(path.parent, file))[0]
                variables |= {
                    k: v
                    for k, v in yaml_hash(file_doc, "env.file").items()
                    if isinstance(k, str) and isinstance(v, str)
                }
            except (FloeError, IndexError):
                pass
        if "vars" in env:
            try:
                variables |= _string_map(env["vars"], "env.vars")
            except FloeError:
                pass
        return variables
    except FloeError:
        return {}


def resolve_vars(
    profile: Mapping[str, str], cli: Mapping[str, str], config: Mapping[str, str]
) -> dict[str, str]:
    """Merge `config > cli > profile`, then expand `${KEY}` references in every value."""
    raw = dict(profile) | dict(cli) | dict(config)
    resolved: dict[str, str] = {}
    in_progress: list[str] = []

    def expand_key(key: str, depth: int) -> str:
        if key in resolved:
            return resolved[key]
        if depth > _MAX_DEPTH:
            raise FloeConfigError(
                f"variable expansion exceeded maximum depth ({_MAX_DEPTH}); "
                f'check for deeply nested or circular references near "${{{key}}}"'
            )
        if key in in_progress:
            chain = " -> ".join([*in_progress, key])
            raise FloeConfigError(f"circular variable reference detected: {chain}")
        if key not in raw:
            raise FloeConfigError(
                f'variable "${{{key}}}" is referenced but not defined'
            )
        in_progress.append(key)
        resolved[key] = expand_value(key, depth + 1)
        in_progress.pop()
        return resolved[key]

    def expand_value(owner: str, depth: int) -> str:
        value = rest = raw[owner]
        result: list[str] = []
        while (start := rest.find("${")) != -1:
            result.append(rest[:start])
            rest = rest[start + 2 :]
            end = rest.find("}")
            if end == -1:
                quoted = json.dumps(value, ensure_ascii=False)
                raise FloeConfigError(
                    f'variable "{owner}": unclosed placeholder in value {quoted}'
                )
            ref = rest[:end].strip()
            if not ref:
                raise FloeConfigError(f'variable "{owner}": empty placeholder ${{}}')
            result.append(expand_key(ref, depth))
            rest = rest[end + 1 :]
        return "".join(result) + rest

    for key in raw:
        expand_key(key, 0)
    return resolved
