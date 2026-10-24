"""Variable resolution, ported from `config/template.rs`, `config/parse.rs` (`extract_raw_env_vars`) and `vars/resolve.rs`."""

from collections.abc import Mapping
from contextlib import suppress
from pathlib import Path

from floe.config.yaml import load_yaml, rust_debug, yaml_hash, yaml_string
from floe.errors import FloeConfigError, FloeError
from floe.storage.paths import normalize_local_path


def _resolve_local_path(config_dir: Path, raw: str) -> Path:
    return normalize_local_path(config_dir / raw)


def string_map(value: object, ctx: str) -> dict[str, str]:
    return {
        yaml_string(k, ctx): yaml_string(v, ctx)
        for k, v in yaml_hash(value, ctx).items()
    }


def replace_placeholders(
    value: str, variables: Mapping[str, str], field: str, entity: str | None = None
) -> str:
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
    merged = dict(profile_vars)
    if env_file is not None:
        path = _resolve_local_path(config_dir, env_file)
        docs = load_yaml(path)
        if not docs:
            raise FloeConfigError(f"env file {path} is empty")
        merged |= string_map(docs[0], "env.file")
    return merged | dict(env_vars)


def extract_config_env_vars(path: Path) -> dict[str, str]:
    variables: dict[str, str] = {}
    with suppress(FloeError):
        docs = load_yaml(path)
        root = yaml_hash(docs[0], "root") if docs else {}
        env = yaml_hash(root.get("env"), "env")
        with suppress(FloeError, IndexError):
            file = _resolve_local_path(
                path.parent, yaml_string(env.get("file"), "env.file")
            )
            for k, v in yaml_hash(load_yaml(file)[0], "env.file").items():
                if isinstance(k, str) and isinstance(v, str):
                    variables[k] = v
        with suppress(FloeError, KeyError):
            variables |= string_map(env["vars"], "env.vars")
    return variables


def resolve_vars(
    profile: Mapping[str, str], cli: Mapping[str, str], config: Mapping[str, str]
) -> dict[str, str]:
    raw = dict(profile) | dict(cli) | dict(config)
    resolved: dict[str, str] = {}
    in_progress: list[str] = []

    def expand_key(key: str, depth: int) -> str:
        if key in resolved:
            return resolved[key]
        if depth > 32:
            raise FloeConfigError(
                f"variable expansion exceeded maximum depth (32); "
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
                quoted = rust_debug(value)
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
