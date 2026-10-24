"""Input resolution, ported from `io/storage/ops/inputs.rs` (`resolve_inputs`)."""

import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePath
from typing import Literal

from floe.errors import FloeConfigError, FloeError, FloeRunError
from floe.glob import MatchOptions, Pattern, PatternError, glob
from floe.storage.paths import normalize_local_path, parse_s3_uri
from floe.storage.types import ObjectRef, StorageClient

_SUFFIXES = {
    "csv": [".csv"],
    "tsv": [".tsv"],
    "parquet": [".parquet"],
    "json": [".json", ".jsonl", ".ndjson", ".djson"],
    "avro": [".avro"],
    "orc": [".orc"],
    "xlsx": [".xlsx"],
    "xml": [".xml"],
}


@dataclass(frozen=True)
class InputFile:
    source_uri: str
    source_name: str
    source_stem: str
    source_size: int | None
    source_mtime: str | None


@dataclass(frozen=True)
class ResolvedInputs:
    files: list[InputFile]
    listed: list[str]
    mode: Literal["file", "directory"]


def suffixes_for_format(fmt: str) -> list[str]:
    if fmt not in _SUFFIXES:
        raise FloeConfigError(f"unsupported source format for input resolution: {fmt}")
    return _SUFFIXES[fmt]


def _rfc3339(ns: int) -> str:
    secs, nanos = divmod(ns, 10**9)
    text = datetime.fromtimestamp(secs, UTC).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{text}.{nanos:09d}".rstrip("0") + "Z" if nanos else f"{text}Z"


def _glob_files(pattern: str) -> list[Path]:
    try:
        found = glob(pattern)
    except PatternError as e:
        raise FloeConfigError(f'invalid glob pattern "{pattern}": {e}') from e
    except FloeError as e:
        raise FloeConfigError(f'glob match failed for "{pattern}": {e}') from e
    return [normalize_local_path(Path(p)) for p in found if Path(p).is_file()]


def _local_files(
    config_dir: Path,
    raw: str,
    fmt: str,
    entity: str,
    storage: str,
    glob_override: str | None,
    recursive: bool,
) -> tuple[list[Path], Literal["file", "directory"]]:
    def no_match(base: object, used: str) -> FloeRunError:
        return FloeRunError(
            f"entity.name={entity} source.storage={storage} no input files matched "
            f"(base_path={base}, glob={used}, recursive={str(recursive).lower()})"
        )

    if any(c in raw for c in "*?["):
        pattern = os.path.join(config_dir, raw)
        files = sorted(set(_glob_files(pattern)), key=str)
        if not files:
            raise no_match(Path(pattern).parent, Path(pattern).name)
        return files, "directory"
    base = normalize_local_path(config_dir / raw)
    if base.is_file():
        return [base], "file"
    globs = (
        [glob_override]
        if glob_override is not None
        else [
            "*" + "".join(f"[{c.lower()}{c.upper()}]" if c.isalpha() else c for c in s)
            for s in suffixes_for_format(fmt)
        ]
    )
    if not base.is_dir():
        raise no_match(base, ",".join(globs))
    patterns = [
        os.path.join(base, "**", g) if recursive else os.path.join(base, g)
        for g in globs
    ]
    files = sorted({f for p in patterns for f in _glob_files(p)}, key=str)
    if not files:
        raise no_match(base, ",".join(globs))
    return files, "directory"


_CLOUD = MatchOptions(case_sensitive=True, require_literal_separator=True)


def _first_glob(value: str) -> int:
    escaped = False
    for i, ch in enumerate(value):
        if escaped:
            escaped = False
        elif ch == "\\":
            escaped = True
        elif ch in "*?":
            return i
    return -1


def _cloud_refs(
    client: StorageClient,
    key: str,
    suffixes: list[str],
    entity: str,
    storage: str,
    location: str,
) -> list[ObjectRef]:
    where = f"entity.name={entity} source.storage={storage}"
    first = _first_glob(key)
    prefix = key if first == -1 else key[:first]
    matcher = None
    if first != -1:
        try:
            if not prefix.strip("/"):
                raise FloeRunError(
                    "glob patterns for cloud sources must include a non-empty "
                    "literal prefix before the first wildcard"
                )
            try:
                matcher = Pattern(key)
            except PatternError as e:
                raise FloeRunError(f'invalid cloud glob pattern "{key}": {e}') from e
        except FloeRunError as e:
            raise FloeRunError(
                f"{where} invalid cloud source path ({location}, path={key}): {e}"
            ) from e
    lowered = tuple(s.lower() for s in suffixes)
    refs = [
        r
        for r in client.list(prefix)
        if (matcher is None or matcher.matches_with(r.key, _CLOUD))
        and not (r.key.startswith(".floe/") or "/.floe/" in r.key)
        and r.uri.lower().endswith(lowered)
    ]
    if not refs:
        used = "glob=<none>" if matcher is None else f"glob={key}"
        raise FloeRunError(
            f"{where} no input objects matched ({location}, prefix={prefix}, "
            f"{used}, suffixes={','.join(suffixes)})"
        )
    return sorted(refs, key=lambda r: r.uri)


def resolve_inputs(
    config_dir: Path,
    path: str,
    fmt: str,
    entity: str,
    storage: str,
    client: StorageClient | None,
    *,
    list_only: bool = False,
    recursive: bool = False,
    glob_override: str | None = None,
) -> ResolvedInputs:
    """`path` is the source path, or a full `s3://` URI for a remote storage."""
    files: list[InputFile] = []
    if path.startswith("s3://"):
        if client is None:
            raise FloeRunError("s3 storage client missing")
        bucket, key = parse_s3_uri(path)
        refs = _cloud_refs(
            client, key, suffixes_for_format(fmt), entity, storage, f"bucket={bucket}"
        )
        if not list_only:
            for r in refs:
                name = PurePath(r.key).name or entity
                files.append(
                    InputFile(
                        r.uri,
                        name,
                        PurePath(name).stem or entity,
                        r.size,
                        r.last_modified,
                    )
                )
        return ResolvedInputs(files, [r.uri for r in refs], "directory")
    if "://" in path:
        raise FloeConfigError(f"unsupported storage uri: {path}")
    paths, mode = _local_files(
        config_dir, path, fmt, entity, storage, glob_override, recursive
    )
    uris = [client.resolve_uri(str(p)) if client else str(p) for p in paths]
    if not list_only:
        for p, uri in zip(paths, uris, strict=True):
            st = p.stat()
            name = p.name or entity
            files.append(
                InputFile(
                    uri,
                    name,
                    PurePath(name).stem or entity,
                    st.st_size,
                    _rfc3339(st.st_mtime_ns),
                )
            )
    return ResolvedInputs(files, uris, mode)
