"""`floe add-entity`, ported from `crates/floe-core/src/add_entity.rs`: infer an entity from a sample file."""

import os
import re
import stat
import urllib.parse
import urllib.request
from pathlib import Path
from typing import cast

import polars as pl

from floe.errors import FloeConfigError, FloeIoError

SAMPLE_ROWS = 200
Hash = dict[object, object]


def _os(e: OSError) -> str:
    return f"{e.strerror} (os error {e.errno})"


def _file_uri_path(input: str) -> str:
    url = urllib.parse.urlparse(input)
    if url.netloc not in ("", "localhost"):
        raise FloeConfigError(f"invalid file:// URI for add-entity input: {input}")
    return urllib.request.url2pathname(url.path)


def _entity_name(input: str) -> str:
    scheme, sep, _ = input.partition("://")
    if sep and scheme.lower() != "file":
        raw = input.rstrip("/").rsplit("/", 1)[-1].split("?")[0]
    else:
        path = Path(_file_uri_path(input) if sep else input)
        raw = path.stem if path.name not in ("", "..") else "entity"
    name = re.sub("[^A-Za-z0-9]+", "_", raw).strip("_").lower()
    if not name:
        raise FloeConfigError(f"failed to derive entity name from input: {input}")
    return name


def _local_path(input: str) -> str:
    scheme, sep, _ = input.partition("://")
    if sep:
        if scheme.lower() != "file":
            raise FloeConfigError(
                f"remote URI inference is not supported yet for add-entity input: {input}"
            )
        return _file_uri_path(input)
    path = os.path.join(os.getcwd(), input)
    try:
        mode = os.stat(path).st_mode
    except OSError as e:
        raise FloeIoError(f"failed to access input at {path}: {_os(e)}") from e
    if stat.S_ISDIR(mode):
        raise FloeConfigError(
            f"add-entity expects a file input for schema inference, got directory: {path}"
        )
    return path


def _sniff_separator(path: str) -> str:
    """Rust `sniff_delimiter`: the candidate most consistent over the first non-blank lines."""
    lines: list[str] = []
    try:
        with open(path, "rb") as f:
            for _, raw in zip(range(12), f, strict=False):
                if line := raw.decode().strip():
                    lines.append(line)
                if len(lines) >= 6:
                    break
    except (OSError, UnicodeDecodeError):
        return ","
    best: tuple[int, str] | None = None
    for candidate in ",;\t|":
        counts = [line.count(candidate) for line in lines]
        if not counts or counts[0] == 0:
            continue
        bonus = 10_000 if min(counts) > 0 and max(counts) == min(counts) else 0
        score = bonus + min(counts) * 100 + sum(counts)
        if best is None or score > best[0]:
            best = (score, candidate)
    return best[1] if best else ","


def _floe_type(dtype: pl.DataType) -> str:
    for kind, name in ((pl.Boolean, "boolean"), (pl.Date, "date"), (pl.Time, "time"), (pl.Datetime, "datetime")):  # fmt: skip
        if dtype == kind:
            return name
    return "number" if dtype.is_numeric() else "string"


def _columns(fmt: str, path: str) -> tuple[Hash | None, list[Hash]]:
    """(source options, schema columns), from Polars' own schema inference."""
    if fmt == "csv":
        separator = _sniff_separator(path)
        try:
            df = pl.read_csv(path, separator=separator, n_rows=SAMPLE_ROWS, infer_schema_length=SAMPLE_ROWS)  # fmt: skip
        except (OSError, pl.exceptions.PolarsError) as e:
            raise FloeIoError(f"csv schema inference failed: {e}") from e
        nullable = {c.name: df.height == 0 or c.null_count() > 0 for c in df.iter_columns()}  # fmt: skip
        schema, options = df.schema, cast(Hash, {"separator": separator})
    elif fmt == "parquet":
        try:
            schema = pl.read_parquet_schema(path)
        except (OSError, pl.exceptions.PolarsError) as e:
            raise FloeIoError(f"failed to read parquet schema at {path}: {e}") from e
        nullable, options = dict.fromkeys(schema, True), None
    else:  # ponytail: Rust infers JSON by hand (dates in strings, mixed kinds); needs a decision, see #852
        raise FloeConfigError(f"{fmt} inference is not supported yet for add-entity")
    return options, [
        {
            "name": n,
            "source": n,
            "type": _floe_type(t),
            "nullable": nullable[n],
            "unique": False,
        }
        for n, t in schema.items()
    ]


def infer_entity(
    input: str, fmt: str | None, name: str | None, domain: str | None,
    source_ref: str | None, resource: str | None,
) -> tuple[Hash, str]:  # fmt: skip
    """The entity generated for a sample file, and its format (Rust `infer_entity_from_input`)."""
    name = _entity_name(input) if name is None else name
    if not name.strip():
        raise FloeConfigError("entity name must not be empty")
    path = _local_path(input)
    if fmt is None:
        # `.csv` alone has no extension, as in Rust
        fmt = os.path.splitext(path)[1][1:].lower()
        if fmt not in ("csv", "json", "parquet"):
            raise FloeConfigError(
                f"could not infer add-entity format from input {input} "
                "(supported extensions: .csv, .json, .parquet); use --format"
            )
    options, columns = _columns(fmt, path)
    if not columns:
        raise FloeConfigError(f"no columns inferred from {path}")
    if source_ref is not None:
        source: Hash = {"ref": source_ref, "resource": resource}
    else:
        persisted = path if input.partition("://")[1] else input
        source = {"format": fmt, "path": persisted}
        if options is not None:
            source["options"] = options
    entity: Hash = {
        "name": name,
        **({} if domain is None else {"domain": domain}),
        "source": source,
        "sink": {
            "accepted": {"format": "parquet", "path": f"out/accepted/{name}/"},
            "rejected": {"format": "csv", "path": f"out/rejected/{name}/"},
        },
        "policy": {"severity": "reject"},
        "schema": {
            "mismatch": {"missing_columns": "reject_file", "extra_columns": "ignore"},
            "columns": columns,
        },
    }
    return entity, fmt
