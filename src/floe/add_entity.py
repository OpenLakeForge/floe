"""`floe add-entity`, ported from `crates/floe-core/src/add_entity.rs`: infer an entity from a sample file."""

import math
import os
import re
import secrets
import stat
import tempfile
import urllib.parse
import urllib.request
from pathlib import Path
from typing import NamedTuple, cast

import polars as pl

from floe import __version__
from floe.config.include import glob_base
from floe.config.parse import parse_config
from floe.config.project import V0_3, config_version, validate_domains
from floe.config.validate import validate_entities
from floe.config.yaml import _FLOAT, load_yaml, rust_debug  # pyright: ignore[reportPrivateUsage] -- the YAML float grammar the emitter must quote
from floe.errors import FloeConfigError, FloeError, FloeIoError
from floe.glob import PatternError, glob

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


class Added(NamedTuple):
    name: str
    format: str
    columns: int
    output: str
    rendered: str | None  # the YAML of a dry run


_WORDS = {"true", "false", "True", "False", "TRUE", "FALSE", "null", "Null", "NULL", "~", "y", "Y", "n", "N"} | {
    "yes", "Yes", "YES", "no", "No", "NO", "on", "On", "ON", "off", "Off", "OFF"
}  # fmt: skip
_SPECIAL = re.compile("[:{}\\[\\],#`\"'\\\\\x00-\x06\t\n\r\x0e-\x1a\x1c-\x1f]")
_ESCAPES = {i: f"\\u{i:04x}" for i in (*range(0x20), 0x7F)} | {
    0x22: '\\"', 0x5C: "\\\\", 0x08: "\\b", 0x09: "\\t", 0x0A: "\\n", 0x0C: "\\f", 0x0D: "\\r"
}  # fmt: skip


def _scalar(value: object) -> str:
    """yaml-rust2 `YamlEmitter::emit_node` for a scalar."""
    if isinstance(value, str):
        quote = (
            not value or value[0] == " " or value[-1] == " " or value[0] in "&*?|-<>=!%@"
            or bool(_SPECIAL.search(value)) or value in _WORDS or value.startswith((".", "0x"))
            or bool(_FLOAT.fullmatch(value)) or value.lstrip("+-").lower() in ("inf", "infinity", "nan")
        )  # fmt: skip
        return f'"{value.translate(_ESCAPES)}"' if quote else value
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, float):
        # ponytail: yaml-rust2 re-emits a float's source text (`1.50`); this writes `1.5`
        if math.isnan(value):
            return ".nan"
        return {math.inf: ".inf", -math.inf: "-.inf"}.get(value, repr(value))
    return "~" if value is None else str(value)


def _node(value: object, level: int) -> str:
    if isinstance(value, list):
        items = cast("list[object]", value)
        if not items:
            return "[]"
        sep = "\n" + "  " * (level + 1)
        return sep.join("-" + _val(True, x, level + 1) for x in items)
    if isinstance(value, dict):
        pairs = cast(Hash, value)
        if not pairs:
            return "{}"
        sep = "\n" + "  " * (level + 1)
        return sep.join(_scalar(k) + ":" + _val(False, v, level + 1) for k, v in pairs.items())  # fmt: skip
    return _scalar(value)


def _val(inline: bool, value: object, level: int) -> str:
    text = _node(value, level)
    block = not inline and text not in ("[]", "{}") and isinstance(value, (list, dict))
    return ("\n" + "  " * (level + 1) if block else " ") + text


def _render(doc: object) -> str:
    """yaml-rust2 `YamlEmitter::dump` without the `---` line, as Rust's `render_yaml`."""
    return _node(doc, -1) + "\n"


def _included_domain(
    config: str, root: object, domain: str
) -> tuple[str, object] | None:
    """The directory and `defaults` of the included domain file named `domain`."""
    include = cast(Hash, root).get("include") if isinstance(root, dict) else None
    patterns = cast(Hash, include).get("domains") if isinstance(include, dict) else None
    if not isinstance(patterns, list):
        return None
    parent = os.path.dirname(config)
    base = glob_base(Path(parent)) if parent else ""
    for pattern in cast("list[object]", patterns):
        if not isinstance(pattern, str):
            continue
        try:
            matches = glob(os.path.join(base, pattern))
        except PatternError as e:
            raise FloeConfigError(f"invalid include.domains pattern {rust_debug(pattern)} in {config}: {e}") from e  # fmt: skip
        except FloeError as e:
            raise FloeConfigError(f"failed to match include.domains pattern {rust_debug(pattern)} in {config}: {e}") from e  # fmt: skip
        for file in matches:
            try:
                docs: list[object] = load_yaml(Path(file))
            except FloeIoError as e:
                raise FloeIoError(f"failed to read domain file: {e}") from e
            except FloeError as e:
                raise FloeConfigError(f"failed to parse domain file {file}: {e}") from e
            doc = docs[0] if docs else None
            if isinstance(doc, dict) and cast(Hash, doc).get("name") == domain:
                return os.path.dirname(file), cast(Hash, doc).get("defaults", {})
    return None


def _strip_defaults(entity: Hash, defaults: Hash) -> None:
    for key, default in defaults.items():
        if key not in entity:
            continue
        value = entity[key]
        if isinstance(value, dict) and isinstance(default, dict):
            _strip_defaults(cast(Hash, value), cast(Hash, default))
            if value:
                continue
        del entity[key]


def _validate(config: Path, extra_entity: Hash | None = None) -> None:
    root = parse_config(config, extra_entity=extra_entity)
    validate_domains(root)
    validate_entities(root)


def _validate_text(text: str, target: str) -> None:
    """Validate `text` as a config placed next to `target` (Rust `validate_generated_config`)."""
    try:
        fd, tmp = tempfile.mkstemp(prefix=".tmp", dir=os.path.dirname(target) or ".")
    except OSError:
        fd, tmp = tempfile.mkstemp(prefix=".tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(text.encode())
        _validate(Path(tmp))
    finally:
        os.unlink(tmp)


def _write_new(path: str, text: str) -> None:
    """Stage a temp file and hard-link it in, so the entity file appears whole and is never clobbered."""
    tmp = os.path.join(
        os.path.dirname(path), f".floe-add-entity-{secrets.token_hex(6)}.yml"
    )
    try:
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666)
        with os.fdopen(fd, "wb") as f:
            f.write(text.encode())
    except OSError as e:
        raise FloeIoError(f"failed to stage entity file: {_os(e)}") from e
    try:
        os.link(tmp, path)
    except OSError as e:
        raise FloeIoError(f"failed to write entity file: {_os(e)}") from e
    finally:
        os.unlink(tmp)


def add_entity(
    config: str, input: str, fmt: str | None, name: str | None, domain: str | None,
    source_ref: str | None, resource: str | None, output: str | None, dry_run: bool,
) -> Added:  # fmt: skip
    if (source_ref is None) != (resource is None):
        raise FloeConfigError("--source and --resource must be provided together")
    entity, fmt = infer_entity(input, fmt, name, domain, source_ref, resource)
    name = cast(str, entity["name"])
    columns = len(cast("list[object]", cast(Hash, entity["schema"])["columns"]))

    exists = os.path.exists(config)
    try:  # D21 (#619): a new config targets this Floe major.minor
        docs: list[object] = load_yaml(Path(config)) if exists else [{"version": ".".join(__version__.split(".")[:2]), "entities": []}]  # fmt: skip
    except FloeIoError as e:
        raise FloeIoError(f"failed to read config at {config}: {e}") from e
    included = None
    if output is None and domain is not None and exists:
        included = _included_domain(config, docs[0] if docs else None, domain)
    if included is not None:
        directory, defaults = included
        if "/" in name or "\\" in name:
            raise FloeConfigError(f"entity name cannot contain a path separator for an included domain at {directory}")  # fmt: skip
        output = os.path.join(directory, f"{name}.yml")
        if os.path.exists(output):
            raise FloeIoError(f"refusing to overwrite existing entity file at {output}")
        if isinstance(defaults, dict):
            # Rust first drops `name`, `schema.columns` and `primary_key` from the defaults; D7 rejects those
            _strip_defaults(entity, cast(Hash, defaults))
            entity.pop("domain", None)
        rendered = _render(entity)
        try:
            _validate(Path(config), {**entity, "domain": domain})
        except FloeError as e:
            raise FloeConfigError(f"generated entity file {output} failed validation: {e}") from e  # fmt: skip
        if not dry_run:
            _write_new(output, rendered)
        return Added(name, fmt, columns, output, rendered if dry_run else None)

    if len(docs) > 1:
        raise FloeConfigError("YAML contains multiple documents; expected one")
    root = docs[0] if docs else None
    if not isinstance(root, dict):
        raise FloeConfigError("config root must be a YAML mapping")
    root = cast(Hash, root)
    version = root.get("version")
    try:  # 0.3+ entity identity is (domain, name) (#464); older configs compare names only
        qualified = isinstance(version, str) and config_version(version) >= V0_3
    except FloeConfigError:
        qualified = False
    if not isinstance(entities := root.setdefault("entities", []), list):
        raise FloeConfigError("root.entities must be a YAML sequence")
    entities = cast("list[object]", entities)

    def field(e: object, key: str) -> object:
        value = cast(Hash, e).get(key) if isinstance(e, dict) else None
        return value if isinstance(value, str) else None

    for e in entities:
        if field(e, "name") == name and (not qualified or field(e, "domain") == domain):
            raise FloeConfigError(f"entity already exists: {name}")
    entities.append(entity)
    rendered = _render(root)
    output = config if output is None else output
    if not dry_run and (parent := os.path.dirname(output)):
        os.makedirs(parent, exist_ok=True)
    _validate_text(rendered, config if dry_run else output)
    if not dry_run:
        try:
            Path(output).write_bytes(rendered.encode())
        except OSError as e:
            raise FloeIoError(f"failed to write config at {output}: {_os(e)}") from e
    return Added(name, fmt, columns, output, rendered if dry_run else None)
