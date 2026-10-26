"""Path and URI helpers, ported from `io/storage/core/{paths,uri}.rs`."""

from pathlib import Path

from floe.errors import FloeConfigError


def parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise FloeConfigError(f"expected s3 uri, got {uri}")
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    if not bucket:
        raise FloeConfigError(f"missing bucket in s3 uri: {uri}")
    return bucket, key


def normalize_local_path(path: Path) -> Path:
    """Collapse `.` and `..` lexically, but keep a `..` that follows a symlink."""
    out = Path(path.anchor)
    for part in path.parts[1 if path.anchor else 0 :]:
        if part != "..":
            out /= part
        elif out.name not in ("", "..") and not out.is_symlink():
            out = out.parent
        elif not (path.is_absolute() and out == Path(out.anchor)):
            out /= ".."
    if out == Path():
        return Path(path.anchor or ".")
    return out


_MAX_FILENAME_COMPONENT_BYTES = 255
_MAX_ARCHIVE_RUN_COMPONENT_BYTES = 48


def fnv1a_hex(value: str) -> str:
    h = 0xCBF29CE484222325
    for byte in value.encode():
        h = ((h ^ byte) * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return f"{h:016x}"


def _truncate_utf8(value: str, max_bytes: int) -> str:
    return value.encode()[:max_bytes].decode(errors="ignore")


def _run_component(run_id: str) -> str:
    safe = "".join(
        c if c.isascii() and (c.isalnum() or c in "-_.") else "_" for c in run_id
    )
    safe = safe.strip("_") or "run"
    if len(safe) <= _MAX_ARCHIVE_RUN_COMPONENT_BYTES:
        return safe
    return f"{_truncate_utf8(safe, _MAX_ARCHIVE_RUN_COMPONENT_BYTES)}_{fnv1a_hex(run_id)[:8]}"


def archive_relative_path_for_run(
    entity: str, filename: str, run_id: str, source_uri: str
) -> str:
    name = filename.rsplit("/", 1)[-1] or filename
    stem, _, ext = name.rpartition(".")
    if not ext or not stem:
        stem, ext = name, ""
    suffix = f"__run-{_run_component(run_id)}__src-{fnv1a_hex(source_uri)}"
    ext_suffix = f".{ext}" if ext else ""
    available = _MAX_FILENAME_COMPONENT_BYTES - len(suffix) - len(ext_suffix.encode())
    archived = f"{_truncate_utf8(stem, max(available, 0))}{suffix}{ext_suffix}"
    entity = entity.strip("/")
    return f"{entity}/{archived}" if entity else archived
