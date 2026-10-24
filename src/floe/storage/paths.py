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
