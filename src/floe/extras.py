"""Optional backends are `floe-python[<extra>]` extras: imported where used, and a missing one names its extra."""

from importlib.util import find_spec

from floe.errors import FloeConfigError


def missing(extra: str, needed_by: str) -> FloeConfigError:
    return FloeConfigError(f"{needed_by} needs the floe[{extra}] extra: pip install 'floe-python[{extra}]'")  # fmt: skip


def require(extra: str, module: str, needed_by: str) -> None:
    """Raise unless `module` is installed; for validation, which must not import the backend."""
    if find_spec(module) is None:
        raise missing(extra, needed_by)
