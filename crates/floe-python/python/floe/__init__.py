"""
floe — Python bindings for the floe ingestion engine.

Run floe pipelines at Rust speed from Python notebooks::

    import floe

    # Live progress
    floe.set_observer(lambda e: print(f"[{e['event']}]", e.get("name", "")))

    # Validate config
    floe.validate("pipeline.yml")

    # Execute pipeline
    outcome = floe.run("pipeline.yml", entities=["customers"])
    print(outcome.summary["results"])

    # Dry run
    outcome = floe.run("pipeline.yml", dry_run=True)
    for p in outcome.dry_run_previews:
        print(p["name"], p["scanned_files"])

    # Inspect incremental state
    state = floe.inspect_entity_state("pipeline.yml", "customers")
    print(state["incremental_mode"], state["state"])

    # Use a profile (with catalog / variable overrides)
    floe.run("pipeline.yml", profile_path="profile.yml")
    floe.run("pipeline.yml", profile_vars={"output_bucket": "s3://my-bucket"})
"""

import importlib
import importlib.util
import sys
from importlib.metadata import version as _pkg_version

try:
    __version__ = _pkg_version("floe-python")
except Exception:
    __version__ = "unknown"

from floe._floe import (
    FloeError,
    FloeConfigError,
    FloeRunError,
    FloeStorageError,
    FloeIoError,
    RootConfig,
    EntityConfig,
    RunOutcome,
    validate,
    run as _run,
    load_config,
    extract_config_env_vars,
    inspect_entity_state,
    reset_entity_state,
    set_observer as _lean_set_observer,
    clear_observer as _lean_clear_observer,
)
from floe import _floe as _lean

# DuckDB sinks compile a bundled native build that is too large for PyPI, so the
# published `floe` wheel is lean. DuckDB support ships as a separate off-PyPI
# `floe-duckdb` wheel that installs a `floe._floe_duckdb` companion extension into
# this same package. When a run targets a DuckDB sink and this is the lean build,
# `run` transparently delegates to that companion, mirroring the lean `floe` CLI
# re-execing the `floe-duckdb` binary. Install instructions for the companion:
# `--index-url` (not `--extra-index-url`) so pip resolves the `floe-duckdb`
# name ONLY from the off-PyPI index: `floe-duckdb` is intentionally absent from
# PyPI, and `--extra-index-url` would leave PyPI in play, letting a squatted /
# higher-version `floe-duckdb` there shadow the real companion (dependency
# confusion). The companion's `floe-python` dependency is already satisfied
# here — this hint only fires from an installed lean `floe` — so replacing PyPI
# for this single install does not break dependency resolution.
_DUCKDB_INSTALL_HINT = (
    "this config writes to a DuckDB sink, but the installed `floe` wheel is the "
    "lean build without DuckDB support and the `floe-duckdb` companion wheel is "
    "not installed. Install it from the off-PyPI index, e.g. "
    "`pip install floe-duckdb --index-url "
    "https://openlakeforge.github.io/floe/simple/`, or use the "
    "`ghcr.io/openlakeforge/floe-duckdb` image."
)


# The lean `_floe` and companion `_floe_duckdb` are separate native libraries
# with independent observer state, so the registered callback must be mirrored
# into the companion before a delegated run or its progress/log events are lost.
# Track the active observer here so delegation can re-install it.
_current_observer = None


def set_observer(callback):
    """Register a callback for live run/validation events.

    Mirrors into the DuckDB companion only if it is *already* loaded, so
    delegated DuckDB runs emit the same events as native runs. Registering an
    observer must not trigger a companion import: if the companion is installed
    but its native extension fails to load, that error should surface when a
    DuckDB run is actually attempted, not as a side effect of setting an
    observer. `run` re-installs the tracked observer into the companion right
    before delegating, so a companion loaded later still gets the callback.
    """
    global _current_observer
    _current_observer = callback
    companion = _loaded_duckdb_module()
    if companion is not None:
        companion.set_observer(callback)
    return _lean_set_observer(callback)


def clear_observer():
    """Remove the previously registered event callback."""
    global _current_observer
    _current_observer = None
    companion = _loaded_duckdb_module()
    if companion is not None:
        companion.clear_observer()
    return _lean_clear_observer()


# The companion extension's import path depends on how maturin lays out the
# off-PyPI `floe-duckdb` wheel. The intended layout nests it as the
# `floe._floe_duckdb` submodule, but maturin — building a pure-Rust wheel with no
# `floe/` python-source to nest under — emits the pyo3 `#[pymodule] _floe_duckdb`
# as a TOP-LEVEL `_floe_duckdb` package instead. Accept both names so delegation
# works regardless of the wheel's layout, preferring the nested name when present.
_DUCKDB_MODULE_NAMES = ("floe._floe_duckdb", "_floe_duckdb")


def _loaded_duckdb_module():
    """Return the companion module only if it is already imported, else None.

    Unlike `_duckdb_module`, this never triggers an import, so it cannot raise a
    companion load error. Used on paths (observer registration) where a broken
    companion must not surface until a DuckDB run is actually attempted.
    """
    for name in _DUCKDB_MODULE_NAMES:
        module = sys.modules.get(name)
        if module is not None:
            return module
    return None


def _duckdb_module():
    """Return the `floe-duckdb` companion extension, or None if not installed.

    A genuinely absent companion maps to None. If the companion IS installed but
    its native extension fails to load (incompatible libc, unresolved symbol), the
    loader error is allowed to propagate so it surfaces instead of a misleading
    "companion wheel is not installed" hint. `find_spec` distinguishes the two:
    a bare import would raise a plain ImportError for the absent case too
    (attribute fallback), so the exception type alone cannot tell them apart — but
    `find_spec` returns None only when the module is truly not there.

    The companion may resolve under either the nested `floe._floe_duckdb` name
    (intended layout) or a top-level `_floe_duckdb` (maturin's actual pure-Rust
    output); try both and import the first that resolves.
    """
    for name in _DUCKDB_MODULE_NAMES:
        if importlib.util.find_spec(name) is not None:
            return importlib.import_module(name)
    return None


def run(config_path, *args, **kwargs):
    """Execute a floe pipeline.

    Transparently delegates to the `floe-duckdb` companion when the config
    targets a DuckDB sink and this lean build lacks native DuckDB support.
    """
    # Dry runs resolve inputs and return previews before any sink write, so they
    # never touch the DuckDB writer — run them on the lean build without the
    # companion. Positional order mirrors the native `run`:
    # (config_path, entities, dry_run, run_id, full_refresh, profile_vars, profile_path).
    dry_run = kwargs.get("dry_run", args[1] if len(args) >= 2 else False)
    # The sink format can be selected through a profile variable (e.g.
    # `format: "{{SINK_FORMAT}}"`), so the delegation check must apply the same
    # profile inputs as the run or it would miss the DuckDB sink and run lean.
    profile_vars = kwargs.get("profile_vars", args[4] if len(args) >= 5 else None)
    profile_path = kwargs.get("profile_path", args[5] if len(args) >= 6 else None)
    if (
        not dry_run
        and not getattr(_lean, "HAS_DUCKDB", False)
        and _lean.config_targets_duckdb(
            config_path, profile_vars=profile_vars, profile_path=profile_path
        )
    ):
        companion = _duckdb_module()
        if companion is None:
            raise FloeConfigError(_DUCKDB_INSTALL_HINT)
        if _current_observer is not None:
            companion.set_observer(_current_observer)
        try:
            outcome = companion.run(config_path, *args, **kwargs)
        except companion.FloeError as exc:
            raise _translate_companion_error(exc) from exc
        # The companion is a separate native library, so its RunOutcome is a
        # distinct Python class. Round-trip through the dict form so callers get
        # the lean `RunOutcome` type they imported from `floe`.
        return RunOutcome.from_dict(outcome.to_dict())
    return _run(config_path, *args, **kwargs)


# The companion's exception classes are distinct objects from the lean ones
# (separate native libraries), so `except floe.FloeRunError` on a caller's side
# would not catch a companion-raised error. Map each companion exception to the
# matching lean class by name so delegated runs raise the types callers expect.
_LEAN_EXCEPTIONS = {
    "FloeConfigError": FloeConfigError,
    "FloeRunError": FloeRunError,
    "FloeStorageError": FloeStorageError,
    "FloeIoError": FloeIoError,
    "FloeError": FloeError,
}


def _translate_companion_error(exc):
    lean_cls = _LEAN_EXCEPTIONS.get(type(exc).__name__, FloeError)
    return lean_cls(str(exc))


__all__ = [
    "__version__",
    "FloeError",
    "FloeConfigError",
    "FloeRunError",
    "FloeStorageError",
    "FloeIoError",
    "RootConfig",
    "EntityConfig",
    "RunOutcome",
    "validate",
    "run",
    "load_config",
    "extract_config_env_vars",
    "inspect_entity_state",
    "reset_entity_state",
    "set_observer",
    "clear_observer",
]
