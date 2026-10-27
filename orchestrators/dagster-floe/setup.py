#!/usr/bin/env python3
"""Copy the canonical manifest schema into the package before building.

dagster-floe validates every manifest against a bundled copy of
``orchestrators/schemas/floe.manifest.schema.json`` (loaded via
``importlib.resources``, see ``floe_dagster/manifest.py``) so it never needs
a runtime dependency on floe-core. That copy previously had to be kept in
sync by hand and silently drifted (issue #446): this refreshes it from the
canonical source on every build (`pip install -e .`, `pip install .`,
`python -m build`), so a stale vendored copy can no longer reach a release.

`scripts/check_orchestrator_drift.py` still checks the checked-in vendored
copy against canonical in CI - that guards the source tree itself (e.g. for
test runs against `src/` directly, which never invoke this file), while this
guards what actually gets published.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import setuptools

_HERE = Path(__file__).resolve().parent
_CANONICAL_SCHEMA = _HERE.parent / "schemas" / "floe.manifest.schema.json"
_VENDORED_SCHEMA = _HERE / "src" / "floe_dagster" / "schemas" / "floe.manifest.schema.json"

if _CANONICAL_SCHEMA.exists():
    _VENDORED_SCHEMA.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(_CANONICAL_SCHEMA, _VENDORED_SCHEMA)
    # Building outside the monorepo (e.g. from an sdist) has no canonical
    # source to copy from - the vendored copy already in the sdist (itself
    # produced by this same step from a monorepo checkout) is used as-is.

setuptools.setup()
