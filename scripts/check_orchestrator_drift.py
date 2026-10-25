#!/usr/bin/env python3
"""Fail when designated shared orchestrator modules drift out of lockstep.

The ``dagster-floe`` and ``airflow-floe`` packages each vendor a copy of a few
modules that are meant to stay byte-identical (see issue #394). They are
published as independent PyPI packages with no shared runtime dependency, so the
copies can silently diverge — and historically did. This guard compares each
designated pair and exits non-zero on any difference, turning drift into a
red CI check instead of a latent production bug.

Run locally with::

    python scripts/check_orchestrator_drift.py

Only modules that are intended to be identical belong here. ``manifest.py`` and
the ``*_runner.py`` modules are intentionally framework-specific and are NOT
guarded.

``dagster-floe``'s vendored manifest schema (see ``CANONICAL_COPIES`` below) has
a second line of defense: ``orchestrators/dagster-floe/setup.py`` copies it
fresh from the canonical source on every build (`pip install -e .`, `python -m
build`), so a stale copy can no longer reach a release even if this check is
skipped. This script still matters because tests and local dev commonly run
against ``src/`` directly (e.g. ``PYTHONPATH=src pytest``), which never
triggers that build step.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DAGSTER_PKG = REPO_ROOT / "orchestrators" / "dagster-floe" / "src" / "floe_dagster"
AIRFLOW_PKG = REPO_ROOT / "orchestrators" / "airflow-floe" / "src" / "airflow_floe"

# Module filenames that must stay byte-identical between the two packages.
SHARED_MODULES = (
    "databricks_client.py",
    "k8s_status.py",
)

# (canonical source, vendored copy) pairs that must stay byte-identical. Unlike
# SHARED_MODULES these aren't peers of each other - one side is the single
# source of truth and the other is a package-local copy (e.g. so it can be
# loaded via importlib.resources without a runtime dependency on floe-core).
CANONICAL_COPIES = (
    (
        REPO_ROOT / "orchestrators" / "schemas" / "floe.manifest.schema.json",
        DAGSTER_PKG / "schemas" / "floe.manifest.schema.json",
    ),
)


def main() -> int:
    drifted: list[str] = []
    missing: list[str] = []

    for name in SHARED_MODULES:
        dagster_file = DAGSTER_PKG / name
        airflow_file = AIRFLOW_PKG / name
        if not dagster_file.exists() or not airflow_file.exists():
            missing.append(name)
            continue
        if dagster_file.read_bytes() != airflow_file.read_bytes():
            drifted.append(name)

    if missing:
        print("ERROR: designated shared modules are missing in a package:")
        for name in missing:
            print(f"  - {name}")

    if drifted:
        print("ERROR: shared orchestrator modules have drifted out of sync:")
        for name in drifted:
            print(f"  - {name}")
            print(f"      {DAGSTER_PKG / name}")
            print(f"      {AIRFLOW_PKG / name}")
        print(
            "\nThese files must stay byte-identical between dagster-floe and "
            "airflow-floe.\nApply the change to both copies (see issue #394)."
        )

    copy_drifted: list[tuple[Path, Path]] = []
    copy_missing: list[tuple[Path, Path]] = []
    for canonical, vendored in CANONICAL_COPIES:
        if not canonical.exists() or not vendored.exists():
            copy_missing.append((canonical, vendored))
            continue
        if canonical.read_bytes() != vendored.read_bytes():
            copy_drifted.append((canonical, vendored))

    if copy_missing:
        print("ERROR: canonical file or its vendored copy is missing:")
        for canonical, vendored in copy_missing:
            print(f"  - {canonical}")
            print(f"    {vendored}")

    if copy_drifted:
        print("ERROR: vendored copies have drifted from their canonical source:")
        for canonical, vendored in copy_drifted:
            print(f"  - {vendored}")
            print(f"      canonical: {canonical}")
        print(
            "\nThese vendored copies must stay byte-identical to their canonical "
            "source. Copy the canonical file over the vendored one."
        )

    if missing or drifted or copy_missing or copy_drifted:
        return 1

    print(
        f"OK: {len(SHARED_MODULES)} shared orchestrator module(s) and "
        f"{len(CANONICAL_COPIES)} vendored copy(ies) in sync."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
