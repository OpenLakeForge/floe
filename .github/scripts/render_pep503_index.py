#!/usr/bin/env python3
"""Render a PEP 503 "simple" index for the off-PyPI ``floe-duckdb`` wheels.

The DuckDB companion wheel bundles a native C++ DuckDB build that is too large
for PyPI (>100 MB) and does not cross-compile in the manylinux/musllinux wheel
containers, so it is published OFF PyPI as GitHub Release assets. This script
builds a self-hosted simple index (à la ``download.pytorch.org``) that ``pip``
can consume::

    pip install floe-python  # lean wheel from PyPI (provides the companion's dependency)
    pip install floe-duckdb --index-url https://openlakeforge.github.io/floe/simple/

It enumerates every ``floe_duckdb-*.whl`` asset across ALL GitHub Releases via the
GitHub REST API so the regenerated index always lists the full version history,
then writes the two-level PEP 503 layout::

    <out>/index.html                     # project list
    <out>/floe-duckdb/index.html         # file list with download URLs + #sha256

Each file link points at the Release asset download URL and carries the asset's
SHA-256 as a ``#sha256=`` fragment (PEP 503 hash) when GitHub reports a digest.
"""

from __future__ import annotations

import argparse
import html
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

PROJECT = "floe-duckdb"
# PEP 503 normalizes the project name to lowercase with runs of [-_.] collapsed
# to a single dash; "floe-duckdb" is already normalized.
NORMALIZED = "floe-duckdb"


def _api(url: str, token: str | None) -> list | dict:
    req = urllib.request.Request(url)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req) as resp:
        return json.load(resp)


def collect_wheels(repo: str, token: str | None) -> list[dict]:
    """Return [{name, url, sha256}] for every floe-duckdb wheel across releases."""
    wheels: list[dict] = []
    page = 1
    while True:
        releases = _api(
            f"https://api.github.com/repos/{repo}/releases?per_page=100&page={page}",
            token,
        )
        if not releases:
            break
        for rel in releases:
            for asset in rel.get("assets", []):
                name = asset.get("name", "")
                if not name.startswith("floe_duckdb-") or not name.endswith(".whl"):
                    continue
                digest = asset.get("digest") or ""
                sha256 = digest.split(":", 1)[1] if digest.startswith("sha256:") else ""
                wheels.append(
                    {
                        "name": name,
                        "url": asset["browser_download_url"],
                        "sha256": sha256,
                    }
                )
        page += 1
    # Stable, de-duplicated ordering (a wheel name is unique per release tag).
    seen: dict[str, dict] = {}
    for wheel in wheels:
        seen[wheel["name"]] = wheel
    return [seen[k] for k in sorted(seen)]


def write_index(out: Path, wheels: list[dict]) -> None:
    out.mkdir(parents=True, exist_ok=True)

    root = (
        "<!DOCTYPE html>\n<html><body>\n"
        f'<a href="{NORMALIZED}/">{PROJECT}</a>\n'
        "</body></html>\n"
    )
    (out / "index.html").write_text(root, encoding="utf-8")

    project_dir = out / NORMALIZED
    project_dir.mkdir(parents=True, exist_ok=True)
    links = []
    for wheel in wheels:
        href = wheel["url"]
        if wheel["sha256"]:
            href = f"{href}#sha256={wheel['sha256']}"
        links.append(f'<a href="{html.escape(href)}">{html.escape(wheel["name"])}</a>')
    body = (
        "<!DOCTYPE html>\n<html><body>\n"
        + "<br>\n".join(links)
        + ("\n" if links else "")
        + "</body></html>\n"
    )
    (project_dir / "index.html").write_text(body, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repo",
        default=os.environ.get("GITHUB_REPOSITORY", "OpenLakeForge/floe"),
        help="owner/name of the GitHub repository holding the release assets",
    )
    parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="output directory for the PEP 503 simple index",
    )
    args = parser.parse_args()

    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    try:
        wheels = collect_wheels(args.repo, token)
    except urllib.error.HTTPError as err:  # pragma: no cover - network failure path
        print(f"ERROR: failed to list releases for {args.repo}: {err}", file=sys.stderr)
        return 1

    write_index(args.out, wheels)
    print(f"Wrote PEP 503 index for {len(wheels)} {PROJECT} wheel(s) to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
