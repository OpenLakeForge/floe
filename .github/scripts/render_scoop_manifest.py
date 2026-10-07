#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


def build_manifest(version: str, windows_sha: str) -> dict:
    base = (
        "https://github.com/OpenLakeForge/floe/releases/download/"
        f"v{version}/floe-v{version}-x86_64-pc-windows-msvc.zip"
    )
    return {
        "version": version,
        "description": "YAML-driven technical ingestion tool",
        "homepage": "https://github.com/OpenLakeForge/floe",
        "license": "MIT",
        "architecture": {
            "64bit": {
                "url": base,
                "hash": windows_sha,
                "bin": "floe.exe",
                "extract_dir": ".",
            }
        },
        "checkver": {"github": "https://github.com/OpenLakeForge/floe"},
        "autoupdate": {
            "architecture": {
                "64bit": {
                    "url": (
                        "https://github.com/OpenLakeForge/floe/releases/download/"
                        "v$version/floe-v$version-x86_64-pc-windows-msvc.zip"
                    )
                }
            }
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Render the Scoop bucket manifest.")
    parser.add_argument("--version", required=True)
    parser.add_argument("--windows-sha", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    manifest = build_manifest(args.version, args.windows_sha)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
