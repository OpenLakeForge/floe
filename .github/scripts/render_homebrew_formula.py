#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path


def build_formula(
    version: str,
    linux_sha: str,
    mac_x86_sha: str,
    mac_arm_sha: str,
) -> str:
    base = (
        "https://github.com/OpenLakeForge/floe/releases/download/"
        f"v{version}/floe-v{version}-"
    )
    return f"""class Floe < Formula
  desc "YAML-driven technical ingestion tool"
  homepage "https://github.com/OpenLakeForge/floe"
  version "{version}"
  license "MIT"

  if OS.mac?
    if Hardware::CPU.arm?
      url "{base}aarch64-apple-darwin.tar.gz"
      sha256 "{mac_arm_sha}"
    else
      url "{base}x86_64-apple-darwin.tar.gz"
      sha256 "{mac_x86_sha}"
    end
  elsif OS.linux?
    url "{base}x86_64-unknown-linux-gnu.tar.gz"
    sha256 "{linux_sha}"
  else
    odie "Unsupported platform"
  end

  def install
    bin.install "floe"
  end

  test do
    system "#{bin}/floe", "--help"
  end
end
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Render the Homebrew formula.")
    parser.add_argument("--version", required=True)
    parser.add_argument("--linux-sha", required=True)
    parser.add_argument("--mac-x86-sha", required=True)
    parser.add_argument("--mac-arm-sha", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    formula = build_formula(
        args.version,
        args.linux_sha,
        args.mac_x86_sha,
        args.mac_arm_sha,
    )
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(formula, encoding="utf-8")


if __name__ == "__main__":
    main()
