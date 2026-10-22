"""Parity diff: run configs through Rust `floe run` and the Python path, diff the outputs.

Compares, per config:
  - every report file (run.summary.json and each entity run.json), after masking
    run ids, timestamps, durations and absolute paths;
  - accepted rows (all accepted parquet parts, read in part-name order);
  - rejected CSV files, byte for byte.

Exits 1 on any difference. `--ignore-writer-bytes` also masks the two report fields that depend
on the Parquet writer build (`total_bytes_written`, `avg_file_size_mb`). Per the G5 ruling on #594
those are excluded from parity, so `--ignore-writer-bytes` is the G5 verdict; the default strict
run is kept for information.

Usage: python parity.py --floe /path/to/floe [--ignore-writer-bytes] [config.yml ...]
"""

from __future__ import annotations

import argparse
import difflib
import glob
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import polars as pl

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
DEFAULT_CONFIGS = [
    REPO / "example/standalone/config.yml",
    HERE / "fixtures/casts/config.yml",
    HERE / "fixtures/unique_pii/config.yml",
    HERE / "fixtures/mismatch/config.yml",
]

# Fields that differ between any two runs of the same binary.
VOLATILE_KEYS = {"run_id", "started_at", "finished_at", "duration_ms", "elapsed_ms"}
WRITER_BYTES_KEYS = {"total_bytes_written", "avg_file_size_mb"}
RUN_DIR = re.compile(r"run_[0-9T:.\-Z]+")
UUID_PART = re.compile(r"part-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def run(cmd: list[str], cwd: Path) -> None:
    proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout + proc.stderr)
        raise SystemExit(f"command failed ({proc.returncode}): {' '.join(cmd)}")


def normalize(value, root: str, masked: set[str]):
    if isinstance(value, dict):
        return {k: ("<volatile>" if k in masked else normalize(v, root, masked)) for k, v in value.items()}
    if isinstance(value, list):
        return [normalize(v, root, masked) for v in value]
    if isinstance(value, str):
        value = value.replace(root, "<ROOT>")
        value = RUN_DIR.sub("run_<id>", value)
        return UUID_PART.sub("part-<uuid>", value)
    return value


def reports(work: Path, masked: set[str]) -> dict[str, str]:
    out = {}
    for path in sorted(glob.glob(str(work / "report" / "run_*" / "**" / "*.json"), recursive=True)):
        rel = RUN_DIR.sub("run_<id>", os.path.relpath(path, work))
        doc = normalize(json.loads(Path(path).read_text()), str(work), masked)
        out[rel] = json.dumps(doc, indent=2)  # key order preserved: order is part of the contract
    return out


def accepted(work: Path) -> dict[str, pl.DataFrame]:
    out = {}
    for d in sorted({Path(p).parent for p in glob.glob(str(work / "out/accepted/**/*.parquet"), recursive=True)}):
        parts = sorted(d.glob("part-*.parquet"))
        out[os.path.relpath(d, work)] = pl.concat([pl.read_parquet(p) for p in parts])
    return out


def rejected(work: Path) -> dict[str, str]:
    # Rejected parts are listed in report order; append-mode parts are uuid-named.
    out = {}
    for path in sorted(glob.glob(str(work / "report" / "run_*" / "*" / "*" / "run.json"))):
        for f in json.loads(Path(path).read_text())["files"]:
            p = f["output"].get("rejected_path")
            if p:
                key = UUID_PART.sub("part-<uuid>", os.path.relpath(p, work))
                out[key] = Path(p).read_text()
    return out


def diff_text(label: str, left: str, right: str) -> list[str]:
    if left == right:
        return []
    lines = difflib.unified_diff(left.splitlines(), right.splitlines(), f"rust/{label}", f"python/{label}", lineterm="", n=1)
    return list(lines)


def compare(config: Path, floe: str, masked: set[str]) -> list[str]:
    problems: list[str] = []
    with tempfile.TemporaryDirectory(prefix="parity-") as tmp:
        sides = {}
        for side in ("rust", "python"):
            work = Path(tmp) / side
            shutil.copytree(config.parent, work, ignore=shutil.ignore_patterns("out", "report"))
            if side == "rust":
                run([floe, "run", "-c", config.name], work)
            else:
                run([sys.executable, str(HERE / "floe_py.py"), "run", "-c", config.name], work)
            sides[side] = work
        # Mask each side's own root so the paths line up.
        r, p = sides["rust"], sides["python"]

        rr, pr = reports(r, masked), reports(p, masked)
        for key in sorted(set(rr) | set(pr)):
            if key not in rr or key not in pr:
                problems.append(f"report {key}: only in {'rust' if key in rr else 'python'}")
                continue
            problems += diff_text(key, rr[key], pr[key])

        ra, pa = accepted(r), accepted(p)
        for key in sorted(set(ra) | set(pa)):
            if key not in ra or key not in pa:
                problems.append(f"accepted {key}: only in {'rust' if key in ra else 'python'}")
                continue
            if ra[key].schema != pa[key].schema:
                problems.append(f"accepted {key}: schema rust={ra[key].schema} python={pa[key].schema}")
            elif not ra[key].equals(pa[key], null_equal=True):
                problems.append(f"accepted {key}: rows differ\nrust:\n{ra[key]}\npython:\n{pa[key]}")

        rj, pj = rejected(r), rejected(p)
        for key in sorted(set(rj) | set(pj)):
            if key not in rj or key not in pj:
                problems.append(f"rejected {key}: only in {'rust' if key in rj else 'python'}")
                continue
            problems += diff_text(key, rj[key], pj[key])
    return problems


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--floe", default=os.environ.get("FLOE_BIN", "floe"))
    parser.add_argument("--ignore-writer-bytes", action="store_true")
    parser.add_argument("configs", nargs="*", type=Path, default=DEFAULT_CONFIGS)
    args = parser.parse_args()
    masked = VOLATILE_KEYS | (WRITER_BYTES_KEYS if args.ignore_writer_bytes else set())
    if args.ignore_writer_bytes:
        print("NOTE: --ignore-writer-bytes: total_bytes_written / avg_file_size_mb masked\n")
    failed = 0
    for config in args.configs:
        problems = compare(config.resolve(), args.floe, masked)
        name = os.path.relpath(config.resolve(), REPO)
        print(f"{'DIFF' if problems else 'OK  '} {name}")
        for line in problems:
            print("    " + line)
        failed += bool(problems)
    print(f"\n{len(args.configs) - failed}/{len(args.configs)} configs identical")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
