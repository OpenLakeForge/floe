"""Record the expected outputs of every conformance case from the Rust v0.7.0 release binary.

Usage: uv run --group conformance python conformance/record.py [--case NAME]

The binary is downloaded once, checksum-verified and cached outside the repo.
`snapshot` is shared with check.py: it turns one finished run into the normalized files that
`expected/` holds.
"""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
VERSION = "0.7.0"
ASSET = f"floe-v{VERSION}-x86_64-unknown-linux-gnu.tar.gz"
CACHE = Path.home() / ".cache" / "floe-conformance" / f"v{VERSION}"

# Paths in expected/ use filesystem-safe tokens (run_ID, part-UUID): `<` and `>` break Windows checkouts.
# Differ between any two runs of the same binary: masked when recording.
VOLATILE_KEYS = {"run_id", "started_at", "finished_at", "duration_ms", "elapsed_ms"}
# Recorded, but ignored on compare (G5 ruling on #594).
WRITER_BYTES_KEYS = {"total_bytes_written", "avg_file_size_mb"}
RUN_DIR = re.compile(r"run_[0-9T:.\-Z]+")
UUID_PART = re.compile(
    r"part-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
)

Snapshot = dict[str, str | Any]  # str: text file; Any: pyarrow Table


def cases() -> list[Path]:
    found = (
        p.parent
        for p in HERE.rglob("config.yml")
        if not {"in", "expected"} & set(p.relative_to(HERE).parts)
    )
    return sorted(found)


def case_name(case: Path) -> str:
    return case.relative_to(HERE).as_posix()


def is_validate(case: Path) -> bool:
    """Cases under `config/` run `floe validate` (outcome only); all others run `floe run`."""
    return case.relative_to(HERE).parts[0] == "config"


def delta_ids(case: Path) -> list[str]:
    """Ids in `delta.yml` (a one-line list such as `[D9]`); empty when the case has no delta."""
    marker = case / "delta.yml"
    return re.findall(r"D\d+", marker.read_text()) if marker.exists() else []


def recorded(case: Path) -> bool:
    """D1 cases (dropped features) are not recorded from Rust: only `expected_python/` exists."""
    return "D1" not in delta_ids(case)


def rust_binary() -> Path:
    binary = CACHE / "floe"
    if binary.exists():
        return binary
    CACHE.mkdir(parents=True, exist_ok=True)
    # `gh api` rather than `gh release download`: the latter needs GraphQL, which some hosts block.
    assets: dict[str, bytes] = {}
    for name in (ASSET, ASSET + ".sha256"):
        meta = [
            "gh",
            "api",
            f"repos/OpenLakeForge/floe/releases/tags/v{VERSION}",
            "--jq",
            f'.assets[]|select(.name=="{name}").id',
        ]
        asset_id = subprocess.run(
            meta, check=True, capture_output=True, text=True
        ).stdout.strip()
        url = f"repos/OpenLakeForge/floe/releases/assets/{asset_id}"
        assets[name] = subprocess.run(
            ["gh", "api", "-H", "Accept: application/octet-stream", url],
            check=True,
            capture_output=True,
        ).stdout
    expected = assets[ASSET + ".sha256"].split()[0].decode()
    if hashlib.sha256(assets[ASSET]).hexdigest() != expected:
        raise SystemExit(f"checksum mismatch for {ASSET}")
    with tempfile.TemporaryDirectory() as tmp:
        tarball = Path(tmp) / ASSET
        tarball.write_bytes(assets[ASSET])
        with tarfile.open(tarball) as tar:
            tar.extract("floe", CACHE, filter="data")
    return binary


def normalize(value: object, root: str) -> object:
    if isinstance(value, dict):
        return {
            k: "<volatile>"
            if k in VOLATILE_KEYS
            else "<snapshot_id>"
            if k == "snapshot_id"
            else normalize(v, root)
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [normalize(v, root) for v in value]
    if isinstance(value, str):
        return UUID_PART.sub(
            "part-<uuid>", RUN_DIR.sub("run_<id>", value.replace(root, "<ROOT>"))
        )
    return value


def snapshot(work: Path) -> Snapshot:
    """Normalized reports, accepted rows (one table per accepted dir) and rejected CSVs of a run."""
    root = str(work.resolve())
    out: Snapshot = {}
    for path in sorted(work.glob("report/run_*/**/*.json")):
        rel = RUN_DIR.sub("run_ID", path.relative_to(work).as_posix())
        # key order is part of the contract, so it is preserved
        out[f"reports/{rel}"] = (
            json.dumps(normalize(json.loads(path.read_text()), root), indent=2) + "\n"
        )
    for d in sorted({p.parent for p in work.glob("out/accepted/**/*.parquet")}):
        parts = sorted(d.glob("part-*.parquet"))
        out[f"accepted/{d.relative_to(work / 'out/accepted').as_posix()}.parquet"] = (
            pa.concat_tables([pq.read_table(p) for p in parts])
        )
    for path in sorted(work.glob("out/rejected/**/*.csv")):
        key = f"rejected/{UUID_PART.sub('part-UUID', path.relative_to(work / 'out/rejected').as_posix())}"
        if key in out:
            raise SystemExit(f"{key}: two rejected files with the same normalized name")
        out[key] = path.read_text()
    return out


def validate_snapshot(work: Path, proc: subprocess.CompletedProcess[str]) -> Snapshot:
    """`floe validate` outcome: `ok`, or the message from the last `Error: ` line of stderr."""
    out: dict[str, object] = {"ok": proc.returncode == 0}
    if proc.returncode != 0:
        lines = proc.stderr.strip().splitlines()
        start = max(
            (i for i, l in enumerate(lines) if l.startswith("Error: ")), default=0
        )
        message = "\n".join(lines[start:]).removeprefix("Error: ")
        out["error"] = normalize(message, str(work.resolve()))
    return {"validate.json": json.dumps(out, indent=2) + "\n"}


def case_snapshot(
    case: Path, work: Path, proc: subprocess.CompletedProcess[str]
) -> Snapshot:
    return validate_snapshot(work, proc) if is_validate(case) else snapshot(work)


def run_in_copy(
    case: Path, cmd: list[str], tmp: Path
) -> tuple[Path, subprocess.CompletedProcess[str]]:
    work = tmp / "work"
    shutil.copytree(
        case, work, ignore=shutil.ignore_patterns("expected", "expected_python")
    )
    args = ["run", "-c", "config.yml"]
    if is_validate(case):
        profile = (
            ["--profile", "profile.yml"] if (case / "profile.yml").exists() else []
        )
        args = ["validate", "-c", "config.yml", *profile]
    # optional `runs`: a run count, or one line of extra `run` args per run (`-` for none); only the last run's report is kept
    marker = case / "runs"
    text = marker.read_text().strip() if marker.exists() else "1"
    runs = ["-"] * int(text) if text.isdigit() else text.splitlines()
    while True:
        shutil.rmtree(work / "report", ignore_errors=True)
        line = runs.pop(0).strip()
        extra = [] if line == "-" else line.split()
        proc = subprocess.run(
            [*cmd, *args, *extra], cwd=work, check=False, capture_output=True, text=True
        )
        if not runs:
            return work, proc


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--case")
    args = parser.parse_args()
    binary = rust_binary()
    for case in cases():
        if (args.case and args.case != case_name(case)) or not recorded(case):
            continue
        with tempfile.TemporaryDirectory(prefix="conformance-") as tmp:
            work, proc = run_in_copy(case, [str(binary)], Path(tmp))
            if proc.returncode not in (0, 2) and not is_validate(case):  # 2 = aborted
                sys.stderr.write(proc.stdout + proc.stderr)
                raise SystemExit(f"{case_name(case)}: floe exited {proc.returncode}")
            shutil.rmtree(case / "expected", ignore_errors=True)
            snap = case_snapshot(case, work, proc)
            if proc.returncode and not is_validate(case):
                snap["exit_code"] = f"{proc.returncode}\n"  # absent file means 0
            if not is_validate(case) and not any(
                k.startswith(("accepted/", "rejected/")) for k in snap
            ):
                raise SystemExit(
                    f"{case_name(case)}: no accepted or rejected output found under out/"
                )
            for rel, content in snap.items():
                dest = case / "expected" / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                if isinstance(content, str):
                    dest.write_text(content)
                else:
                    pq.write_table(content, dest)
        print(f"recorded {case_name(case)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
