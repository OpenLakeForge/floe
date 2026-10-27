"""Run an implementation on every conformance case and diff it against `expected/`.

Usage: uv run --group conformance python conformance/check.py --impl rust|python [--case NAME | --cases-file PATH]

Exits 1 on any difference. Writer byte metrics are ignored on compare (G5 ruling on #594).
"""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs

from __future__ import annotations

import argparse
import difflib
import json
import os
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from record import (
    WRITER_BYTES_KEYS,
    Snapshot,
    case_name,
    case_snapshot,
    cases,
    delta_ids,
    is_validate,
    manifest_snapshot,
    recorded,
    run_in_copy,
    rust_binary,
)

REPO = Path(__file__).resolve().parent.parent


def comparable(name: str, value: Any) -> str:
    """Text form of a snapshot entry, so every kind of output diffs the same way."""
    if isinstance(value, str):
        if name.endswith(".json"):
            doc = json.loads(value)
            if isinstance(doc, dict) and isinstance(doc.get("tool"), dict):
                doc["tool"]["version"] = (
                    "<volatile>"  # the implementation's own version
                )
            if name == "manifest.json":
                # D22: Python has no `schema`; its spec_version is its own Floe major.minor
                doc.pop("schema", None)
                doc["spec_version"] = "<volatile>"
                # D20: the `sources` index and project `metadata` are Python-only additions
                doc.pop("sources", None)
                doc.pop("metadata", None)
            return json.dumps(mask_bytes(doc), indent=2) + "\n"
        return value
    # parquet: schema, then one line per row
    table = value
    # Arrow renders dates/timestamps itself: Python's `date` cannot hold years <= 0 or >= 10000
    flat = pa.table(
        [
            pc.cast(c, pa.string())
            if pa.types.is_date(c.type) or pa.types.is_timestamp(c.type)
            else c
            for c in table.columns
        ],
        names=table.column_names,
    )
    rows = (json.dumps(r, default=str) for r in flat.to_pylist())
    return f"schema:\n{table.schema}\nrows:\n" + "\n".join(rows) + "\n"


def mask_bytes(value: object) -> object:
    if isinstance(value, dict):
        return {
            k: "<ignored>" if k in WRITER_BYTES_KEYS else mask_bytes(v)
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [mask_bytes(v) for v in value]
    return value


def load_expected(case: Path, impl: str) -> Snapshot:
    """`expected/`, with `expected_python/<file>` replacing the same file when checking Python."""
    expected: Snapshot = {}
    dirs = ["expected", "expected_python"] if impl == "python" else ["expected"]
    for d in dirs:
        if d == "expected_python" and (case / d / "reports").is_dir():
            # D37 moves every report file of a single-domain run, so Python's reports replace Rust's whole set
            expected = {k: v for k, v in expected.items() if not k.startswith("reports/")}  # fmt: skip
        for path in sorted((case / d).rglob("*")):
            if path.is_file():
                rel = path.relative_to(case / d).as_posix()
                expected[rel] = (
                    pq.read_table(path)
                    if path.suffix == ".parquet"
                    else path.read_text()
                )
    return expected


def check_case(
    case: Path, cmd: list[str], impl: str, with_manifest: bool = True
) -> list[str]:
    problems: list[str] = []
    with tempfile.TemporaryDirectory(prefix="conformance-") as tmp:
        work, proc = run_in_copy(case, cmd, Path(tmp))
        actual = case_snapshot(case, work, proc)
        if with_manifest:
            actual |= manifest_snapshot(case, cmd, work)
        if proc.returncode and not is_validate(case):
            actual["exit_code"] = f"{proc.returncode}\n"
    expected = load_expected(case, impl)
    if (
        expected.get("exit_code") == "0\n"
    ):  # an `expected_python/exit_code` of 0: Python succeeds where Rust failed
        del expected["exit_code"]
    if not with_manifest:
        expected.pop("manifest.json", None)
    for name in sorted(set(expected) | set(actual)):
        if name not in actual:
            problems.append(f"missing output: {name}")
        elif name not in expected:
            problems.append(f"unexpected output: {name}")
        else:
            left, right = (
                comparable(name, expected[name]),
                comparable(name, actual[name]),
            )
            problems += difflib.unified_diff(
                left.splitlines(),
                right.splitlines(),
                f"expected/{name}",
                f"actual/{name}",
                lineterm="",
                n=1,
            )
    return problems


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--impl", choices=["rust", "python"], required=True)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--case")
    group.add_argument("--cases-file", type=Path, help="run only the cases named here")
    args = parser.parse_args()
    cmd = (
        [str(rust_binary())]
        if args.impl == "rust"
        else ["uv", "run", "--project", str(REPO), "floe"]
    )
    wanted = [args.case] if args.case else None
    manifest_cases: set[str] = (
        set()
    )  # `<case> manifest`: Python must also match manifest.json
    if args.cases_file:
        lines = [
            raw.split("#")[0].split()
            for raw in args.cases_file.read_text().splitlines()
        ]
        wanted = [line[0] for line in lines if line]
        manifest_cases = {line[0] for line in lines if line[1:] == ["manifest"]}
    selected = [c for c in cases() if wanted is None or case_name(c) in wanted]
    if missing := set(wanted or []) - {case_name(c) for c in selected}:
        raise SystemExit(f"no such case: {', '.join(sorted(missing))}")
    failed = skipped = 0
    for case in selected:
        if args.impl == "rust" and not recorded(case):
            print(f"skip {case_name(case)} (D1: not recorded)")
            skipped += 1
    run = [c for c in selected if args.impl == "python" or recorded(c)]

    def check(case: Path) -> list[str]:
        # Rust 0.7 rejects some configs Python accepts (D2): no manifest was recorded, nothing to compare
        with_manifest = args.impl == "rust" or bool(
            (args.case or case_name(case) in manifest_cases)
            and (case / "expected" / "manifest.json").exists()
        )
        return check_case(case, cmd, args.impl, with_manifest)

    # each case runs in its own temp dir, so they run in parallel; threads only wait on subprocesses
    with ThreadPoolExecutor(os.cpu_count()) as pool:
        results = list(zip(run, pool.map(check, run), strict=True))
    for case, problems in results:
        if problems and args.impl == "python" and (ids := delta_ids(case)):
            problems.insert(
                0, f"intentional delta {', '.join(ids)}: expected_python/ applies"
            )
        print(f"{'FAIL' if problems else 'ok  '} {case_name(case)}")
        for line in problems:
            print("    " + line)
        failed += bool(problems)
    checked = len(selected) - skipped
    print(
        f"\n{checked - failed}/{checked} cases match"
        + (f" ({skipped} skipped)" if skipped else "")
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
