"""Run the Python core on every golden case and diff it against `expected/`.

Usage: uv run --group conformance python conformance/check.py [--case NAME]

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
    FLOE,
    WRITER_BYTES_KEYS,
    Snapshot,
    case_name,
    case_snapshot,
    cases,
    is_validate,
    manifest_snapshot,
    run_in_copy,
)


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
                # not recorded: tests/unit/test_manifest.py checks the `sources` index and project `metadata`
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


def load_expected(case: Path) -> Snapshot:
    expected: Snapshot = {}
    for path in sorted((case / "expected").rglob("*")):
        if path.is_file():
            rel = path.relative_to(case / "expected").as_posix()
            expected[rel] = (
                pq.read_table(path) if path.suffix == ".parquet" else path.read_text()
            )
    return expected


def check_case(case: Path) -> list[str]:
    problems: list[str] = []
    expected = load_expected(case)
    with tempfile.TemporaryDirectory(prefix="conformance-") as tmp:
        work, proc = run_in_copy(case, FLOE, Path(tmp))
        actual = case_snapshot(case, work, proc)
        if "manifest.json" in expected:  # a case without one does not compare it
            actual |= manifest_snapshot(case, FLOE, work)
        if proc.returncode and not is_validate(case):
            actual["exit_code"] = f"{proc.returncode}\n"
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
    parser.add_argument("--case")
    args = parser.parse_args()
    selected = [c for c in cases() if args.case in (None, case_name(c))]
    if not selected:
        raise SystemExit(f"no such case: {args.case}")
    # each case runs in its own temp dir, so they run in parallel; threads only wait on subprocesses
    with ThreadPoolExecutor(os.cpu_count()) as pool:
        results = list(zip(selected, pool.map(check_case, selected), strict=True))
    failed = 0
    for case, problems in results:
        print(f"{'FAIL' if problems else 'ok  '} {case_name(case)}")
        for line in problems:
            print("    " + line)
        failed += bool(problems)
    print(f"\n{len(selected) - failed}/{len(selected)} cases match")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
