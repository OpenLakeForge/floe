"""Run an implementation on every conformance case and diff it against `expected/`.

Usage: uv run --group conformance python conformance/check.py --impl rust|python [--case NAME]

Exits 1 on any difference. Writer byte metrics are ignored on compare (G5 ruling on #594).
"""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs

from __future__ import annotations

import argparse
import difflib
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from record import (
    WRITER_BYTES_KEYS,
    Snapshot,
    case_name,
    cases,
    run_in_copy,
    rust_binary,
    snapshot,
)

REPO = Path(__file__).resolve().parent.parent


def comparable(name: str, value: Any) -> str:
    """Text form of a snapshot entry, so every kind of output diffs the same way."""
    if isinstance(value, str):
        if name.endswith(".json"):
            doc = json.loads(value)
            return json.dumps(mask_bytes(doc), indent=2) + "\n"
        return value
    # parquet: schema, then one line per row
    table = value
    rows = (json.dumps(r, default=str) for r in table.to_pylist())
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


def check_case(case: Path, cmd: list[str]) -> list[str]:
    problems: list[str] = []
    with tempfile.TemporaryDirectory(prefix="conformance-") as tmp:
        work, proc = run_in_copy(case, cmd, Path(tmp))
        if proc.returncode != 0:
            problems.append(
                f"`{' '.join(cmd)} run -c config.yml` exited {proc.returncode}: {proc.stderr.strip()}"
            )
        actual = snapshot(work)
    expected = load_expected(case)
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
    parser.add_argument("--case")
    args = parser.parse_args()
    cmd = (
        [str(rust_binary())]
        if args.impl == "rust"
        else ["uv", "run", "--project", str(REPO), "floe"]
    )
    selected = [c for c in cases() if not args.case or args.case == case_name(c)]
    if not selected:
        raise SystemExit(f"no such case: {args.case}")
    failed = 0
    for case in selected:
        problems = check_case(case, cmd)
        print(f"{'FAIL' if problems else 'ok  '} {case_name(case)}")
        for line in problems:
            print("    " + line)
        failed += bool(problems)
    print(f"\n{len(selected) - failed}/{len(selected)} cases match")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
