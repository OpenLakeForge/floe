# /// script
# requires-python = ">=3.10"
# dependencies = ["pyiceberg==0.12.0", "pyarrow==25.0.1", "polars==2.0.0", "boto3==1.43.117"]
# ///
"""G2 driver (#597): for each mode and crash point, seed the table, start a run that pauses at the
crash point, kill -9 it, rerun with the same run id, and check rows, duplicates and orphans.

--negative runs only the "test that must fail first": append, kill after commit before the state
marker, rerun with the run-id guard disabled. It must produce duplicates.
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
POINTS = ["read", "pre-commit", "post-commit"]


def rows_of(csv: str) -> int:
    with open(csv + ".meta.json") as f:
        return json.load(f)["rows"]


def load(a, table, mode, run_id, csv, *extra) -> subprocess.Popen:
    cmd = [sys.executable, f"{HERE}/load.py", "--csv", csv, "--table", table, "--mode", mode, "--path", a.path,
           "--run-id", run_id, "--chunk-rows", str(a.chunk_rows), "--state-dir", a.state_dir, "--stats-out", a.stats, *extra]
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def finish(proc: subprocess.Popen) -> dict:
    out, _ = proc.communicate()
    if proc.returncode != 0:
        raise SystemExit(f"load failed ({proc.returncode}):\n{out}")
    return json.loads(out.strip().splitlines()[-1])


def check(table: str, expected: int) -> dict:
    out = subprocess.run([sys.executable, f"{HERE}/check.py", "--table", table, "--expected-rows", str(expected)],
                         capture_output=True, text=True)
    if not out.stdout:
        raise SystemExit(out.stderr)
    return json.loads(out.stdout)


def killed_run(a, table, mode, run_id, point) -> None:
    pause_file = os.path.join(a.state_dir, f"{table}.paused")
    proc = load(a, table, mode, run_id, a.csv, "--pause-at", point, "--pause-file", pause_file)
    while not os.path.exists(pause_file):
        if proc.poll() is not None:
            raise SystemExit(f"run ended before reaching {point}:\n{proc.stdout.read()}")
        time.sleep(0.2)
    os.kill(proc.pid, signal.SIGKILL)
    proc.wait()


def scenario(a, mode, point, no_guard=False) -> dict:
    table = f"s1_{mode}_{point.replace('-', '_')}_{a.path}_{uuid.uuid4().hex[:6]}"
    n = rows_of(a.csv)
    # Seed: overwrite seeds with the same file (a full load replaces it); append seeds with a file of
    # disjoint ids so the expected total is exact and any repeated id is a duplicate.
    seed_csv = a.csv if mode == "overwrite" else a.seed_csv
    finish(load(a, table, mode, f"seed-{table}", seed_csv))
    expected = n if mode == "overwrite" else rows_of(a.seed_csv) + n
    run_id = f"run-{table}"
    killed_run(a, table, mode, run_id, point)
    after_kill = check(table, expected)
    rerun = finish(load(a, table, mode, run_id, a.csv, *(["--no-guard"] if no_guard else [])))
    final = check(table, expected)
    return {
        "mode": mode, "point": point, "path": a.path, "guard": not no_guard, "table": table,
        "rows_after_kill": after_kill["rows"], "rerun_outcome": rerun["outcome"],
        "rerun_snapshots": rerun["snapshots_added"], "rows": final["rows"], "distinct_ids": final["distinct_ids"],
        "expected_rows": expected, "ok": final["ok"],
        "orphan_data_files": final["orphan_data_files"], "orphan_metadata_files": final["orphan_metadata_files"],
    }


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True)
    p.add_argument("--seed-csv", required=True, help="ids disjoint from --csv, used to seed append tables")
    p.add_argument("--path", default="chunked")
    p.add_argument("--chunk-rows", type=int, default=1_000_000)
    p.add_argument("--state-dir", required=True)
    p.add_argument("--stats", required=True, help="JSON lines file for load.py stats")
    p.add_argument("--negative", action="store_true")
    a = p.parse_args()
    os.makedirs(a.state_dir, exist_ok=True)
    cases = [("append", "post-commit", True)] if a.negative else [(m, pt, False) for m in ("overwrite", "append") for pt in POINTS]
    ok = True
    for mode, point, no_guard in cases:
        r = scenario(a, mode, point, no_guard)
        print(json.dumps(r), flush=True)
        ok &= r["ok"]
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
