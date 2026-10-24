"""Sort / z-order on rewrite: compaction with no sort, linear sort, z-order over 2 and 3 columns, then data-skipping.
Each variant gets its own identically seeded table. Skipping = data files PyIceberg's planner keeps for a filter
(min/max pruning), plus DuckDB wall time for the same predicate.

    python sort_eval.py --rows 50000000 --target-file-mb 128
"""
import argparse
import json
import subprocess
import sys
import time

import maintain
from check import duckdb_fp, pyiceberg_fp
from common import NAMESPACE, catalog, duck, emit, file_counts, vmhwm_mb

ap = argparse.ArgumentParser()
ap.add_argument("--rows", type=int, default=50_000_000)
ap.add_argument("--target-file-mb", type=int, default=128)
ap.add_argument("--variants", default="none,linear2,zorder2,zorder3")
ap.add_argument("--out", default="results/sort.jsonl")
a = ap.parse_args()

QUERIES = {
    "cat=123": ("cat = 123", "cat = 123"),
    "d=2024-03-05": ("d = '2024-03-05'", "d = DATE '2024-03-05'"),
    "cat 100..109 and d in Mar": ("cat >= 100 AND cat <= 109 AND d >= '2024-03-01' AND d < '2024-04-01'",
                                  "cat BETWEEN 100 AND 109 AND d >= DATE '2024-03-01' AND d < DATE '2024-04-01'"),
    "amount>=999000 (merge-touched)": ("amount >= 999000", "amount >= 999000"),
    "id range 10M..10.1M": ("id >= 10000000 AND id < 10100000", "id >= 10000000 AND id < 10100000"),
}
VARIANTS = {
    "none": (None, []),
    "linear2": ("linear", ["cat", "d"]),
    "zorder2": ("zorder", ["cat", "d"]),
    "zorder3": ("zorder", ["cat", "d", "id"]),
}

cat = catalog()
for v in a.variants.split(","):
    kind, cols = VARIANTS[v]
    tname = f"s5_sort_{v}"
    subprocess.run([sys.executable, "seed.py", "--table", tname, "--rows", str(a.rows), "--merges", "5",
                    "--small-appends", "30", "--memory-limit", "4GB"], check=True, capture_output=True)
    name = f"{NAMESPACE}.{tname}"
    tbl = cat.load_table(name)
    with tbl.transaction() as tx:
        tx.set_properties({"write.target-file-size-bytes": str(a.target_file_mb << 20)})
    rec = {"variant": v, "kind": kind, "cols": cols, "rows": a.rows, "target_file_mb": a.target_file_mb, "before": file_counts(cat.load_table(name))}
    # fresh process per variant => VmHWM is the rewrite's own
    proc = subprocess.run([sys.executable, "run_compact.py", "--table", tname, "--label", f"sort-{v}", "--out", "results/sort-runs.jsonl",
                           *( ["--sort", kind, "--cols", ",".join(cols)] if kind else [] ), "--target-file-mb", str(a.target_file_mb)],
                          capture_output=True, text=True)
    rec["run_ok"] = proc.returncode == 0
    run = json.loads(open("results/sort-runs.jsonl").read().strip().splitlines()[-1])
    rec["wall_s"], rec["vmhwm_mb"], rec["after"], rec["parity_equal"] = run["wall_s"], run["vmhwm_mb"], run["after"], run["parity"]["equal"] and run["parity"]["equal_to_before"]
    rec["compact_timings"] = run["compact"]["timings"]
    tbl = cat.load_table(name)
    total = len(list(tbl.scan().plan_files()))
    con = duck("4GB")
    q = {}
    for label, (py, dk) in QUERIES.items():
        kept = len(list(tbl.scan(row_filter=py).plan_files()))
        best = 1e9
        for _ in range(3):
            t0 = time.time()
            n = con.execute(f"SELECT count(*), sum(amount) FROM polaris.{name} WHERE {dk}").fetchone()
            best = min(best, time.time() - t0)
        q[label] = {"files_kept": kept, "files_total": total, "duckdb_best_of_3_s": round(best, 2), "rows": n[0]}
    rec["queries"] = q
    emit(a.out, rec)
    print(json.dumps(rec), flush=True)
