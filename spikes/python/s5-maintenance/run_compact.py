"""Compaction measurement. Usage:
    python run_compact.py --table s5_main_50m [--sort zorder --cols cat,d] [--stub-noop] [--expire-and-clean]
Emits one JSON record to results/runs.jsonl.
"""
import argparse
import sys
import time
from datetime import timedelta

from check import duckdb_fp, parity, pyiceberg_fp
from common import NAMESPACE, catalog, duck, emit, file_counts, key_of, list_objects, timed, vmhwm_mb
import maintain


def n_metadata_json(tbl):
    return len([o for o in list_objects(key_of(tbl.location()).rstrip("/") + "/metadata/") if o["Key"].endswith(".metadata.json")])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", required=True)
    ap.add_argument("--sort", choices=["linear", "zorder"])
    ap.add_argument("--cols", default="")
    ap.add_argument("--partition-col")
    ap.add_argument("--stub-noop", action="store_true", help="test that must fail first: compaction is a no-op")
    ap.add_argument("--memory-limit", default="4GB")
    ap.add_argument("--label", default="compact")
    ap.add_argument("--out", default="results/runs.jsonl")
    ap.add_argument("--target-file-mb", type=int, default=0, help="write.target-file-size-bytes override (0 = table default)")
    a = ap.parse_args()
    name = f"{NAMESPACE}.{a.table}"
    cat = catalog()
    con = duck(a.memory_limit)
    tbl = cat.load_table(name)
    rec = {"label": a.label, "table": a.table, "sort": a.sort, "cols": a.cols, "stub_noop": a.stub_noop, "memory_limit": a.memory_limit}
    before = file_counts(tbl)
    commits_before = n_metadata_json(tbl)
    base_snap = tbl.current_snapshot().snapshot_id
    rec["before"] = before
    rec["snapshots_before"] = len(tbl.snapshots())
    print("before", before, flush=True)

    wall = time.time()
    if not a.stub_noop:
        if a.target_file_mb:
            with tbl.transaction() as tx:
                tx.set_properties({"write.target-file-size-bytes": str(a.target_file_mb << 20)})
        res = maintain.compact(cat, name, con, sort=a.sort, sort_cols=a.cols.split(",") if a.cols else None,
                               partition_col=a.partition_col)
        rec["compact"] = vars(res)
    rec["wall_s"] = round(time.time() - wall, 2)
    rec["vmhwm_mb"] = round(vmhwm_mb(), 1)  # compaction process only: fingerprints run in a fresh process below

    tbl = cat.load_table(name)
    after = file_counts(tbl)
    rec["after"] = after
    rec["commits"] = n_metadata_json(tbl) - commits_before
    rec["snapshots_added"] = len(tbl.snapshots()) - rec["snapshots_before"]
    rec["new_snapshot_op"] = tbl.current_snapshot().summary.operation.value
    print("after", after, flush=True)

    print("compaction done", {k: rec[k] for k in ("wall_s", "vmhwm_mb", "commits")}, flush=True)
    import json, subprocess
    out = subprocess.run([sys.executable, "verify.py", name, str(base_snap)], capture_output=True, text=True, check=True).stdout
    v = json.loads(out.strip().splitlines()[-1])
    d, p, fp_before = tuple(v["duckdb_after"]), tuple(v["pyiceberg_after"]), tuple(v["duckdb_before"])
    ok = d == p
    rec["parity"] = {"duckdb": d, "pyiceberg": p, "equal": ok, "equal_to_before": d == fp_before, "verify_s": v["s"], "verify_vmhwm_mb": v["vmhwm_mb"]}
    rec["fp_before"] = fp_before
    emit(a.out, rec)
    print(rec, flush=True)
    # acceptance assertions (the stubbed run must fail the first one)
    assert after["pos_delete_files"] == 0, f"delete-file count after compaction is {after['pos_delete_files']}, expected 0"
    assert ok, f"read parity failed: duckdb={d} pyiceberg={p}"
    assert d == fp_before, "compaction changed the table contents"
    assert rec["commits"] == 1, f"expected exactly 1 commit, got {rec['commits']}"
    print("ALL ASSERTIONS PASSED")


if __name__ == "__main__":
    main()
