"""Snapshot expiry + orphan removal on a table that was already compacted.
    python ops_expire_orphans.py --table s5_main_50m
Steps: (1) inventory, (2) create an old orphan and an in-flight one, (3) orphan dry run + removal with a
safety delay, (4) in-flight file still commits, (5) expire (retain-last / older-than), (6) orphan removal
reclaims the files only the expired snapshots referenced, (7) integrity: every reachable object exists and
PyIceberg = DuckDB.
"""
import argparse
import json
import time
import uuid
from datetime import datetime, timedelta, timezone

import pyarrow as pa
from pyiceberg.io.pyarrow import _dataframe_to_data_files

import maintain
from check import duckdb_fp, pyiceberg_fp
from common import NAMESPACE, catalog, duck, emit, file_counts, key_of, list_objects, s3, BUCKET

ap = argparse.ArgumentParser()
ap.add_argument("--table", required=True)
ap.add_argument("--retain-last", type=int, default=1)
ap.add_argument("--safety-s", type=int, default=30)
ap.add_argument("--out", default="results/ops.jsonl")
a = ap.parse_args()
name = f"{NAMESPACE}.{a.table}"
cat = catalog(); tbl = cat.load_table(name); con = duck("2GB")
rec = {"table": a.table, "safety_s": a.safety_s}


def storage(tbl):
    objs = list_objects(key_of(tbl.location()).rstrip("/") + "/")
    return {"objects": len(objs), "bytes": sum(o["Size"] for o in objs)}


def batch(n, lo):
    con2 = duck("1GB")
    t = con2.execute(f"SELECT i AS id, 1::INT AS cat, DATE '2024-01-01' AS d, 1.0 AS amount, 'orphan' AS payload FROM range({lo},{lo+n}) t(i)").to_arrow_table()
    return t.cast(pa.schema([pa.field(f.name, f.type, nullable=False) for f in tbl.schema().as_arrow()]))


rec["start"] = {"snapshots": len(tbl.snapshots()), "storage": storage(tbl), **file_counts(tbl)}
print("start", rec["start"], flush=True)
rec["fp_start"] = duckdb_fp(con, name)

# (2) one orphan older than the safety delay (a failed write), then wait, then one in-flight write
old = list(_dataframe_to_data_files(tbl.metadata, batch(1000, 10**12), tbl.io))
time.sleep(a.safety_s + 2)
inflight = list(_dataframe_to_data_files(tbl.metadata, batch(1000, 10**12 + 5000), tbl.io))
old_key, inflight_key = key_of(old[0].file_path), key_of(inflight[0].file_path)

# (3) safety delay protects the in-flight file
t0 = time.time()
dry = maintain.remove_orphans(tbl, timedelta(seconds=a.safety_s), dry_run=True)
rec["orphan_scan_s"] = round(time.time() - t0, 1)
rec["orphan_dry_run"] = dry
print("dry run", dry, flush=True)
keys_now = {o["Key"] for o in list_objects(key_of(tbl.location()).rstrip("/") + "/")}
assert old_key in keys_now and inflight_key in keys_now
res = maintain.remove_orphans(tbl, timedelta(seconds=a.safety_s))
keys_after = {o["Key"] for o in list_objects(key_of(tbl.location()).rstrip("/") + "/")}
rec["orphan_removal_1"] = res
rec["old_orphan_removed"] = old_key not in keys_after
rec["inflight_kept"] = inflight_key in keys_after
assert old_key not in keys_after, "old orphan should have been removed"
assert inflight_key in keys_after, "in-flight file must survive the safety delay"

# (4) the in-flight write can still commit
tbl.refresh()
with tbl.transaction() as tx:
    fa = tx.update_snapshot().fast_append()
    for f in inflight:
        fa.append_data_file(f)
    fa.commit()
rec["inflight_commit_ok"] = True

# (5) expire: older-than first (nothing newer than 1 day is older -> expires none of the recent), then retain-last
tbl.refresh(); n0 = len(tbl.snapshots())
rec["expire_older_than_1d"] = maintain.expire_snapshots(tbl, older_than=datetime.now(timezone.utc) - timedelta(days=1))
t0 = time.time()
n_exp = maintain.expire_snapshots(tbl, retain_last=a.retain_last)
rec["expire"] = {"retain_last": a.retain_last, "expired": n_exp, "snapshots_before": n0, "s": round(time.time() - t0, 2)}
tbl.refresh()
rec["expire"]["snapshots_after"] = len(tbl.snapshots())
print("expire", rec["expire"], flush=True)
rec["storage_after_expire"] = storage(tbl)

# (6) reclaim: files only the expired snapshots referenced
t0 = time.time()
res2 = maintain.remove_orphans(tbl, timedelta(seconds=a.safety_s))
rec["orphan_removal_2"] = {**res2, "s": round(time.time() - t0, 1)}
tbl.refresh()
rec["end"] = {"snapshots": len(tbl.snapshots()), "storage": storage(tbl), **file_counts(tbl)}
print("end", rec["end"], flush=True)

# (7) integrity
keep = maintain.reachable_keys(tbl)
c = s3()
missing = [k for k in keep if not list_objects(k)]  # head via list: cheap enough for the spike's file counts
rec["missing_reachable_objects"] = len(missing)
d, p = duckdb_fp(con, name), pyiceberg_fp(tbl)
rec["parity"] = {"duckdb": d, "pyiceberg": p, "equal": d == p, "rows_added_by_inflight": d[0] - rec["fp_start"][0]}
assert d[0] - rec["fp_start"][0] == 1000, "the in-flight file's 1000 rows must be in the table after its commit"
print(rec["parity"], "missing:", len(missing))
# time travel to an expired snapshot must fail cleanly
try:
    tbl.scan(snapshot_id=tbl.metadata.snapshots[0].snapshot_id - 1).to_arrow()
except Exception as e:
    rec["time_travel_expired"] = f"{type(e).__name__}: {str(e)[:100]}"
emit(a.out, rec)
assert not missing and d == p
print("OK")
