"""G3 scale run: SCD2 MERGE of a staged Parquet batch into a large Iceberg target in Polaris.

  scale.py seed  --table T --rows N [--partition bucket|day|none]
  scale.py run   --table T --target-rows N --batch-rows M --merges K --memory-limit 4GB

`run` stages K batches (one per merge, disjoint key ranges), then runs each MERGE in a child
process and records wall time, peak RSS (the child's VmHWM, polled every 100 ms; wait4's
ru_maxrss is not used because Linux carries the parent's high-water mark across fork/exec),
peak spill (temp dir size polled every 100 ms), snapshots and catalog commits added, data and delete file counts,
and SCD2 contract counts. Results go to results/scale-<table>.json.

Batch composition (placeholder, no OLF number yet): 50% changed existing keys, 30% unchanged
existing keys, 20% new keys.
"""

import argparse
import datetime as dt
import json
import os
import pathlib
import shutil
import subprocess
import sys
import threading
import time

HERE = pathlib.Path(__file__).parent
SYS = ("__floe_is_current", "__floe_valid_from", "__floe_valid_to")
BUSINESS = ["id", "name", "amount", "event_date", "region"]
T0 = "TIMESTAMP '2026-01-01 00:00:00'"

# Deterministic business values of key i as first loaded; `name` is what a change rewrites.
ROW = """
    i::BIGINT AS id,
    'n' || (hash(i) % 100000000)::VARCHAR AS name,
    ((hash(i) >> 20) % 1000000)::DOUBLE / 100 AS amount,
    (DATE '2025-01-01' + (i % 365)::INTEGER) AS event_date,
    ['eu', 'us', 'apac'][(i % 3)::INTEGER + 1] AS region
"""


def seed(args):
    from pyiceberg.transforms import BucketTransform, DayTransform
    from pyiceberg.types import BooleanType, DateType, DoubleType, LongType, NestedField, StringType, TimestampType
    from pyiceberg.schema import Schema

    from common import NAMESPACE, catalog, duck, fq

    cat = catalog()
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "name", StringType()),
        NestedField(3, "amount", DoubleType()),
        NestedField(4, "event_date", DateType()),
        NestedField(5, "region", StringType()),
        NestedField(6, SYS[0], BooleanType(), required=True),
        NestedField(7, SYS[1], TimestampType(), required=True),
        NestedField(8, SYS[2], TimestampType()),
    )
    tbl = cat.create_table(f"{NAMESPACE}.{args.table}", schema=schema)
    if args.partition != "none":
        with tbl.update_spec() as upd:
            if args.partition == "bucket":
                upd.add_field("id", BucketTransform(16), "id_bucket")
            else:
                upd.add_field("event_date", DayTransform(), "event_day")
    con = duck(memory_limit=args.memory_limit, temp_dir=args.temp_dir)
    start = time.monotonic()
    # One INSERT per chunk keeps each Iceberg commit bounded; chunks are sequential key ranges.
    for lo in range(0, args.rows, args.chunk):
        hi = min(lo + args.chunk, args.rows)
        con.execute(
            f"INSERT INTO {fq(args.table)} SELECT {ROW}, true, {T0}, NULL::TIMESTAMP FROM range({lo}, {hi}) r(i)"
        )
        print(f"seeded {hi:,} rows ({time.monotonic() - start:.0f}s)", flush=True)
    print(json.dumps({"seed_rows": args.rows, "seed_s": round(time.monotonic() - start, 1)}))


def stage(con, path, k, target_rows, batch_rows):
    """Batch k: changed + unchanged keys from a disjoint range of the original keys, new keys
    above every key used so far."""
    changed, unchanged = batch_rows // 2, batch_rows * 3 // 10
    new = batch_rows - changed - unchanged
    base = (k - 1) * (changed + unchanged)
    assert base + changed + unchanged <= target_rows, "target too small for this many merges"
    new_base = target_rows + (k - 1) * new
    con.execute(f"""
        COPY (
            SELECT * REPLACE ('n' || id::VARCHAR || '_v{k}' AS name)
            FROM (SELECT {ROW} FROM range({base}, {base + changed}) r(i))
            UNION ALL
            SELECT {ROW} FROM range({base + changed}, {base + changed + unchanged}) r(i)
            UNION ALL
            SELECT {ROW} FROM range({new_base}, {new_base + new}) r(i)
        ) TO '{path}' (FORMAT parquet)
    """)
    return {"changed": changed, "unchanged": unchanged, "new": new}


def merge_child(args):
    """Runs in a child process: one SCD2 MERGE, prints the timestamp it used."""
    import merge
    from common import duck, fq

    con = duck(memory_limit=args.memory_limit, temp_dir=args.temp_dir, threads=args.threads)
    con.execute(f"SET max_temp_directory_size = '{args.max_spill}'")
    ts = dt.datetime.fromisoformat(args.ts)
    compare = [c for c in BUSINESS if c != "id"]
    t = time.monotonic()
    try:
        rows = merge.scd2(con, fq(args.table), args.source, ["id"], BUSINESS, compare, ts)
    except Exception as e:  # noqa: BLE001 - the parent records the error as a result
        print(json.dumps({"merge_s": round(time.monotonic() - t, 2), "error": str(e).splitlines()[0][:400]}))
        sys.exit(1)
    print(json.dumps({"merge_s": round(time.monotonic() - t, 2), "duckdb_rows_affected": rows[0][0]}))


def dir_size(path):
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except FileNotFoundError:
                pass
    return total


def vm_hwm_kb(pid):
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1])
    except (FileNotFoundError, ProcessLookupError):
        pass
    return 0


def run_one_merge(args, source, ts):
    shutil.rmtree(args.temp_dir, ignore_errors=True)
    os.makedirs(args.temp_dir)
    cmd = [sys.executable, __file__, "merge-child", "--table", args.table, "--source", str(source),
           "--ts", ts.isoformat(), "--memory-limit", args.memory_limit, "--temp-dir", args.temp_dir,
           "--threads", str(args.threads), "--max-spill", args.max_spill]
    start = time.monotonic()
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
    peak_spill = [0]
    hwm_kb = [0]
    done = threading.Event()

    def poll():
        while not done.is_set():
            peak_spill[0] = max(peak_spill[0], dir_size(args.temp_dir))
            hwm_kb[0] = max(hwm_kb[0], vm_hwm_kb(proc.pid))
            time.sleep(0.1)

    th = threading.Thread(target=poll)
    th.start()
    _, status, _ = os.wait4(proc.pid, 0)
    done.set()
    th.join()
    out = proc.stdout.read()
    wall = time.monotonic() - start
    if status != 0:
        child = {"error": out.strip().splitlines()[-1][:400] if out.strip() else f"exit status {status}"}
    else:
        child = json.loads(out.strip().splitlines()[-1])
    return {"wall_s": round(wall, 1), **child, "peak_rss_mb": round(hwm_kb[0] / 1024),
            "peak_spill_mb": round(peak_spill[0] / 2**20)}


def table_state(tbl):
    tbl.refresh()
    files = tbl.inspect.files().select(["content"]).to_pylist()
    return {
        "snapshots": len(tbl.snapshots()),
        "metadata_files": len(tbl.metadata.metadata_log) + 1,
        "data_files": sum(f["content"] == 0 for f in files),
        "position_delete_files": sum(f["content"] == 1 for f in files),
        "equality_delete_files": sum(f["content"] == 2 for f in files),
    }


def contract(con, name, ts, mix, keys_total):
    from common import fq

    t = fq(name)
    ts_lit = f"TIMESTAMP '{ts.isoformat(sep=' ')}'"
    got = con.execute(f"""
        SELECT
            count(*) FILTER ({SYS[2]} = {ts_lit}),
            count(*) FILTER ({SYS[1]} = {ts_lit}),
            count(*) FILTER ({SYS[0]}),
            count(*)
        FROM {t}
    """).fetchone()
    dup = con.execute(f"SELECT count(*) FROM (SELECT id FROM {t} WHERE {SYS[0]} GROUP BY id HAVING count(*) > 1)").fetchone()[0]
    want = (mix["changed"], mix["changed"] + mix["new"], keys_total)
    return {
        "closed": got[0], "inserted": got[1], "current_rows": got[2], "total_rows": got[3],
        "keys_with_2_current": dup,
        "pass": (got[0], got[1], got[2]) == want and dup == 0,
    }


def pyiceberg_parity(tbl, con, name, k, args):
    """PyIceberg vs DuckDB on the keys batch k touched (delete files must be honoured), plus a
    full streamed row count by PyIceberg."""
    from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan
    from common import fq

    changed = args.batch_rows // 2
    lo = (k - 1) * (changed + args.batch_rows * 3 // 10)
    hi = lo + 200_000  # 200k keys: changed ones get 2 rows each
    cols = BUSINESS + list(SYS)
    py = tbl.scan(row_filter=And(GreaterThanOrEqual("id", lo), LessThan("id", hi)), selected_fields=tuple(cols)).to_arrow()
    py_rows = sorted(tuple(r.values()) for r in py.select(cols).to_pylist())
    dd_rows = sorted(con.execute(f"SELECT {', '.join(cols)} FROM {fq(name)} WHERE id >= {lo} AND id < {hi}").fetchall())
    t = time.monotonic()
    count = 0
    if args.full_count:
        for batch in tbl.scan(selected_fields=("id",)).to_arrow_batch_reader():
            count += batch.num_rows
    return {"sample_keys": hi - lo, "sample_rows": len(py_rows), "sample_identical": py_rows == dd_rows,
            "pyiceberg_full_count": count if args.full_count else None,
            "pyiceberg_full_count_s": round(time.monotonic() - t, 1) if args.full_count else None}


def run(args):
    from common import NAMESPACE, catalog, duck

    cat = catalog()
    tbl = cat.load_table(f"{NAMESPACE}.{args.table}")
    stage_dir = pathlib.Path(args.stage_dir)
    stage_dir.mkdir(parents=True, exist_ok=True)
    con = duck()
    results = {"args": vars(args), "merges": [], "before": table_state(tbl)}
    keys_total = args.target_rows + (args.first_merge - 1) * (args.batch_rows - args.batch_rows // 2 - args.batch_rows * 3 // 10)
    last = args.first_merge + args.merges - 1
    for k in range(args.first_merge, last + 1):
        source = stage_dir / f"{args.table}_batch_{k}.parquet"
        mix = stage(con, source, k, args.target_rows, args.batch_rows)
        keys_total += mix["new"]
        before = table_state(tbl)
        ts = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
        perf = run_one_merge(args, source, ts)
        if "error" in perf:
            keys_total -= mix["new"]
        after = table_state(tbl)
        rec = {"merge": k, **mix, "staged_mb": round(source.stat().st_size / 2**20), **perf,
               "new_snapshots": after["snapshots"] - before["snapshots"],
               "new_catalog_commits": after["metadata_files"] - before["metadata_files"], **after}
        if args.verify and "error" not in perf:
            rec["contract"] = contract(con, args.table, ts, mix, keys_total)
            rec["pyiceberg"] = pyiceberg_parity(tbl, con, args.table, k, args)
        results["merges"].append(rec)
        print(json.dumps(rec), flush=True)
        source.unlink()
    # Rerun safety at scale: the last batch again has no changed rows left -> no new versions.
    if args.rerun:
        source = stage_dir / f"{args.table}_rerun.parquet"
        stage(con, source, last, args.target_rows, args.batch_rows)
        before = table_state(tbl)
        rows_before = con.execute(f"SELECT count(*) FROM polaris.{NAMESPACE}.{args.table}").fetchone()[0]
        perf = run_one_merge(args, source, dt.datetime.now(dt.timezone.utc).replace(tzinfo=None))
        after = table_state(tbl)
        rows_after = con.execute(f"SELECT count(*) FROM polaris.{NAMESPACE}.{args.table}").fetchone()[0]
        results["rerun"] = {**perf, "rows_before": rows_before, "rows_after": rows_after,
                            "new_snapshots": after["snapshots"] - before["snapshots"]}
        print(json.dumps(results["rerun"]), flush=True)
        source.unlink()
    out = HERE / "results" / f"scale-{args.table}-{args.first_merge}-{last}-{args.memory_limit}.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(results, indent=2, default=str) + "\n")


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("seed")
    s.add_argument("--table", required=True)
    s.add_argument("--rows", type=int, required=True)
    s.add_argument("--chunk", type=int, default=50_000_000)
    s.add_argument("--partition", choices=["none", "bucket", "day"], default="none")
    r = sub.add_parser("run")
    r.add_argument("--table", required=True)
    r.add_argument("--target-rows", type=int, required=True)
    r.add_argument("--batch-rows", type=int, required=True)
    r.add_argument("--merges", type=int, default=1)
    r.add_argument("--first-merge", type=int, default=1)
    r.add_argument("--verify", action="store_true")
    r.add_argument("--full-count", action="store_true")
    r.add_argument("--rerun", action="store_true")
    r.add_argument("--threads", type=int, default=os.cpu_count())
    r.add_argument("--stage-dir", default=str(HERE / ".stage"))
    c = sub.add_parser("merge-child")
    c.add_argument("--table", required=True)
    c.add_argument("--source", required=True)
    c.add_argument("--ts", required=True)
    c.add_argument("--threads", type=int)
    for sp in (s, r, c):
        sp.add_argument("--memory-limit", default="4GB")
        sp.add_argument("--temp-dir", default=str(HERE / ".spill"))
        sp.add_argument("--max-spill", default="15GB")
    args = p.parse_args()
    sys.path.insert(0, str(HERE))
    {"seed": seed, "run": run, "merge-child": merge_child}[args.cmd](args)


if __name__ == "__main__":
    main()
