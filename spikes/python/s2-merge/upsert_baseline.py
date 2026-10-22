"""Baseline only: PyIceberg `table.upsert` (copy-on-write, matched rows loaded in memory) vs the
DuckDB SCD1 MERGE, same unpartitioned target and batch, each in a fresh child process.

  upsert_baseline.py --target-rows 1000000 --batch-rows 100000
"""

import argparse
import json
import os
import pathlib
import subprocess
import sys
import time
import uuid

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))

from scale import ROW, vm_hwm_kb  # noqa: E402 - sys.path set above so the script runs from anywhere

BUSINESS = ["id", "name", "amount", "event_date", "region"]


def child(args):
    import pyarrow.parquet as pq

    import merge
    from common import NAMESPACE, catalog, duck, fq

    t = time.monotonic()
    if args.engine == "pyiceberg":
        tbl = catalog().load_table(f"{NAMESPACE}.{args.table}")
        res = tbl.upsert(pq.read_table(args.source), join_cols=["id"])
        out = {"updated": res.rows_updated, "inserted": res.rows_inserted}
    else:
        rows = merge.scd1(duck(), fq(args.table), args.source, ["id"], BUSINESS)
        out = {"rows_affected": rows[0][0]}
    # The child's own VmHWM: wait4's ru_maxrss keeps the parent's high-water mark across exec.
    out["peak_rss_mb"] = round(vm_hwm_kb(os.getpid()) / 1024)
    print(json.dumps({"engine_s": round(time.monotonic() - t, 2), **out}))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--target-rows", type=int, default=1_000_000)
    p.add_argument("--batch-rows", type=int, default=100_000)
    p.add_argument("--child", action="store_true")
    p.add_argument("--engine")
    p.add_argument("--table")
    p.add_argument("--source")
    args = p.parse_args()
    if args.child:
        return child(args)

    from common import NAMESPACE, catalog, duck, fq

    cat = catalog()
    con = duck()
    stage = HERE / ".stage"
    stage.mkdir(exist_ok=True)
    source = stage / "upsert_batch.parquet"
    half = args.batch_rows // 2
    # Half the batch updates existing keys, half inserts new ones.
    con.execute(f"""
        COPY (
            SELECT * REPLACE ('n' || id::VARCHAR || '_u' AS name) FROM (SELECT {ROW} FROM range(0, {half}) r(i))
            UNION ALL SELECT {ROW} FROM range({args.target_rows}, {args.target_rows + args.batch_rows - half}) r(i)
        ) TO '{source}' (FORMAT parquet)
    """)
    results = {"target_rows": args.target_rows, "batch_rows": args.batch_rows}
    for engine in ("pyiceberg", "duckdb"):
        name = f"upsert_{engine}_{uuid.uuid4().hex[:6]}"
        con.execute(f"CREATE TABLE {fq(name)} AS SELECT {ROW} FROM range(0, {args.target_rows}) r(i)")
        tbl = cat.load_table(f"{NAMESPACE}.{name}")
        snaps = len(tbl.snapshots())
        start = time.monotonic()
        proc = subprocess.Popen([sys.executable, __file__, "--child", "--engine", engine, "--table", name,
                                 "--source", str(source)], stdout=subprocess.PIPE, text=True)
        _, status, _ = os.wait4(proc.pid, 0)
        out = proc.stdout.read()
        tbl.refresh()
        files = tbl.inspect.files().select(["content"]).to_pylist()
        n = con.execute(f"SELECT count(*), count(*) FILTER (name LIKE '%_u') FROM {fq(name)}").fetchone()
        results[engine] = {
            "ok": status == 0,
            "wall_s": round(time.monotonic() - start, 1),
            **(json.loads(out.strip().splitlines()[-1]) if status == 0 else {"error": out[-400:]}),
            "new_snapshots": len(tbl.snapshots()) - snaps,
            "data_files": sum(f["content"] == 0 for f in files),
            "delete_files": sum(f["content"] != 0 for f in files),
            "rows_after": n[0],
            "updated_rows_visible": n[1],
        }
        cat.drop_table(f"{NAMESPACE}.{name}")
    print(json.dumps(results, indent=2))
    out = HERE / "results" / f"upsert-baseline-{args.target_rows}.json"
    out.write_text(json.dumps(results, indent=2) + "\n")
    source.unlink()


if __name__ == "__main__":
    main()
