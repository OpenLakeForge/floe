"""#609: DuckDB SCD2 MERGE into partitioned Iceberg targets at scale.

Reuses #598 (`../s2-merge`): `common.py` (Polaris/S3 connections), `merge.py` (the SCD2 MERGE),
`scale.py` (seed and row generator). Nothing there is modified.

  spike.py seed  --table T --rows N --partition bucket|day|none
  spike.py run   --table T --batch-rows M --approach plain|split|chunks [--slices S] [--chunks K]
                 --memory-limit 4GB [--set name=value ...] [--python .venv2/bin/python] [--verify]

Approaches (each run = one child process, all statements of a run in one DuckDB transaction):
  plain   one MERGE of the whole batch (#598's code path)
  split   the batch split by target partition (bucket of `id` computed with pyiceberg-core, the
          Iceberg reference implementation, or the day of `event_date`), grouped into S slices;
          one MERGE per slice inside one BEGIN ... COMMIT
  chunks  the batch split into K key-range chunks (partition-agnostic); one MERGE per chunk
          inside one BEGIN ... COMMIT

Batches use disjoint key ranges across runs (tracked in results/keys-<table>.json and advanced
only after a committed run), so each run's SCD2 contract is checkable: 50% changed existing keys,
30% unchanged existing keys, 20% new keys (same mix as #598).

Measured per run: wall time, the child's peak VmHWM and peak RssAnon (polled every 100 ms from
/proc, not wait4's ru_maxrss), peak spill (temp dir size), new snapshots, new catalog commits
(new *.metadata.json objects in the table's metadata prefix on S3), file counts, SCD2 contract.
Results are appended to results/runs.jsonl.
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

HERE = pathlib.Path(__file__).resolve().parent
S2 = HERE.parent / "s2-merge"
sys.path.insert(0, str(S2))

BUSINESS = ["id", "name", "amount", "event_date", "region"]
SYS = ("__floe_is_current", "__floe_valid_from", "__floe_valid_to")
RESULTS = HERE / "results"


# ---------------------------------------------------------------- seed (delegates to #598)

def seed(args):
    """#598's seed; --set applies DuckDB settings to its connection (scale.seed imports
    common.duck at call time, so wrapping it here is enough)."""
    import common
    import scale

    duck = common.duck

    def duck_with_settings(*a, **kw):
        con = duck(*a, **kw)
        for s in args.set or []:
            con.execute("SET {} = {}".format(*s.split("=", 1)))
        return con

    common.duck = duck_with_settings
    if not args.days_per_insert:
        scale.seed(args)
        return
    # Same table and rows as scale.seed, but each INSERT holds only the rows of a group of days
    # (event_date = 2025-01-01 + i % 365): a partitioned INSERT's memory grows with the number of
    # partitions it writes at once, and 365 at a time runs out of memory even at 10GB.
    rows = args.rows
    args.rows = 0
    scale.seed(args)  # creates the table only
    con = common.duck(memory_limit=args.memory_limit, temp_dir=args.temp_dir)
    start = time.monotonic()
    for d0 in range(0, 365, args.days_per_insert):
        d1 = min(d0 + args.days_per_insert, 365)
        con.execute(
            f"INSERT INTO {common.fq(args.table)} SELECT {scale.ROW}, true, {scale.T0}, NULL::TIMESTAMP "
            f"FROM range(0, {rows}) r(i) WHERE i % 365 >= {d0} AND i % 365 < {d1}"
        )
        print(f"seeded days [{d0}, {d1}) ({time.monotonic() - start:.0f}s)", flush=True)
    print(json.dumps({"seed_rows": rows, "seed_s": round(time.monotonic() - start, 1)}))


# ---------------------------------------------------------------- staging

def keys_state(table, target_rows=None):
    path = RESULTS / f"keys-{table}.json"
    if path.exists():
        return json.loads(path.read_text())
    assert target_rows, "first run on a table needs --target-rows"
    return {"next_old": 0, "next_new": target_rows, "runs": 0}


def stage(con, path, st, batch_rows, tag):
    """One batch at the current key offsets: changed + unchanged keys taken from never-touched
    original keys, new keys above every key used so far."""
    from scale import ROW

    changed, unchanged = batch_rows // 2, batch_rows * 3 // 10
    new = batch_rows - changed - unchanged
    old, nxt = st["next_old"], st["next_new"]
    con.execute(f"""
        COPY (
            SELECT * REPLACE ('n' || id::VARCHAR || '_r{tag}' AS name)
            FROM (SELECT {ROW} FROM range({old}, {old + changed}) r(i))
            UNION ALL
            SELECT {ROW} FROM range({old + changed}, {old + changed + unchanged}) r(i)
            UNION ALL
            SELECT {ROW} FROM range({nxt}, {nxt + new}) r(i)
        ) TO '{path}' (FORMAT parquet)
    """)
    return {"changed": changed, "unchanged": unchanged, "new": new}


def partition_slices(tbl, path, n_slices, out_dir):
    """Split a staged batch by the target's partition value into n_slices files. Each slice
    holds whole partitions: bucket b -> slice b % n; day d -> slice d % n."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    from pyiceberg.transforms import BucketTransform, DayTransform

    fields = tbl.spec().fields
    assert len(fields) == 1, f"one partition field expected, got {fields}"
    f = fields[0]
    data = pq.read_table(path)
    if isinstance(f.transform, BucketTransform):
        from pyiceberg_core import transform

        part = transform.bucket(data.column("id").combine_chunks(), f.transform.num_buckets)
    elif isinstance(f.transform, DayTransform):
        part = pc.cast(pc.cast(data.column("event_date"), "date32"), "int32")
    else:
        raise SystemExit(f"split: unsupported transform {f.transform}")
    part = pc.cast(part, "int64")  # >= 0 here (bucket, or days after 1970), so truncating division is floor
    slot = pc.subtract(part, pc.multiply(pc.divide(part, n_slices), n_slices))
    files = []
    for s in range(n_slices):
        piece = data.filter(pc.equal(slot, s))
        if piece.num_rows:
            p = out_dir / f"slice_{s:03d}.parquet"
            pq.write_table(piece, p)
            files.append(str(p))
    return files


def chunk_slices(path, n_chunks, out_dir):
    """Split a staged batch into n_chunks row ranges, in staged order (key ranges)."""
    import pyarrow.parquet as pq

    data = pq.read_table(path)
    step = -(-data.num_rows // n_chunks)
    files = []
    for c in range(n_chunks):
        p = out_dir / f"chunk_{c:03d}.parquet"
        pq.write_table(data.slice(c * step, step), p)
        files.append(str(p))
    return files


# ---------------------------------------------------------------- child: the timed merge

def merge_child(args):
    import duckdb
    import merge
    from common import duck, fq

    con = duck(memory_limit=args.memory_limit, temp_dir=args.temp_dir, threads=args.threads)
    con.execute(f"SET max_temp_directory_size = '{args.max_spill}'")
    for s in args.set or []:
        k, v = s.split("=", 1)
        con.execute(f"SET {k} = {v}")
    ts = dt.datetime.fromisoformat(args.ts)
    sources = json.loads(pathlib.Path(args.sources).read_text())
    compare = [c for c in BUSINESS if c != "id"]
    exts = con.execute(
        "SELECT extension_name, extension_version FROM duckdb_extensions() WHERE loaded AND extension_name IN ('iceberg','httpfs','avro')"
    ).fetchall()
    info = {"duckdb": duckdb.__version__, "extensions": dict(exts)}
    t = time.monotonic()
    per = []
    affected = 0
    try:
        if not args.autocommit:
            con.execute("BEGIN")
        for src in sources:
            s0 = time.monotonic()
            affected += merge.scd2(con, fq(args.table), src, ["id"], BUSINESS, compare, ts)[0][0]
            per.append(round(time.monotonic() - s0, 1))
            print(json.dumps({"progress": len(per), "of": len(sources), "s": per[-1]}), file=sys.stderr, flush=True)
        c0 = time.monotonic()
        if not args.autocommit:
            con.execute("COMMIT")
        commit_s = round(time.monotonic() - c0, 1)
    except Exception as e:  # noqa: BLE001 - the parent records the error as a result
        print(json.dumps({**info, "merge_s": round(time.monotonic() - t, 1), "statements_done": len(per),
                          "statement_s": per, "error": str(e).splitlines()[0][:500]}))
        sys.exit(1)
    print(json.dumps({**info, "merge_s": round(time.monotonic() - t, 1), "statements": len(per),
                      "statement_s": per, "commit_s": commit_s, "duckdb_rows_affected": affected}))


# ---------------------------------------------------------------- parent: measure

def dir_size(path):
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except FileNotFoundError:
                pass
    return total


def proc_mem_kb(pid):
    out = {}
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith(("VmHWM:", "RssAnon:")):
                    k, v = line.split()[:2]
                    out[k[:-1]] = int(v)
    except (FileNotFoundError, ProcessLookupError):
        pass
    return out


def run_child(args, sources_file, ts):
    shutil.rmtree(args.temp_dir, ignore_errors=True)
    os.makedirs(args.temp_dir)
    cmd = [args.python, __file__, "merge-child", "--table", args.table, "--sources", str(sources_file),
           "--ts", ts.isoformat(), "--memory-limit", args.memory_limit, "--temp-dir", args.temp_dir,
           "--threads", str(args.threads), "--max-spill", args.max_spill]
    for s in args.set or []:
        cmd += ["--set", s]
    if args.approach == "plain":
        cmd.append("--autocommit")  # exactly #598's single autocommitted MERGE
    start = time.monotonic()
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
    peak = {"spill": 0, "VmHWM": 0, "RssAnon": 0, "disk_free_min": shutil.disk_usage(HERE).free}
    done = threading.Event()

    def poll():
        while not done.is_set():
            peak["spill"] = max(peak["spill"], dir_size(args.temp_dir))
            for k, v in proc_mem_kb(proc.pid).items():
                peak[k] = max(peak[k], v)
            peak["disk_free_min"] = min(peak["disk_free_min"], shutil.disk_usage(HERE).free)
            time.sleep(0.1)

    th = threading.Thread(target=poll)
    th.start()
    out, _ = proc.communicate()
    done.set()
    th.join()
    wall = time.monotonic() - start
    lines = out.strip().splitlines()
    child = json.loads(lines[-1]) if lines else {"error": f"exit status {proc.returncode}"}
    if proc.returncode != 0 and "error" not in child:
        child["error"] = f"exit status {proc.returncode}"
    return {"wall_s": round(wall, 1), **child,
            "vm_hwm_mib": round(peak["VmHWM"] / 1024), "rss_anon_peak_mib": round(peak["RssAnon"] / 1024),
            "peak_spill_mib": round(peak["spill"] / 2**20), "disk_free_min_gib": round(peak["disk_free_min"] / 2**30, 1)}


def metadata_json_count(tbl):
    """Catalog commits = *.metadata.json objects in the table's metadata prefix (not
    metadata_log, which is capped by write.metadata.previous-versions-max)."""
    import boto3
    from common import S3_ENDPOINT, S3_KEY, S3_REGION, S3_SECRET

    loc = tbl.metadata.location.removeprefix("s3://")
    bucket, prefix = loc.split("/", 1)
    s3 = boto3.client("s3", endpoint_url=f"http://{S3_ENDPOINT}", aws_access_key_id=S3_KEY,
                      aws_secret_access_key=S3_SECRET, region_name=S3_REGION)
    n = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/metadata/"):
        n += sum(o["Key"].endswith(".metadata.json") for o in page.get("Contents", []))
    return n


def table_state(tbl):
    tbl.refresh()
    files = tbl.inspect.files().select(["content"]).to_pylist()
    return {
        "snapshots": len(tbl.snapshots()),
        "metadata_json": metadata_json_count(tbl),
        "data_files": sum(f["content"] == 0 for f in files),
        "position_delete_files": sum(f["content"] == 1 for f in files),
        "equality_delete_files": sum(f["content"] == 2 for f in files),
    }


def contract(con, name, ts, mix, current_before):
    from common import fq

    t = fq(name)
    ts_lit = f"TIMESTAMP '{ts.isoformat(sep=' ')}'"
    got = con.execute(f"""
        SELECT count(*) FILTER ({SYS[2]} = {ts_lit}), count(*) FILTER ({SYS[1]} = {ts_lit}),
               count(*) FILTER ({SYS[0]}), count(*)
        FROM {t}
    """).fetchone()
    dup = con.execute(f"SELECT count(*) FROM (SELECT id FROM {t} WHERE {SYS[0]} GROUP BY id HAVING count(*) > 1)").fetchone()[0]
    want = (mix["changed"], mix["changed"] + mix["new"], current_before + mix["new"])
    return {"closed": got[0], "inserted": got[1], "current_rows": got[2], "total_rows": got[3],
            "keys_with_2_current": dup, "pass": (got[0], got[1], got[2]) == want and dup == 0}


def pyiceberg_parity(tbl, con, name, st, mix):
    """PyIceberg vs DuckDB, row for row, on the first 100k changed keys, 50k unchanged keys and
    50k new keys this run touched (delete files must be honoured)."""
    from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan, Or
    from common import fq

    old, nxt = st["next_old"], st["next_new"]
    ranges = [(old, old + 100_000), (old + mix["changed"], old + mix["changed"] + 50_000), (nxt, nxt + 50_000)]
    cols = BUSINESS + list(SYS)
    flt = Or(*[And(GreaterThanOrEqual("id", lo), LessThan("id", hi)) for lo, hi in ranges])
    py = tbl.scan(row_filter=flt, selected_fields=tuple(cols)).to_arrow().select(cols)
    py_rows = sorted(tuple(r.values()) for r in py.to_pylist())
    where = " OR ".join(f"(id >= {lo} AND id < {hi})" for lo, hi in ranges)
    dd_rows = sorted(con.execute(f"SELECT {', '.join(cols)} FROM {fq(name)} WHERE {where}").fetchall())
    return {"sample_rows": len(py_rows), "identical": py_rows == dd_rows}


def run(args):
    from common import NAMESPACE, catalog, duck

    RESULTS.mkdir(exist_ok=True)
    cat = catalog()
    tbl = cat.load_table(f"{NAMESPACE}.{args.table}")
    st = keys_state(args.table, args.target_rows)
    work = pathlib.Path(args.stage_dir) / args.table
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    con = duck()
    tag = st["runs"] + 1
    full = work / "batch.parquet"
    mix = stage(con, full, st, args.batch_rows, tag)
    t = time.monotonic()
    if args.approach == "plain":
        sources = [str(full)]
    elif args.approach == "split":
        sources = partition_slices(tbl, full, args.slices, work)
    else:
        sources = chunk_slices(full, args.chunks, work)
    split_s = round(time.monotonic() - t, 1)
    sources_file = work / "sources.json"
    sources_file.write_text(json.dumps(sources))
    before = table_state(tbl)
    current_before = None
    if args.verify:
        from common import fq

        current_before = con.execute(f"SELECT count(*) FROM {fq(args.table)} WHERE {SYS[0]}").fetchone()[0]
    ts = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    perf = run_child(args, sources_file, ts)
    after = table_state(tbl)
    rec = {"label": args.label, "table": args.table, "spec": str(tbl.spec()), "approach": args.approach,
           "slices": len(sources), "batch_rows": args.batch_rows, "memory_limit": args.memory_limit,
           "threads": args.threads, "settings": args.set or [], **mix, "split_s": split_s, **perf,
           "new_snapshots": after["snapshots"] - before["snapshots"],
           "new_catalog_commits": after["metadata_json"] - before["metadata_json"],
           "new_snapshot_ops": [s.summary.operation.value for s in tbl.snapshots()[before["snapshots"]:]],
           **{k: v for k, v in after.items() if k != "metadata_json"}}
    if "error" not in perf:
        if args.verify:
            rec["contract"] = contract(con, args.table, ts, mix, current_before)
            rec["pyiceberg"] = pyiceberg_parity(tbl, con, args.table, st, mix)
        st = {"next_old": st["next_old"] + mix["changed"] + mix["unchanged"],
              "next_new": st["next_new"] + mix["new"], "runs": tag}
        (RESULTS / f"keys-{args.table}.json").write_text(json.dumps(st) + "\n")
    rec["finished_utc"] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    with open(RESULTS / "runs.jsonl", "a") as f:
        f.write(json.dumps(rec, default=str) + "\n")
    print(json.dumps(rec, default=str), flush=True)
    shutil.rmtree(work, ignore_errors=True)
    return 0 if "error" not in perf else 1


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("seed")
    s.add_argument("--table", required=True)
    s.add_argument("--rows", type=int, required=True)
    s.add_argument("--chunk", type=int, default=50_000_000)
    s.add_argument("--partition", choices=["none", "bucket", "day"], default="none")
    s.add_argument("--set", action="append", help="DuckDB setting name=value for the seed connection")
    s.add_argument("--days-per-insert", type=int, help="day layout: one INSERT per group of this many days")
    r = sub.add_parser("run")
    r.add_argument("--table", required=True)
    r.add_argument("--target-rows", type=int, help="seed size, needed on the first run only")
    r.add_argument("--batch-rows", type=int, required=True)
    r.add_argument("--approach", choices=["plain", "split", "chunks"], default="plain")
    r.add_argument("--slices", type=int, default=16)
    r.add_argument("--chunks", type=int, default=1)
    r.add_argument("--verify", action="store_true")
    r.add_argument("--label", default="")
    r.add_argument("--threads", type=int, default=os.cpu_count())
    r.add_argument("--python", default=sys.executable, help="interpreter of the timed child (DuckDB version)")
    r.add_argument("--stage-dir", default=str(HERE / ".stage"))
    r.add_argument("--set", action="append", help="DuckDB setting name=value, applied in the child")
    c = sub.add_parser("merge-child")
    c.add_argument("--table", required=True)
    c.add_argument("--sources", required=True)
    c.add_argument("--ts", required=True)
    c.add_argument("--threads", type=int)
    c.add_argument("--set", action="append")
    c.add_argument("--autocommit", action="store_true")
    for sp in (s, r, c):
        sp.add_argument("--memory-limit", default="4GB")
        sp.add_argument("--temp-dir", default=str(HERE / ".spill"))
        sp.add_argument("--max-spill", default="12GB")
    args = p.parse_args()
    sys.exit({"seed": seed, "run": run, "merge-child": merge_child}[args.cmd](args) or 0)


if __name__ == "__main__":
    main()
