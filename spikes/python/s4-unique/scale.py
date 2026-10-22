"""G6 scale run: exact unique check of a 10M-row staged batch against a large Iceberg history.

  scale.py seed   --table T --rows N [--partition none|bucket] [--chunk C]
  scale.py stage  --table T --history-rows N --batch-rows M
  scale.py verify --table T --cols id            # full brute force, writes the expected flags
  scale.py run    --table T --cols id --memory-limit 1GB [--memory-limit 512MB ...]

History row i: id = i, account = 'a' || (i % 1000), seq = i // 1000, plus two payload columns.
Batch row j (placeholder mix, no OLF number yet), by hash(j) % 1000:
  < 10   collision: the key of a random history row (~1%)
  = 10   near miss: a history `seq` with an account the history never has (`b...`), new id
  = 11   in-batch duplicate of the key of an earlier batch row
  12-14  id / account / seq is NULL (on top of the row's key)
  else   new key.

`run` executes unique.unique_check in a child process (DuckDB only, no PyIceberg) and records
wall time, peak memory (the child's VmHWM and its peak RssAnon, polled every 100 ms; not wait4's
ru_maxrss, which Linux carries over from the parent across fork/exec) and peak spill (temp dir
size polled every 100 ms). Each output is compared row for row with the expected flags written by
`verify`, which recomputes them without DuckDB: PyArrow over the batch, and a PyIceberg stream of
the history's key columns probed against the batch's distinct keys.
"""

import argparse
import json
import os
import pathlib
import shutil
import subprocess
import sys
import threading
import time

HERE = pathlib.Path(__file__).parent
STAGE = HERE / ".stage"
SPILL = HERE / ".spill"

HISTORY_ROW = """
    i::BIGINT AS id,
    'a' || (i % 1000)::VARCHAR AS account,
    (i // 1000)::BIGINT AS seq,
    ((hash(i) >> 20) % 1000000)::DOUBLE / 100 AS amount,
    'n' || (hash(i) % 100000000)::VARCHAR AS name
"""


def batch_path(table):
    return STAGE / f"{table}_batch.parquet"


def expected_path(table, cols):
    return STAGE / f"{table}_expected_{'_'.join(cols)}.parquet"


def seed(args):
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import BucketTransform
    from pyiceberg.types import DoubleType, LongType, NestedField, StringType

    from common import NAMESPACE, catalog, duck, fq

    schema = Schema(
        NestedField(1, "id", LongType()),
        NestedField(2, "account", StringType()),
        NestedField(3, "seq", LongType()),
        NestedField(4, "amount", DoubleType()),
        NestedField(5, "name", StringType()),
    )
    tbl = catalog().create_table(f"{NAMESPACE}.{args.table}", schema=schema)
    if args.partition == "bucket":
        with tbl.update_spec() as upd:
            upd.add_field("id", BucketTransform(16), "id_bucket")
    # PyIceberg 0.12 cannot append to bucketed tables without pyiceberg-core: DuckDB INSERT seeds
    # both layouts, one commit per chunk of sequential keys. The chunk sets the file size of a
    # bucketed table (each chunk writes one file per bucket).
    con = duck(memory_limit="4GB", temp_dir=str(SPILL))
    start = time.monotonic()
    for lo in range(0, args.rows, args.chunk):
        hi = min(lo + args.chunk, args.rows)
        con.execute(f"INSERT INTO {fq(args.table)} SELECT {HISTORY_ROW} FROM range({lo}, {hi}) r(i)")
        print(f"seeded {hi:,} rows ({time.monotonic() - start:.0f}s)", flush=True)
    tbl.refresh()
    print(json.dumps({"seed_rows": args.rows, "seed_s": round(time.monotonic() - start, 1), **table_stats(tbl)}))


def table_stats(tbl):
    files = tbl.inspect.files().select(["record_count", "file_size_in_bytes"]).to_pylist()
    counts = sorted(f["record_count"] for f in files)
    return {
        "data_files": len(files),
        "rows_per_file_median": counts[len(counts) // 2] if counts else 0,
        "table_mb": round(sum(f["file_size_in_bytes"] for f in files) / 2**20),
    }


def stage(args):
    from common import duck

    STAGE.mkdir(exist_ok=True)
    h, b = args.history_rows, args.batch_rows
    con = duck()
    con.execute(f"""
        COPY (
            SELECT
                CASE WHEN c = 12 THEN NULL ELSE id END AS id,
                CASE WHEN c = 13 THEN NULL ELSE account END AS account,
                CASE WHEN c = 14 THEN NULL ELSE seq END AS seq,
                amount, name
            FROM (
                SELECT j, c,
                    CASE WHEN cs < 10 THEN hi::BIGINT ELSE {h} + s END AS id,
                    CASE WHEN cs < 10 THEN 'a' || (hi % 1000)::VARCHAR
                         WHEN cs = 10 THEN 'b' || (hi % 1000)::VARCHAR
                         ELSE 'a' || (s % 1000)::VARCHAR END AS account,
                    CASE WHEN cs <= 10 THEN (hi // 1000)::BIGINT ELSE {h // 1000 + 1} + s // 1000 END AS seq,
                    ((hash(j) >> 20) % 1000000)::DOUBLE / 100 AS amount,
                    'm' || (hash(j) % 100000000)::VARCHAR AS name
                FROM (
                    SELECT j, c, s, hash(s) % 1000 AS cs, hash(s + 1000000007) % {h} AS hi
                    FROM (
                        SELECT j, c,
                            CASE WHEN c = 11 AND j > 5000 THEN j - 1 - (hash(j * 3) % 5000)::BIGINT ELSE j END AS s
                        FROM (SELECT range AS j, hash(range) % 1000 AS c FROM range({b}))
                    )
                )
            )
            ORDER BY j
        ) TO '{batch_path(args.table)}' (FORMAT parquet)
    """)
    print(json.dumps({"batch": str(batch_path(args.table)), "rows": b,
                      "mb": round(batch_path(args.table).stat().st_size / 2**20)}))


def _key(tbl, cols):
    import pyarrow as pa
    import pyarrow.compute as pc

    if len(cols) == 1:
        return tbl.column(cols[0]).combine_chunks()
    parts = [pc.cast(tbl.column(c), pa.string()) for c in cols]
    return pc.binary_join_element_wise(*parts, "\x1f").combine_chunks()


def verify(args):
    """Expected flags without DuckDB: the Rust semantics (reference.py) expressed in PyArrow."""
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    from common import NAMESPACE, catalog

    cols = args.cols
    t = time.monotonic()
    inc = pq.read_table(batch_path(args.table), columns=cols)
    inc = inc.append_column("row_idx", pa.array(range(inc.num_rows), pa.int64()))
    valid = pc.and_kleene(*[pc.is_valid(inc.column(c)) for c in cols]) if len(cols) > 1 else pc.is_valid(inc.column(cols[0]))
    inc = inc.filter(valid)
    inc = pa.table({"row_idx": inc.column("row_idx"), "key": _key(inc, cols)})
    batch_keys = pc.unique(inc.column("key"))

    # Probe in chunks of >= 20M history rows: is_in builds its hash set on every call.
    hit = pa.array([False] * len(batch_keys))
    hist_rows = 0
    pending = []

    def probe():
        part = pa.Table.from_batches(pending)
        part = part.filter(pc.and_kleene(*[pc.is_valid(part.column(c)) for c in cols]) if len(cols) > 1 else pc.is_valid(part.column(cols[0])))
        pending.clear()
        return pc.or_(hit, pc.is_in(batch_keys, value_set=_key(part, cols)))

    tbl = catalog().load_table(f"{NAMESPACE}.{args.table}")
    for rb in tbl.scan(selected_fields=tuple(cols)).to_arrow_batch_reader():
        hist_rows += rb.num_rows
        pending.append(rb)
        if sum(b.num_rows for b in pending) >= 20_000_000:
            hit = probe()
    if pending:
        hit = probe()
    hit_keys = batch_keys.filter(hit)

    first = inc.group_by("key").aggregate([("row_idx", "min")])
    joined = inc.join(first, "key")
    in_target = pc.is_in(joined.column("key"), value_set=hit_keys)
    later = pc.not_equal(joined.column("row_idx"), joined.column("row_idx_min"))
    flagged = joined.filter(pc.or_(in_target, later))
    kind = pc.if_else(pc.is_in(flagged.column("key"), value_set=hit_keys), "target", "batch")
    out = pa.table({"row_idx": flagged.column("row_idx"), "kind": kind}).sort_by("row_idx")
    pq.write_table(out, expected_path(args.table, cols))
    summary = {"table": args.table, "cols": cols, "history_rows_streamed": hist_rows, "batch_rows_checked": inc.num_rows,
               "flagged": out.num_rows, "target": pc.sum(pc.equal(out.column("kind"), "target")).as_py() or 0,
               "verify_s": round(time.monotonic() - t, 1)}
    summary["batch"] = summary["flagged"] - summary["target"]
    if args.python_reference:  # small scale only: the verifier itself against reference.py
        import reference

        hist = tbl.scan(selected_fields=tuple(cols)).to_arrow().to_pylist()
        want = reference.unique_check(hist, pq.read_table(batch_path(args.table), columns=cols).to_pylist(), cols)
        summary["matches_python_reference"] = want == dict(zip(out.column("row_idx").to_pylist(), out.column("kind").to_pylist()))
    print(json.dumps(summary))
    append_result(args.table, {"verify": summary})


def check_child(args):
    """Runs in the measured child process: one unique check, DuckDB only."""
    import unique
    from common import duck, fq

    con = duck(memory_limit=args.memory_limit, temp_dir=str(SPILL), threads=args.threads)
    con.execute(f"SET max_temp_directory_size = '{args.max_spill}'")
    if args.profile:
        con.execute("SET enable_profiling = 'query_tree'")
        con.execute(f"SET profiling_output = '{args.profile}'")
    t = time.monotonic()
    try:
        unique.unique_check(con, fq(args.table), str(batch_path(args.table)), args.cols, args.out)
    except Exception as e:  # noqa: BLE001 - the parent records the error as a result
        print(json.dumps({"check_s": round(time.monotonic() - t, 2), "error": str(e).splitlines()[0][:400]}))
        sys.exit(1)
    print(json.dumps({"check_s": round(time.monotonic() - t, 2)}))


def dir_size(path):
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except FileNotFoundError:
                pass
    return total


def proc_status_kb(pid, fields):
    out = dict.fromkeys(fields, 0)
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                name = line.split(":")[0]
                if name in out:
                    out[name] = int(line.split()[1])
    except (FileNotFoundError, ProcessLookupError):
        pass
    return out


def run_one(args, limit, out, profile):
    shutil.rmtree(SPILL, ignore_errors=True)
    SPILL.mkdir()
    cmd = [sys.executable, __file__, "check-child", "--table", args.table, "--cols", *args.cols,
           "--memory-limit", limit, "--threads", str(args.threads), "--max-spill", args.max_spill,
           "--out", str(out), "--profile", profile]
    start = time.monotonic()
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
    peak = {"VmHWM": 0, "RssAnon": 0, "spill": 0}
    done = threading.Event()

    def poll():
        while not done.is_set():
            st = proc_status_kb(proc.pid, ("VmHWM", "RssAnon"))
            peak["VmHWM"] = max(peak["VmHWM"], st["VmHWM"])
            peak["RssAnon"] = max(peak["RssAnon"], st["RssAnon"])
            peak["spill"] = max(peak["spill"], dir_size(SPILL))
            time.sleep(0.1)

    th = threading.Thread(target=poll)
    th.start()
    _, status, _ = os.wait4(proc.pid, 0)
    done.set()
    th.join()
    lines = proc.stdout.read().strip().splitlines()
    child = json.loads(lines[-1]) if lines else {"error": f"exit status {status}"}
    if status != 0 and "error" not in child:
        child["error"] = f"exit status {status}"
    return {"memory_limit": limit, "wall_s": round(time.monotonic() - start, 1), **child,
            "peak_vmhwm_mb": round(peak["VmHWM"] / 1024), "peak_rss_anon_mb": round(peak["RssAnon"] / 1024),
            "peak_spill_mb": round(peak["spill"] / 2**20)}


def compare(out, expected):
    import pyarrow.parquet as pq

    got, want = pq.read_table(out), pq.read_table(expected)
    exact = got.num_rows == want.num_rows and got.column("row_idx").equals(want.column("row_idx")) \
        and got.column("kind").cast("string").equals(want.column("kind").cast("string"))
    return {"flagged": got.num_rows, "expected_flagged": want.num_rows, "exact": exact}


def append_result(table, rec):
    path = HERE / "results" / f"scale-{table}.jsonl"
    path.parent.mkdir(exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps(rec) + "\n")


def run(args):
    expected = expected_path(args.table, args.cols)
    assert expected.exists(), f"run `scale.py verify --table {args.table} --cols {' '.join(args.cols)}` first"
    plans = HERE / "results" / "plans"
    plans.mkdir(parents=True, exist_ok=True)
    for limit in args.memory_limit:
        free_gb = shutil.disk_usage(HERE).free / 2**30
        out = STAGE / f"{args.table}_out.parquet"
        profile = plans / f"{args.table}-{'_'.join(args.cols)}-{limit}.txt"
        rec = {"table": args.table, "cols": args.cols, "threads": args.threads, "max_spill": args.max_spill,
               "free_disk_gb_before": round(free_gb, 1), **run_one(args, limit, out, str(profile))}
        if "error" not in rec:
            rec.update(compare(out, expected))
        out.unlink(missing_ok=True)
        print(json.dumps(rec), flush=True)
        append_result(args.table, {"run": rec})
        if "error" in rec and args.stop_on_error:
            break


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("seed")
    s.add_argument("--rows", type=int, required=True)
    s.add_argument("--chunk", type=int, default=50_000_000)
    s.add_argument("--partition", choices=["none", "bucket"], default="none")
    g = sub.add_parser("stage")
    g.add_argument("--history-rows", type=int, required=True)
    g.add_argument("--batch-rows", type=int, default=10_000_000)
    v = sub.add_parser("verify")
    v.add_argument("--python-reference", action="store_true")
    r = sub.add_parser("run")
    r.add_argument("--memory-limit", action="append", required=True)
    r.add_argument("--stop-on-error", action="store_true")
    c = sub.add_parser("check-child")
    c.add_argument("--memory-limit", required=True)
    c.add_argument("--out", required=True)
    c.add_argument("--profile")
    for sp in (s, g, v, r, c):
        sp.add_argument("--table", required=True)
    for sp in (v, r, c):
        sp.add_argument("--cols", nargs="+", default=["id"])
    for sp in (r, c):
        sp.add_argument("--threads", type=int, default=os.cpu_count())
        sp.add_argument("--max-spill", default="15GB")
    args = p.parse_args()
    sys.path.insert(0, str(HERE))
    {"seed": seed, "stage": stage, "verify": verify, "run": run, "check-child": check_child}[args.cmd](args)


if __name__ == "__main__":
    main()
