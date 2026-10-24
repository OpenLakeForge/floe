"""Concurrency: `floe run`-style commits while `floe maintain` compacts. Each scenario seeds its own table,
injects the concurrent commit at a precise point of the compaction, and checks the invariants.

    python conc.py                       # all scenarios on 10M-row tables
    python conc.py --only c6_append_storm --rows 50000000
"""
import argparse
import logging
import subprocess
import sys
import threading
import time
import traceback

import pyarrow as pa

import maintain
from check import duckdb_fp, pyiceberg_fp
from common import NAMESPACE, catalog, drop_and_purge, duck, emit, file_counts, key_of, list_objects

ap = argparse.ArgumentParser()
ap.add_argument("--rows", type=int, default=10_000_000)
ap.add_argument("--only")
ap.add_argument("--out", default="results/conc.jsonl")
a = ap.parse_args()


class Retries(logging.Handler):
    def __init__(self):
        super().__init__()
        self.n = 0
        self.msgs = []

    def emit(self, record):
        m = record.getMessage()
        if "retrying" in m:
            self.n += 1
            self.msgs.append(m)


def n_meta(tbl):
    return len([o for o in list_objects(key_of(tbl.location()).rstrip("/") + "/metadata/") if o["Key"].endswith(".metadata.json")])


def new_rows(tbl, lo, n=10_000):
    c = duck("1GB")
    t = c.execute(f"SELECT i AS id, 7::INT AS cat, DATE '2025-06-01' AS d, 1.0 AS amount, 'conc' AS payload FROM range({lo},{lo+n}) t(i)").to_arrow_table()
    return t.cast(tbl.schema().as_arrow())


def uniq_ok(con, name):
    n, u = con.execute(f"SELECT count(*), count(DISTINCT (id, d)) FROM polaris.{name}").fetchone()
    return n, u


MERGE = """MERGE INTO polaris.{name} t USING (SELECT i AS id, 'c' AS x, 5555.0 AS amount, DATE '2024-01-01' + ((hash(i + 7) % 365))::INT AS d
        FROM range(0, 100000) t(i)) b ON t.id = b.id AND t.d = b.d
        WHEN MATCHED THEN UPDATE SET amount = b.amount, payload = 'merged'"""


def run_scenario(sid, hook_factory, *, stock=False, rows=a.rows, after=None, spread="contiguous"):
    name = f"{NAMESPACE}.s5_conc_{sid}"
    subprocess.run([sys.executable, "seed.py", "--table", f"s5_conc_{sid}", "--rows", str(rows), "--merges", "3",
                    "--small-appends", "5", "--spread", spread], check=True, capture_output=True)
    cat = catalog(); con = duck("4GB")
    tbl = cat.load_table(name)
    base_fp = duckdb_fp(con, name)
    rec = {"scenario": sid, "rows": rows, "before": file_counts(tbl), "meta_before": n_meta(tbl)}
    state = {}
    h = Retries(); lg = logging.getLogger("pyiceberg.table"); lg.addHandler(h); lg.setLevel(logging.INFO)
    inject = hook_factory(name, state)
    t0 = time.time()
    try:
        res = maintain.compact(cat, name, con, inject=inject, rewrite_cls=maintain.StockValidationRewrite if stock else None)
        rec["outcome"] = "committed"
    except BaseException as e:  # noqa: BLE001 - spike: record any failure
        rec["outcome"] = f"{type(e).__name__}: {str(e)[:160]}"
    rec["wall_s"] = round(time.time() - t0, 1)
    if after:
        rec["after_hook"] = after(name, state)
    lg.removeHandler(h)
    rec["compaction_retries"] = h.n
    tbl = cat.load_table(name)
    rec["after"] = file_counts(tbl)
    rec["commits_total"] = n_meta(tbl) - rec["meta_before"]
    n, u = uniq_ok(con, name)
    d, p = duckdb_fp(con, name), pyiceberg_fp(tbl)
    rec.update(rows_final=n, distinct_keys=u, unique_keys_ok=(n == u), parity=(d == p), state=state.get("note"))
    # unreferenced objects (no safety delay): what a failed attempt left behind
    keep = maintain.reachable_keys(tbl)
    orphans = [o for o in list_objects(key_of(tbl.location()).rstrip("/") + "/") if o["Key"] not in keep]
    rec["orphans_now"] = len(orphans)
    rec["orphan_data_bytes"] = sum(o["Size"] for o in orphans)
    return rec, cat, name, con


def hook_at(phase, fn):
    def factory(name, state):
        def inject(p):
            if p == phase and not state.get("done"):
                state["done"] = True
                fn(name, state)
        return inject
    return factory


def append_pyiceberg(name, state):
    tbl = catalog().load_table(name)
    tbl.append(new_rows(tbl, 10**10))
    state["note"] = "pyiceberg append of 10000 rows"


def append_duckdb(name, state):
    duck("1GB").execute(f"INSERT INTO polaris.{name} SELECT i, 7, DATE '2025-06-01', 1.0, 'conc' FROM range({10**10}, {10**10 + 10000}) t(i)")
    state["note"] = "duckdb INSERT of 10000 rows"


def merge_duckdb(name, state):
    duck("2GB").execute(MERGE.format(name=name))
    state["note"] = "duckdb MERGE of 100000 ids (touches rewritten files)"


def nested_compaction(name, state):
    c = catalog(); maintain.compact(c, name, duck("2GB"))
    state["note"] = "second compaction committed first"


def merge_open_txn(name, state):
    con_b = duck("2GB")
    con_b.execute("BEGIN")
    con_b.execute(MERGE.format(name=name))
    state["con_b"] = con_b
    state["note"] = "duckdb MERGE in an open transaction, committed after the compaction"


def commit_open_txn(name, state):
    try:
        state["con_b"].execute("COMMIT")
        return "COMMIT succeeded"
    except Exception as e:  # noqa: BLE001
        return f"COMMIT failed: {type(e).__name__}: {str(e)[:200]}"


def rerun_compaction(name, state):
    cat = catalog()
    try:
        maintain.compact(cat, name, duck("4GB"))
        return "re-run committed"
    except Exception as e:  # noqa: BLE001
        return f"re-run failed: {type(e).__name__}: {str(e)[:120]}"


def storm(name, state):
    """Background `floe run`: PyIceberg append every ~0.7 s for the whole compaction."""
    stop = threading.Event()
    stats = {"attempted": 0, "ok": 0, "failed": []}
    retries = Retries(); logging.getLogger("pyiceberg.table").addHandler(retries)

    def loop():
        c = catalog()
        lo = 10**10
        while not stop.is_set():
            stats["attempted"] += 1
            try:
                t = c.load_table(name)
                t.append(new_rows(t, lo, 2000)); stats["ok"] += 1
            except Exception as e:  # noqa: BLE001
                stats["failed"].append(f"{type(e).__name__}: {str(e)[:100]}")
            lo += 2000
            stop.wait(0.7)
    th = threading.Thread(target=loop); th.start()
    state.update(stop=stop, thread=th, stats=stats, retries=retries)


def storm_hook(name, state):
    storm(name, state)
    def inject(p):
        pass
    return inject


def storm_after(name, state):
    state["stop"].set(); state["thread"].join()
    s = state["stats"]
    return {"appends_attempted": s["attempted"], "appends_ok": s["ok"], "appends_failed": s["failed"][:3],
            "retries_logged_total(appender+compaction)": state["retries"].n}


SCENARIOS = {
    "c1a_append_before_load": (hook_at("before_commit", append_pyiceberg), {}),
    "c1b_append_in_tx_retry": (hook_at("in_transaction", append_pyiceberg), {}),
    "c1c_duckdb_insert_in_tx_retry": (hook_at("in_transaction", append_duckdb), {}),
    "c2_stock_overwrite_validation": (hook_at("before_commit", append_pyiceberg), {"stock": True}),
    "c3_merge_before_commit": (hook_at("before_commit", merge_duckdb), {"after": rerun_compaction}),
    "c4_merge_txn_open_across_commit": (hook_at("before_commit", merge_open_txn), {}),
    "c5_two_compactions": (hook_at("before_commit", nested_compaction), {}),
    "c6_append_storm": (storm_hook, {"after": storm_after}),
}

for sid, (hook, kw) in SCENARIOS.items():
    if a.only and a.only != sid:
        continue
    print(f"=== {sid}", flush=True)
    try:
        if sid == "c4_merge_txn_open_across_commit":
            holder = {}
            def hf(name, state, _h=hook):
                holder["state"] = state
                return _h(name, state)
            rec, cat, name, con = run_scenario(sid, hf, **{**kw, "after": lambda n, s: commit_open_txn(n, holder["state"])})
        else:
            rec, cat, name, con = run_scenario(sid, hook, **kw)
        if sid == "c4_merge_txn_open_across_commit":
            # recount after the late COMMIT
            tbl = cat.load_table(name)
            n, u = uniq_ok(con, name)
            rec.update(rows_final=n, distinct_keys=u, unique_keys_ok=(n == u), parity=(duckdb_fp(con, name) == pyiceberg_fp(tbl)),
                       after=file_counts(tbl))
            rec["merged_rows_visible"] = con.execute(f"SELECT count(*) FROM polaris.{name} WHERE payload = 'merged'").fetchone()[0]
        if sid in ("c3_merge_before_commit", "c4_merge_txn_open_across_commit"):
            rec["merged_rows_visible"] = con.execute(f"SELECT count(*) FROM polaris.{name} WHERE payload = 'merged'").fetchone()[0]
    except Exception:  # noqa: BLE001
        rec = {"scenario": sid, "error": traceback.format_exc()[-800:]}
    emit(a.out, rec)
    print(rec, flush=True)
