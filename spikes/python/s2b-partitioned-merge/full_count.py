"""Full read-back of a merged table: PyIceberg (streamed, delete files honoured) vs DuckDB, on
the total row count, the current-row count and the closed-row count.

  full_count.py TABLE
"""

import json
import pathlib
import sys
import time

import pyarrow.compute as pc

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "s2-merge"))
from common import NAMESPACE, catalog, duck, fq  # noqa: E402 - after the sys.path insert for ../s2-merge

name = sys.argv[1]
t = time.monotonic()
tbl = catalog().load_table(f"{NAMESPACE}.{name}")
py = {"total": 0, "current": 0}
for b in tbl.scan(selected_fields=("__floe_is_current",)).to_arrow_batch_reader():
    py["total"] += b.num_rows
    py["current"] += pc.sum(b.column(0)).as_py() or 0
py["closed"] = py["total"] - py["current"]
py_s = round(time.monotonic() - t, 1)
dd = dict(zip(("total", "current"), duck().execute(
    f"SELECT count(*), count(*) FILTER (__floe_is_current) FROM {fq(name)}").fetchone()))
dd["closed"] = dd["total"] - dd["current"]
rec = {"table": name, "snapshot_id": tbl.current_snapshot().snapshot_id, "pyiceberg": py, "duckdb": dd,
       "identical": py == dd, "pyiceberg_s": py_s}
print(json.dumps(rec))
out = pathlib.Path(__file__).resolve().parent / "results" / "full-count.jsonl"
with open(out, "a") as f:
    f.write(json.dumps(rec) + "\n")
