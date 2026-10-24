"""Fingerprints before (base snapshot, DuckDB) and after (current, DuckDB and PyIceberg), in their own process.
    python verify.py spike.<table> <base_snapshot_id>   -> last stdout line is JSON
"""
import json
import sys
import time

from check import duckdb_fp, pyiceberg_fp
from common import catalog, duck, vmhwm_mb

name, base = sys.argv[1], int(sys.argv[2])
t0 = time.time()
con = duck("4GB")
tbl = catalog().load_table(name)
print(json.dumps({"duckdb_before": duckdb_fp(con, name, base), "duckdb_after": duckdb_fp(con, name),
                  "pyiceberg_after": pyiceberg_fp(tbl), "s": round(time.time() - t0, 1), "vmhwm_mb": round(vmhwm_mb(), 1)}))
