"""SCD2 contract check after a run made without --verify (spike.py run on a fresh table with
a 10M batch: 5M changed, 3M unchanged, 2M new keys; seed rows all have valid_from = T0).
The merge timestamp is the newest valid_from.

  posthoc_contract.py TABLE SEED_ROWS
"""

import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "s2-merge"))
from common import duck, fq  # noqa: E402 - after the sys.path insert for ../s2-merge

name, seed_rows = sys.argv[1], int(sys.argv[2])
con = duck()
t = fq(name)
ts = con.execute(f"SELECT max(__floe_valid_from) FROM {t}").fetchone()[0]
lit = f"TIMESTAMP '{ts.isoformat(sep=' ')}'"
closed, inserted, current, total = con.execute(f"""
    SELECT count(*) FILTER (__floe_valid_to = {lit}), count(*) FILTER (__floe_valid_from = {lit}),
           count(*) FILTER (__floe_is_current), count(*) FROM {t}""").fetchone()
dup = con.execute(f"SELECT count(*) FROM (SELECT id FROM {t} WHERE __floe_is_current GROUP BY id HAVING count(*) > 1)").fetchone()[0]
# Changed keys [0, 5M) must have a closed and a new version named '..._r1'; unchanged [5M, 8M) untouched.
bad_changed = con.execute(f"""SELECT count(*) FROM {t} WHERE id < 5000000 AND __floe_is_current
    AND (name NOT LIKE '%\\_r1' ESCAPE '\\' OR __floe_valid_from <> {lit})""").fetchone()[0]
bad_unchanged = con.execute(f"""SELECT count(*) FROM {t} WHERE id >= 5000000 AND id < 8000000
    AND (NOT __floe_is_current OR __floe_valid_from <> TIMESTAMP '2026-01-01')""").fetchone()[0]
rec = {"table": name, "merge_ts": str(ts), "closed": closed, "inserted": inserted, "current_rows": current,
       "total_rows": total, "keys_with_2_current": dup, "changed_keys_wrong": bad_changed,
       "unchanged_keys_wrong": bad_unchanged,
       "pass": (closed, inserted, current, dup, bad_changed, bad_unchanged)
       == (5_000_000, 7_000_000, seed_rows + 2_000_000, 0, 0, 0)}
print(json.dumps(rec))
with open(pathlib.Path(__file__).resolve().parent / "results" / "posthoc-contract.jsonl", "a") as f:
    f.write(json.dumps(rec) + "\n")
