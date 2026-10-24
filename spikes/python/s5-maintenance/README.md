# S5: Iceberg table maintenance feasibility for `floe maintain` (#618)

Throwaway spike code on the #596 harness. Results are in the comment on #618, raw records in `results/`.

| File | What |
|---|---|
| `maintain.py` | expire (retain-last-N, older-than), orphan removal with safety delay, `RewriteFiles` compaction (DuckDB read, PyIceberg write, REPLACE commit that also drops position deletes), sort / z-order |
| `seed.py` | builds a #598-shaped table: seed files, MERGE-produced position deletes, small appends |
| `run_compact.py`, `verify.py`, `check.py` | compaction run with assertions (`== 0` delete files, 1 commit, PyIceberg = DuckDB = pre-compaction); `--stub-noop` is the test that must fail first |
| `ops_expire_orphans.py` | expiry + orphans, in-flight write protected by the safety delay |
| `conc.py` | `floe run` commits during compaction (6 scenarios) |
| `sort_eval.py` | no sort / linear / z-order 2 and 3 columns, files kept by the planner |

```bash
cd ../harness && docker compose up -d --wait && python3 bootstrap.py && cd ../s5-maintenance
uv venv .venv && uv pip install --python .venv/bin/python -r requirements.txt
P=.venv/bin/python
$P seed.py --table s5_main_500m --rows 500000000 --merges 5 --small-appends 30 --memory-limit 6GB
$P run_compact.py --table s5_main_500m --stub-noop      # must fail: delete-file count is 34, expected 0
$P run_compact.py --table s5_main_500m                  # compaction
$P ops_expire_orphans.py --table s5_main_500m
$P conc.py --rows 10000000 ; $P conc.py --rows 50000000 --only c6_append_storm
$P sort_eval.py --rows 50000000 --target-file-mb 128
$P seed.py --table s5_day --rows 10000000 --merges 2 --partition day --spread all
$P run_compact.py --table s5_day --partition-col d
```
