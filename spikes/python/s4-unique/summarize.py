"""Prints the results/scale-*.jsonl runs as a Markdown table, one row per run."""

import json
import pathlib

HERE = pathlib.Path(__file__).parent
# DuckDB size units are decimal (1GB = 10^9 bytes); /proc reports KiB, printed here as MiB.
UNITS = {"GB": 10**9, "MB": 10**6}
LAYOUT = {"": "unpartitioned", "_b16": "bucket(16,id), ~3.1M rows/file", "_b16s": "bucket(16,id), ~0.78M rows/file"}

print("| History | Layout | Key | memory_limit | Spill cap | Wall s | VmHWM MiB | RssAnon MiB | VmHWM / limit | Spill MiB | Result |")
print("|---|---|---|---|---|---|---|---|---|---|---|")
for path in sorted(HERE.glob("results/scale-*.jsonl")):
    for line in path.read_text().splitlines():
        r = json.loads(line).get("run")
        if not r:
            continue
        size, _, layout = r["table"][1:].partition("_")
        limit_mib = int(r["memory_limit"][:-2]) * UNITS[r["memory_limit"][-2:]] / 2**20
        result = r["error"] if "error" in r else ("exact" if r["exact"] else f"MISMATCH {r['flagged']} vs {r['expected_flagged']}")
        print(f"| {size.upper()} | {LAYOUT['_' + layout if layout else '']} | {', '.join(r['cols'])} | {r['memory_limit']} "
              f"| {r['max_spill']} | {r['wall_s']} | {r['peak_vmhwm_mb']:,} | {r['peak_rss_anon_mb']:,} | {r['peak_vmhwm_mb'] / limit_mib:.2f} | {r['peak_spill_mb']:,} | {result} |")
