"""Benchmark Rust `floe run` against the Python path on generated data.

For each size, runs each side `--repeat` times (alternating), each run in a fresh process with
its outputs deleted first. Wall time includes process start-up, reads, checks, PII and both
writes. Peak RSS is `ru_maxrss` of the waited-for child (both sides run as one process; DuckDB
runs in-process), measured by a separate `measure` sub-process per run so runs don't mix.
Both sides get POLARS_MAX_THREADS=<threads>; pii_hash.py gives DuckDB the same thread count.

Usage: python bench.py --floe /path/to/floe --data data --sizes 1000000 5000000 20000000
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import resource
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent


def measure(cmd: list[str], cwd: str) -> None:
    t = time.perf_counter()
    proc = subprocess.run(cmd, cwd=cwd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    wall = time.perf_counter() - t
    rss_kb = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    print(json.dumps({"rc": proc.returncode, "wall_s": wall, "peak_rss_mb": rss_kb / 1024, "stderr": proc.stderr[-2000:]}))


def one_run(cmd: list[str], work: Path, env: dict) -> dict:
    for d in ("out", "report"):
        shutil.rmtree(work / d, ignore_errors=True)
    wrapper = [sys.executable, __file__, "--measure", json.dumps(cmd), str(work)]
    out = subprocess.run(wrapper, env=env, capture_output=True, text=True, check=True).stdout
    res = json.loads(out)
    if res["rc"] != 0:
        raise SystemExit(f"run failed: {cmd}\n{res['stderr']}")
    summary = json.loads(next((work / "report").glob("run_*/run.summary.json")).read_text())["results"]
    res["counts"] = {k: summary[k] for k in ("rows_total", "accepted_total", "rejected_total", "errors_total")}
    return res


def versions(floe: str) -> dict:
    import duckdb
    import polars
    import pyarrow
    import yaml

    mem_kb = int(next(line for line in open("/proc/meminfo") if line.startswith("MemTotal")).split()[1])
    cpu = next((line.split(":", 1)[1].strip() for line in open("/proc/cpuinfo") if line.startswith("model name")), "?")
    return {
        "cpu": cpu,
        "cpu_count": os.cpu_count(),
        "mem_gb": round(mem_kb / 1024 / 1024, 1),
        "os": platform.platform(),
        "python": platform.python_version(),
        "polars": polars.__version__,
        "pyarrow": pyarrow.__version__,
        "duckdb": duckdb.__version__,
        "pyyaml": yaml.__version__,
        "floe": subprocess.run([floe, "--version"], capture_output=True, text=True).stdout.strip(),
    }


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "--measure":
        measure(json.loads(sys.argv[2]), sys.argv[3])
        return
    parser = argparse.ArgumentParser()
    parser.add_argument("--floe", required=True)
    parser.add_argument("--data", type=Path, default=Path("data"))
    parser.add_argument("--sizes", type=int, nargs="+", default=[1_000_000, 5_000_000, 20_000_000])
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--threads", type=int, default=os.cpu_count())
    parser.add_argument("--json", type=Path, default=Path("bench_results.json"))
    args = parser.parse_args()

    env = dict(os.environ, POLARS_MAX_THREADS=str(args.threads))
    results = {"env": versions(args.floe), "threads": args.threads, "sizes": {}}
    print(json.dumps(results["env"], indent=1))
    for rows in args.sizes:
        work = args.data / str(rows)
        if not (work / "in" / "orders.csv").exists():
            subprocess.run([sys.executable, str(HERE / "gen_data.py"), "--rows", str(rows), "--out", str(args.data)], check=True)
        sides = {
            "rust": [str(Path(args.floe).resolve()), "run", "-c", "config.yml"],
            "python": [sys.executable, str(HERE / "floe_py.py"), "run", "-c", "config.yml"],
        }
        runs: dict[str, list[dict]] = {"rust": [], "python": []}
        for i in range(args.repeat):
            for side, cmd in sides.items():
                r = one_run(cmd, work.resolve(), env)
                runs[side].append(r)
                print(f"rows={rows} {side:6s} run={i + 1} wall_s={r['wall_s']:.2f} peak_rss_mb={r['peak_rss_mb']:.0f} {r['counts']}", flush=True)
        row = {}
        for side, rs in runs.items():
            row[side] = {
                "wall_s_median": statistics.median(r["wall_s"] for r in rs),
                "peak_rss_mb_median": statistics.median(r["peak_rss_mb"] for r in rs),
                "runs": [{k: r[k] for k in ("wall_s", "peak_rss_mb")} for r in rs],
                "counts": rs[0]["counts"],
            }
        row["counts_match"] = row["rust"]["counts"] == row["python"]["counts"]
        row["wall_ratio"] = row["python"]["wall_s_median"] / row["rust"]["wall_s_median"]
        row["rss_ratio"] = row["python"]["peak_rss_mb_median"] / row["rust"]["peak_rss_mb_median"]
        results["sizes"][str(rows)] = row
        args.json.write_text(json.dumps(results, indent=1))
        print(
            f"== rows={rows} rust {row['rust']['wall_s_median']:.2f}s {row['rust']['peak_rss_mb_median']:.0f}MB | "
            f"python {row['python']['wall_s_median']:.2f}s {row['python']['peak_rss_mb_median']:.0f}MB | "
            f"wall x{row['wall_ratio']:.2f} rss x{row['rss_ratio']:.2f} counts_match={row['counts_match']}",
            flush=True,
        )


if __name__ == "__main__":
    main()
