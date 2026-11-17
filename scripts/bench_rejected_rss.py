"""#1169: peak RSS of the Parquet rejected sink must not grow with the number of rejected rows.

    uv run python scripts/bench_rejected_rss.py            # 100k vs 1M rejected rows
    uv run python scripts/bench_rejected_rss.py 1000000    # one total, e.g. 1M

Every row fails the `int64` cast of `id`, so every row is rejected. The rows are spread over files of
`FILE_ROWS` rows, so a bigger total means more files and chunks, not bigger ones. Each run is a child
process so `ru_maxrss` (KiB on Linux) is the run's own. Not a CI test: results are in docs/reference/support-matrix.md.
"""

import platform
import subprocess
import sys
import tempfile
from pathlib import Path

import polars as pl

FILE_ROWS = 50_000
CONFIG = """\
version: "0.3"
entities:
  - name: bench
    source: {format: csv, path: ./in, options: {header: true, separator: ","}}
    sink:
      accepted: {format: parquet, path: ./out/accepted}
      rejected: {format: parquet, path: ./out/rejected}
    policy: {severity: reject}
    schema:
      columns:
        - {name: id, type: int64}
        - {name: name, type: string}
        - {name: amount, type: float64}
"""
CHILD = """\
import resource, sys
from pathlib import Path
from floe.config.parse import parse_config
from floe.run import run
config = Path(sys.argv[1])
run(config, parse_config(config, {}), run_id="bench")
print(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
"""


def peak_mib(rows: int) -> float:
    with tempfile.TemporaryDirectory(prefix="floe-bench-") as tmp:
        root = Path(tmp)
        (root / "in").mkdir()
        for i in range(rows // FILE_ROWS):
            n = pl.int_range(i * FILE_ROWS, (i + 1) * FILE_ROWS, eager=True).cast(pl.String)
            pl.DataFrame({"id": "bad-" + n, "name": "customer-" + n, "amount": n}).write_csv(root / f"in/f{i:05}.csv")  # fmt: skip
        (root / "config.yml").write_text(CONFIG)
        done = subprocess.run([sys.executable, "-c", CHILD, str(root / "config.yml")], cwd=root, capture_output=True, text=True, check=True)  # fmt: skip
        kept = sum(pl.scan_parquet(p).select(pl.len()).collect().item() for p in (root / "out/rejected").rglob("*.parquet"))  # fmt: skip
        assert kept == rows, f"{kept} rejected rows written, expected {rows}"
        return int(done.stdout.split()[-1]) / 1024


if __name__ == "__main__":
    totals = [int(a) for a in sys.argv[1:]] or [100_000, 1_000_000]
    print(f"{platform.platform()}, Python {platform.python_version()}, polars {pl.__version__}")
    for rows in totals:
        print(f"{rows:>9} rejected rows ({rows // FILE_ROWS} files): peak RSS {peak_mib(rows):.0f} MiB")
