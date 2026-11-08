"""#1063: a 4M-row CSV to Parquet run stays under a generous peak-RSS ceiling (a regression guard, not a proof of bounded memory).

Run with `pytest -m perf` (nightly, `.github/workflows/python-integration.yml`); excluded from `pytest` by default.
The run is a child process so the peak RSS (`ru_maxrss`, KiB on Linux) is the run's own, not pytest's.
"""

import subprocess
import sys
from pathlib import Path

import polars as pl
import pytest

pytestmark = [
    pytest.mark.perf,
    pytest.mark.skipif(
        sys.platform != "linux", reason="ru_maxrss is KiB on Linux only"
    ),
]

ROWS = 4_000_000
# ponytail: a fixed ceiling, ~1.7x the measured 720 MiB peak (RSS grows with rows on the Parquet sink: 308 MiB at 1M, 1022 MiB at 8M); revisit when it is bounded
MAX_RSS_MIB = 1200

CONFIG = """\
version: "0.3"
entities:
  - name: big
    source: {format: csv, path: ./in, options: {header: true, separator: ","}}
    sink:
      accepted: {format: parquet, path: ./out/accepted}
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
run(config, parse_config(config, {}), run_id="perf")
print(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
"""


def test_a_large_csv_to_parquet_stays_under_the_rss_bound(tmp_path: Path) -> None:
    (tmp_path / "in").mkdir()
    csv = tmp_path / "in/big.csv"
    ids = pl.int_range(ROWS, eager=True)
    pl.DataFrame({"id": ids, "name": "customer-" + ids.cast(pl.String), "amount": ids.cast(pl.Float64) / 7}).write_csv(csv)  # fmt: skip
    config = tmp_path / "config.yml"
    config.write_text(CONFIG)
    done = subprocess.run(
        [sys.executable, "-c", CHILD, str(config)], cwd=tmp_path, capture_output=True, text=True, check=True
    )  # fmt: skip
    peak_mib = int(done.stdout.split()[-1]) / 1024
    written = sum(len(pl.read_parquet(p)) for p in (tmp_path / "out/accepted").glob("*.parquet"))  # fmt: skip
    assert written == ROWS
    assert peak_mib < MAX_RSS_MIB, f"peak RSS {peak_mib:.0f} MiB"
