"""`policy.min_rows` (#1409): the `run.json` policy block and the entity and run outcomes."""

from pathlib import Path
from typing import Any

import pytest

from floe.config.parse import parse_config
from floe.report.summary import build_summary, entity_outcome
from floe.run.entity import run_entity

CONFIG = """\
version: "0.3"
domains:
  - name: d
entities:
  - name: orders
    domain: d
    incremental_mode: {mode}
    source:
      format: csv
      path: ./in
      options: {{header: true, separator: ","}}
    sink:
      accepted: {{format: parquet, path: ./out/accepted}}
      rejected: {{format: csv, path: ./out/rejected}}
    policy: {policy}
    schema:
      columns:
        - {{name: id, type: int64}}
report: {{path: ./report}}
"""


def run(tmp_path: Path, policy: str, rows: int = 3, mode: str = "none", runs: int = 1) -> dict[str, Any]:  # fmt: skip
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "orders.csv").write_text("id\n" + "".join(f"{i}\n" for i in range(rows)))  # fmt: skip
    (tmp_path / "config.yml").write_text(CONFIG.format(mode=mode, policy=policy))
    entity = parse_config(tmp_path / "config.yml").entities[0]
    for _ in range(runs - 1):
        run_entity(entity, tmp_path, "0.3")
    return run_entity(entity, tmp_path, "0.3").report


def summary_status(report: dict[str, Any]) -> tuple[str, int]:
    s = build_summary("0.3", [("orders", report)], run_id="r", started_at="t", finished_at="t", duration_ms=0, config_path="c", report_path="p")  # fmt: skip
    return s["run"]["status"], s["run"]["exit_code"]


@pytest.mark.parametrize(("min_rows", "violated"), [(2, False), (3, False), (4, True)])
@pytest.mark.parametrize(
    ("policy", "violated_outcome"),
    [
        ("{{severity: warn, min_rows: {n}}}", ("success_with_warnings", 0)),
        ("{{severity: abort, min_rows: {n}}}", ("aborted", 2)),
        (
            "{{severity: warn, min_rows: {n}, rule_severity: {{min_rows: abort}}}}",
            ("aborted", 2),
        ),
        ("{{severity: reject, min_rows: {n}}}", ("aborted", 2)),
    ],
)
def test_min_rows_against_rows_total(
    tmp_path: Path,
    min_rows: int,
    violated: bool,
    policy: str,
    violated_outcome: tuple[str, int],
) -> None:
    report = run(tmp_path, policy.format(n=min_rows))
    assert report["results"]["rows_total"] == 3
    assert report["policy"]["min_rows"] == min_rows
    assert report["policy"]["min_rows_violated"] is violated
    assert report["results"]["warnings_total"] == 0
    outcome = violated_outcome if violated else ("success", 0)
    assert entity_outcome(report) == outcome
    assert summary_status(report) == outcome
    assert (tmp_path / "out" / "accepted").is_dir()  # post-hoc: the rows stay committed


def test_a_run_with_no_new_files_is_not_evaluated(tmp_path: Path) -> None:
    report = run(tmp_path, "{severity: abort, min_rows: 100}", mode="file", runs=2)
    assert {f["status"] for f in report["files"]} <= {"skipped"}
    assert report["policy"]["min_rows_violated"] is None
    assert entity_outcome(report) == ("success", 0)


def test_the_policy_block_is_unchanged_without_min_rows(tmp_path: Path) -> None:
    assert run(tmp_path, "{severity: warn}")["policy"] == {"severity": "warn"}
