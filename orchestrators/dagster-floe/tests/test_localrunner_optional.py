import os
import shutil
from pathlib import Path

import pytest

from floe_dagster.events import last_run_finished, parse_ndjson_events
from floe_dagster.runner import LocalRunner


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content.lstrip(), encoding="utf-8")


@pytest.mark.skipif(
    shutil.which("floe") is None or not bool(os.environ.get("FLOE_TEST_LOCALRUNNER")),
    reason="requires a local floe binary + FLOE_TEST_LOCALRUNNER=1",
)
def test_localrunner_runs_example_entity_json_logs(tmp_path):
    config_path = tmp_path / "config.yml"
    in_dir = tmp_path / "in"
    _write(
        config_path,
        """
        version: "0.1"
        metadata:
          project: "dagster-floe-tests"
        report:
          path: "./report"
        entities:
          - name: demo
            source:
              format: csv
              path: "./in"
              cast_mode: strict
              options:
                header: true
            sink:
              accepted: { format: parquet, path: "./out/accepted/demo" }
              rejected: { format: csv, path: "./out/rejected/demo" }
            policy: { severity: reject }
            schema:
              columns:
                - { name: id, type: string, nullable: false, unique: false }
        """,
    )
    _write(in_dir / "demo.csv", "id\n1\n2\n")

    runner = LocalRunner("floe")
    result = runner.run_floe_entity(config_uri=str(config_path), run_id=None, entity="demo", log_format="json")
    events = parse_ndjson_events(result.stdout)
    assert last_run_finished(events)["event"] == "run_finished"
