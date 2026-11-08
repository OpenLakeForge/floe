"""Airflow demo DAG using the reusable FloeRunOperator."""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from airflow_floe.hooks import FloeManifestHook
from airflow_floe.manifest_discovery import discover_manifest_dag_specs
from airflow_floe.operators import FloeRunOperator

FLOE_CMD = os.environ.get("FLOE_CMD", "floe")
FLOE_MANIFEST_DIR = os.environ.get("FLOE_MANIFEST_DIR")
FLOE_MANIFEST = os.environ.get(
    "FLOE_MANIFEST",
    str(ROOT_DIR / "example" / "manifest.airflow.json"),
)
DEFAULT_CONFIG = str(ROOT_DIR / "example" / "config.yml")
FLOE_CONFIG_OVERRIDE = os.environ.get("FLOE_CONFIG")


def _register_dag_for_manifest(*, dag_id: str, manifest_path: str):
    manifest_hook = FloeManifestHook(
        manifest_path=manifest_path,
        config_override=FLOE_CONFIG_OVERRIDE,
        default_config_path=DEFAULT_CONFIG,
    )
    context = manifest_hook.get_context()
    config_path = manifest_hook.get_config_path()
    entity_assets = list(context.assets_by_entity.values())

    @dag(
        dag_id=dag_id,
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        tags=["floe", "example", "operator"],
    )
    def _floe_operator_dag() -> None:
        FloeRunOperator(
            task_id="run_floe",
            floe_cmd=FLOE_CMD,
            config_path=config_path,
            manifest_context=context,
            outlets=entity_assets,
        )

    return _floe_operator_dag()


if FLOE_MANIFEST_DIR:
    manifest_specs = discover_manifest_dag_specs(FLOE_MANIFEST_DIR)
    if manifest_specs:
        for spec in manifest_specs:
            globals()[spec.dag_id] = _register_dag_for_manifest(
                dag_id=spec.dag_id,
                manifest_path=spec.manifest_path,
            )
    else:
        # Fallback to single-manifest mode when directory is empty/missing.
        floe_example_operator = _register_dag_for_manifest(
            dag_id="floe_example_operator",
            manifest_path=FLOE_MANIFEST,
        )
else:
    floe_example_operator = _register_dag_for_manifest(
        dag_id="floe_example_operator",
        manifest_path=FLOE_MANIFEST,
    )
