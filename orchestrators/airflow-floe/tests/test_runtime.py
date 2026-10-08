from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import types
import unittest


def _install_airflow_stub() -> None:
    if "airflow.sdk" in sys.modules:
        return

    airflow_module = types.ModuleType("airflow")
    sdk_module = types.ModuleType("airflow.sdk")

    class Asset:  # pragma: no cover - simple import stub
        def __init__(self, name: str, uri: str, group: str | None = None) -> None:
            self.name = name
            self.uri = uri
            self.group = group

    sdk_module.Asset = Asset
    airflow_module.sdk = sdk_module
    sys.modules["airflow"] = airflow_module
    sys.modules["airflow.sdk"] = sdk_module


_install_airflow_stub()
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from airflow_floe.runtime import (  # noqa: E402
    build_asset_event_extra,
    build_dag_manifest_context,
    build_dag_manifest_context_or_empty,
    load_run_summary,
    parse_run_finished,
)
from airflow_floe.manifest import ManifestEntity  # noqa: E402


def _execution_and_runners(config_path: str) -> dict:
    return {
        "execution": {
            "entrypoint": "floe",
            "base_args": [
                "run",
                "-c",
                "{config_uri}",
                "--log-format",
                "json",
                "--quiet",
            ],
            "per_entity_args": ["--entities", "{entity_name}"],
            "log_format": "json",
            "result_contract": {
                "run_finished_event": True,
                "summary_uri_field": "summary_uri",
                "exit_codes": {
                    "0": "success_or_rejected",
                    "1": "technical_failure",
                    "2": "aborted",
                },
            },
            "defaults": {"env": {}, "workdir": None},
        },
        "runners": {
            "default": "local",
            "definitions": {
                "local": {
                    "type": "local_process",
                    "image": None,
                    "namespace": None,
                    "service_account": None,
                    "resources": None,
                    "env": None,
                }
            },
        },
    }


class RuntimeHelpersTests(unittest.TestCase):
    def test_parse_run_finished_event(self) -> None:
        stdout = "\n".join(
            [
                json.dumps({"schema": "floe.log.v1", "event": "run_started"}),
                json.dumps(
                    {
                        "schema": "floe.log.v1",
                        "event": "run_finished",
                        "run_id": "run-1",
                    }
                ),
            ]
        )
        event = parse_run_finished(stdout)
        self.assertEqual(event["event"], "run_finished")
        self.assertEqual(event["run_id"], "run-1")

    def test_parse_run_finished_missing_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, "run_finished event not found"):
            parse_run_finished(json.dumps({"schema": "floe.log.v1", "event": "run_started"}))

    def test_load_run_summary_supports_local_uri(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            config_path = base / "config.yml"
            config_path.write_text("version: v1\n", encoding="utf-8")
            summary_path = base / "report" / "run_1" / "run.summary.json"
            summary_path.parent.mkdir(parents=True, exist_ok=True)
            summary_payload = {"entities": [{"name": "orders"}]}
            summary_path.write_text(json.dumps(summary_payload), encoding="utf-8")

            payload = load_run_summary(f"local://{summary_path}", str(config_path))
            self.assertEqual(payload, summary_payload)

    def test_load_run_summary_supports_relative_local_uri(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            config_path = base / "config.yml"
            config_path.write_text("version: v1\n", encoding="utf-8")
            summary_path = base / "report" / "run.summary.json"
            summary_path.parent.mkdir(parents=True, exist_ok=True)
            summary_payload = {"entities": [{"name": "customer"}]}
            summary_path.write_text(json.dumps(summary_payload), encoding="utf-8")

            payload = load_run_summary("local://report/run.summary.json", str(config_path))
            self.assertEqual(payload, summary_payload)

    def test_build_dag_manifest_context_loads_entities_and_assets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            manifest_path = base / "manifest.airflow.json"
            config_path = base / "config.yml"
            config_path.write_text("version: v1\n", encoding="utf-8")
            accepted_path = base / "out" / "accepted" / "orders"

            manifest_payload = {
                "schema": "floe.manifest.v1",
                "generated_at_ts_ms": 1739500000000,
                "floe_version": "0.2.4",
                "config_uri": str(config_path),
                "config_checksum": None,
                "entities": [
                    {
                        "name": "orders",
                        "domain": "sales",
                        "group_name": "sales",
                        "source_format": "csv",
                        "accepted_sink_uri": f"local://{accepted_path}",
                        "rejected_sink_uri": None,
                        "asset_key": ["sales", "orders"],
                    }
                ],
            }
            manifest_payload.update(_execution_and_runners(str(config_path)))
            manifest_path.write_text(json.dumps(manifest_payload), encoding="utf-8")

            context = build_dag_manifest_context(str(manifest_path))
            self.assertEqual(context.config_path, str(config_path))
            self.assertEqual(context.entity_names, ["orders"])
            self.assertIn("orders", context.assets_by_entity)
            self.assertIn("orders", context.entities_by_name)

    def test_build_dag_manifest_context_supports_local_scheme_config_uri(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            manifest_path = base / "manifest.airflow.json"
            config_path = base / "config.yml"
            config_path.write_text("version: v1\n", encoding="utf-8")

            manifest_payload = {
                "schema": "floe.manifest.v1",
                "generated_at_ts_ms": 1739500000000,
                "floe_version": "0.2.4",
                "config_uri": f"local://{config_path}",
                "config_checksum": None,
                "entities": [
                    {
                        "name": "orders",
                        "domain": "sales",
                        "group_name": "sales",
                        "source_format": "csv",
                        "accepted_sink_uri": f"local://{base}/out/accepted/orders",
                        "rejected_sink_uri": None,
                        "asset_key": ["sales", "orders"],
                    }
                ],
            }
            manifest_payload.update(_execution_and_runners(f"local://{config_path}"))
            manifest_path.write_text(json.dumps(manifest_payload), encoding="utf-8")

            context = build_dag_manifest_context(str(manifest_path))
            self.assertEqual(context.config_path, str(config_path))

    def test_build_dag_manifest_context_filters_by_domain(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            manifest_path = base / "manifest.airflow.json"
            config_path = base / "config.yml"
            config_path.write_text("version: v1\n", encoding="utf-8")

            manifest_payload = {
                "schema": "floe.manifest.v1",
                "generated_at_ts_ms": 1739500000000,
                "floe_version": "0.2.4",
                "config_uri": str(config_path),
                "config_checksum": None,
                "entities": [
                    {
                        "name": f"{domain}.accounts",
                        "domain": domain,
                        "group_name": domain,
                        "source_format": "csv",
                        "accepted_sink_uri": f"local://{base}/out/{domain}/accounts",
                        "rejected_sink_uri": None,
                        "asset_key": [domain, "accounts"],
                    }
                    for domain in ("sales", "finance")
                ],
            }
            manifest_payload.update(_execution_and_runners(str(config_path)))
            manifest_path.write_text(json.dumps(manifest_payload), encoding="utf-8")

            context = build_dag_manifest_context(str(manifest_path), domains=["sales"])
            self.assertEqual(context.entity_names, ["sales.accounts"])
            self.assertEqual(list(context.entities_by_name), ["sales.accounts"])
            self.assertEqual(list(context.assets_by_entity), ["sales.accounts"])

            with self.assertRaisesRegex(ValueError, "hr"):
                build_dag_manifest_context(str(manifest_path), domains=["hr"])

    def test_build_dag_manifest_context_or_empty_when_missing_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            missing_manifest = base / "missing.manifest.json"
            default_config = base / "config.yml"
            default_config.write_text("version: v1\n", encoding="utf-8")

            context = build_dag_manifest_context_or_empty(
                str(missing_manifest),
                default_config_path=str(default_config),
            )
            self.assertIsNone(context.manifest)
            self.assertEqual(context.entity_names, [])
            self.assertEqual(context.assets_by_entity, {})
            self.assertEqual(context.config_path, str(default_config.resolve()))

    def test_build_dag_manifest_context_or_empty_preserves_uri_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            missing_manifest = base / "missing.manifest.json"
            default_config = base / "config.yml"
            default_config.write_text("version: v1\n", encoding="utf-8")

            context = build_dag_manifest_context_or_empty(
                str(missing_manifest),
                config_override="s3://bucket/config.yml",
                default_config_path=str(default_config),
            )
            self.assertIsNone(context.manifest)
            self.assertEqual(context.config_path, "s3://bucket/config.yml")

    def test_build_asset_event_extra_derives_entity_report_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            config_path = base / "config.yml"
            config_path.write_text("version: v1\n", encoding="utf-8")
            summary_path = base / "report" / "run_1" / "run.summary.json"
            summary_path.parent.mkdir(parents=True, exist_ok=True)
            summary_path.write_text('{"entities":[]}', encoding="utf-8")

            entity = ManifestEntity(
                name="orders",
                domain="sales",
                group_name="sales",
                source_format="csv",
                accepted_sink_uri="local:///tmp/out",
                rejected_sink_uri=None,
                asset_key=["sales", "orders"],
                runner=None,
            )
            event = build_asset_event_extra(
                entity=entity,
                run_finished={"run_id": "run-1", "summary_uri": f"local://{summary_path}"},
                summary_entity={"name": "orders", "status": "success", "results": {}},
                config_path=str(config_path),
            )
            self.assertEqual(
                event["entity_report_file"],
                str(summary_path.parent / "orders" / "run.json"),
            )


if __name__ == "__main__":
    unittest.main()
