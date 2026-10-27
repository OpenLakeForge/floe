from __future__ import annotations

import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import patch


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

from airflow_floe.hooks import FloeManifestHook  # noqa: E402
from airflow_floe.manifest import ManifestExecution  # noqa: E402
from airflow_floe.operators import FloeRunHook, FloeRunOperator  # noqa: E402
from airflow_floe.runtime import build_dag_manifest_context  # noqa: E402


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


class HookAndOperatorTests(unittest.TestCase):
    def _write_manifest(self, base: Path, config_path: Path) -> Path:
        manifest_path = base / "manifest.airflow.json"
        payload = {
            "spec_version": "0.8",
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
                    "accepted_sink_uri": f"local://{base}/out/accepted/orders",
                    "rejected_sink_uri": None,
                    "asset_key": ["sales", "orders"],
                    "runner": None,
                }
            ],
        }
        payload.update(_execution_and_runners(str(config_path)))
        manifest_path.write_text(json.dumps(payload), encoding="utf-8")
        return manifest_path

    def test_manifest_hook_loads_context_and_assets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            config_path = base / "config.yml"
            config_path.write_text("version: v1\n", encoding="utf-8")
            manifest_path = self._write_manifest(base, config_path)

            hook = FloeManifestHook(str(manifest_path))
            context = hook.get_context()
            self.assertEqual(context.entity_names, ["orders"])
            self.assertEqual(hook.get_config_path(), str(config_path))
            self.assertEqual(len(hook.get_assets()), 1)

    def test_manifest_hook_fallback_without_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            config_path = base / "config.yml"
            config_path.write_text("version: v1\n", encoding="utf-8")
            missing_manifest = base / "missing.json"

            hook = FloeManifestHook(
                str(missing_manifest),
                default_config_path=str(config_path),
            )
            context = hook.get_context()
            self.assertIsNone(context.manifest)
            self.assertEqual(context.entity_names, [])
            self.assertEqual(hook.get_assets(), [])

    def test_run_hook_build_args_with_entities(self) -> None:
        hook = FloeRunHook(floe_cmd="floe")
        args = hook.build_args("/tmp/config.yml", entities=["orders", "customer"])
        self.assertIn("--entities", args)
        self.assertIn("orders,customer", args)

    def test_run_hook_build_args_from_manifest_execution(self) -> None:
        hook = FloeRunHook(floe_cmd=None)
        execution = ManifestExecution.from_dict(
            _execution_and_runners("/tmp/config.yml")["execution"]
        )
        args = hook.build_args(
            "/tmp/config.yml",
            entities=["orders", "customer"],
            execution=execution,
        )
        self.assertEqual(
            args,
            [
                "floe",
                "run",
                "-c",
                "/tmp/config.yml",
                "--log-format",
                "json",
                "--quiet",
                "--entities",
                "orders,customer",
            ],
        )

    def test_run_hook_run_returns_normalized_payload(self) -> None:
        hook = FloeRunHook(floe_cmd="floe")
        stdout_line = json.dumps(
            {
                "schema": "floe.log.v1",
                "event": "run_finished",
                "run_id": "run-1",
                "status": "success",
                "exit_code": 0,
                "files": 1,
                "rows": 10,
                "accepted": 9,
                "rejected": 1,
                "warnings": 0,
                "errors": 0,
                "summary_uri": "local:///tmp/report/run.summary.json",
                "ts_ms": 1739500000000,
            }
        )
        fake_process = _FakePopen(
            stdout=f"{stdout_line}\n",
            stderr="",
            returncode=0,
        )
        with patch("subprocess.Popen", return_value=fake_process):
            payload = hook.run("/tmp/config.yml", entities=["orders"])

        self.assertEqual(payload["schema"], "floe.airflow.run.v1")
        self.assertEqual(payload["run_id"], "run-1")
        self.assertEqual(payload["entity"], "orders")
        self.assertEqual(payload["config_uri"], "/tmp/config.yml")

    def test_run_hook_streams_stdout_and_stderr_to_loggers(self) -> None:
        hook = FloeRunHook(floe_cmd="floe")
        stdout_line = json.dumps(
            {
                "schema": "floe.log.v1",
                "event": "run_finished",
                "run_id": "run-1",
                "status": "success",
                "exit_code": 0,
                "files": 1,
                "rows": 10,
                "accepted": 9,
                "rejected": 1,
                "warnings": 0,
                "errors": 0,
                "summary_uri": "local:///tmp/report/run.summary.json",
                "ts_ms": 1739500000000,
            }
        )
        fake_process = _FakePopen(
            stdout=f"{stdout_line}\n",
            stderr="warn line\n",
            returncode=0,
        )
        stdout_logs: list[str] = []
        stderr_logs: list[str] = []

        with patch("subprocess.Popen", return_value=fake_process):
            hook.run(
                "/tmp/config.yml",
                entities=["orders"],
                log_stdout=lambda fmt, prefix, text: stdout_logs.append(fmt % (prefix, text)),
                log_stderr=lambda fmt, prefix, text: stderr_logs.append(fmt % (prefix, text)),
            )

        self.assertTrue(any("[floe/stdout]" in line for line in stdout_logs))
        self.assertTrue(any("[floe/stderr]" in line for line in stderr_logs))

    def test_run_operator_execute_uses_hook(self) -> None:
        operator = FloeRunOperator(
            task_id="run_floe",
            config_path="/tmp/config.yml",
            entities=["orders"],
            floe_cmd="floe",
        )
        expected = {"schema": "floe.airflow.run.v1", "run_id": "run-1"}
        with patch("airflow_floe.operators.FloeRunHook.run", return_value=expected) as run_mock:
            actual = operator.execute({})
        self.assertEqual(actual, expected)

        run_mock.assert_called_once()
        args, kwargs = run_mock.call_args
        self.assertEqual(args, ("/tmp/config.yml",))
        self.assertEqual(kwargs["entities"], ["orders"])
        self.assertIsNone(kwargs["execution"])
        self.assertIsNone(kwargs["runner_definition"])
        # The operator resolves its log sinks from `self.log`. Under Airflow 3.x
        # BaseOperator.log is always a bound logger, so the sinks are callables
        # (logger.info / logger.warning); without Airflow installed the fallback
        # BaseOperator has no logger and the sinks are None. Assert against
        # whichever environment the test resolves so it passes in both.
        logger = getattr(operator, "log", None)
        if logger is None:
            self.assertIsNone(kwargs["log_stdout"])
            self.assertIsNone(kwargs["log_stderr"])
        else:
            self.assertTrue(callable(kwargs["log_stdout"]))
            self.assertTrue(callable(kwargs["log_stderr"]))

    def test_run_operator_populates_asset_events_from_manifest_context(self) -> None:
        class OutletEvent:
            def __init__(self) -> None:
                self.extra = None

        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            config_path = base / "config.yml"
            config_path.write_text("version: v1\n", encoding="utf-8")
            manifest_path = self._write_manifest(base, config_path)
            manifest_context = build_dag_manifest_context(str(manifest_path))
            outlet_asset = manifest_context.assets_by_entity["orders"]
            # Airflow SDK Asset may be unhashable in some provider/runtime combos;
            # normalize to a stable hashable key for outlet_events mapping in this test.
            manifest_context.assets_by_entity["orders"] = "orders-asset"
            outlet_asset = "orders-asset"
            outlet_event = OutletEvent()

            summary_path = base / "report" / "run.summary.json"
            summary_path.parent.mkdir(parents=True, exist_ok=True)
            summary_payload = {
                "entities": [
                    {
                        "name": "orders",
                        "status": "success",
                        "report_file": str(base / "report" / "run-1" / "orders" / "run.json"),
                        "results": {
                            "files_total": 2,
                            "rows_total": 11,
                            "accepted_total": 10,
                            "rejected_total": 1,
                            "warnings_total": 0,
                            "errors_total": 0,
                        },
                    }
                ]
            }
            summary_path.write_text(json.dumps(summary_payload), encoding="utf-8")

            operator = FloeRunOperator(
                task_id="run_floe",
                config_path=str(config_path),
                floe_cmd="floe",
                manifest_context=manifest_context,
            )
            payload = {
                "schema": "floe.airflow.run.v1",
                "run_id": "run-1",
                "status": "success",
                "exit_code": 0,
                "files": 1,
                "rows": 10,
                "accepted": 9,
                "rejected": 1,
                "warnings": 0,
                "errors": 0,
                "summary_uri": f"local://{summary_path}",
                "config_uri": str(config_path),
                "floe_log_schema": "floe.log.v1",
                "finished_at_ts_ms": 1739500000000,
            }

            with patch("airflow_floe.operators.FloeRunHook.run", return_value=payload):
                operator.execute({"outlet_events": {outlet_asset: outlet_event}})

            self.assertIsNotNone(outlet_event.extra)
            self.assertEqual(outlet_event.extra["entity"], "orders")
            self.assertEqual(outlet_event.extra["rows_total"], 11)
            self.assertEqual(
                outlet_event.extra["entity_report_file"],
                str(base / "report" / "run-1" / "orders" / "run.json"),
            )


class ManifestRoutingTests(unittest.TestCase):
    """Tests for manifest-driven execution routing in FloeRunOperator / FloeManifestHook."""

    def _write_manifest_with_runner_type(
        self, base: Path, config_path: Path, runner_type: str
    ) -> Path:
        manifest_path = base / "manifest.airflow.json"
        payload = {
            "spec_version": "0.8",
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
                    "accepted_sink_uri": f"local://{base}/out/accepted/orders",
                    "rejected_sink_uri": None,
                    "asset_key": ["sales", "orders"],
                    "runner": None,
                }
            ],
            "execution": {
                "entrypoint": "floe",
                "base_args": ["run", "-c", "{config_uri}", "--log-format", "json"],
                "per_entity_args": ["--entities", "{entity_name}"],
                "log_format": "json",
                "result_contract": {
                    "run_finished_event": True,
                    "summary_uri_field": "summary_uri",
                    "exit_codes": {"0": "success_or_rejected", "1": "technical_failure"},
                },
                "defaults": {"env": {}, "workdir": None},
            },
            "runners": {
                "default": "primary",
                "definitions": {
                    "primary": {
                        "type": runner_type,
                        "image": None,
                        "namespace": None,
                        "service_account": None,
                        "resources": None,
                        "env": None,
                    }
                },
            },
        }
        manifest_path.write_text(json.dumps(payload), encoding="utf-8")
        return manifest_path

    def test_run_operator_no_manifest_defaults_to_local(self) -> None:
        """No manifest_context → local subprocess path."""
        operator = FloeRunOperator(
            task_id="run_floe",
            config_path="/tmp/config.yml",
            floe_cmd="floe",
        )
        expected = {"schema": "floe.airflow.run.v1", "run_id": "run-1"}
        with patch("airflow_floe.operators.FloeRunHook.run", return_value=expected) as run_mock:
            result = operator.execute({})
        run_mock.assert_called_once()
        self.assertEqual(result, expected)

    def test_run_operator_local_process_manifest_routes_to_local(self) -> None:
        """manifest runner type=local_process → FloeRunHook subprocess path."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            config_path = base / "config.yml"
            config_path.write_text("version: v1\n", encoding="utf-8")
            manifest_path = self._write_manifest_with_runner_type(base, config_path, "local_process")

            from airflow_floe.runtime import build_dag_manifest_context
            manifest_context = build_dag_manifest_context(str(manifest_path))

            operator = FloeRunOperator(
                task_id="run_floe",
                config_path=str(config_path),
                floe_cmd="floe",
                manifest_context=manifest_context,
            )
            expected = {"schema": "floe.airflow.run.v1", "run_id": "run-2"}
            with patch("airflow_floe.operators.FloeRunHook.run", return_value=expected) as run_mock:
                result = operator.execute({})
            run_mock.assert_called_once()
            self.assertEqual(result, expected)

    def test_run_operator_unknown_runner_manifest_raises_not_implemented(self) -> None:
        """Unknown non-local runner types still raise NotImplementedError."""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            config_path = base / "config.yml"
            config_path.write_text("version: v1\n", encoding="utf-8")
            manifest_path = self._write_manifest_with_runner_type(base, config_path, "docker")

            from airflow_floe.runtime import build_dag_manifest_context
            manifest_context = build_dag_manifest_context(str(manifest_path))

            operator = FloeRunOperator(
                task_id="run_floe",
                config_path=str(config_path),
                floe_cmd="floe",
                manifest_context=manifest_context,
            )
            with self.assertRaises(NotImplementedError) as cm:
                operator.execute({})
            self.assertIn("docker", str(cm.exception).lower())

    def test_profile_path_not_in_template_fields(self) -> None:
        """profile_path must NOT be in FloeRunOperator.template_fields."""
        self.assertNotIn("profile_path", FloeRunOperator.template_fields)


if __name__ == "__main__":
    unittest.main()


class _FakePopen:
    def __init__(self, *, stdout: str, stderr: str, returncode: int) -> None:
        self.stdout = io.StringIO(stdout)
        self.stderr = io.StringIO(stderr)
        self._returncode = returncode

    def wait(self) -> int:
        return self._returncode
