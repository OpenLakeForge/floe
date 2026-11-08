from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from airflow_floe.databricks_client import DatabricksRunResult
from airflow_floe.databricks_runner import run_databricks_job
from airflow_floe.manifest import ManifestRunnerDefinition


class DatabricksRunnerTests(unittest.TestCase):
    def _runner(self, **overrides: object) -> ManifestRunnerDefinition:
        base: dict[str, object] = {
            "runner_type": "databricks_job",
            "image": None,
            "namespace": None,
            "service_account": None,
            "resources": None,
            "env": None,
            "command": None,
            "args": None,
            "timeout_seconds": 30,
            "ttl_seconds_after_finished": None,
            "poll_interval_seconds": 1,
            "secrets": None,
            "workspace_url": "https://adb.example.com",
            "existing_cluster_id": "1111-222222-abc123",
            "config_uri": "dbfs:/floe/config.yml",
            "python_file_uri": "dbfs:/floe/bin/floe_entry.py",
            "job_name": "floe-sales-prod",
            "auth": {"service_principal_oauth_ref": "env://DATABRICKS_TOKEN"},
            "env_parameters": {"FLOE_ENV": "prod"},
        }
        base.update(overrides)
        return ManifestRunnerDefinition(**base)

    @patch.dict(os.environ, {"DATABRICKS_TOKEN": "token"}, clear=False)
    def test_success_returns_normalized_payload(self) -> None:
        runner = self._runner()

        with patch("airflow_floe.databricks_runner.DatabricksJobsClient") as client_cls:
            client = client_cls.return_value
            client.ensure_domain_job.return_value = 123
            client.run_now.return_value = 456
            client.poll_run_to_terminal.return_value = DatabricksRunResult(
                run_id=456,
                state="TERMINATED",
                result_state="SUCCESS",
                state_message="ok",
                run_page_url="https://adb.example.com/jobs/456",
            )

            payload = run_databricks_job(
                cmd_args=["floe", "run", "-c", "dbfs:/floe/config.yml"],
                runner=runner,
                entities=["orders"],
            )

        self.assertEqual(payload["schema"], "floe.airflow.run.v1")
        self.assertEqual(payload["status"], "success")
        self.assertEqual(payload["backend_type"], "databricks")
        self.assertEqual(payload["backend_run_id"], 456)
        self.assertEqual(payload["backend_status"], "SUCCESS")
        self.assertEqual(payload["entity"], "orders")
        self.assertNotIn("failure_reason", payload)
        self.assertEqual(payload["backend_metadata"]["job_id"], 123)
        self.assertEqual(
            payload["backend_metadata"]["run_page_url"], "https://adb.example.com/jobs/456"
        )

    @patch.dict(os.environ, {"DATABRICKS_TOKEN": "token"}, clear=False)
    def test_failure_maps_to_failed_with_failure_reason(self) -> None:
        with patch("airflow_floe.databricks_runner.DatabricksJobsClient") as client_cls:
            client = client_cls.return_value
            client.ensure_domain_job.return_value = 99
            client.run_now.return_value = 77
            client.poll_run_to_terminal.return_value = DatabricksRunResult(
                run_id=77,
                state="TERMINATED",
                result_state="FAILED",
                state_message="task crashed",
                run_page_url="https://adb.example.com/jobs/77",
            )

            payload = run_databricks_job(
                cmd_args=["floe", "run", "-c", "dbfs:/floe/config.yml"],
                runner=self._runner(),
                entities=None,
            )

        self.assertEqual(payload["status"], "failed")
        self.assertEqual(payload["exit_code"], 1)
        self.assertEqual(payload["failure_reason"], "task crashed")

    @patch.dict(os.environ, {"DATABRICKS_TOKEN": "token"}, clear=False)
    def test_timeout_maps_to_timeout(self) -> None:
        with patch("airflow_floe.databricks_runner.DatabricksJobsClient") as client_cls:
            client = client_cls.return_value
            client.ensure_domain_job.return_value = 99
            client.run_now.return_value = 77
            client.poll_run_to_terminal.return_value = DatabricksRunResult(
                run_id=77,
                state="TERMINATED",
                result_state="TIMEDOUT",
                state_message="run timed out",
                run_page_url="https://adb.example.com/jobs/77",
            )

            payload = run_databricks_job(
                cmd_args=["floe", "run", "-c", "dbfs:/floe/config.yml"],
                runner=self._runner(),
                entities=["orders", "customers"],
            )

        self.assertEqual(payload["status"], "timeout")
        self.assertEqual(payload["backend_type"], "databricks")
        self.assertEqual(payload["failure_reason"], "run timed out")
        self.assertNotIn("entity", payload)

    @patch.dict(os.environ, {"DATABRICKS_TOKEN": "token"}, clear=False)
    def test_canceled_maps_to_canceled(self) -> None:
        with patch("airflow_floe.databricks_runner.DatabricksJobsClient") as client_cls:
            client = client_cls.return_value
            client.ensure_domain_job.return_value = 99
            client.run_now.return_value = 77
            client.poll_run_to_terminal.return_value = DatabricksRunResult(
                run_id=77,
                state="TERMINATED",
                result_state="CANCELED",
                state_message="terminated by user",
                run_page_url="https://adb.example.com/jobs/77",
            )

            payload = run_databricks_job(
                cmd_args=["floe", "run", "-c", "dbfs:/floe/config.yml"],
                runner=self._runner(),
                entities=None,
            )

        self.assertEqual(payload["status"], "canceled")
        self.assertEqual(payload["backend_status"], "CANCELED")
        self.assertEqual(payload["failure_reason"], "terminated by user")

    @patch.dict(os.environ, {"DATABRICKS_TOKEN": "token"}, clear=False)
    def test_job_name_placeholders_are_rendered_before_ensure(self) -> None:
        with patch("airflow_floe.databricks_runner.DatabricksJobsClient") as client_cls:
            client = client_cls.return_value
            client.ensure_domain_job.return_value = 99
            client.run_now.return_value = 77
            client.poll_run_to_terminal.return_value = DatabricksRunResult(
                run_id=77,
                state="TERMINATED",
                result_state="SUCCESS",
                state_message="ok",
                run_page_url="https://adb.example.com/jobs/77",
            )

            run_databricks_job(
                cmd_args=["floe", "run", "-c", "dbfs:/floe/config.yml"],
                runner=self._runner(job_name="floe-{domain}-{env}"),
                entities=["sales.orders"],
            )

            spec = client.ensure_domain_job.call_args.args[0]
            self.assertEqual(spec.job_name, "floe-sales-prod")

    @patch.dict(os.environ, {"DATABRICKS_TOKEN": "token"}, clear=False)
    def test_job_name_separates_domains(self) -> None:
        with patch("airflow_floe.databricks_runner.DatabricksJobsClient") as client_cls:
            client = client_cls.return_value
            client.ensure_domain_job.return_value = 99
            client.run_now.return_value = 77
            client.poll_run_to_terminal.return_value = DatabricksRunResult(
                run_id=77,
                state="TERMINATED",
                result_state="SUCCESS",
                state_message="ok",
                run_page_url="https://adb.example.com/jobs/77",
            )

            run_databricks_job(
                cmd_args=["floe", "run", "-c", "dbfs:/floe/config.yml"],
                runner=self._runner(job_name="floe-{domain}-{env}"),
                entities=["sales.orders"],
            )
            run_databricks_job(
                cmd_args=["floe", "run", "-c", "dbfs:/floe/config.yml"],
                runner=self._runner(job_name="floe-{domain}-{env}"),
                entities=["finance.invoices"],
            )

            first = client.ensure_domain_job.call_args_list[0].args[0]
            second = client.ensure_domain_job.call_args_list[1].args[0]
            self.assertEqual(first.job_name, "floe-sales-prod")
            self.assertEqual(second.job_name, "floe-finance-prod")

    @patch.dict(os.environ, {"DATABRICKS_TOKEN": "token"}, clear=False)
    def test_mixed_domains_in_single_run_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "one domain"):
            run_databricks_job(
                cmd_args=["floe", "run", "-c", "dbfs:/floe/config.yml"],
                runner=self._runner(job_name="floe-{domain}-{env}"),
                entities=["sales.orders", "finance.invoices"],
            )

    @patch.dict(os.environ, {"DATABRICKS_TOKEN": "token"}, clear=False)
    def test_timeout_error_returns_structured_payload(self) -> None:
        with patch("airflow_floe.databricks_runner.DatabricksJobsClient") as client_cls:
            client = client_cls.return_value
            client.ensure_domain_job.return_value = 99
            client.run_now.return_value = 77
            client.poll_run_to_terminal.side_effect = TimeoutError("run timed out")

            payload = run_databricks_job(
                cmd_args=["floe", "run", "-c", "dbfs:/floe/config.yml"],
                runner=self._runner(),
                entities=["orders"],
            )

        self.assertEqual(payload["status"], "timeout")
        self.assertEqual(payload["failure_reason"], "run timed out")
        self.assertEqual(payload["backend_metadata"]["error_type"], "timeout")

    @patch.dict(os.environ, {"DATABRICKS_TOKEN": "token"}, clear=False)
    def test_infra_error_returns_structured_payload(self) -> None:
        with patch("airflow_floe.databricks_runner.DatabricksJobsClient") as client_cls:
            client = client_cls.return_value
            client.ensure_domain_job.side_effect = RuntimeError("jobs api unavailable")

            payload = run_databricks_job(
                cmd_args=["floe", "run", "-c", "dbfs:/floe/config.yml"],
                runner=self._runner(),
                entities=["orders"],
            )

        self.assertEqual(payload["status"], "failed")
        self.assertEqual(payload["failure_reason"], "jobs api unavailable")
        self.assertEqual(payload["backend_metadata"]["error_type"], "RuntimeError")

    def test_auth_ref_env_scheme_is_used_when_fallback_missing(self) -> None:
        with patch.dict(os.environ, {"DATABRICKS_TOKEN": "token"}, clear=True):
            with patch("airflow_floe.databricks_runner.DatabricksJobsClient") as client_cls:
                client = client_cls.return_value
                client.ensure_domain_job.return_value = 99
                client.run_now.return_value = 77
                client.poll_run_to_terminal.return_value = DatabricksRunResult(
                    run_id=77,
                    state="TERMINATED",
                    result_state="SUCCESS",
                    state_message="ok",
                    run_page_url="https://adb.example.com/jobs/77",
                )

                run_databricks_job(
                    cmd_args=["floe", "run", "-c", "dbfs:/floe/config.yml"],
                    runner=self._runner(),
                    entities=["orders"],
                )

            kwargs = client_cls.call_args.kwargs
            self.assertEqual(kwargs["oauth_bearer_token"], "token")

    def test_auth_ref_takes_precedence_over_fallback(self) -> None:
        env = {
            "DATABRICKS_TOKEN": "from-auth-ref",
            "FLOE_DATABRICKS_OAUTH_TOKEN": "from-fallback",
        }
        with patch.dict(os.environ, env, clear=True):
            with patch("airflow_floe.databricks_runner.DatabricksJobsClient") as client_cls:
                client = client_cls.return_value
                client.ensure_domain_job.return_value = 1
                client.run_now.return_value = 2
                client.poll_run_to_terminal.return_value = DatabricksRunResult(
                    run_id=2,
                    state="TERMINATED",
                    result_state="SUCCESS",
                    state_message="ok",
                    run_page_url=None,
                )
                run_databricks_job(
                    cmd_args=["floe", "run"],
                    runner=self._runner(),
                    entities=["orders"],
                )
            self.assertEqual(client_cls.call_args.kwargs["oauth_bearer_token"], "from-auth-ref")

    def test_auth_ref_missing_env_raises_loudly(self) -> None:
        with patch.dict(os.environ, {"FLOE_DATABRICKS_OAUTH_TOKEN": "fallback"}, clear=True):
            with self.assertRaisesRegex(ValueError, "DATABRICKS_TOKEN"):
                run_databricks_job(
                    cmd_args=["floe", "run"],
                    runner=self._runner(),
                    entities=["orders"],
                )

    def test_non_env_auth_scheme_is_rejected(self) -> None:
        runner = self._runner(auth={"service_principal_oauth_ref": "secret://kv/dbx"})
        with patch.dict(os.environ, {"FLOE_DATABRICKS_OAUTH_TOKEN": "fallback"}, clear=True):
            with self.assertRaisesRegex(ValueError, "env://"):
                run_databricks_job(
                    cmd_args=["floe", "run"],
                    runner=runner,
                    entities=["orders"],
                )

    def test_fallback_used_when_no_auth_ref(self) -> None:
        runner = self._runner(auth=None)
        with patch.dict(os.environ, {"FLOE_DATABRICKS_OAUTH_TOKEN": "fallback"}, clear=True):
            with patch("airflow_floe.databricks_runner.DatabricksJobsClient") as client_cls:
                client = client_cls.return_value
                client.ensure_domain_job.return_value = 1
                client.run_now.return_value = 2
                client.poll_run_to_terminal.return_value = DatabricksRunResult(
                    run_id=2,
                    state="TERMINATED",
                    result_state="SUCCESS",
                    state_message="ok",
                    run_page_url=None,
                )
                run_databricks_job(
                    cmd_args=["floe", "run"],
                    runner=runner,
                    entities=["orders"],
                )
            self.assertEqual(client_cls.call_args.kwargs["oauth_bearer_token"], "fallback")


if __name__ == "__main__":
    unittest.main()
