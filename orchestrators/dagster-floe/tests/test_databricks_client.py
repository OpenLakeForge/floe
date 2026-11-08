from floe_dagster.databricks_client import DatabricksJobsClient, DatabricksJobSpec


class FakeHttp:
    def __init__(self) -> None:
        self.list_jobs = {"jobs": []}
        self.calls: list[tuple[str, str, dict | None]] = []
        self.list_calls = 0

    def request(self, method, url, *, headers=None, json_body=None, timeout_seconds=None):
        del headers, timeout_seconds
        self.calls.append((method, url, json_body))
        if "/jobs/list" in url:
            self.list_calls += 1
            if self.list_calls == 1 and self.list_jobs.get("next_page"):
                return {
                    "jobs": self.list_jobs.get("jobs", []),
                    "has_more": True,
                    "next_page_token": "page-2",
                }
            if "page_token=page-2" in url:
                return {"jobs": self.list_jobs.get("next_page", [])}
            return self.list_jobs
        if "/jobs/create" in url:
            return {"job_id": 42}
        if "/jobs/run-now" in url:
            return {"run_id": 88}
        if "/jobs/runs/get" in url:
            return {
                "state": {
                    "life_cycle_state": "TERMINATED",
                    "result_state": "SUCCESS",
                    "state_message": "ok",
                },
                "run_page_url": "https://example/run/88",
            }
        if "/jobs/reset" in url:
            return {}
        raise AssertionError(f"unexpected call: {method} {url} {json_body}")


def test_databricks_client_ensure_and_run() -> None:
    http = FakeHttp()
    client = DatabricksJobsClient(
        http_client=http,
        workspace_url="https://adb.example.com",
        oauth_bearer_token="token",
    )
    spec = DatabricksJobSpec(
        workspace_url="https://adb.example.com",
        existing_cluster_id="1111-222222-abc123",
        python_file_uri="dbfs:/floe/bin/floe_entry.py",
        job_name="floe-sales-prod",
        parameters=["run", "-c", "dbfs:/floe/configs/prod.yml"],
    )

    job_id = client.ensure_domain_job(spec)
    run_id = client.run_now(job_id=job_id)
    terminal = client.poll_run_to_terminal(run_id=run_id, poll_interval_seconds=1, timeout_seconds=5)

    assert job_id == 42
    assert run_id == 88
    assert terminal.result_state == "SUCCESS"


def test_databricks_client_get_query_params_are_urlencoded() -> None:
    http = FakeHttp()
    client = DatabricksJobsClient(
        http_client=http,
        workspace_url="https://adb.example.com",
        oauth_bearer_token="token",
    )

    client._call("GET", "/api/2.1/jobs/list", {"name": "floe sales/prod"})
    _, url, _ = http.calls[-1]
    assert "name=floe+sales%2Fprod" in url


def test_databricks_client_find_job_by_name_paginates_until_match() -> None:
    http = FakeHttp()
    http.list_jobs = {
        "jobs": [{"job_id": 1, "settings": {"name": "other"}}],
        "next_page": [{"job_id": 2, "settings": {"name": "floe-sales-prod"}}],
    }
    client = DatabricksJobsClient(
        http_client=http,
        workspace_url="https://adb.example.com",
        oauth_bearer_token="token",
    )

    found = client._find_job_by_name("floe-sales-prod")
    assert found is not None
    assert found["job_id"] == 2
