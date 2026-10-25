"""OpenLineage event bodies: job identity, datasets, facets, parent run and api key (Rust: `tests/unit/run/lineage.rs`)."""

from pathlib import Path
from typing import Any

import pytest

from floe.config.parse import parse_config
from floe.config.project import LineageConfig
from floe.config.types import EntityConfig
from floe.lineage import events
from floe.lineage.events import EventBuilder, event_type, iso, resolve_api_key


@pytest.fixture(autouse=True)
def no_orchestrator(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in ("AIRFLOW_CTX_DAG_RUN_ID", "DAGSTER_RUN_ID"):
        monkeypatch.delenv(var, raising=False)


def config(**kw: Any) -> LineageConfig:
    fields: dict[str, Any] = {
        **dict.fromkeys(("endpoint", "api_key", "dataset_namespace", "producer", "job_name")),
        "url": "http://x", "timeout_secs": 2, "namespace": "test-ns", "max_failures": None,
    }  # fmt: skip
    return LineageConfig(**(fields | kw))


ENTITY = """\
  - name: {name}
    {domain}
    source: {{format: csv, path: "{source}"}}
    sink:
      accepted: {{format: {fmt}, path: "{accepted}"{iceberg}}}
      {rejected}
    policy: {{severity: warn}}
    schema:
      columns: [{columns}]
"""


def entity(
    tmp_path: Path,
    source: str = "/data/in/",
    accepted: str = "/data/out/",
    rejected: str | None = None,
    *,
    name: str = "orders",
    domain: str | None = None,
    iceberg: str = "",
    columns: str = "",
) -> EntityConfig:
    text = ENTITY.format(
        name=name, source=source, accepted=accepted, columns=columns,
        domain=f"domain: {domain}" if domain else "",
        fmt="iceberg" if iceberg else "parquet",
        iceberg=f", iceberg: {iceberg}" if iceberg else "",
        rejected=f'rejected: {{format: csv, path: "{rejected}"}}' if rejected else "",
    )  # fmt: skip
    domains = f"domains:\n  - name: {domain}\n" if domain else ""
    path = tmp_path / f"{name}-{domain}.yml"
    path.write_text(
        f'version: "0.3"\n{domains}entities:\n{text}report: {{path: ./r}}\n'
    )
    return parse_config(path, {}).entities[0]


STATS = {"files": 2, "rows": 100, "accepted": 90, "rejected": 10, "warnings": 1, "errors": 0}  # fmt: skip


def datasets(
    entities: list[EntityConfig], name: str = "orders", **kw: Any
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return EventBuilder(config(**kw), entities, "config.yml").datasets(name, STATS)


def test_body_shape() -> None:
    b = EventBuilder(config(), [], "config.yml")
    assert b.body("START", 1_000_000, "rid", "orders") == {
        "eventType": "START",
        "eventTime": "1970-01-01T00:16:40Z",
        "run": {"runId": "rid", "facets": {}},
        "job": {"namespace": "test-ns", "name": "orders", "facets": {}},
        "inputs": [],
        "outputs": [],
        "producer": events.DEFAULT_PRODUCER,
        "schemaURL": events.RUN_EVENT_SCHEMA,
    }


def test_event_time_trims_trailing_fraction_zeros() -> None:
    assert [iso(1_000_000), iso(1_000_500), iso(1_000_123)] == [
        "1970-01-01T00:16:40Z",
        "1970-01-01T00:16:40.5Z",
        "1970-01-01T00:16:40.123Z",
    ]


def test_event_type() -> None:
    assert [event_type(s) for s in ("success", "rejected", "failed", "aborted")] == [
        "COMPLETE",
        "COMPLETE",
        "FAIL",
        "FAIL",
    ]


def test_run_job_name_is_job_name_then_config_stem_then_floe_run() -> None:
    names = [
        EventBuilder(config(), [], "pipelines/orders.yml").run_job_name,
        EventBuilder(config(job_name="my-pipeline"), [], "orders.yml").run_job_name,
        EventBuilder(config(), [], "").run_job_name,
    ]
    assert names == ["orders", "my-pipeline", "floe-run"]


def test_source_input_and_accepted_output(tmp_path: Path) -> None:
    inputs, outputs = datasets([entity(tmp_path)])
    assert inputs == [{"namespace": "file", "name": "/data/in/"}]
    assert [(o["namespace"], o["name"]) for o in outputs] == [("file", "/data/out/")]


def test_unknown_entity_has_no_datasets() -> None:
    assert datasets([]) == ([], [])


def test_rejected_output_and_data_quality_metrics(tmp_path: Path) -> None:
    _, (accepted, rejected) = datasets([entity(tmp_path, rejected="/data/rejected/")])
    assert accepted["facets"]["dataQualityMetrics"] == {
        "rowCount": 90, "validCount": 90, "invalidCount": 0,
        "_producer": events.DEFAULT_PRODUCER, "_schemaURL": events.DQ_SCHEMA,
    }  # fmt: skip
    assert rejected == {
        "namespace": "file",
        "name": "/data/rejected/",
        "facets": {"dataQualityMetrics": {
            "rowCount": 10, "validCount": 0, "invalidCount": 10,
            "_producer": events.DEFAULT_PRODUCER, "_schemaURL": events.DQ_SCHEMA,
        }},
    }  # fmt: skip


def test_schema_and_floe_quality_run_facets(tmp_path: Path) -> None:
    _, outputs = datasets([entity(tmp_path)])
    facets = outputs[0]["facets"]
    assert facets["schema"]["fields"] == []
    assert "columnLineage" not in facets  # no columns, no column lineage
    assert {k: v for k, v in facets["floeQualityRun"].items() if k[0] != "_"} == {
        "entity": "orders", "rejectionRate": 0.1, "files": 2, "rows": 100,
        "warnings": 1, "errors": 0,
    }  # fmt: skip


def test_column_lineage_maps_to_source_field(tmp_path: Path) -> None:
    cols = "{name: order_id, type: string}, {name: amount, type: float64, source: raw_amt}"  # fmt: skip
    facets = datasets([entity(tmp_path, columns=cols)])[1][0]["facets"]
    assert facets["schema"]["fields"] == [
        {"name": "order_id", "type": "string"}, {"name": "amount", "type": "float64"},
    ]  # fmt: skip
    src = {"namespace": "file", "name": "/data/in/"}
    assert facets["columnLineage"]["fields"] == {
        "order_id": {"inputFields": [src | {"field": "order_id"}]},
        "amount": {"inputFields": [src | {"field": "raw_amt"}]},
    }


@pytest.mark.parametrize(
    ("source", "namespace", "name"),
    [
        ("s3://my-bucket/data/raw/", "s3://my-bucket", "data/raw/"),
        ("abfss://container@acct.dfs.core.windows.net/raw/", "abfss://container@acct.dfs.core.windows.net", "raw/"),
        ("abfs://container@acct.dfs.core.windows.net/raw/", "abfs://container@acct.dfs.core.windows.net", "raw/"),
    ],
)  # fmt: skip
def test_cloud_uris_split_into_namespace_and_name(
    tmp_path: Path, source: str, namespace: str, name: str
) -> None:
    inputs, outputs = datasets([entity(tmp_path, source, "s3://my-bucket/data/out/")])
    assert inputs == [{"namespace": namespace, "name": name}]
    assert (outputs[0]["namespace"], outputs[0]["name"]) == ("s3://my-bucket", "data/out/")  # fmt: skip


@pytest.mark.parametrize(("dataset_namespace", "expected"), [("iceberg.prod", "iceberg.prod"), (None, "test-ns")])  # fmt: skip
def test_iceberg_accepted_uses_dataset_namespace_and_table_name(
    tmp_path: Path, dataset_namespace: str | None, expected: str
) -> None:
    e = entity(
        tmp_path, "s3://iceberg-data/bronze/sales/customers", "s3://warehouse/silver/customers",
        name="customers", iceberg="{namespace: sales_dev, table: customers}",
    )  # fmt: skip
    inputs, outputs = datasets([e], "customers", dataset_namespace=dataset_namespace)
    assert inputs == [{"namespace": "s3://iceberg-data", "name": "bronze/sales/customers"}]  # fmt: skip
    assert (outputs[0]["namespace"], outputs[0]["name"]) == (expected, "sales_dev.customers")  # fmt: skip


def test_qualified_and_bare_entity_ids_resolve_their_own_datasets(
    tmp_path: Path,
) -> None:
    bare = entity(tmp_path)
    qualified = entity(tmp_path, "/data/sales/in/", "/data/sales/out/", domain="sales")
    assert datasets([bare, qualified], "orders")[0][0]["name"] == "/data/in/"
    assert datasets([bare, qualified], "sales.orders")[0][0]["name"] == "/data/sales/in/"  # fmt: skip


def test_parent_run_facet_from_airflow_then_dagster(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    b = EventBuilder(config(producer="p"), [], "")
    schema = "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json"
    monkeypatch.setenv("DAGSTER_RUN_ID", "dg-1")
    monkeypatch.setenv("DAGSTER_JOB_NAME", "job")
    assert b.body("START", 0, "r", "j")["run"]["facets"]["parent"] == {
        "run": {"runId": "dg-1"}, "job": {"namespace": "test-ns", "name": "job"},
        "_producer": "p", "_schemaURL": schema,
    }  # fmt: skip
    monkeypatch.setenv("AIRFLOW_CTX_DAG_RUN_ID", "af-1")
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "dag")
    monkeypatch.setenv("AIRFLOW_CTX_TASK_ID", "task")
    assert b.parent() == {
        "run": {"runId": "af-1"}, "job": {"namespace": "test-ns", "name": "dag.task"},
        "_producer": "p", "_schemaURL": schema,
    }  # fmt: skip


@pytest.mark.parametrize(
    ("configured", "ambient", "expected"),
    [
        ("cfg-token", "env-token", "cfg-token"),
        (None, "env-token", "env-token"),
        ("{{OPENLINEAGE_API_KEY}}", "env-token", "env-token"),
        ("{{OPENLINEAGE_API_KEY}}", None, None),
        (None, None, None),
        ("   ", "   ", None),
    ],
)
def test_resolve_api_key(
    configured: str | None, ambient: str | None, expected: str | None
) -> None:
    assert resolve_api_key(configured, ambient) == expected
