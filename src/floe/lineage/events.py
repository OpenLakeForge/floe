"""OpenLineage `RunEvent` bodies, built as Rust builds them (Rust: `lineage/mod.rs`)."""

import os
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from floe import __version__
from floe.config.project import LineageConfig
from floe.config.types import EntityConfig

DEFAULT_PRODUCER = f"https://github.com/OpenLakeForge/floe/releases/tag/v{__version__}"
RUN_EVENT_SCHEMA = "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunEvent"
DQ_SCHEMA = "https://openlineage.io/spec/facets/1-0-2/DataQualityMetricsOutputDatasetFacet.json"  # fmt: skip
CLOUD_PREFIXES = ("s3://", "gs://", "gcs://", "az://", "abfss://", "abfs://")

Datasets = list[dict[str, Any]]


def split_storage_uri(uri: str) -> tuple[str, str]:
    """(namespace, name): a cloud URI splits at its authority; anything else is a `file` path."""
    for prefix in CLOUD_PREFIXES:
        if uri.startswith(prefix):
            slash = uri.find("/", len(prefix))
            return (uri, "") if slash < 0 else (uri[:slash], uri[slash + 1 :])
    return "file", uri


def resolve_api_key(configured: str | None, ambient: str | None) -> str | None:
    """An explicit key wins; else `OPENLINEAGE_API_KEY`. An unexpanded `{{...}}` counts as absent."""
    key = (configured or "").strip()
    if key and not (key.startswith("{{") and key.endswith("}}")):
        return key
    return (ambient or "").strip() or None


def event_type(status: str) -> str:
    return "FAIL" if status in ("failed", "aborted") else "COMPLETE"


def iso(ms: int) -> str:
    """RFC 3339 in UTC with trailing fractional zeros trimmed, as Rust's `time` crate prints it."""
    base = datetime.fromtimestamp(ms // 1000, UTC).strftime("%Y-%m-%dT%H:%M:%S")
    frac = f"{ms % 1000:03d}".rstrip("0")
    return f"{base}.{frac}Z" if frac else f"{base}Z"


def _dataset(namespace: str, name: str) -> dict[str, Any]:
    return {"namespace": namespace, "name": name}


class EventBuilder:
    """Job identity and per-entity datasets, fixed when the run starts."""

    def __init__(
        self, config: LineageConfig, entities: Sequence[EntityConfig], config_path: str
    ) -> None:
        self.namespace = config.namespace
        self.producer = config.producer or DEFAULT_PRODUCER
        self.run_job_name = config.job_name or Path(config_path).stem or "floe-run"
        ds = config.dataset_namespace
        dataset_ns = ds if ds and ds.strip() else config.namespace
        self.entities = {e.id: e for e in entities}
        self.uris: dict[str, tuple[dict[str, Any], ...]] = {}
        for e in entities:
            iceberg = e.sink.accepted.iceberg
            if iceberg:
                ns = e.name if iceberg.namespace is None else iceberg.namespace
                table = e.name if iceberg.table is None else iceberg.table
                accepted = _dataset(dataset_ns, f"{ns}.{table}")
            else:
                accepted = _dataset(*split_storage_uri(e.sink.accepted.path))
            rejected = e.sink.rejected
            self.uris[e.id] = (
                _dataset(*split_storage_uri(e.source.path)),
                accepted,
                *([_dataset(*split_storage_uri(rejected.path))] if rejected else []),
            )

    def body(
        self,
        event_type: str,
        ts_ms: int,
        run_id: str,
        job: str,
        inputs: Datasets | None = None,
        outputs: Datasets | None = None,
    ) -> dict[str, Any]:
        parent = self.parent()
        return {
            "eventType": event_type,
            "eventTime": iso(ts_ms),
            "run": {"runId": run_id, "facets": {"parent": parent} if parent else {}},
            "job": {"namespace": self.namespace, "name": job, "facets": {}},
            "inputs": inputs or [],
            "outputs": outputs or [],
            "producer": self.producer,
            "schemaURL": RUN_EVENT_SCHEMA,
        }

    def _facet(self, schema: str, **fields: Any) -> dict[str, Any]:
        return fields | {"_producer": self.producer, "_schemaURL": schema}

    def datasets(self, name: str, s: dict[str, Any]) -> tuple[Datasets, Datasets]:
        """Inputs and outputs of an `entity_finished` event; none for an unknown entity."""
        if name not in self.uris:
            return [], []
        source, accepted, *rejected = self.uris[name]
        columns = self.entities[name].schema_.columns
        facets: dict[str, Any] = {
            "schema": self._facet(
                "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json",
                fields=[{"name": c.name, "type": c.column_type} for c in columns],
            ),
            "dataQualityMetrics": self._facet(
                DQ_SCHEMA,
                rowCount=s["accepted"],
                validCount=s["accepted"],
                invalidCount=0,
            ),
            "floeQualityRun": self._facet(
                "https://github.com/OpenLakeForge/floe/schemas/FloeQualityRunFacet.json",
                entity=name,
                rejectionRate=s["rejected"] / s["rows"] if s["rows"] > 0 else 0.0,
                **{k: s[k] for k in ("files", "rows", "warnings", "errors")},
            ),
        }
        if columns:
            facets["columnLineage"] = self._facet(
                "https://openlineage.io/spec/facets/1-1-1/ColumnLineageDatasetFacet.json",
                fields={
                    c.name: {"inputFields": [source | {"field": c.source or c.name}]}
                    for c in columns
                },
            )
        outputs = [accepted | {"facets": facets}]
        for r in rejected:
            dq = self._facet(
                DQ_SCHEMA,
                rowCount=s["rejected"],
                validCount=0,
                invalidCount=s["rejected"],
            )
            outputs.append(r | {"facets": {"dataQualityMetrics": dq}})
        return [source], outputs

    def parent(self) -> dict[str, Any] | None:
        """The Airflow, else Dagster, parent run read from the environment."""
        env = os.environ
        if (run_id := env.get("AIRFLOW_CTX_DAG_RUN_ID")) is not None:
            dag = env.get("AIRFLOW_CTX_DAG_ID", "")
            task = env.get("AIRFLOW_CTX_TASK_ID", "")
            job = f"{dag}.{task}" if task else dag
        elif (run_id := env.get("DAGSTER_RUN_ID")) is not None:
            job = env.get("DAGSTER_JOB_NAME", "")
        else:
            return None
        return self._facet(
            "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json",
            run={"runId": run_id},
            job={"namespace": self.namespace, "name": job},
        )
