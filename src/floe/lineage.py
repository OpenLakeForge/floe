"""OpenLineage observer for `floe run` over plain HTTP (Rust: `lineage/mod.rs`). Lineage failures never fail the run."""

import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
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
RETRY_DELAYS_S = (0.0, 0.1, 0.5)
CLOUD_PREFIXES = ("s3://", "gs://", "gcs://", "az://", "abfss://", "abfs://")


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


def _iso(ms: int) -> str:
    """RFC 3339 in UTC with trailing fractional zeros trimmed, as Rust's `time` crate prints it."""
    base = datetime.fromtimestamp(ms // 1000, UTC).strftime("%Y-%m-%dT%H:%M:%S")
    frac = f"{ms % 1000:03d}".rstrip("0")
    return f"{base}.{frac}Z" if frac else f"{base}Z"


def _dataset(namespace: str, name: str) -> dict[str, Any]:
    return {"namespace": namespace, "name": name}


class OpenLineageObserver:
    """A run observer (`floe.run.events.Observer`) posting OpenLineage `RunEvent`s."""

    # ponytail: unsynchronised breaker counters; add a lock when entities run in parallel.
    def __init__(
        self, config: LineageConfig, entities: Sequence[EntityConfig], config_path: str
    ) -> None:
        self.config = config
        path = (config.endpoint or "").strip() or "api/v1/lineage"
        self.url = f"{config.url.rstrip('/')}/{path.lstrip('/')}"
        self.producer = config.producer or DEFAULT_PRODUCER
        secret = config.api_key.get_secret_value() if config.api_key else None
        self.api_key = resolve_api_key(secret, os.environ.get("OPENLINEAGE_API_KEY"))
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
        self.entity_run_ids: dict[str, str] = {}
        self.run_uuid: str | None = None
        self.consecutive_failures = 0
        self.circuit_open = False

    def __call__(self, event: dict[str, Any]) -> None:
        kind, ts = event["event"], event["ts_ms"]
        if kind == "run_started":
            self.consecutive_failures, self.circuit_open = 0, False
            self.run_uuid = str(uuid.uuid4())
            self._post("START", ts, self.run_uuid, self.run_job_name)
        elif kind == "entity_started":
            run_id = self.entity_run_ids[event["name"]] = str(uuid.uuid4())
            self._post("START", ts, run_id, event["name"])
        elif kind == "entity_finished":
            name = event["name"]
            run_id = self.entity_run_ids.get(name, f"{event['run_id']}.entity.{name}")
            inputs, outputs = self._datasets(name, event)
            self._post(_type(event), ts, run_id, name, inputs, outputs)
        elif kind == "run_finished":
            run_id = self.run_uuid or str(uuid.uuid4())
            self._post(_type(event), ts, run_id, self.run_job_name)

    def fail_run(self) -> None:
        """A run that raised: `START` if it never started, then `FAIL` (Rust: the CLI failure path)."""
        ts = time.time_ns() // 1_000_000
        if self.run_uuid is None:
            self({"event": "run_started", "ts_ms": ts})
        self({"event": "run_finished", "status": "failed", "ts_ms": ts})

    def _facet(self, schema: str, **fields: Any) -> dict[str, Any]:
        return fields | {"_producer": self.producer, "_schemaURL": schema}

    def _datasets(
        self, name: str, s: dict[str, Any]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
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

    def _parent(self) -> dict[str, Any] | None:
        env = os.environ
        if (run_id := env.get("AIRFLOW_CTX_DAG_RUN_ID")) is not None:
            dag, task = (
                env.get("AIRFLOW_CTX_DAG_ID", ""),
                env.get("AIRFLOW_CTX_TASK_ID", ""),
            )
            job = f"{dag}.{task}" if task else dag
        elif (run_id := env.get("DAGSTER_RUN_ID")) is not None:
            job = env.get("DAGSTER_JOB_NAME", "")
        else:
            return None
        return self._facet(
            "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json",
            run={"runId": run_id},
            job={"namespace": self.config.namespace, "name": job},
        )

    def _post(
        self,
        event_type: str,
        ts_ms: int,
        run_id: str,
        job: str,
        inputs: list[dict[str, Any]] | None = None,
        outputs: list[dict[str, Any]] | None = None,
    ) -> None:
        if self.circuit_open:
            return
        parent = self._parent()
        body: dict[str, Any] = {
            "eventType": event_type,
            "eventTime": _iso(ts_ms),
            "run": {"runId": run_id, "facets": {"parent": parent} if parent else {}},
            "job": {"namespace": self.config.namespace, "name": job, "facets": {}},
            "inputs": inputs or [],
            "outputs": outputs or [],
            "producer": self.producer,
            "schemaURL": RUN_EVENT_SCHEMA,
        }
        data = json.dumps(body).encode()
        for delay in RETRY_DELAYS_S:
            time.sleep(delay)
            retryable = self._attempt(data)
            if retryable is None:
                self.consecutive_failures = 0
                return
            if not retryable:
                break
        print(f"warn: lineage: POST {self.url} failed — event dropped", file=sys.stderr)
        self.consecutive_failures += 1
        failures = self.consecutive_failures
        if failures >= (self.config.max_failures or 3) and not self.circuit_open:
            self.circuit_open = True
            print(
                f"warn: lineage: disabled for this run after {failures} consecutive failures — check endpoint {self.url}",
                file=sys.stderr,
            )

    def _attempt(self, data: bytes) -> bool | None:
        """None on success, else whether the failure is retryable (transport, 5xx, 429)."""
        timeout = 5 if self.config.timeout_secs is None else self.config.timeout_secs
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        request = urllib.request.Request(self.url, data, headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=timeout):
                return None
        except urllib.error.HTTPError as e:
            return e.code == 429 or e.code >= 500
        except OSError:
            return True


def _type(event: dict[str, Any]) -> str:
    return "FAIL" if event["status"] in ("failed", "aborted") else "COMPLETE"
