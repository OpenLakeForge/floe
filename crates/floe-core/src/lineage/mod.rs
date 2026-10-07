use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use uuid::Uuid;

use crate::config::{EntityConfig, LineageConfig};
use crate::run::events::{RunEvent, RunObserver};
use crate::secret::Secret;

const DEFAULT_PRODUCER: &str = concat!(
    "https://github.com/OpenLakeForge/floe/releases/tag/v",
    env!("CARGO_PKG_VERSION")
);

const OPENLINEAGE_API_KEY_ENV: &str = "OPENLINEAGE_API_KEY";

#[derive(Clone)]
struct ColumnMapping {
    output_name: String,
    column_type: String,
    source_field: Option<String>,
}

struct OlDataset {
    namespace: String,
    name: String,
}

struct EntityUris {
    source: OlDataset,
    accepted: OlDataset,
    rejected: Option<OlDataset>,
}

fn resolve_lineage_url(base: &str, endpoint: Option<&str>) -> String {
    let path = endpoint
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("api/v1/lineage");
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

fn is_unresolved_placeholder(value: &str) -> bool {
    let value = value.trim();
    value.starts_with("{{") && value.ends_with("}}")
}

/// Effective Bearer token for lineage POSTs.
///
/// An explicit `lineage.api_key` wins for backward compatibility. Otherwise Floe falls back
/// to the ambient `OPENLINEAGE_API_KEY`, mirroring the OpenLineage Python client so a replayed
/// manifest authenticates from a runner-mounted Secret without the credential ever being
/// persisted in the manifest. An unexpanded `{{...}}` placeholder is treated as absent:
/// profile-supplied lineage is merged after config templating (see `apply_profile_lineage`),
/// so the raw placeholder would otherwise be sent verbatim as the token.
fn resolve_api_key(configured: Option<&str>, ambient: Option<String>) -> Option<Secret> {
    let explicit = configured
        .map(str::trim)
        .filter(|key| !key.is_empty() && !is_unresolved_placeholder(key));
    match explicit {
        Some(key) => Some(Secret::from(key)),
        None => ambient
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .map(Secret::from),
    }
}

pub struct OpenLineageObserver {
    client: reqwest::blocking::Client,
    config: LineageConfig,
    api_key: Option<Secret>,
    entity_start_ms: Mutex<HashMap<String, u128>>,
    entity_run_ids: Mutex<HashMap<String, String>>,
    run_start_ms: Mutex<Option<u128>>,
    run_uuid: Mutex<Option<String>>,
    entity_schemas: HashMap<String, Vec<ColumnMapping>>,
    entity_uris: HashMap<String, EntityUris>,
    run_job_name: String,
    consecutive_failures: AtomicUsize,
    circuit_open: AtomicBool,
}

impl OpenLineageObserver {
    pub fn new(
        config: &LineageConfig,
        entities: &[EntityConfig],
        config_path: &str,
    ) -> crate::FloeResult<Self> {
        let timeout = Duration::from_secs(config.timeout_secs.unwrap_or(5));
        let client = reqwest::blocking::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| {
                crate::errors::FloeError::config(format!(
                    "lineage: failed to build HTTP client: {e}"
                ))
            })?;

        let run_job_name = config
            .job_name
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| {
                std::path::Path::new(config_path)
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("floe-run")
                    .to_string()
            });

        let entity_schemas = entities
            .iter()
            .map(|e| {
                let fields: Vec<ColumnMapping> = e
                    .schema
                    .columns
                    .iter()
                    .map(|c| ColumnMapping {
                        output_name: c.name.clone(),
                        column_type: c.column_type.clone(),
                        source_field: c.source.clone(),
                    })
                    .collect();
                (e.name.clone(), fields)
            })
            .collect();

        let dataset_namespace = config
            .dataset_namespace
            .clone()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| config.namespace.clone());

        let entity_uris = entities
            .iter()
            .map(|e| {
                let (src_ns, src_name) = split_storage_uri(&e.source.path);
                let source = OlDataset {
                    namespace: src_ns,
                    name: src_name,
                };

                let accepted = match e.sink.accepted.iceberg.as_ref() {
                    Some(iceberg) => {
                        let ns = iceberg.namespace.as_deref().unwrap_or(e.name.as_str());
                        let tbl = iceberg.table.as_deref().unwrap_or(e.name.as_str());
                        OlDataset {
                            namespace: dataset_namespace.clone(),
                            name: format!("{ns}.{tbl}"),
                        }
                    }
                    None => {
                        let (acc_ns, acc_name) = split_storage_uri(&e.sink.accepted.path);
                        OlDataset {
                            namespace: acc_ns,
                            name: acc_name,
                        }
                    }
                };

                let rejected = e.sink.rejected.as_ref().map(|r| {
                    let (rej_ns, rej_name) = split_storage_uri(&r.path);
                    OlDataset {
                        namespace: rej_ns,
                        name: rej_name,
                    }
                });

                (
                    e.name.clone(),
                    EntityUris {
                        source,
                        accepted,
                        rejected,
                    },
                )
            })
            .collect();

        let api_key = resolve_api_key(
            config.api_key.as_deref(),
            std::env::var(OPENLINEAGE_API_KEY_ENV).ok(),
        );

        Ok(Self {
            client,
            config: config.clone(),
            api_key,
            entity_start_ms: Mutex::new(HashMap::new()),
            entity_run_ids: Mutex::new(HashMap::new()),
            run_start_ms: Mutex::new(None),
            run_uuid: Mutex::new(None),
            entity_schemas,
            entity_uris,
            run_job_name,
            consecutive_failures: AtomicUsize::new(0),
            circuit_open: AtomicBool::new(false),
        })
    }

    fn attempt_post(&self, url: &str, body: &Value) -> Result<(), bool> {
        let mut req = self.client.post(url).json(body);
        if let Some(api_key) = self.api_key.as_ref() {
            req = req.bearer_auth(api_key.expose());
        }
        match req.send() {
            Err(_) => Err(true),
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    Ok(())
                } else if status.as_u16() == 429 || status.is_server_error() {
                    Err(true)
                } else {
                    Err(false)
                }
            }
        }
    }

    fn post_event(&self, body: Value) {
        if self.circuit_open.load(Ordering::Relaxed) {
            return;
        }

        let url = resolve_lineage_url(&self.config.url, self.config.endpoint.as_deref());
        let max_failures = self.config.max_failures.unwrap_or(3) as usize;
        let retry_delays_ms: &[u64] = &[0, 100, 500];

        let mut succeeded = false;

        'retry: for (attempt, &delay_ms) in retry_delays_ms.iter().enumerate() {
            if delay_ms > 0 {
                std::thread::sleep(Duration::from_millis(delay_ms));
            }
            match self.attempt_post(&url, &body) {
                Ok(()) => {
                    self.consecutive_failures.store(0, Ordering::Relaxed);
                    succeeded = true;
                    break 'retry;
                }
                Err(is_retryable) => {
                    if !is_retryable || attempt == retry_delays_ms.len() - 1 {
                        break 'retry;
                    }
                }
            }
        }

        if succeeded {
            return;
        }

        // Always warn when an event is dropped, whether the failure was retryable or not.
        crate::warnings::emit(
            "",
            None,
            None,
            Some("lineage_http_error"),
            &format!("lineage: POST {url} failed — event dropped"),
        );

        let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if failures >= max_failures && !self.circuit_open.swap(true, Ordering::Relaxed) {
            crate::warnings::emit(
                "",
                None,
                None,
                Some("lineage_circuit_open"),
                &format!(
                    "lineage: disabled for this run after {failures} consecutive failures — check endpoint {url}"
                ),
            );
        }
    }

    fn producer(&self) -> &str {
        self.config.producer.as_deref().unwrap_or(DEFAULT_PRODUCER)
    }

    fn parent_run_facet(&self) -> Option<Value> {
        if let Ok(run_id) = std::env::var("AIRFLOW_CTX_DAG_RUN_ID") {
            let dag = std::env::var("AIRFLOW_CTX_DAG_ID").unwrap_or_default();
            let task = std::env::var("AIRFLOW_CTX_TASK_ID").unwrap_or_default();
            let job_name = if task.is_empty() {
                dag.clone()
            } else {
                format!("{dag}.{task}")
            };
            return Some(json!({
                "run": { "runId": run_id },
                "job": {
                    "namespace": self.config.namespace,
                    "name": job_name
                },
                "_producer": self.producer(),
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json"
            }));
        }
        if let Ok(run_id) = std::env::var("DAGSTER_RUN_ID") {
            let job = std::env::var("DAGSTER_JOB_NAME").unwrap_or_default();
            return Some(json!({
                "run": { "runId": run_id },
                "job": {
                    "namespace": self.config.namespace,
                    "name": job
                },
                "_producer": self.producer(),
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json"
            }));
        }
        None
    }

    fn emit_entity_run_event(
        &self,
        entity_run_id: &str,
        name: &str,
        event_type: &str,
        ts_ms: u128,
        stats: Option<EntityStats>,
        uris: Option<&EntityUris>,
    ) {
        let event_time = ms_to_iso8601(ts_ms);

        let mut run_facets = json!({});
        if let Some(parent) = self.parent_run_facet() {
            run_facets["parent"] = parent;
        }

        let (inputs, outputs) = match (stats.as_ref(), uris) {
            (Some(s), Some(u)) => {
                let rejection_rate = if s.rows > 0 {
                    s.rejected as f64 / s.rows as f64
                } else {
                    0.0
                };

                let inputs = json!([{
                    "namespace": u.source.namespace,
                    "name": u.source.name
                }]);

                let schema_facet = json!({
                    "fields": s.schema_fields.iter().map(|col| {
                        json!({ "name": col.output_name, "type": col.column_type })
                    }).collect::<Vec<_>>(),
                    "_producer": self.producer(),
                    "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json"
                });
                let accepted_dq_facet = json!({
                    "rowCount": s.accepted,
                    "validCount": s.accepted,
                    "invalidCount": 0u64,
                    "_producer": self.producer(),
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-2/DataQualityMetricsOutputDatasetFacet.json"
                });
                let floe_facet = json!({
                    "entity": name,
                    "rejectionRate": rejection_rate,
                    "files": s.files,
                    "rows": s.rows,
                    "warnings": s.warnings,
                    "errors": s.errors,
                    "_producer": self.producer(),
                    "_schemaURL": "https://github.com/OpenLakeForge/floe/schemas/FloeQualityRunFacet.json"
                });

                let mut accepted_facets = json!({
                    "schema": schema_facet,
                    "dataQualityMetrics": accepted_dq_facet,
                    "floeQualityRun": floe_facet
                });

                if !s.schema_fields.is_empty() {
                    let fields_map: serde_json::Map<String, Value> = s
                        .schema_fields
                        .iter()
                        .map(|col| {
                            let src = col.source_field.as_deref().unwrap_or(&col.output_name);
                            let entry = json!({
                                "inputFields": [{
                                    "namespace": u.source.namespace,
                                    "name": u.source.name,
                                    "field": src
                                }]
                            });
                            (col.output_name.clone(), entry)
                        })
                        .collect();
                    accepted_facets["columnLineage"] = json!({
                        "fields": fields_map,
                        "_producer": self.producer(),
                        "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/ColumnLineageDatasetFacet.json"
                    });
                }

                let mut out = vec![json!({
                    "namespace": u.accepted.namespace,
                    "name": u.accepted.name,
                    "facets": accepted_facets
                })];

                if let Some(ref rej) = u.rejected {
                    let rejected_dq_facet = json!({
                        "rowCount": s.rejected,
                        "validCount": 0u64,
                        "invalidCount": s.rejected,
                        "_producer": self.producer(),
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-2/DataQualityMetricsOutputDatasetFacet.json"
                    });
                    out.push(json!({
                        "namespace": rej.namespace,
                        "name": rej.name,
                        "facets": {
                            "dataQualityMetrics": rejected_dq_facet
                        }
                    }));
                }

                (inputs, json!(out))
            }
            _ => (json!([]), json!([])),
        };

        let body = json!({
            "eventType": event_type,
            "eventTime": event_time,
            "run": {
                "runId": entity_run_id,
                "facets": run_facets
            },
            "job": {
                "namespace": self.config.namespace,
                "name": name,
                "facets": {}
            },
            "inputs": inputs,
            "outputs": outputs,
            "producer": self.producer(),
            "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunEvent"
        });
        self.post_event(body);
    }
}

struct EntityStats {
    files: u64,
    rows: u64,
    accepted: u64,
    rejected: u64,
    warnings: u64,
    errors: u64,
    schema_fields: Vec<ColumnMapping>,
}

fn ms_to_iso8601(ms: u128) -> String {
    let secs = (ms / 1000) as i64;
    let nanos = ((ms % 1000) * 1_000_000) as i64;
    match time::OffsetDateTime::from_unix_timestamp(secs) {
        Ok(dt) => {
            let ns = time::Duration::nanoseconds(nanos);
            let dt = dt.saturating_add(ns);
            dt.format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_else(|_| ms.to_string())
        }
        Err(_) => ms.to_string(),
    }
}

fn split_storage_uri(uri: &str) -> (String, String) {
    // abfss:// must precede abfs:// so the longer prefix matches first.
    let cloud_prefixes = ["s3://", "gs://", "gcs://", "az://", "abfss://", "abfs://"];
    for prefix in cloud_prefixes {
        if let Some(after_scheme) = uri.strip_prefix(prefix) {
            if let Some(slash) = after_scheme.find('/') {
                let authority = uri[..prefix.len() + slash].to_string();
                let path = after_scheme[slash + 1..].to_string();
                return (authority, path);
            }
            return (uri.to_string(), String::new());
        }
    }
    ("file".to_string(), uri.to_string())
}

impl RunObserver for OpenLineageObserver {
    fn on_event(&self, event: RunEvent) {
        match event {
            RunEvent::RunStarted { ts_ms, .. } => {
                // Reset circuit breaker at the start of each run so a recovered endpoint
                // is retried in subsequent runs within the same long-lived process.
                self.consecutive_failures.store(0, Ordering::Relaxed);
                self.circuit_open.store(false, Ordering::Relaxed);
                if let Ok(mut guard) = self.run_start_ms.lock() {
                    *guard = Some(ts_ms);
                }
                let run_uuid = Uuid::new_v4().to_string();
                if let Ok(mut guard) = self.run_uuid.lock() {
                    *guard = Some(run_uuid.clone());
                }
                let event_time = ms_to_iso8601(ts_ms);
                let mut run_facets = json!({});
                if let Some(parent) = self.parent_run_facet() {
                    run_facets["parent"] = parent;
                }
                let body = json!({
                    "eventType": "START",
                    "eventTime": event_time,
                    "run": {
                        "runId": run_uuid,
                        "facets": run_facets
                    },
                    "job": {
                        "namespace": self.config.namespace,
                        "name": self.run_job_name,
                        "facets": {}
                    },
                    "inputs": [],
                    "outputs": [],
                    "producer": self.producer(),
                    "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunEvent"
                });
                self.post_event(body);
            }
            RunEvent::EntityStarted { name, ts_ms, .. } => {
                let entity_run_id = Uuid::new_v4().to_string();
                if let Ok(mut guard) = self.entity_start_ms.lock() {
                    guard.insert(name.clone(), ts_ms);
                }
                if let Ok(mut guard) = self.entity_run_ids.lock() {
                    guard.insert(name.clone(), entity_run_id.clone());
                }
                self.emit_entity_run_event(&entity_run_id, &name, "START", ts_ms, None, None);
            }
            RunEvent::EntityFinished {
                run_id,
                name,
                status,
                files,
                files_skipped: _,
                rows,
                accepted,
                rejected,
                warnings,
                errors,
                ts_ms,
            } => {
                let entity_run_id = self
                    .entity_run_ids
                    .lock()
                    .ok()
                    .and_then(|g| g.get(&name).cloned())
                    .unwrap_or_else(|| format!("{run_id}.entity.{name}"));
                let event_type = if status == "failed" || status == "aborted" {
                    "FAIL"
                } else {
                    "COMPLETE"
                };
                let schema_fields = self.entity_schemas.get(&name).cloned().unwrap_or_default();
                let stats = EntityStats {
                    files,
                    rows,
                    accepted,
                    rejected,
                    warnings,
                    errors,
                    schema_fields,
                };
                let uris = self.entity_uris.get(&name);
                self.emit_entity_run_event(
                    &entity_run_id,
                    &name,
                    event_type,
                    ts_ms,
                    Some(stats),
                    uris,
                );
            }
            RunEvent::RunFinished { status, ts_ms, .. } => {
                let event_type = if status == "failed" || status == "aborted" {
                    "FAIL"
                } else {
                    "COMPLETE"
                };
                let run_uuid = self
                    .run_uuid
                    .lock()
                    .ok()
                    .and_then(|g| g.clone())
                    .unwrap_or_else(|| Uuid::new_v4().to_string());
                let event_time = ms_to_iso8601(ts_ms);
                let mut run_facets = json!({});
                if let Some(parent) = self.parent_run_facet() {
                    run_facets["parent"] = parent;
                }
                let body = json!({
                    "eventType": event_type,
                    "eventTime": event_time,
                    "run": {
                        "runId": run_uuid,
                        "facets": run_facets
                    },
                    "job": {
                        "namespace": self.config.namespace,
                        "name": self.run_job_name,
                        "facets": {}
                    },
                    "inputs": [],
                    "outputs": [],
                    "producer": self.producer(),
                    "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunEvent"
                });
                self.post_event(body);
            }
            _ => {}
        }
    }
}

pub fn build_observer(
    config: &LineageConfig,
    entities: &[EntityConfig],
    config_path: &str,
) -> crate::FloeResult<Arc<dyn RunObserver>> {
    let obs = OpenLineageObserver::new(config, entities, config_path)?;
    Ok(Arc::new(obs))
}

/// Build a lineage observer for a `--manifest` replay from the manifest's
/// embedded lineage block and its entities' resolved cloud datasets, so replay
/// emits the same input/output lineage as a direct config run. `Ok(None)` when
/// the manifest has no `lineage` block. See
/// [`crate::manifest::reconstruct::lineage_inputs_from_manifest_json`].
pub fn build_observer_from_manifest_json(
    manifest_json: &str,
    config_path: &str,
) -> crate::FloeResult<Option<Arc<dyn RunObserver>>> {
    match crate::manifest::reconstruct::lineage_inputs_from_manifest_json(manifest_json)? {
        Some((lineage_cfg, entities)) => {
            build_observer(&lineage_cfg, &entities, config_path).map(Some)
        }
        None => Ok(None),
    }
}

impl OpenLineageObserver {
    pub fn is_circuit_open(&self) -> bool {
        self.circuit_open.load(Ordering::Relaxed)
    }

    pub fn consecutive_failures(&self) -> usize {
        self.consecutive_failures.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expose(key: Option<Secret>) -> Option<String> {
        key.map(|s| s.expose().to_string())
    }

    #[test]
    fn explicit_config_key_wins_over_env() {
        let key = resolve_api_key(Some("cfg-token"), Some("env-token".to_string()));
        assert_eq!(expose(key), Some("cfg-token".to_string()));
    }

    #[test]
    fn falls_back_to_env_when_config_absent() {
        let key = resolve_api_key(None, Some("env-token".to_string()));
        assert_eq!(expose(key), Some("env-token".to_string()));
    }

    #[test]
    fn unresolved_placeholder_falls_back_to_env() {
        let key = resolve_api_key(
            Some("{{OPENLINEAGE_API_KEY}}"),
            Some("env-token".to_string()),
        );
        assert_eq!(expose(key), Some("env-token".to_string()));
    }

    #[test]
    fn unresolved_placeholder_without_env_is_none() {
        assert!(resolve_api_key(Some("{{OPENLINEAGE_API_KEY}}"), None).is_none());
    }

    #[test]
    fn none_when_neither_source_present() {
        assert!(resolve_api_key(None, None).is_none());
    }

    #[test]
    fn blank_values_are_ignored() {
        assert!(resolve_api_key(Some("   "), Some("   ".to_string())).is_none());
    }
}
