use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Serialize)]
pub struct CommonManifest {
    pub schema: &'static str,
    pub generated_at_ts_ms: u64,
    pub floe_version: &'static str,
    pub spec_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest_name: Option<String>,
    pub manifest_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest_revision: Option<String>,
    /// Runtime the manifest targets: `"image"` (local URIs absolute under the container
    /// work-root) or `"cli"` (local URIs relative / as-typed). Drives how a replaying
    /// runner resolves local paths. See `RuntimeEnv`.
    pub runtime_env: &'static str,
    /// Work-root local paths are resolved under at replay time (e.g. `/work`). `Some` only
    /// for `runtime_env == "image"`; `None` for `"cli"` (paths are relative, no fixed root).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub work_root: Option<String>,
    pub config_uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_checksum: Option<String>,
    /// Every file of a multi-file config, sorted by path; absent for a single file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_files: Option<Vec<ManifestConfigFile>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_checksum: Option<String>,
    pub report_base_uri: String,
    pub domains: Vec<ManifestDomain>,
    pub execution: ManifestExecution,
    pub runners: ManifestRunners,
    pub entities: Vec<ManifestEntity>,
    /// Storage backend definitions from the profile (if a profile was supplied).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storages: Option<serde_json::Value>,
    /// Catalog definitions from the profile (if a profile was supplied).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalogs: Option<serde_json::Value>,
    /// Lineage configuration from the profile (if a profile was supplied).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lineage: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct ManifestConfigFile {
    /// Relative to the root config's directory, `/`-separated.
    pub path: String,
    pub sha256: String,
}

#[derive(Debug, Serialize)]
pub struct ManifestDomain {
    pub name: String,
    pub incoming_dir: String,
}

#[derive(Debug, Serialize)]
pub struct ManifestExecution {
    pub entrypoint: &'static str,
    pub base_args: Vec<String>,
    pub per_entity_args: Vec<String>,
    pub log_format: &'static str,
    pub result_contract: ManifestResultContract,
    pub defaults: ManifestExecutionDefaults,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub orchestration: Option<ManifestOrchestration>,
}

#[derive(Debug, Serialize)]
pub struct ManifestOrchestration {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_concurrent_entities: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ManifestResultContract {
    pub run_finished_event: bool,
    pub summary_uri_field: &'static str,
    pub exit_codes: BTreeMap<&'static str, &'static str>,
}

#[derive(Debug, Serialize)]
pub struct ManifestExecutionDefaults {
    pub env: BTreeMap<String, String>,
    pub workdir: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ManifestRunners {
    pub default: &'static str,
    pub definitions: BTreeMap<&'static str, ManifestRunnerDefinition>,
}

#[derive(Debug, Serialize)]
pub struct ManifestRunnerSecret {
    pub name: String,
    pub secret_name: String,
    pub key: String,
}

#[derive(Debug, Serialize)]
pub struct ManifestRunnerDefinition {
    #[serde(rename = "type")]
    pub runner_type: &'static str,
    pub command: Option<String>,
    pub args: Option<Vec<String>>,
    pub timeout_seconds: Option<u64>,
    pub ttl_seconds_after_finished: Option<u64>,
    pub poll_interval_seconds: Option<u64>,
    pub secrets: Option<Vec<ManifestRunnerSecret>>,
    pub image: Option<String>,
    pub namespace: Option<String>,
    pub service_account: Option<String>,
    pub resources: Option<ManifestRunnerResources>,
    pub env: Option<BTreeMap<String, String>>,
    pub workspace_url: Option<String>,
    pub existing_cluster_id: Option<String>,
    pub config_uri: Option<String>,
    pub python_file_uri: Option<String>,
    pub job_name: Option<String>,
    pub auth: Option<ManifestRunnerAuth>,
    pub env_parameters: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Serialize)]
pub struct ManifestRunnerAuth {
    pub service_principal_oauth_ref: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ManifestRunnerResources {
    pub cpu: Option<String>,
    pub memory_mb: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct ManifestEntity {
    pub name: String,
    pub domain: Option<String>,
    pub group_name: String,
    pub asset_key: Vec<String>,
    pub source_format: String,
    pub accepted_sink_uri: String,
    pub rejected_sink_uri: Option<String>,
    pub tags: Option<BTreeMap<String, String>>,
    pub source: ManifestSource,
    pub sinks: ManifestSinks,
    pub runner: Option<String>,
    /// Policy severity: "warn" | "reject" | "abort".
    pub policy_severity: String,
    /// Sink write mode: "overwrite" | "append" | "merge_scd1" | "merge_scd2".
    pub write_mode: String,
    /// Incremental processing mode: "none" | "archive" | "file" | "row".
    pub incremental_mode: String,
    /// Full schema definition for this entity.
    pub schema: ManifestEntitySchema,
    /// PII masking configuration, if configured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pii: Option<serde_json::Value>,
    /// Incremental state file path, if configured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_path: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ManifestEntitySchema {
    pub columns: Vec<ManifestColumnDef>,
    pub primary_key: Vec<String>,
    pub unique_keys: Vec<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub normalize_columns: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mismatch: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_evolution: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct ManifestColumnDef {
    pub name: String,
    pub column_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nullable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trim: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct ManifestSource {
    pub format: String,
    pub storage: String,
    pub uri: String,
    pub path: String,
    pub resolved: bool,
    pub cast_mode: Option<String>,
    pub options: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct ManifestSinks {
    pub accepted: ManifestSinkTarget,
    pub rejected: Option<ManifestSinkTarget>,
    pub archive: Option<ManifestArchiveTarget>,
}

#[derive(Debug, Serialize)]
pub struct ManifestSinkTarget {
    pub format: String,
    pub storage: String,
    pub uri: String,
    pub path: String,
    pub resolved: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_by: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub merge: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iceberg: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duckdb: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct ManifestArchiveTarget {
    pub storage: String,
    pub uri: String,
    pub path: String,
    pub resolved: bool,
}
