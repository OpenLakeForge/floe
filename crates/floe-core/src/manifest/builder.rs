use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

use crate::config::{ConfigLocation, EntityConfig, RootConfig, SourceOptions, StorageResolver};
use crate::manifest::model::{
    CommonManifest, ManifestArchiveTarget, ManifestColumnDef, ManifestDomain, ManifestEntity,
    ManifestEntitySchema, ManifestExecution, ManifestExecutionDefaults, ManifestOrchestration,
    ManifestResultContract, ManifestRunnerAuth, ManifestRunnerDefinition, ManifestRunnerResources,
    ManifestRunnerSecret, ManifestRunners, ManifestSinkTarget, ManifestSinks, ManifestSource,
};
use crate::profile::ProfileConfig;
use crate::FloeResult;

/// Controls how the `path` field is populated in source and sink entries.
#[derive(Debug, Default, PartialEq)]
pub enum PathMode {
    /// Keep the original relative path from the config file (default).
    #[default]
    Default,
    /// When a path has been resolved to a full URI, set `path = uri`.
    /// This makes the manifest self-contained for remote replay without
    /// needing the original config base directory.
    ResolvedUri,
}

/// Options that control manifest generation behaviour.
#[derive(Debug, Default)]
pub struct ManifestOptions {
    /// When true, `generated_at_ts_ms` is set to `0` so that the same inputs
    /// always produce byte-identical output. Use this for committed deployment
    /// artifacts that are diffed in CI.
    pub deterministic: bool,
    /// Optional stable logical name for this manifest (e.g. `"sales.prod"`).
    /// Stored as `manifest_name` in the output JSON.
    pub manifest_name: Option<String>,
    /// Display URI of the profile file (e.g. `local:///path/to/prod.yml`).
    /// Stored as `profile_uri` in the output JSON.
    pub profile_uri: Option<String>,
    /// Local filesystem path to the profile file used during generation.
    /// When supplied, a SHA-256 checksum of the file is computed and stored
    /// as `profile_checksum`.
    pub profile_path: Option<std::path::PathBuf>,
    /// When set, replaces the `{manifest_uri}` placeholder in
    /// `execution.base_args` with this URI. Use this to bake the deployed
    /// manifest location into a self-contained deployment contract.
    pub manifest_uri: Option<String>,
    /// Default domain applied to entities that have no explicit `domain`
    /// field. Drives `domain`, `group_name`, and `asset_key` generation.
    pub default_domain: Option<String>,
    /// Controls how the `path` field is set in source and sink entries.
    pub path_mode: PathMode,
    /// Runtime the manifest targets. `Image` records local `config_uri` / `profile_uri`
    /// absolute under the container work-root (`/work`), restoring the portable pre-0.6.6
    /// form; `Cli` keeps them relative / as-typed for cross-checkout reproducibility.
    pub runtime_env: crate::manifest::RuntimeEnv,
}

#[derive(Debug)]
struct ResolvedOrRaw {
    storage: String,
    uri: String,
    resolved: bool,
}

pub fn build_common_manifest_json(
    config_location: &ConfigLocation,
    config: &RootConfig,
    selected_entities: &[String],
    profile: Option<&ProfileConfig>,
    options: &ManifestOptions,
) -> FloeResult<String> {
    let resolver = StorageResolver::new(config, config_location.base.clone())?;
    let selected_entities = config.select_entities(selected_entities)?;
    let mut manifest = build_common_manifest(
        config_location,
        config,
        selected_entities,
        &resolver,
        profile,
        options,
    );

    // Compute manifest_revision: SHA-256 of the canonical content, which
    // excludes volatile fields (generated_at_ts_ms, manifest_revision itself).
    let revision = compute_manifest_revision(&manifest)?;
    manifest.manifest_revision = Some(revision);

    Ok(serde_json::to_string_pretty(&manifest)?)
}

/// Compute a stable content hash over the manifest, excluding fields that
/// change on every generation (timestamp) or are themselves the hash output.
fn compute_manifest_revision(manifest: &CommonManifest) -> FloeResult<String> {
    let mut value: serde_json::Value = serde_json::to_value(manifest)?;
    if let Some(obj) = value.as_object_mut() {
        obj.remove("generated_at_ts_ms");
        obj.remove("manifest_revision");
    }
    let canonical = serde_json::to_string(&value)?;
    Ok(sha256_hex(canonical.as_bytes()))
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn build_common_manifest(
    config_location: &ConfigLocation,
    config: &RootConfig,
    mut entities: Vec<&EntityConfig>,
    resolver: &StorageResolver,
    profile: Option<&ProfileConfig>,
    options: &ManifestOptions,
) -> CommonManifest {
    entities.sort_by(|left, right| left.id.cmp(&right.id));

    let report_path = config
        .report
        .as_ref()
        .map(|report| report.path.as_str())
        .unwrap_or("report");
    let report_storage = config
        .report
        .as_ref()
        .and_then(|report| report.storage.as_deref());
    let report_base = resolve_or_raw(
        resolver,
        "__manifest__",
        "report.path",
        report_storage,
        report_path,
    );

    let mut manifest_entities = Vec::with_capacity(entities.len());
    for entity in entities {
        let source = resolve_or_raw(
            resolver,
            &entity.name,
            "source.path",
            entity.source.storage.as_deref(),
            &entity.source.path,
        );
        // A MotherDuck DuckDB sink has no filesystem path: resolving the empty
        // `sink.accepted.path` would either error (remote config -> uri="") or record
        // the config directory (local config) instead of the `md:<database>` target.
        // Record the connection string as the accepted URI so orchestrators reading
        // the manifest see the real sink location, mirroring run resolution.
        let accepted = match motherduck_connection(&entity.sink.accepted) {
            Some(connection) => ResolvedOrRaw {
                storage: "motherduck".to_string(),
                uri: connection,
                resolved: true,
            },
            None => resolve_or_raw(
                resolver,
                &entity.name,
                "sink.accepted.path",
                entity.sink.accepted.storage.as_deref(),
                &entity.sink.accepted.path,
            ),
        };
        let rejected = entity.sink.rejected.as_ref().map(|target| {
            resolve_or_raw(
                resolver,
                &entity.name,
                "sink.rejected.path",
                target.storage.as_deref(),
                &target.path,
            )
        });
        let archive = entity.sink.archive.as_ref().map(|target| {
            resolve_or_raw(
                resolver,
                &entity.name,
                "sink.archive.path",
                target.storage.as_deref(),
                &target.path,
            )
        });

        let effective_domain = entity.domain.as_ref().or(options.default_domain.as_ref());
        let (asset_key, group_name, entity_domain) = if let Some(domain) = effective_domain {
            (
                vec![domain.clone(), entity.name.clone()],
                domain.clone(),
                Some(domain.clone()),
            )
        } else {
            (
                vec!["default".to_string(), entity.name.clone()],
                "default".to_string(),
                None,
            )
        };

        let mut tags = BTreeMap::new();
        if let Some(metadata) = &entity.metadata {
            if let Some(owner) = &metadata.owner {
                tags.insert("owner".to_string(), owner.clone());
            }
            if let Some(product) = &metadata.data_product {
                tags.insert("data_product".to_string(), product.clone());
            }
            if let Some(domain_tag) = &metadata.domain {
                tags.insert("domain".to_string(), domain_tag.clone());
            }
        }
        let tags = if tags.is_empty() { None } else { Some(tags) };

        let schema = ManifestEntitySchema {
            columns: entity
                .schema
                .columns
                .iter()
                .map(|c| ManifestColumnDef {
                    name: c.name.clone(),
                    column_type: c.column_type.clone(),
                    source: c.source.clone(),
                    nullable: c.nullable,
                    unique: c.unique,
                    width: c.width,
                    trim: c.trim,
                })
                .collect(),
            primary_key: entity.schema.primary_key.clone().unwrap_or_default(),
            unique_keys: entity.schema.unique_keys.clone().unwrap_or_default(),
            normalize_columns: entity
                .schema
                .normalize_columns
                .as_ref()
                .and_then(|v| serde_json::to_value(v).ok()),
            mismatch: entity
                .schema
                .mismatch
                .as_ref()
                .and_then(|v| serde_json::to_value(v).ok()),
            schema_evolution: entity
                .schema
                .schema_evolution
                .as_ref()
                .and_then(|v| serde_json::to_value(v).ok()),
        };

        let pii = entity.pii.as_ref().and_then(redact_pii_for_manifest);

        let source_path = if options.path_mode == PathMode::ResolvedUri && source.resolved {
            resolved_uri_to_path(&source.uri)
        } else {
            entity.source.path.clone()
        };
        let accepted_path = if let Some(connection) = motherduck_connection(&entity.sink.accepted) {
            // The MotherDuck connection string is the meaningful target identity in
            // both path modes (there is no filesystem path to fall back on).
            connection
        } else if options.path_mode == PathMode::ResolvedUri && accepted.resolved {
            resolved_uri_to_path(&accepted.uri)
        } else {
            entity.sink.accepted.path.clone()
        };

        manifest_entities.push(ManifestEntity {
            name: entity.id.clone(),
            domain: entity_domain,
            group_name,
            asset_key,
            source_format: entity.source.format.clone(),
            accepted_sink_uri: accepted.uri.clone(),
            rejected_sink_uri: rejected.as_ref().map(|value| value.uri.clone()),
            tags,
            source: ManifestSource {
                format: entity.source.format.clone(),
                storage: source.storage,
                uri: source.uri,
                path: source_path,
                resolved: source.resolved,
                cast_mode: entity.source.cast_mode.clone(),
                options: map_source_options(entity.source.options.as_ref()),
            },
            sinks: ManifestSinks {
                accepted: ManifestSinkTarget {
                    format: entity.sink.accepted.format.clone(),
                    storage: accepted.storage,
                    uri: accepted.uri,
                    path: accepted_path,
                    resolved: accepted.resolved,
                    options: entity
                        .sink
                        .accepted
                        .options
                        .as_ref()
                        .and_then(|v| serde_json::to_value(v).ok()),
                    partition_by: entity.sink.accepted.partition_by.clone(),
                    merge: entity
                        .sink
                        .accepted
                        .merge
                        .as_ref()
                        .and_then(|v| serde_json::to_value(v).ok()),
                    iceberg: entity
                        .sink
                        .accepted
                        .iceberg
                        .as_ref()
                        .and_then(|v| serde_json::to_value(v).ok()),
                    delta: entity
                        .sink
                        .accepted
                        .delta
                        .as_ref()
                        .and_then(|v| serde_json::to_value(v).ok()),
                    duckdb: redact_duckdb_for_manifest(entity.sink.accepted.duckdb.as_ref()),
                },
                rejected: rejected.map(|value| {
                    let rej = entity.sink.rejected.as_ref();
                    let rej_raw_path = rej.map(|t| t.path.clone()).unwrap_or_default();
                    let rej_path = if options.path_mode == PathMode::ResolvedUri && value.resolved {
                        resolved_uri_to_path(&value.uri)
                    } else {
                        rej_raw_path
                    };
                    ManifestSinkTarget {
                        format: rej
                            .map(|t| t.format.clone())
                            .unwrap_or_else(|| "csv".to_string()),
                        storage: value.storage,
                        uri: value.uri,
                        path: rej_path,
                        resolved: value.resolved,
                        options: rej
                            .and_then(|t| t.options.as_ref())
                            .and_then(|v| serde_json::to_value(v).ok()),
                        partition_by: rej.and_then(|t| t.partition_by.clone()),
                        merge: rej
                            .and_then(|t| t.merge.as_ref())
                            .and_then(|v| serde_json::to_value(v).ok()),
                        iceberg: rej
                            .and_then(|t| t.iceberg.as_ref())
                            .and_then(|v| serde_json::to_value(v).ok()),
                        delta: rej
                            .and_then(|t| t.delta.as_ref())
                            .and_then(|v| serde_json::to_value(v).ok()),
                        duckdb: redact_duckdb_for_manifest(rej.and_then(|t| t.duckdb.as_ref())),
                    }
                }),
                archive: archive.map(|value| {
                    let arc_raw_path = entity
                        .sink
                        .archive
                        .as_ref()
                        .map(|target| target.path.clone())
                        .unwrap_or_default();
                    let arc_path = if options.path_mode == PathMode::ResolvedUri && value.resolved {
                        resolved_uri_to_path(&value.uri)
                    } else {
                        arc_raw_path
                    };
                    ManifestArchiveTarget {
                        storage: value.storage,
                        uri: value.uri,
                        path: arc_path,
                        resolved: value.resolved,
                    }
                }),
            },
            runner: None,
            policy_severity: entity.policy.severity.as_str().to_string(),
            write_mode: entity.sink.write_mode.as_str().to_string(),
            incremental_mode: entity.incremental_mode.as_str().to_string(),
            schema,
            pii,
            state_path: entity.state.as_ref().and_then(|s| s.path.clone()),
        });
    }

    // A manifest is replayed by a different runner than the one that generated it, so
    // `config_uri` must be absolute. `local_uri_for_env` absolutizes the local path against
    // the target runtime's work-root: `Image` rebases the relative path under the container
    // work-root (`local:///work/...`, portable + reproducible across containers), `Cli`
    // records the host-canonical path. Remote config URIs pass through unchanged.
    let config_uri = crate::manifest::local_uri_for_env(
        &config_location.uri,
        &config_location.path,
        options.runtime_env,
    );
    let config_checksum = std::fs::read(&config_location.path)
        .ok()
        .map(|b| sha256_hex(&b));

    let profile_checksum = options
        .profile_path
        .as_ref()
        .and_then(|p| std::fs::read(p).ok())
        .map(|b| sha256_hex(&b));

    let generated_at_ts_ms = if options.deterministic {
        0
    } else {
        now_ts_ms()
    };

    // Serialize the effective config sections (config-level definitions already merged with
    // the profile via `apply_profile_*`, and `{{VAR}}` placeholders resolved) as opaque JSON
    // values so the manifest is self-contained and can be replayed by `floe run --manifest`
    // without re-supplying the config or profile.
    let storages = config
        .storages
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let catalogs = config
        .catalogs
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let lineage = config
        .lineage
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());

    CommonManifest {
        schema: "floe.manifest.v1",
        generated_at_ts_ms,
        floe_version: env!("CARGO_PKG_VERSION"),
        spec_version: config.version.clone(),
        manifest_name: options.manifest_name.clone(),
        manifest_id: build_manifest_id(&config_uri, config_checksum.as_deref()),
        manifest_revision: None,
        runtime_env: options.runtime_env.as_str(),
        work_root: options.runtime_env.manifest_work_root(),
        config_uri,
        config_checksum,
        profile_uri: options.profile_uri.clone(),
        profile_checksum,
        report_base_uri: report_base.uri,
        domains: config
            .domains
            .iter()
            .map(|domain| ManifestDomain {
                name: domain.name.clone(),
                incoming_dir: domain
                    .resolved_incoming_dir
                    .clone()
                    .or_else(|| domain.incoming_dir.clone()),
            })
            .collect(),
        execution: default_execution_contract(options, profile),
        runners: runners_contract(profile),
        entities: manifest_entities,
        storages,
        catalogs,
        lineage,
    }
}

fn resolve_or_raw(
    resolver: &StorageResolver,
    entity_name: &str,
    field: &str,
    storage_name: Option<&str>,
    raw_path: &str,
) -> ResolvedOrRaw {
    match resolver.resolve_path(entity_name, field, storage_name, raw_path) {
        Ok(resolved) => ResolvedOrRaw {
            storage: resolved.storage,
            uri: resolved.uri,
            resolved: true,
        },
        Err(_) => ResolvedOrRaw {
            storage: storage_name.unwrap_or("local").to_string(),
            uri: raw_path.to_string(),
            resolved: false,
        },
    }
}

/// If the accepted sink is a MotherDuck DuckDB target, return its (trimmed)
/// `md:<database>` connection string. Returns `None` for every other sink, so the
/// normal filesystem path-resolution flow applies.
/// Serialize the PII config for the manifest with any literal HMAC key redacted.
/// Only exact `${ENV_VAR}` references are preserved (the secret lives in the
/// environment, not in the manifest); literal keys are dropped so they are never
/// written to persisted or shared manifest JSON.
fn redact_pii_for_manifest(pii: &crate::config::PiiConfig) -> Option<serde_json::Value> {
    let mut sanitized = pii.clone();
    for col in &mut sanitized.columns {
        col.key = match col.key.take() {
            Some(key) if is_exact_env_placeholder(&key) => Some(key),
            _ => None,
        };
    }
    serde_json::to_value(&sanitized).ok()
}

/// Serialize a DuckDB sink config for the manifest with the MotherDuck token
/// redacted. Manifests are orchestration/replay artifacts that may be persisted
/// and shared, so a literal secret token must never be written. A `${ENV}`
/// reference is non-secret and is preserved verbatim (replay re-expands it at
/// connect time); any other (literal) token is dropped.
fn redact_duckdb_for_manifest(
    duckdb: Option<&crate::config::DuckDbSinkTargetConfig>,
) -> Option<serde_json::Value> {
    let cfg = duckdb?;
    let mut sanitized = cfg.clone();
    sanitized.token = match sanitized.token {
        Some(token) if is_exact_env_placeholder(token.expose()) => Some(token),
        _ => None,
    };
    serde_json::to_value(&sanitized).ok()
}

/// True only for a token that is exactly one `${VAR}` env reference — the sole
/// non-secret form `expand_env_token` accepts. Anything else (a literal, or a
/// mixed/malformed value like `tok-${ENV}`) carries potential secret material
/// and must be redacted rather than written to the manifest.
fn is_exact_env_placeholder(token: &str) -> bool {
    let Some(inner) = token.strip_prefix("${").and_then(|s| s.strip_suffix('}')) else {
        return false;
    };
    !inner.is_empty() && !inner.contains('{') && !inner.contains('}')
}

fn motherduck_connection(accepted: &crate::config::SinkTarget) -> Option<String> {
    if accepted.format != "duckdb" {
        return None;
    }
    let connection = accepted.duckdb.as_ref()?.connection.as_deref()?.trim();
    if crate::config::is_motherduck_connection(connection) {
        Some(connection.to_string())
    } else {
        None
    }
}

/// Convert a resolved URI to the path value used in manifest source/sink entries.
/// Remote URIs (s3://, gs://, abfs://) are kept as-is.
/// Local URIs (local:///abs/path) have the scheme stripped so the StorageResolver
/// receives a plain filesystem path rather than an unrecognised `local://` prefix.
fn resolved_uri_to_path(uri: &str) -> String {
    if let Some(path) = uri.strip_prefix("local://") {
        path.to_string()
    } else {
        uri.to_string()
    }
}

fn build_manifest_id(config_uri: &str, config_checksum: Option<&str>) -> String {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET_BASIS;
    hash = fnv1a_update(hash, config_uri.as_bytes(), FNV_PRIME);
    hash = fnv1a_update(hash, &[0], FNV_PRIME);
    hash = fnv1a_update(hash, config_checksum.unwrap_or("").as_bytes(), FNV_PRIME);

    format!("mfv1-{hash:016x}")
}

fn fnv1a_update(mut hash: u64, bytes: &[u8], prime: u64) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(prime);
    }
    hash
}

fn map_source_options(options: Option<&SourceOptions>) -> Option<serde_json::Value> {
    let options = options?;
    let mut map = serde_json::Map::new();
    map.insert("header".to_string(), serde_json::json!(options.header));
    map.insert(
        "separator".to_string(),
        serde_json::json!(options.separator),
    );
    map.insert("encoding".to_string(), serde_json::json!(options.encoding));
    map.insert(
        "null_values".to_string(),
        serde_json::json!(options.null_values),
    );
    map.insert(
        "recursive".to_string(),
        serde_json::json!(options.recursive),
    );
    map.insert("glob".to_string(), serde_json::json!(options.glob));
    map.insert(
        "json_mode".to_string(),
        serde_json::json!(options.json_mode),
    );
    map.insert("sheet".to_string(), serde_json::json!(options.sheet));
    map.insert(
        "header_row".to_string(),
        serde_json::json!(options.header_row),
    );
    map.insert("data_row".to_string(), serde_json::json!(options.data_row));
    map.insert("row_tag".to_string(), serde_json::json!(options.row_tag));
    map.insert(
        "namespace".to_string(),
        serde_json::json!(options.namespace),
    );
    map.insert(
        "value_tag".to_string(),
        serde_json::json!(options.value_tag),
    );
    Some(serde_json::Value::Object(map))
}

fn default_execution_contract(
    options: &ManifestOptions,
    profile: Option<&ProfileConfig>,
) -> ManifestExecution {
    let mut exit_codes = BTreeMap::new();
    exit_codes.insert("0", "success_or_rejected");
    exit_codes.insert("1", "technical_failure");
    exit_codes.insert("2", "aborted");

    const PLACEHOLDER: &str = "{manifest_uri}";
    let base_args = [
        "run",
        "--manifest",
        PLACEHOLDER,
        "--log-format",
        "json",
        "--quiet",
    ]
    .iter()
    .map(|&a| {
        if a == PLACEHOLDER {
            options
                .manifest_uri
                .as_deref()
                .unwrap_or(PLACEHOLDER)
                .to_string()
        } else {
            a.to_string()
        }
    })
    .collect();

    let orchestration = profile
        .and_then(|p| p.execution.as_ref())
        .and_then(|e| e.orchestration.as_ref())
        .map(|o| ManifestOrchestration {
            max_concurrent_entities: o.max_concurrent_entities,
            strategy: o.strategy.clone(),
        });

    ManifestExecution {
        entrypoint: "floe",
        base_args,
        per_entity_args: vec!["--entities".to_string(), "{entity_name}".to_string()],
        log_format: "json",
        result_contract: ManifestResultContract {
            run_finished_event: true,
            summary_uri_field: "summary_uri",
            exit_codes,
        },
        defaults: ManifestExecutionDefaults {
            env: BTreeMap::new(),
            workdir: None,
        },
        orchestration,
    }
}

fn runners_contract(profile: Option<&ProfileConfig>) -> ManifestRunners {
    let profile_runner_type = profile
        .and_then(|p| p.execution.as_ref())
        .map(|e| e.runner.runner_type.as_str());

    match profile_runner_type {
        Some("kubernetes_job") => {
            let profile_runner = profile
                .and_then(|p| p.execution.as_ref())
                .map(|e| &e.runner);
            let mut definitions = BTreeMap::new();
            definitions.insert(
                "default",
                ManifestRunnerDefinition {
                    runner_type: "kubernetes_job",
                    command: profile_runner.and_then(|r| r.command.clone()),
                    args: profile_runner.and_then(|r| r.args.clone()),
                    timeout_seconds: profile_runner.and_then(|r| r.timeout_seconds),
                    ttl_seconds_after_finished: profile_runner
                        .and_then(|r| r.ttl_seconds_after_finished),
                    poll_interval_seconds: profile_runner.and_then(|r| r.poll_interval_seconds),
                    secrets: profile_runner.and_then(|r| {
                        r.secrets.as_ref().map(|secrets| {
                            secrets
                                .iter()
                                .map(|s| ManifestRunnerSecret {
                                    name: s.name.clone(),
                                    secret_name: s.secret_name.clone(),
                                    key: s.key.clone(),
                                })
                                .collect()
                        })
                    }),
                    image: profile_runner.and_then(|r| r.image.clone()),
                    namespace: profile_runner.and_then(|r| r.namespace.clone()),
                    service_account: profile_runner.and_then(|r| r.service_account.clone()),
                    resources: profile_runner.and_then(|r| {
                        r.resources.as_ref().map(|res| ManifestRunnerResources {
                            cpu: res.cpu.clone(),
                            memory_mb: res.memory_mb,
                        })
                    }),
                    env: profile_runner.and_then(|r| {
                        r.env
                            .as_ref()
                            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                    }),
                    workspace_url: None,
                    existing_cluster_id: None,
                    config_uri: None,
                    python_file_uri: None,
                    job_name: None,
                    auth: None,
                    env_parameters: None,
                },
            );
            ManifestRunners {
                default: "default",
                definitions,
            }
        }
        Some("databricks_job") => {
            let profile_runner = profile
                .and_then(|p| p.execution.as_ref())
                .map(|e| &e.runner);
            let mut definitions = BTreeMap::new();
            definitions.insert(
                "default",
                ManifestRunnerDefinition {
                    runner_type: "databricks_job",
                    command: profile_runner.and_then(|r| r.command.clone()),
                    args: profile_runner.and_then(|r| r.args.clone()),
                    timeout_seconds: profile_runner.and_then(|r| r.timeout_seconds),
                    ttl_seconds_after_finished: None,
                    poll_interval_seconds: profile_runner.and_then(|r| r.poll_interval_seconds),
                    secrets: None,
                    image: None,
                    namespace: None,
                    service_account: None,
                    resources: None,
                    env: None,
                    workspace_url: profile_runner.and_then(|r| r.workspace_url.clone()),
                    existing_cluster_id: profile_runner.and_then(|r| r.existing_cluster_id.clone()),
                    config_uri: profile_runner.and_then(|r| r.config_uri.clone()),
                    python_file_uri: profile_runner.and_then(|r| r.python_file_uri.clone()),
                    job_name: profile_runner
                        .and_then(|r| r.job_name.clone())
                        .or_else(|| Some("floe-{domain}-{env}".to_string())),
                    auth: profile_runner.and_then(|r| {
                        r.auth.as_ref().map(|auth| ManifestRunnerAuth {
                            service_principal_oauth_ref: auth.service_principal_oauth_ref.clone(),
                        })
                    }),
                    env_parameters: profile_runner.and_then(|r| {
                        r.env_parameters
                            .as_ref()
                            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                    }),
                },
            );
            ManifestRunners {
                default: "default",
                definitions,
            }
        }
        // "local" or absent → local_process (backward-compatible default)
        _ => {
            let mut definitions = BTreeMap::new();
            definitions.insert(
                "local",
                ManifestRunnerDefinition {
                    runner_type: "local_process",
                    command: None,
                    args: None,
                    timeout_seconds: None,
                    ttl_seconds_after_finished: None,
                    poll_interval_seconds: None,
                    secrets: None,
                    image: None,
                    namespace: None,
                    service_account: None,
                    resources: None,
                    env: None,
                    workspace_url: None,
                    existing_cluster_id: None,
                    config_uri: None,
                    python_file_uri: None,
                    job_name: None,
                    auth: None,
                    env_parameters: None,
                },
            );
            ManifestRunners {
                default: "local",
                definitions,
            }
        }
    }
}

fn now_ts_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}
