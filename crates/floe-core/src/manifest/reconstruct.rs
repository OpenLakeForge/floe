use crate::errors::FloeError;
use serde::Deserialize;

use crate::config::{
    ArchiveTarget, CatalogsConfig, ColumnConfig, EntityConfig, EntityStateConfig, IncrementalMode,
    LineageConfig, MergeOptionsConfig, PiiConfig, PolicyConfig, PolicySeverity, SchemaConfig,
    SchemaMismatchConfig, SinkConfig, SinkOptions, SinkTarget, SourceConfig, SourceOptions,
    StoragesConfig, WriteMode,
};
use crate::FloeResult;

// Minimal deserializable mirror of CommonManifest — only the fields needed to reconstruct
// a RootConfig and run an entity.
#[derive(Debug, Deserialize)]
pub struct ManifestForRun {
    pub spec_version: String,
    pub report_base_uri: String,
    pub entities: Vec<ManifestEntityForRun>,
    pub storages: Option<serde_json::Value>,
    pub catalogs: Option<serde_json::Value>,
    pub lineage: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct ManifestEntityForRun {
    pub name: String,
    pub domain: Option<String>,
    pub source: ManifestSourceForRun,
    pub sinks: ManifestSinksForRun,
    pub policy_severity: Option<String>,
    pub write_mode: Option<String>,
    pub incremental_mode: Option<String>,
    pub schema: ManifestEntitySchemaForRun,
    pub pii: Option<serde_json::Value>,
    pub state_path: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ManifestSourceForRun {
    pub format: String,
    pub storage: String,
    pub uri: String,
    pub path: String,
    pub cast_mode: Option<String>,
    pub options: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct ManifestSinksForRun {
    pub accepted: ManifestSinkTargetForRun,
    pub rejected: Option<ManifestSinkTargetForRun>,
    pub archive: Option<ManifestArchiveTargetForRun>,
}

#[derive(Debug, Deserialize)]
pub struct ManifestSinkTargetForRun {
    pub format: String,
    pub storage: String,
    /// Resolved cloud URI (e.g. `s3://bucket/key`). Optional so hand-authored
    /// manifests that omit it still parse; the generator always writes it.
    #[serde(default)]
    pub uri: Option<String>,
    pub path: String,
    pub options: Option<serde_json::Value>,
    pub partition_by: Option<Vec<String>>,
    pub merge: Option<serde_json::Value>,
    pub iceberg: Option<serde_json::Value>,
    pub delta: Option<serde_json::Value>,
    pub duckdb: Option<serde_json::Value>,
    pub write_mode: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ManifestArchiveTargetForRun {
    pub storage: String,
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct ManifestEntitySchemaForRun {
    pub columns: Vec<ManifestColumnDefForRun>,
    pub primary_key: Vec<String>,
    pub unique_keys: Vec<Vec<String>>,
    pub normalize_columns: Option<serde_json::Value>,
    pub mismatch: Option<serde_json::Value>,
    pub schema_evolution: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct ManifestColumnDefForRun {
    pub name: String,
    pub column_type: String,
    pub source: Option<String>,
    pub nullable: Option<bool>,
    pub unique: Option<bool>,
    pub width: Option<u64>,
    pub trim: Option<bool>,
}

/// Deserialize an optional embedded manifest section (storages/catalogs/lineage),
/// returning a clear error if the block is present but malformed rather than silently
/// discarding it.
fn deserialize_manifest_section<T: serde::de::DeserializeOwned>(
    value: Option<&serde_json::Value>,
    section: &str,
) -> FloeResult<Option<T>> {
    match value {
        Some(v) => serde_json::from_value::<T>(v.clone()).map(Some).map_err(
            |err| -> Box<dyn std::error::Error + Send + Sync> {
                FloeError::config(format!("manifest {section} block is malformed: {err}")).into()
            },
        ),
        None => Ok(None),
    }
}

/// The replay runtime hints recorded by the generator. Deserialized on their own (serde
/// ignores the rest of the manifest) so a runner can read them without depending on the full
/// `ManifestForRun` shape parsing cleanly.
#[derive(Debug, Default, Deserialize)]
struct ManifestRuntimeHints {
    #[serde(default)]
    runtime_env: Option<String>,
    #[serde(default)]
    work_root: Option<String>,
}

/// Parse the replay runtime hints (`runtime_env` / `work_root`) from a manifest JSON string.
/// Missing or malformed hints map to `(RuntimeEnv::Cli, None)`, so pre-0.6.7 manifests keep
/// their previous resolution behavior.
pub fn manifest_runtime(json: &str) -> (crate::manifest::RuntimeEnv, Option<String>) {
    let hints: ManifestRuntimeHints = serde_json::from_str(json).unwrap_or_default();
    (
        crate::manifest::RuntimeEnv::from_manifest_str(hints.runtime_env.as_deref()),
        hints.work_root,
    )
}

/// Parse a manifest JSON string and reconstruct a minimal RootConfig.
/// Returns (config, report_base_uri).
pub fn config_from_manifest_json(json: &str) -> FloeResult<(crate::config::RootConfig, String)> {
    let manifest: ManifestForRun =
        serde_json::from_str(json).map_err(|err| -> Box<dyn std::error::Error + Send + Sync> {
            FloeError::config(format!("manifest parse error: {err}")).into()
        })?;

    // Deserialize the embedded sections, surfacing malformed blocks as a clear error instead
    // of silently dropping them (a swallowed error here previously surfaced downstream as the
    // confusing "no storages block" failure when a hand-edited manifest had a slightly-off
    // storages shape).
    let storages =
        deserialize_manifest_section::<StoragesConfig>(manifest.storages.as_ref(), "storages")?;
    let catalogs =
        deserialize_manifest_section::<CatalogsConfig>(manifest.catalogs.as_ref(), "catalogs")?;
    let lineage =
        deserialize_manifest_section::<LineageConfig>(manifest.lineage.as_ref(), "lineage")?;

    let entities = manifest
        .entities
        .iter()
        .map(entity_from_manifest)
        .collect::<FloeResult<Vec<_>>>()?;

    let config = crate::config::RootConfig {
        version: manifest.spec_version,
        metadata: None,
        storages,
        catalogs,
        env: None,
        domains: Vec::new(),
        report: None,
        lineage,
        entities,
    };

    Ok((config, manifest.report_base_uri))
}

/// Reconstruct the lineage config and entities for a `--manifest` replay's
/// OpenLineage observer, with each source/sink dataset path resolved to the
/// manifest's cloud `uri`.
///
/// The run reconstruction (`config_from_manifest_json`) intentionally keeps the
/// raw `path` fields — in the default manifest path mode those hold the original
/// config paths (e.g. `sales/orders`) while the resolved cloud identity lives in
/// `uri` (e.g. `s3://bucket/sales/orders`). The observer derives a dataset's
/// namespace/name from the path, so for lineage it must see the resolved cloud
/// URI; otherwise a named S3/GCS/ABFS source is reported under the `file`
/// namespace. Local and relative paths are left as reconstructed.
///
/// Returns `Ok(None)` when the manifest has no `lineage` block.
pub fn lineage_inputs_from_manifest_json(
    json: &str,
) -> FloeResult<Option<(LineageConfig, Vec<EntityConfig>)>> {
    let manifest: ManifestForRun =
        serde_json::from_str(json).map_err(|err| -> Box<dyn std::error::Error + Send + Sync> {
            FloeError::config(format!("manifest parse error: {err}")).into()
        })?;

    let lineage = match deserialize_manifest_section::<LineageConfig>(
        manifest.lineage.as_ref(),
        "lineage",
    )? {
        Some(lineage) => lineage,
        None => return Ok(None),
    };

    let mut entities = manifest
        .entities
        .iter()
        .map(entity_from_manifest)
        .collect::<FloeResult<Vec<_>>>()?;

    // Reconstructed entities are in manifest order, so zip pairs each with its
    // source manifest entry to overlay the resolved cloud URIs.
    for (entity, m) in entities.iter_mut().zip(manifest.entities.iter()) {
        overlay_cloud_uri(&mut entity.source.path, Some(m.source.uri.as_str()));
        overlay_cloud_uri(
            &mut entity.sink.accepted.path,
            m.sinks.accepted.uri.as_deref(),
        );
        if let (Some(rejected), Some(m_rejected)) =
            (entity.sink.rejected.as_mut(), m.sinks.rejected.as_ref())
        {
            overlay_cloud_uri(&mut rejected.path, m_rejected.uri.as_deref());
        }
    }

    Ok(Some((lineage, entities)))
}

/// Replace `path` with `uri` when `uri` names a cloud object store; other
/// schemes (local / relative) keep the reconstructed path. The prefix list
/// mirrors `lineage::split_storage_uri` and must stay in sync with it.
fn overlay_cloud_uri(path: &mut String, uri: Option<&str>) {
    const CLOUD_SCHEMES: [&str; 6] = ["s3://", "gs://", "gcs://", "az://", "abfss://", "abfs://"];
    if let Some(uri) = uri {
        if CLOUD_SCHEMES.iter().any(|scheme| uri.starts_with(scheme)) {
            *path = uri.to_string();
        }
    }
}

fn entity_from_manifest(m: &ManifestEntityForRun) -> FloeResult<EntityConfig> {
    let policy_severity = parse_policy_severity(m.policy_severity.as_deref().unwrap_or("warn"));
    let write_mode = parse_write_mode(m.write_mode.as_deref().unwrap_or("overwrite"));
    let incremental_mode = parse_incremental_mode(m.incremental_mode.as_deref().unwrap_or("none"));

    let source_options: Option<SourceOptions> = m
        .source
        .options
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok());

    let source = SourceConfig {
        format: m.source.format.clone(),
        path: m.source.path.clone(),
        storage: if m.source.storage == "local" {
            None
        } else {
            Some(m.source.storage.clone())
        },
        options: source_options,
        cast_mode: m.source.cast_mode.clone(),
    };

    let accepted = sink_target_from_manifest(&m.sinks.accepted, write_mode);
    let rejected = m
        .sinks
        .rejected
        .as_ref()
        .map(|t| sink_target_from_manifest(t, write_mode));
    let archive = m.sinks.archive.as_ref().map(|a| ArchiveTarget {
        path: a.path.clone(),
        storage: if a.storage == "local" {
            None
        } else {
            Some(a.storage.clone())
        },
    });

    let schema = schema_from_manifest(&m.schema)?;
    let pii: Option<PiiConfig> = m
        .pii
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok());

    let state = m.state_path.as_ref().map(|p| EntityStateConfig {
        path: Some(p.clone()),
    });

    Ok(EntityConfig {
        name: m.name.clone(),
        metadata: None,
        domain: m.domain.clone(),
        incremental_mode,
        state,
        source,
        sink: SinkConfig {
            write_mode,
            accepted,
            rejected,
            archive,
        },
        policy: PolicyConfig {
            severity: policy_severity,
        },
        schema,
        pii,
    })
}

fn sink_target_from_manifest(
    m: &ManifestSinkTargetForRun,
    default_write_mode: WriteMode,
) -> SinkTarget {
    let write_mode = m
        .write_mode
        .as_deref()
        .map(parse_write_mode)
        .unwrap_or(default_write_mode);
    let options: Option<SinkOptions> = m
        .options
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok());
    let merge: Option<MergeOptionsConfig> = m
        .merge
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok());
    let iceberg = m
        .iceberg
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok());
    let delta = m
        .delta
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok());
    let duckdb = m
        .duckdb
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok());

    // `"motherduck"` is a synthetic placeholder the manifest builder records for
    // MotherDuck DuckDB sinks (which live over the network and bind no filesystem
    // storage). Like `"local"`, it must reconstruct to an unset `storage`: a MotherDuck
    // target's real location is its `duckdb.connection`, and `validate_duckdb_sink`
    // rejects MotherDuck sinks that carry an explicit `sink.accepted.storage`. Gate this
    // on the sink actually being a MotherDuck DuckDB target so a real, user-defined
    // storage definition that happens to be named "motherduck" on a non-DuckDB sink is
    // preserved verbatim rather than silently dropped.
    let is_motherduck_placeholder = m.storage == "motherduck"
        && m.format == "duckdb"
        && duckdb
            .as_ref()
            .and_then(|cfg: &crate::config::DuckDbSinkTargetConfig| cfg.connection.as_deref())
            .map(|connection| crate::config::is_motherduck_connection(connection.trim()))
            .unwrap_or(false);

    SinkTarget {
        format: m.format.clone(),
        path: m.path.clone(),
        storage: if m.storage == "local" || is_motherduck_placeholder {
            None
        } else {
            Some(m.storage.clone())
        },
        options,
        merge,
        iceberg,
        delta,
        duckdb,
        partition_by: m.partition_by.clone(),
        partition_spec: None,
        write_mode,
    }
}

fn schema_from_manifest(m: &ManifestEntitySchemaForRun) -> FloeResult<SchemaConfig> {
    let columns = m
        .columns
        .iter()
        .map(|c| ColumnConfig {
            name: c.name.clone(),
            source: c.source.clone(),
            column_type: c.column_type.clone(),
            nullable: c.nullable,
            unique: c.unique,
            width: c.width,
            trim: c.trim,
        })
        .collect();

    let normalize_columns: Option<crate::config::NormalizeColumnsConfig> = m
        .normalize_columns
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok());
    let mismatch: Option<SchemaMismatchConfig> = m
        .mismatch
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok());
    let schema_evolution = m
        .schema_evolution
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok());

    Ok(SchemaConfig {
        columns,
        normalize_columns,
        mismatch,
        schema_evolution,
        primary_key: if m.primary_key.is_empty() {
            None
        } else {
            Some(m.primary_key.clone())
        },
        unique_keys: if m.unique_keys.is_empty() {
            None
        } else {
            Some(m.unique_keys.clone())
        },
    })
}

fn parse_policy_severity(s: &str) -> PolicySeverity {
    match s {
        "reject" => PolicySeverity::Reject,
        "abort" => PolicySeverity::Abort,
        _ => PolicySeverity::Warn,
    }
}

fn parse_write_mode(s: &str) -> WriteMode {
    match s {
        "append" => WriteMode::Append,
        "merge_scd1" => WriteMode::MergeScd1,
        "merge_scd2" => WriteMode::MergeScd2,
        _ => WriteMode::Overwrite,
    }
}

fn parse_incremental_mode(s: &str) -> IncrementalMode {
    match s {
        "archive" => IncrementalMode::Archive,
        "file" => IncrementalMode::File,
        "row" => IncrementalMode::Row,
        _ => IncrementalMode::None,
    }
}
