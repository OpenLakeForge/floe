use crate::errors::FloeError;
use std::collections::HashSet;

use crate::config::storage::is_remote_uri;
use crate::config::{
    CatalogDefinition, CatalogTypeConfig, EntityConfig, IncrementalMode, PolicySeverity,
    RootConfig, SourceOptions, StorageDefinition,
};
use crate::io::format;
use crate::io::read::json_selector::parse_selector;
use crate::io::read::xml_selector;
use crate::io::write::sink_format::sink_format;
use crate::{warnings, FloeResult};

const ALLOWED_COLUMN_TYPES: &[&str] = &["string", "number", "boolean", "datetime", "date", "time"];
const ALLOWED_CAST_MODES: &[&str] = &["strict", "coerce"];
const ALLOWED_NORMALIZE_STRATEGIES: &[&str] = &["snake_case", "lower", "camel_case", "none"];
const ALLOWED_MISSING_POLICIES: &[&str] = &["reject_file", "fill_nulls"];
const ALLOWED_EXTRA_POLICIES: &[&str] = &["reject_file", "ignore"];
const ALLOWED_STORAGE_TYPES: &[&str] = &["local", "s3", "adls", "gcs"];
const ALLOWED_ICEBERG_PARTITION_TRANSFORMS: &[&str] = &["identity", "year", "month", "day", "hour"];
const MIN_SUPPORTED_CONFIG_VERSION: ConfigVersion = ConfigVersion::new(0, 1);
const MIN_SCHEMA_EVOLUTION_CONFIG_VERSION: ConfigVersion = ConfigVersion::new(0, 2);
const MAX_JSON_COLUMNS: usize = 1024;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct ConfigVersion {
    major: u64,
    minor: u64,
}

impl ConfigVersion {
    pub(crate) const fn new(major: u64, minor: u64) -> Self {
        Self { major, minor }
    }

    pub(crate) fn parse(raw: &str) -> FloeResult<Self> {
        let (major, minor) = raw.split_once('.').ok_or_else(|| {
            FloeError::config(format!(
                "root.version={raw} is invalid; expected numeric major.minor format like \"0.1\""
            ))
        })?;

        if minor.contains('.') {
            return Err(FloeError::config(format!(
                "root.version={raw} is invalid; expected numeric major.minor format like \"0.1\""
            ))
            .into());
        }

        let major = major.parse::<u64>().map_err(|_| {
            FloeError::config(format!(
                "root.version={raw} is invalid; expected numeric major.minor format like \"0.1\""
            ))
        })?;
        let minor = minor.parse::<u64>().map_err(|_| {
            FloeError::config(format!(
                "root.version={raw} is invalid; expected numeric major.minor format like \"0.1\""
            ))
        })?;

        Ok(Self { major, minor })
    }
}

pub(crate) fn validate_config(config: &RootConfig) -> FloeResult<()> {
    let config_version = validate_version(config)?;

    if config.entities.is_empty() {
        return Err(FloeError::config(
            "entities list is empty (at least one entity is required)".to_string(),
        )
        .into());
    }

    let storage_registry = StorageRegistry::new(config)?;
    let catalog_registry = CatalogRegistry::new(config, &storage_registry)?;
    if let Some(report) = &config.report {
        validate_report(report, &storage_registry)?;
    }
    if let Some(lineage) = &config.lineage {
        validate_lineage(lineage)?;
    }

    let mut names = HashSet::new();
    for entity in &config.entities {
        validate_entity(entity, config_version, &storage_registry, &catalog_registry)?;
        if !names.insert(entity.name.as_str()) {
            return Err(FloeError::config(format!(
                "entity.name={} is duplicated in config",
                entity.name
            ))
            .into());
        }
    }

    Ok(())
}

fn validate_lineage(lineage: &crate::config::LineageConfig) -> FloeResult<()> {
    if lineage.url.trim().is_empty() {
        return Err(FloeError::config("lineage.url must not be empty".to_string()).into());
    }
    if lineage.namespace.trim().is_empty() {
        return Err(FloeError::config("lineage.namespace must not be empty".to_string()).into());
    }
    if lineage.max_failures == Some(0) {
        return Err(
            FloeError::config("lineage.max_failures must be at least 1".to_string()).into(),
        );
    }
    Ok(())
}

fn validate_version(config: &RootConfig) -> FloeResult<ConfigVersion> {
    let version = ConfigVersion::parse(&config.version)?;
    if version < MIN_SUPPORTED_CONFIG_VERSION {
        return Err(FloeError::config(format!(
            "root.version={} is unsupported (minimum supported version: {}.{})",
            config.version, MIN_SUPPORTED_CONFIG_VERSION.major, MIN_SUPPORTED_CONFIG_VERSION.minor
        ))
        .into());
    }
    Ok(version)
}

fn validate_report(
    report: &crate::config::ReportConfig,
    storages: &StorageRegistry,
) -> FloeResult<()> {
    let storage_name = storages.resolve_report_name(report.storage.as_deref())?;
    storages.validate_report_reference("report.storage", &storage_name)?;
    if storages.definition_type(&storage_name) == Some("local") && is_remote_uri(&report.path) {
        return Err(FloeError::config(format!(
            "report.path must be a local path (got {})",
            report.path
        ))
        .into());
    }
    Ok(())
}

fn validate_entity(
    entity: &EntityConfig,
    config_version: ConfigVersion,
    storages: &StorageRegistry,
    catalogs: &CatalogRegistry,
) -> FloeResult<()> {
    validate_source(entity, storages)?;
    validate_state(entity)?;
    validate_sink(entity, storages, catalogs)?;
    validate_schema(entity, config_version)?;
    if let Some(pii) = &entity.pii {
        validate_pii(entity, pii)?;
    }
    Ok(())
}

fn validate_pii(entity: &EntityConfig, pii: &crate::config::PiiConfig) -> FloeResult<()> {
    use crate::config::PiiStrategy;
    // Abort severity writes the raw input file to the rejected sink without
    // loading a DataFrame, bypassing masking entirely.
    if entity.policy.severity == PolicySeverity::Abort {
        return Err(FloeError::config(format!(
            "entity.name={} pii: masking is not applied when policy.severity=abort \
             because the raw file is written to sink.rejected without DataFrame processing",
            entity.name
        ))
        .into());
    }
    // sink.archive copies/moves the original source file after processing.
    // The archive always contains the raw unmasked input regardless of PII config.
    if entity.sink.archive.is_some() {
        return Err(FloeError::config(format!(
            "entity.name={} pii: sink.archive copies the original unmasked source file \
             to the archive sink; remove sink.archive or disable pii masking",
            entity.name
        ))
        .into());
    }
    // schema.mismatch reject_file writes the raw unmasked file to sink.rejected
    // in the precheck phase, before any DataFrame processing or PII masking.
    if let Some(mismatch) = &entity.schema.mismatch {
        if mismatch.missing_columns.as_deref() == Some("reject_file") {
            return Err(FloeError::config(format!(
                "entity.name={} pii: schema.mismatch.missing_columns=reject_file writes the \
                 raw unmasked file to sink.rejected before PII masking can be applied; \
                 use missing_columns=fill_nulls instead",
                entity.name
            ))
            .into());
        }
        if mismatch.extra_columns.as_deref() == Some("reject_file") {
            return Err(FloeError::config(format!(
                "entity.name={} pii: schema.mismatch.extra_columns=reject_file writes the \
                 raw unmasked file to sink.rejected before PII masking can be applied; \
                 use extra_columns=ignore instead",
                entity.name
            ))
            .into());
        }
    }
    let accepted_format = entity.sink.accepted.format.as_str();
    let schema_col_map: std::collections::HashMap<&str, &crate::config::ColumnConfig> = entity
        .schema
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c))
        .collect();
    let primary_keys: std::collections::HashSet<&str> = entity
        .schema
        .primary_key
        .iter()
        .flatten()
        .map(|s| s.as_str())
        .collect();
    let write_mode = entity.sink.write_mode;
    let is_merge_mode = matches!(
        write_mode,
        crate::config::WriteMode::MergeScd1 | crate::config::WriteMode::MergeScd2
    );
    // Collect every column that participates in any uniqueness constraint.
    // Strategies that collapse values (nullify, redact, mask, drop) violate
    // uniqueness when applied to these columns; hash is allowed because
    // SHA-256 of distinct values are distinct.
    let mut unique_key_cols: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for k in entity.schema.primary_key.iter().flatten() {
        unique_key_cols.insert(k.as_str());
    }
    if let Some(groups) = &entity.schema.unique_keys {
        for group in groups {
            for col_name in group {
                unique_key_cols.insert(col_name.as_str());
            }
        }
    }
    // Legacy columns[].unique=true is ignored at runtime when schema.unique_keys
    // is set (validate_schema_unique_keys warns about this). Only honour it here
    // when unique_keys is absent so we don't reject PII on columns whose uniqueness
    // constraint no longer exists.
    if entity.schema.unique_keys.is_none() {
        for schema_col in &entity.schema.columns {
            if schema_col.unique == Some(true) {
                unique_key_cols.insert(schema_col.name.as_str());
            }
        }
    }
    let mut seen = std::collections::HashSet::new();
    for col in &pii.columns {
        if !seen.insert(col.name.as_str()) {
            return Err(FloeError::config(format!(
                "entity.name={} pii.columns: column name {} is duplicated",
                entity.name, col.name
            ))
            .into());
        }
        if !schema_col_map.contains_key(col.name.as_str()) {
            return Err(FloeError::config(format!(
                "entity.name={} pii.columns: {} is not an unknown schema column",
                entity.name, col.name
            ))
            .into());
        }
        if col.strategy == PiiStrategy::Tokenize {
            return Err(FloeError::config(format!(
                "entity.name={} pii.columns[name={}].strategy=tokenize is not yet supported",
                entity.name, col.name
            ))
            .into());
        }
        // hash, redact, and mask cast the column to String internally; applying
        // them to a non-string column changes the runtime type in the output,
        // breaking schema-aware sinks (Parquet, Delta, Iceberg).
        if matches!(
            col.strategy,
            PiiStrategy::Hash | PiiStrategy::Redact | PiiStrategy::Mask
        ) {
            if let Some(col_cfg) = schema_col_map.get(col.name.as_str()) {
                if canonical_column_type(&col_cfg.column_type) != Some("string") {
                    return Err(FloeError::config(format!(
                        "entity.name={} pii.columns[name={}].strategy={:?} can only be applied \
                         to string columns (declared type={}); use strategy=nullify or strategy=drop instead",
                        entity.name, col.name, col.strategy, col_cfg.column_type
                    )).into());
                }
            }
        }
        if col.strategy == PiiStrategy::Nullify {
            if let Some(col_cfg) = schema_col_map.get(col.name.as_str()) {
                if col_cfg.nullable == Some(false) {
                    return Err(FloeError::config(format!(
                        "entity.name={} pii.columns[name={}].strategy=nullify cannot be applied to a nullable=false column",
                        entity.name, col.name
                    )).into());
                }
            }
        }
        if col.strategy == PiiStrategy::Drop && primary_keys.contains(col.name.as_str()) {
            return Err(FloeError::config(format!(
                "entity.name={} pii.columns[name={}].strategy=drop cannot be applied to a primary_key column",
                entity.name, col.name
            )).into());
        }
        if primary_keys.contains(col.name.as_str()) && is_merge_mode {
            return Err(FloeError::config(format!(
                "entity.name={} pii.columns[name={}]: PII strategy cannot be applied to a \
                 primary_key column with write_mode={}: masking the merge key corrupts the merge predicate",
                entity.name, col.name, write_mode.as_str()
            )).into());
        }
        let collapses_values = matches!(
            col.strategy,
            PiiStrategy::Drop | PiiStrategy::Nullify | PiiStrategy::Redact | PiiStrategy::Mask
        );
        if collapses_values && unique_key_cols.contains(col.name.as_str()) {
            return Err(FloeError::config(format!(
                "entity.name={} pii.columns[name={}].strategy={:?} cannot be applied to a \
                 unique-key column: the strategy collapses or removes values, violating \
                 uniqueness constraints",
                entity.name, col.name, col.strategy
            ))
            .into());
        }
        // With write_mode=append, the unique tracker is seeded from previously-written
        // Duplicate checks run before apply_pii_masking and record raw column
        // values in UniqueTracker samples, which are copied into the run report.
        // Hashing the column after the fact does not protect raw PII that was
        // already captured in those samples. Reject hash on any unique-key column.
        if col.strategy == PiiStrategy::Hash && unique_key_cols.contains(col.name.as_str()) {
            return Err(FloeError::config(format!(
                "entity.name={} pii.columns[name={}].strategy=hash cannot be applied to a \
                 unique-key column: uniqueness checks record raw values in the run report \
                 before masking, leaking unmasked PII",
                entity.name, col.name
            ))
            .into());
        }
        if col.strategy == PiiStrategy::Hash && col.key.is_none() {
            eprintln!(
                "warn: entity.name={} pii.columns[name={}].strategy=hash has no key: \
                 unsalted SHA-256 is reversible for low-entropy identifiers (email, phone, ID); \
                 set `key:` to a secret value or `${{ENV_VAR}}` reference to use HMAC-SHA256",
                entity.name, col.name
            );
        }
        if col.strategy == PiiStrategy::Drop
            && (accepted_format == "iceberg" || accepted_format == "delta")
        {
            return Err(FloeError::config(format!(
                "entity.name={} pii.columns[name={}].strategy=drop is not supported with \
                 format={accepted_format} because the sink enforces the full declared schema",
                entity.name, col.name
            ))
            .into());
        }
        if col.strategy == PiiStrategy::Mask {
            let pattern = col.mask_pattern.as_deref().ok_or_else(|| {
                FloeError::config(format!(
                    "entity.name={} pii.columns[name={}].strategy=mask requires mask_pattern",
                    entity.name, col.name
                ))
            })?;
            let has_last = extract_last_n(pattern).is_some();
            let has_first = extract_first_n(pattern).is_some();
            if !has_last && !has_first {
                return Err(FloeError::config(format!(
                    "entity.name={} pii.columns[name={}].mask_pattern must contain at least one token ({{firstN}} or {{lastN}})",
                    entity.name, col.name
                )).into());
            }
        }
    }
    Ok(())
}

pub(crate) fn extract_last_n(pattern: &str) -> Option<usize> {
    let start = pattern.find("{last")?;
    let rest = &pattern[start + 5..];
    let end = rest.find('}')?;
    rest[..end].parse::<usize>().ok().filter(|&n| n > 0)
}

pub(crate) fn extract_first_n(pattern: &str) -> Option<usize> {
    let start = pattern.find("{first")?;
    let rest = &pattern[start + 6..];
    let end = rest.find('}')?;
    rest[..end].parse::<usize>().ok().filter(|&n| n > 0)
}

fn validate_state(entity: &EntityConfig) -> FloeResult<()> {
    if let Some(path) = entity
        .state
        .as_ref()
        .and_then(|state| state.path.as_deref())
        .map(str::trim)
    {
        if path.is_empty() {
            return Err(FloeError::config(format!(
                "entity.name={} entity.state.path must not be empty",
                entity.name
            ))
            .into());
        }
    }

    if entity.incremental_mode == IncrementalMode::Archive && entity.sink.archive.is_none() {
        return Err(FloeError::config(format!(
            "entity.name={} incremental_mode=archive requires sink.archive",
            entity.name
        ))
        .into());
    }

    if entity.sink.archive.is_some() {
        match entity.incremental_mode {
            IncrementalMode::None => warnings::emit(
                "validate",
                Some(&entity.name),
                None,
                Some("incremental_archive_legacy"),
                &format!(
                    "entity.name={} sink.archive without incremental_mode is deprecated; treating it as incremental_mode=archive for backward compatibility",
                    entity.name
                ),
            ),
            IncrementalMode::File | IncrementalMode::Row => {
                return Err(FloeError::config(format!(
                    "entity.name={} sink.archive conflicts with incremental_mode={}; use incremental_mode=archive or remove sink.archive",
                    entity.name,
                    entity.incremental_mode.as_str()
                )).into());
            }
            IncrementalMode::Archive => {}
        }
    }

    Ok(())
}

fn validate_source(entity: &EntityConfig, storages: &StorageRegistry) -> FloeResult<()> {
    format::ensure_input_format(&entity.name, entity.source.format.as_str())?;

    if let Some(cast_mode) = &entity.source.cast_mode {
        if !ALLOWED_CAST_MODES.contains(&cast_mode.as_str()) {
            return Err(FloeError::config(format!(
                "entity.name={} source.cast_mode={} is unsupported (allowed: {})",
                entity.name,
                cast_mode,
                ALLOWED_CAST_MODES.join(", ")
            ))
            .into());
        }
    }

    let storage_name =
        storages.resolve_name(entity, "source.storage", entity.source.storage.as_deref())?;
    storages.validate_reference(entity, "source.storage", &storage_name)?;
    if entity.source.format == "json" {
        let options = entity.source.options.as_ref();
        let mode = options
            .and_then(|options| options.json_mode.as_deref())
            .unwrap_or("array");
        match mode {
            "array" | "ndjson" => {}
            _ => {
                return Err(FloeError::config(format!(
                    "entity.name={} source.options.json_mode={} is unsupported (allowed: array, ndjson)",
                    entity.name, mode
                )).into());
            }
        }
    }
    if entity.source.format == "xlsx" {
        let default_options = SourceOptions::default();
        let options = entity.source.options.as_ref().unwrap_or(&default_options);
        if let Some(sheet) = options.sheet.as_ref() {
            if sheet.trim().is_empty() {
                return Err(FloeError::config(format!(
                    "entity.name={} source.options.sheet must not be empty",
                    entity.name
                ))
                .into());
            }
        }
        let header_row = options.header_row.unwrap_or(1);
        if header_row == 0 {
            return Err(FloeError::config(format!(
                "entity.name={} source.options.header_row must be greater than 0",
                entity.name
            ))
            .into());
        }
        let data_row = options.data_row.unwrap_or(header_row + 1);
        if data_row <= header_row {
            return Err(FloeError::config(format!(
                "entity.name={} source.options.data_row must be greater than source.options.header_row",
                entity.name
            )).into());
        }
    }
    if entity.source.format == "xml" {
        let options = entity.source.options.as_ref();
        let row_tag = options
            .and_then(|options| options.row_tag.as_deref())
            .map(|value| value.trim())
            .filter(|value| !value.is_empty());
        if row_tag.is_none() {
            return Err(FloeError::config(format!(
                "entity.name={} source.options.row_tag is required for xml input",
                entity.name
            ))
            .into());
        }
        if let Some(namespace) = options
            .and_then(|options| options.namespace.as_deref())
            .map(|value| value.trim())
        {
            if namespace.is_empty() {
                return Err(FloeError::config(format!(
                    "entity.name={} source.options.namespace must not be empty",
                    entity.name
                ))
                .into());
            }
        }
        if let Some(value_tag) = options
            .and_then(|options| options.value_tag.as_deref())
            .map(|value| value.trim())
        {
            if value_tag.is_empty() {
                return Err(FloeError::config(format!(
                    "entity.name={} source.options.value_tag must not be empty",
                    entity.name
                ))
                .into());
            }
        }
    }

    let _ = storages.definition_type(&storage_name);

    Ok(())
}

fn validate_sink(
    entity: &EntityConfig,
    storages: &StorageRegistry,
    catalogs: &CatalogRegistry,
) -> FloeResult<()> {
    format::ensure_accepted_sink_format(&entity.name, entity.sink.accepted.format.as_str())?;
    format::validate_sink_options(
        &entity.name,
        entity.sink.accepted.format.as_str(),
        entity.sink.accepted.options.as_ref(),
    )?;

    if entity.policy.severity == PolicySeverity::Reject && entity.sink.rejected.is_none() {
        return Err(FloeError::config(format!(
            "entity.name={} sink.rejected is required when policy.severity=reject",
            entity.name
        ))
        .into());
    }

    if let Some(rejected) = &entity.sink.rejected {
        format::ensure_rejected_sink_format(&entity.name, rejected.format.as_str())?;
    }

    let source_storage =
        storages.resolve_name(entity, "source.storage", entity.source.storage.as_deref())?;
    let accepted_storage = storages.resolve_name(
        entity,
        "sink.accepted.storage",
        entity.sink.accepted.storage.as_deref(),
    )?;
    storages.validate_reference(entity, "sink.accepted.storage", &accepted_storage)?;
    validate_duckdb_sink(entity, storages, &accepted_storage)?;
    // MotherDuck DuckDB targets carry no filesystem storage (they live over the
    // network), so the generic storage-capability check does not apply to them.
    if !is_duckdb_motherduck(entity) {
        if let Some(storage_type) = storages.definition_type(&accepted_storage) {
            // A feature-gated sink that isn't compiled into this build returns Err
            // here; skip the storage-capability check (it cannot be introspected) and
            // let the runtime write path enforce the feature. Compiled sinks still
            // validate fully.
            if let Ok(fmt) = sink_format(entity.sink.accepted.format.as_str()) {
                if !fmt.supported_storages().contains(&storage_type) {
                    let supported = fmt.supported_storages().join(", ");
                    return Err(FloeError::config(format!(
                        "entity.name={} sink.accepted.format={} is not supported on {} storage (supported: {})",
                        entity.name, entity.sink.accepted.format, storage_type, supported
                    )).into());
                }
            }
        }
    }
    validate_iceberg_catalog_binding(entity, storages, catalogs, &accepted_storage)?;
    validate_delta_catalog_binding(entity, storages, catalogs, &accepted_storage)?;

    let _ = storages.definition_type(&accepted_storage);

    validate_sink_partitioning(entity)?;
    validate_sink_write_mode(entity)?;

    if let Some(rejected) = &entity.sink.rejected {
        let rejected_storage =
            storages.resolve_name(entity, "sink.rejected.storage", rejected.storage.as_deref())?;
        storages.validate_reference(entity, "sink.rejected.storage", &rejected_storage)?;
    }

    if let Some(archive) = &entity.sink.archive {
        let archive_storage = archive.storage.as_deref().unwrap_or(&source_storage);
        if let Some(storage) = archive.storage.as_deref() {
            if storage != source_storage {
                return Err(FloeError::config(format!(
                    "entity.name={} sink.archive.storage must match source.storage ({})",
                    entity.name, source_storage
                ))
                .into());
            }
        }
        storages.validate_reference(entity, "sink.archive.storage", archive_storage)?;
    }

    Ok(())
}

fn validate_sink_write_mode(entity: &EntityConfig) -> FloeResult<()> {
    let write_mode = entity.sink.write_mode;
    // A feature-gated sink not compiled into this build cannot report its supported
    // write modes; skip the capability check (deferred to the runtime write path)
    // while still validating the format-agnostic merge requirements below.
    if let Ok(fmt) = sink_format(entity.sink.accepted.format.as_str()) {
        if !fmt.supported_modes().contains(&write_mode) {
            return Err(FloeError::config(format!(
                "entity.name={} sink.write_mode={} is not supported by sink.accepted.format={}",
                entity.name,
                write_mode.as_str(),
                entity.sink.accepted.format
            ))
            .into());
        }
    }

    let is_merge_mode = matches!(
        write_mode,
        crate::config::WriteMode::MergeScd1 | crate::config::WriteMode::MergeScd2
    );
    if is_merge_mode {
        let mode_name = write_mode.as_str();
        let primary_key = entity.schema.primary_key.as_ref().ok_or_else(|| {
            FloeError::config(format!(
                "entity.name={} sink.write_mode={} requires schema.primary_key",
                entity.name, mode_name
            ))
        })?;
        if primary_key.is_empty() {
            return Err(FloeError::config(format!(
                "entity.name={} sink.write_mode={} requires non-empty schema.primary_key",
                entity.name, mode_name
            ))
            .into());
        }
    }

    validate_merge_options(entity, write_mode)
}

/// True if this entity's accepted sink is a MotherDuck DuckDB target (a DuckDB
/// sink whose `duckdb.connection` is an `md:` connection string).
fn is_duckdb_motherduck(entity: &EntityConfig) -> bool {
    if entity.sink.accepted.format.as_str() != "duckdb" {
        return false;
    }
    entity
        .sink
        .accepted
        .duckdb
        .as_ref()
        .and_then(|cfg| cfg.connection.as_deref())
        .map(|connection| connection.trim_start().starts_with("md:"))
        .unwrap_or(false)
}

/// DuckDB-specific config validation: the `duckdb` block is required, the target
/// table must be named, MotherDuck and a filesystem storage are mutually
/// exclusive, and object-store file paths are rejected with a MotherDuck hint.
fn validate_duckdb_sink(
    entity: &EntityConfig,
    storages: &StorageRegistry,
    accepted_storage: &str,
) -> FloeResult<()> {
    if entity.sink.accepted.format.as_str() != "duckdb" {
        return Ok(());
    }
    let cfg = entity.sink.accepted.duckdb.as_ref().ok_or_else(|| {
        FloeError::config(format!(
            "entity.name={} sink.accepted.format=duckdb requires a sink.accepted.duckdb block",
            entity.name
        ))
    })?;
    if cfg.table.trim().is_empty() {
        return Err(FloeError::config(format!(
            "entity.name={} sink.accepted.duckdb.table must not be empty",
            entity.name
        ))
        .into());
    }

    match cfg.connection.as_deref().map(str::trim) {
        // A present-but-blank connection is ambiguous: it isn't a MotherDuck target,
        // but its mere presence may already have made `path` optional during parsing
        // (and a non-`md:` connection fails at runtime). Reject it explicitly rather
        // than silently falling through to the local-file branch.
        Some("") => {
            return Err(FloeError::config(format!(
                "entity.name={} sink.accepted.duckdb.connection must not be blank; omit it for a \
                 local-file target or set a MotherDuck connection string (md:<database>)",
                entity.name
            ))
            .into());
        }
        Some(connection) => {
            if !connection.starts_with("md:") {
                return Err(FloeError::config(format!(
                    "entity.name={} sink.accepted.duckdb.connection={connection:?} is unsupported; \
                     only MotherDuck connection strings (md:<database>) are accepted for remote DuckDB writes",
                    entity.name
                )).into());
            }
            // MotherDuck lives over the network and must not bind a filesystem storage.
            if entity.sink.accepted.storage.is_some() {
                return Err(FloeError::config(format!(
                    "entity.name={} sink.accepted.storage must be unset for a MotherDuck DuckDB target \
                     (duckdb.connection={connection:?})",
                    entity.name
                )).into());
            }
        }
        _ => {
            // Local file target: DuckDB cannot read-write a database file over object
            // storage, so reject non-local storage with a pointer to MotherDuck.
            if let Some(storage_type) = storages.definition_type(accepted_storage) {
                if storage_type != "local" {
                    return Err(FloeError::config(format!(
                        "entity.name={} sink.accepted.format=duckdb cannot write a database file to {storage_type} \
                         object storage; DuckDB only supports read-write on local files. Use a MotherDuck target \
                         (sink.accepted.duckdb.connection: md:<database>) for remote writes",
                        entity.name
                    )).into());
                }
            }
        }
    }

    Ok(())
}

fn validate_merge_options(
    entity: &EntityConfig,
    write_mode: crate::config::WriteMode,
) -> FloeResult<()> {
    let Some(merge) = entity.sink.accepted.merge.as_ref() else {
        return Ok(());
    };

    // For a feature-gated sink absent from this build we cannot introspect merge
    // support; skip the capability check (the runtime write path enforces the
    // feature) and validate the format-agnostic merge constraints below. Compiled
    // sinks are still checked.
    if let Ok(fmt) = sink_format(entity.sink.accepted.format.as_str()) {
        let supports_merge = fmt.supported_modes().iter().any(|m| {
            matches!(
                m,
                crate::config::WriteMode::MergeScd1 | crate::config::WriteMode::MergeScd2
            )
        });
        if !supports_merge {
            return Err(FloeError::config(format!(
                "entity.name={} sink.accepted.merge is only supported when sink.accepted.format supports merge (e.g. delta)",
                entity.name
            )).into());
        }
    }
    if !matches!(
        write_mode,
        crate::config::WriteMode::MergeScd1 | crate::config::WriteMode::MergeScd2
    ) {
        return Err(FloeError::config(format!(
            "entity.name={} sink.accepted.merge is only supported with sink.write_mode=merge_scd1 or merge_scd2",
            entity.name
        )).into());
    }

    let schema_columns = entity
        .schema
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<HashSet<_>>();
    let normalize_strategy = if entity
        .schema
        .normalize_columns
        .as_ref()
        .and_then(|normalize| normalize.enabled)
        .unwrap_or(false)
    {
        entity
            .schema
            .normalize_columns
            .as_ref()
            .and_then(|normalize| normalize.strategy.as_deref())
            .or(Some("snake_case"))
    } else {
        None
    };
    let resolved_output_columns = crate::checks::normalize::resolve_output_columns(
        &entity.schema.columns,
        normalize_strategy,
    );
    let resolved_output_column_names = resolved_output_columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<HashSet<_>>();
    let primary_key_columns = entity
        .schema
        .primary_key
        .as_ref()
        .map(|columns| {
            columns
                .iter()
                .map(|column| column.trim().to_string())
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();

    if let Some(ignore_columns) = merge.ignore_columns.as_ref() {
        validate_merge_column_list(
            entity,
            "sink.accepted.merge.ignore_columns",
            ignore_columns,
            &schema_columns,
        )?;
        for (index, column_name) in ignore_columns.iter().enumerate() {
            let value = column_name.trim();
            if primary_key_columns.contains(value) {
                return Err(FloeError::config(format!(
                    "entity.name={} sink.accepted.merge.ignore_columns[{}]={} cannot reference schema.primary_key column",
                    entity.name, index, value
                )).into());
            }
        }
    }

    if let Some(compare_columns) = merge.compare_columns.as_ref() {
        validate_merge_column_list(
            entity,
            "sink.accepted.merge.compare_columns",
            compare_columns,
            &schema_columns,
        )?;
        for (index, column_name) in compare_columns.iter().enumerate() {
            let value = column_name.trim();
            if primary_key_columns.contains(value) {
                return Err(FloeError::config(format!(
                    "entity.name={} sink.accepted.merge.compare_columns[{}]={} cannot reference schema.primary_key column",
                    entity.name, index, value
                )).into());
            }
        }
    }

    if let Some(scd2) = merge.scd2.as_ref() {
        if write_mode != crate::config::WriteMode::MergeScd2 {
            return Err(FloeError::config(format!(
                "entity.name={} sink.accepted.merge.scd2 is only supported with sink.write_mode=merge_scd2",
                entity.name
            )).into());
        }
        let current_flag_column = scd2
            .current_flag_column
            .as_deref()
            .unwrap_or(crate::config::DEFAULT_SCD2_CURRENT_FLAG_COLUMN)
            .trim();
        let valid_from_column = scd2
            .valid_from_column
            .as_deref()
            .unwrap_or(crate::config::DEFAULT_SCD2_VALID_FROM_COLUMN)
            .trim();
        let valid_to_column = scd2
            .valid_to_column
            .as_deref()
            .unwrap_or(crate::config::DEFAULT_SCD2_VALID_TO_COLUMN)
            .trim();
        let resolved_columns = [
            ("current_flag_column", current_flag_column),
            ("valid_from_column", valid_from_column),
            ("valid_to_column", valid_to_column),
        ];
        for (field, value) in resolved_columns {
            if value.is_empty() {
                return Err(FloeError::config(format!(
                    "entity.name={} sink.accepted.merge.scd2.{} must not be empty",
                    entity.name, field
                ))
                .into());
            }
            if resolved_output_column_names.contains(value) {
                return Err(FloeError::config(format!(
                    "entity.name={} sink.accepted.merge.scd2.{}={} collides with schema column name",
                    entity.name, field, value
                )).into());
            }
        }
        let unique_columns = resolved_columns
            .iter()
            .map(|(_, value)| *value)
            .collect::<HashSet<_>>();
        if unique_columns.len() != resolved_columns.len() {
            return Err(FloeError::config(format!(
                "entity.name={} sink.accepted.merge.scd2 column names must be unique",
                entity.name
            ))
            .into());
        }
    }

    Ok(())
}

fn validate_merge_column_list(
    entity: &EntityConfig,
    field: &str,
    values: &[String],
    schema_columns: &HashSet<&str>,
) -> FloeResult<()> {
    let mut seen = HashSet::new();
    for (index, value) in values.iter().enumerate() {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(FloeError::config(format!(
                "entity.name={} {}[{}] must not be empty",
                entity.name, field, index
            ))
            .into());
        }
        if !schema_columns.contains(trimmed) {
            return Err(FloeError::config(format!(
                "entity.name={} {}[{}]={} references unknown schema column",
                entity.name, field, index, trimmed
            ))
            .into());
        }
        if !seen.insert(trimmed.to_string()) {
            return Err(FloeError::config(format!(
                "entity.name={} {} has duplicate column {}",
                entity.name, field, trimmed
            ))
            .into());
        }
    }
    Ok(())
}

fn validate_iceberg_catalog_binding(
    entity: &EntityConfig,
    storages: &StorageRegistry,
    catalogs: &CatalogRegistry,
    accepted_storage: &str,
) -> FloeResult<()> {
    let accepted = &entity.sink.accepted;
    if accepted.format != "iceberg" {
        if accepted.iceberg.is_some() {
            return Err(FloeError::config(format!(
                "entity.name={} sink.accepted.iceberg is only supported for sink.accepted.format=iceberg",
                entity.name
            )).into());
        }
        return Ok(());
    }

    let Some(iceberg_cfg) = accepted.iceberg.as_ref() else {
        return Ok(());
    };

    let catalog_name = if let Some(name) = iceberg_cfg.catalog.as_deref() {
        name.to_string()
    } else if let Some(name) = catalogs.default_name() {
        name.to_string()
    } else {
        return Err(FloeError::config(format!(
            "entity.name={} sink.accepted.iceberg.catalog is required (or set catalogs.default)",
            entity.name
        ))
        .into());
    };

    let definition = catalogs.definition(&catalog_name).ok_or_else(|| {
        FloeError::config(format!(
            "entity.name={} sink.accepted.iceberg.catalog references unknown catalog {}",
            entity.name, catalog_name
        ))
    })?;

    if matches!(&definition.type_config, CatalogTypeConfig::Unity { .. }) {
        return Err(FloeError::config(format!(
            "entity.name={} sink.accepted.iceberg.catalog={} has type=unity: unity catalogs only support Delta Lake, not Iceberg",
            entity.name, catalog_name
        )).into());
    }

    // REST catalogs with warehouse_storage use that storage as the actual table root, so
    // accepted_storage is not required to be cloud-backed. The warehouse_storage block below
    // enforces the s3/gcs constraint for REST independently.
    let rest_with_warehouse = matches!(&definition.type_config, CatalogTypeConfig::Rest { .. })
        && definition.warehouse_storage.is_some();
    if !rest_with_warehouse {
        let accepted_storage_type = storages
            .definition_type(accepted_storage)
            .unwrap_or("local");
        if accepted_storage_type != "s3" && accepted_storage_type != "gcs" {
            return Err(FloeError::config(format!(
                "entity.name={} sink.accepted.iceberg.catalog requires sink.accepted storage type s3 or gcs (got {})",
                entity.name, accepted_storage_type
            )).into());
        }
    }
    if let Some(storage_name) = definition.warehouse_storage.as_deref() {
        let storage_type = storages.definition_type(storage_name).ok_or_else(|| {
            FloeError::config(format!(
                "catalogs.definitions name={} warehouse_storage references unknown storage {}",
                definition.name, storage_name
            ))
        })?;
        match &definition.type_config {
            CatalogTypeConfig::Glue { .. } if storage_type != "s3" => {
                return Err(FloeError::config(format!(
                    "catalogs.definitions name={} warehouse_storage must reference s3 storage for glue catalog (got {})",
                    definition.name, storage_type
                )).into());
            }
            CatalogTypeConfig::Rest { .. } if storage_type != "s3" && storage_type != "gcs" => {
                return Err(FloeError::config(format!(
                    "catalogs.definitions name={} warehouse_storage must reference s3 or gcs storage for rest catalog (got {})",
                    definition.name, storage_type
                )).into());
            }
            _ => {}
        }
    }

    Ok(())
}

fn validate_delta_catalog_binding(
    entity: &EntityConfig,
    storages: &StorageRegistry,
    catalogs: &CatalogRegistry,
    accepted_storage: &str,
) -> FloeResult<()> {
    let accepted = &entity.sink.accepted;
    if accepted.format != "delta" {
        if accepted.delta.is_some() {
            return Err(FloeError::config(format!(
                "entity.name={} sink.accepted.delta is only supported for sink.accepted.format=delta",
                entity.name
            )).into());
        }
        return Ok(());
    }

    let Some(delta_cfg) = accepted.delta.as_ref() else {
        return Ok(());
    };

    let catalog_name = match delta_cfg.catalog.as_deref() {
        Some(name) => name.to_string(),
        None => match catalogs.default_name() {
            Some(name) => name.to_string(),
            None => {
                return Err(FloeError::config(format!(
                    "entity.name={} sink.accepted.delta.catalog is required when no catalogs.default is set",
                    entity.name
                )).into());
            }
        },
    };

    let definition = catalogs.definition(&catalog_name).ok_or_else(|| {
        FloeError::config(format!(
            "entity.name={} sink.accepted.delta.catalog references unknown catalog {}",
            entity.name, catalog_name
        ))
    })?;

    if !matches!(definition.type_config, CatalogTypeConfig::Unity { .. }) {
        return Err(FloeError::config(format!(
            "entity.name={} sink.accepted.delta.catalog={} has type={}: only unity catalogs support Delta Lake registration",
            entity.name, catalog_name, definition.type_config.catalog_type_str()
        )).into());
    }

    // Unity Catalog registration only makes sense for cloud-backed storage (not local).
    let storage_type = storages
        .definition_type(accepted_storage)
        .unwrap_or("local");
    if storage_type == "local" {
        return Err(FloeError::config(format!(
            "entity.name={} sink.accepted.delta.catalog={} requires cloud storage (s3, gcs, or adls), got local",
            entity.name, catalog_name
        )).into());
    }

    Ok(())
}

fn validate_sink_partitioning(entity: &EntityConfig) -> FloeResult<()> {
    let schema_columns = entity
        .schema
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<HashSet<_>>();
    let accepted = &entity.sink.accepted;

    if accepted.partition_by.is_some() && accepted.partition_spec.is_some() {
        return Err(FloeError::config(format!(
            "entity.name={} sink.accepted.partition_by and sink.accepted.partition_spec are mutually exclusive",
            entity.name
        )).into());
    }

    if let Some(partition_by) = &accepted.partition_by {
        if accepted.format != "delta" {
            return Err(FloeError::config(format!(
                "entity.name={} sink.accepted.partition_by is only supported for sink.accepted.format=delta",
                entity.name
            )).into());
        }
        if partition_by.is_empty() {
            return Err(FloeError::config(format!(
                "entity.name={} sink.accepted.partition_by must not be empty",
                entity.name
            ))
            .into());
        }
        let mut seen = HashSet::new();
        for (index, column) in partition_by.iter().enumerate() {
            let value = column.trim();
            if value.is_empty() {
                return Err(FloeError::config(format!(
                    "entity.name={} sink.accepted.partition_by[{}] must not be empty",
                    entity.name, index
                ))
                .into());
            }
            if !seen.insert(value.to_string()) {
                return Err(FloeError::config(format!(
                    "entity.name={} sink.accepted.partition_by has duplicate column {}",
                    entity.name, value
                ))
                .into());
            }
            if !schema_columns.contains(value) {
                return Err(FloeError::config(format!(
                    "entity.name={} sink.accepted.partition_by[{}]={} references unknown schema column",
                    entity.name, index, value
                )).into());
            }
        }
    }

    if let Some(partition_spec) = &accepted.partition_spec {
        if accepted.format != "iceberg" {
            return Err(FloeError::config(format!(
                "entity.name={} sink.accepted.partition_spec is only supported for sink.accepted.format=iceberg",
                entity.name
            )).into());
        }
        if partition_spec.is_empty() {
            return Err(FloeError::config(format!(
                "entity.name={} sink.accepted.partition_spec must not be empty",
                entity.name
            ))
            .into());
        }
        let mut seen = HashSet::new();
        for (index, field) in partition_spec.iter().enumerate() {
            let column = field.column.trim();
            if column.is_empty() {
                return Err(FloeError::config(format!(
                    "entity.name={} sink.accepted.partition_spec[{}].column must not be empty",
                    entity.name, index
                ))
                .into());
            }
            if !schema_columns.contains(column) {
                return Err(FloeError::config(format!(
                    "entity.name={} sink.accepted.partition_spec[{}].column={} references unknown schema column",
                    entity.name, index, column
                )).into());
            }
            let transform = field.transform.trim().to_ascii_lowercase();
            if !ALLOWED_ICEBERG_PARTITION_TRANSFORMS.contains(&transform.as_str()) {
                return Err(FloeError::config(format!(
                    "entity.name={} sink.accepted.partition_spec[{}].transform={} is unsupported (allowed: {})",
                    entity.name,
                    index,
                    field.transform,
                    ALLOWED_ICEBERG_PARTITION_TRANSFORMS.join(", ")
                )).into());
            }
            if !seen.insert((column.to_string(), transform.clone())) {
                return Err(FloeError::config(format!(
                    "entity.name={} sink.accepted.partition_spec has duplicate entry ({}, {})",
                    entity.name, column, transform
                ))
                .into());
            }
        }
    }

    Ok(())
}

fn validate_schema(entity: &EntityConfig, config_version: ConfigVersion) -> FloeResult<()> {
    if entity.source.format == "json" && entity.schema.columns.len() > MAX_JSON_COLUMNS {
        return Err(FloeError::config(format!(
            "entity.name={} schema.columns has {} entries which exceeds the JSON selector limit of {}",
            entity.name,
            entity.schema.columns.len(),
            MAX_JSON_COLUMNS
        )).into());
    }
    if entity.source.format == "fixed" {
        for (index, column) in entity.schema.columns.iter().enumerate() {
            let width = column.width.ok_or_else(|| {
                FloeError::config(format!(
                    "entity.name={} schema.columns[{}].width is required for source.format=fixed",
                    entity.name, index
                ))
            })?;
            if width == 0 {
                return Err(FloeError::config(format!(
                    "entity.name={} schema.columns[{}].width must be greater than 0",
                    entity.name, index
                ))
                .into());
            }
        }
    }
    if let Some(normalize) = &entity.schema.normalize_columns {
        if let Some(strategy) = &normalize.strategy {
            if !is_allowed_normalize_strategy(strategy) {
                return Err(FloeError::config(format!(
                    "entity.name={} schema.normalize_columns.strategy={} is unsupported (allowed: {})",
                    entity.name,
                    strategy,
                    ALLOWED_NORMALIZE_STRATEGIES.join(", ")
                )).into());
            }
        }
    }

    if let Some(mismatch) = &entity.schema.mismatch {
        if let Some(policy) = &mismatch.missing_columns {
            if !ALLOWED_MISSING_POLICIES.contains(&policy.as_str()) {
                return Err(FloeError::config(format!(
                    "entity.name={} schema.mismatch.missing_columns={} is unsupported (allowed: {})",
                    entity.name,
                    policy,
                    ALLOWED_MISSING_POLICIES.join(", ")
                )).into());
            }
        }
        if let Some(policy) = &mismatch.extra_columns {
            if !ALLOWED_EXTRA_POLICIES.contains(&policy.as_str()) {
                return Err(FloeError::config(format!(
                    "entity.name={} schema.mismatch.extra_columns={} is unsupported (allowed: {})",
                    entity.name,
                    policy,
                    ALLOWED_EXTRA_POLICIES.join(", ")
                ))
                .into());
            }
        }
    }

    for (index, column) in entity.schema.columns.iter().enumerate() {
        if canonical_column_type(&column.column_type).is_none() {
            return Err(FloeError::config(format!(
                "entity.name={} schema.columns[{}].type={} is unsupported for column {} (allowed: {})",
                entity.name,
                index,
                column.column_type,
                column.name,
                ALLOWED_COLUMN_TYPES.join(", ")
            )).into());
        }
        if entity.source.format == "json" {
            let selector = column.source_or_name();
            if let Err(err) = parse_selector(selector) {
                return Err(FloeError::config(format!(
                    "entity.name={} schema.columns[{}].source={} is invalid: {}",
                    entity.name, index, selector, err.message
                ))
                .into());
            }
        }
        if entity.source.format == "xml" {
            let selector = column.source_or_name();
            if let Err(err) = xml_selector::parse_selector(selector) {
                return Err(FloeError::config(format!(
                    "entity.name={} schema.columns[{}].source={} is invalid: {}",
                    entity.name, index, selector, err.message
                ))
                .into());
            }
        }
    }

    validate_schema_primary_key(entity)?;
    validate_schema_unique_keys(entity)?;
    validate_schema_evolution(entity, config_version)?;

    Ok(())
}

fn validate_schema_evolution(
    entity: &EntityConfig,
    config_version: ConfigVersion,
) -> FloeResult<()> {
    let Some(schema_evolution) = entity.schema.schema_evolution else {
        return Ok(());
    };

    if config_version < MIN_SCHEMA_EVOLUTION_CONFIG_VERSION {
        return Err(FloeError::config(format!(
            "entity.name={} schema.schema_evolution requires root.version >= \"0.2\"",
            entity.name,
        ))
        .into());
    }

    if entity.sink.accepted.format != "delta"
        && schema_evolution.mode == crate::config::SchemaEvolutionMode::AddColumns
    {
        return Err(FloeError::config(format!(
            "entity.name={} schema.schema_evolution.mode={} requires sink.accepted.format=delta (Delta-only additive schema evolution in this phase)",
            entity.name,
            schema_evolution.mode.as_str()
        )).into());
    }

    Ok(())
}

fn validate_schema_primary_key(entity: &EntityConfig) -> FloeResult<()> {
    let Some(primary_key) = entity.schema.primary_key.as_ref() else {
        return Ok(());
    };
    if primary_key.is_empty() {
        return Err(FloeError::config(format!(
            "entity.name={} schema.primary_key must not be empty",
            entity.name
        ))
        .into());
    }
    let mut seen = HashSet::new();
    for (index, column_name) in primary_key.iter().enumerate() {
        let value = column_name.trim();
        if value.is_empty() {
            return Err(FloeError::config(format!(
                "entity.name={} schema.primary_key[{}] must not be empty",
                entity.name, index
            ))
            .into());
        }
        if !seen.insert(value.to_string()) {
            return Err(FloeError::config(format!(
                "entity.name={} schema.primary_key has duplicate column {}",
                entity.name, value
            ))
            .into());
        }
        let column = entity
            .schema
            .columns
            .iter()
            .find(|column| column.name == value)
            .ok_or_else(|| {
                FloeError::config(format!(
                    "entity.name={} schema.primary_key[{}]={} references unknown schema column",
                    entity.name, index, value
                ))
            })?;
        if column.nullable == Some(true) {
            return Err(FloeError::config(format!(
                "entity.name={} schema.primary_key column {} cannot set nullable=true",
                entity.name, value
            ))
            .into());
        }
    }
    Ok(())
}

fn validate_schema_unique_keys(entity: &EntityConfig) -> FloeResult<()> {
    let Some(unique_keys) = entity.schema.unique_keys.as_ref() else {
        return Ok(());
    };
    if unique_keys.is_empty() {
        return Err(FloeError::config(format!(
            "entity.name={} schema.unique_keys must not be empty",
            entity.name
        ))
        .into());
    }

    let has_legacy_unique = entity
        .schema
        .columns
        .iter()
        .any(|column| column.unique == Some(true));
    if has_legacy_unique {
        warnings::emit(
            "validate",
            Some(&entity.name),
            None,
            Some("schema_unique_keys_override"),
            &format!(
                "entity.name={} schema.unique_keys is set; schema.columns[].unique is ignored",
                entity.name
            ),
        );
    }

    let schema_columns = entity
        .schema
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<HashSet<_>>();
    let mut seen_constraints = HashSet::new();
    let mut duplicate_constraints = 0_u64;
    for (index, key) in unique_keys.iter().enumerate() {
        if key.is_empty() {
            return Err(FloeError::config(format!(
                "entity.name={} schema.unique_keys[{}] must not be empty",
                entity.name, index
            ))
            .into());
        }
        let mut seen_columns = HashSet::new();
        let mut signature_parts = Vec::with_capacity(key.len());
        for (column_index, column_name) in key.iter().enumerate() {
            let value = column_name.trim();
            if value.is_empty() {
                return Err(FloeError::config(format!(
                    "entity.name={} schema.unique_keys[{}][{}] must not be empty",
                    entity.name, index, column_index
                ))
                .into());
            }
            if !schema_columns.contains(value) {
                return Err(FloeError::config(format!(
                    "entity.name={} schema.unique_keys[{}][{}]={} references unknown schema column",
                    entity.name, index, column_index, value
                ))
                .into());
            }
            if !seen_columns.insert(value.to_string()) {
                return Err(FloeError::config(format!(
                    "entity.name={} schema.unique_keys[{}] has duplicate column {}",
                    entity.name, index, value
                ))
                .into());
            }
            signature_parts.push(value.to_string());
        }
        let signature = signature_parts.join("\u{1f}");
        if !seen_constraints.insert(signature) {
            duplicate_constraints += 1;
        }
    }

    if duplicate_constraints > 0 {
        warnings::emit(
            "validate",
            Some(&entity.name),
            None,
            Some("schema_unique_keys_dedup"),
            &format!(
                "entity.name={} schema.unique_keys contains {} duplicated constraint(s); duplicates are ignored",
                entity.name, duplicate_constraints
            ),
        );
    }

    Ok(())
}

fn is_allowed_normalize_strategy(value: &str) -> bool {
    let normalized = normalize_value(value);
    ALLOWED_NORMALIZE_STRATEGIES
        .iter()
        .any(|allowed| normalize_value(allowed).as_str() == normalized)
}

fn canonical_column_type(value: &str) -> Option<&'static str> {
    let normalized = normalize_value(value);
    match normalized.as_str() {
        "string" | "str" | "text" => Some("string"),
        "number" | "float" | "float32" | "float64" | "double" | "decimal" | "int" | "int8"
        | "int16" | "int32" | "int64" | "integer" | "long" | "uint8" | "uint16" | "uint32"
        | "uint64" => Some("number"),
        "boolean" | "bool" => Some("boolean"),
        "datetime" | "timestamp" => Some("datetime"),
        "date" => Some("date"),
        "time" => Some("time"),
        _ => None,
    }
}

fn normalize_value(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace(['-', '_'], "")
}

struct StorageRegistry {
    has_config: bool,
    default_name: Option<String>,
    definitions: std::collections::HashMap<String, StorageDefinition>,
}

struct CatalogRegistry {
    has_config: bool,
    default_name: Option<String>,
    definitions: std::collections::HashMap<String, CatalogDefinition>,
}

impl CatalogRegistry {
    fn new(config: &RootConfig, storages: &StorageRegistry) -> FloeResult<Self> {
        let Some(catalogs) = &config.catalogs else {
            return Ok(Self {
                has_config: false,
                default_name: None,
                definitions: std::collections::HashMap::new(),
            });
        };

        if catalogs.definitions.is_empty() {
            return Err(
                FloeError::config("catalogs.definitions must not be empty".to_string()).into(),
            );
        }

        let mut definitions = std::collections::HashMap::new();
        for definition in &catalogs.definitions {
            // catalog type is validated at parse time via CatalogTypeConfig enum
            match &definition.type_config {
                CatalogTypeConfig::Glue { .. } => {
                    if let Some(storage_name) = definition.warehouse_storage.as_deref() {
                        let storage_type =
                            storages.definition_type(storage_name).ok_or_else(|| {
                                FloeError::config(format!(
                                    "catalogs.definitions name={} warehouse_storage references unknown storage {}",
                                    definition.name, storage_name
                                ))

                            })?;
                        if storage_type != "s3" {
                            return Err(FloeError::config(format!(
                                "catalogs.definitions name={} warehouse_storage must reference s3 storage for glue catalog (got {})",
                                definition.name, storage_type
                            )).into());
                        }
                    }
                }
                CatalogTypeConfig::Rest { .. } => {
                    if let Some(storage_name) = definition.warehouse_storage.as_deref() {
                        let storage_type =
                            storages.definition_type(storage_name).ok_or_else(|| {
                                FloeError::config(format!(
                                    "catalogs.definitions name={} warehouse_storage references unknown storage {}",
                                    definition.name, storage_name
                                ))

                            })?;
                        if storage_type != "s3" && storage_type != "gcs" {
                            return Err(FloeError::config(format!(
                                "catalogs.definitions name={} warehouse_storage must reference s3 or gcs storage for rest catalog (got {})",
                                definition.name, storage_type
                            )).into());
                        }
                    }
                }
                // Unity catalogs use the entity sink path as the storage location; no warehouse_storage.
                CatalogTypeConfig::Unity { .. } => {}
            }
            if definitions
                .insert(definition.name.clone(), definition.clone())
                .is_some()
            {
                return Err(FloeError::config(format!(
                    "catalogs.definitions name={} is duplicated",
                    definition.name
                ))
                .into());
            }
        }

        if let Some(default_name) = &catalogs.default {
            if !definitions.contains_key(default_name) {
                return Err(FloeError::config(format!(
                    "catalogs.default={} does not match any definition",
                    default_name
                ))
                .into());
            }
        }

        Ok(Self {
            has_config: true,
            default_name: catalogs.default.clone(),
            definitions,
        })
    }

    fn definition(&self, name: &str) -> Option<&CatalogDefinition> {
        if !self.has_config {
            return None;
        }
        self.definitions.get(name)
    }

    fn default_name(&self) -> Option<&str> {
        self.default_name.as_deref()
    }
}

impl StorageRegistry {
    fn new(config: &RootConfig) -> FloeResult<Self> {
        let Some(storages) = &config.storages else {
            return Ok(Self {
                has_config: false,
                default_name: None,
                definitions: std::collections::HashMap::new(),
            });
        };

        if storages.definitions.is_empty() {
            return Err(
                FloeError::config("storages.definitions must not be empty".to_string()).into(),
            );
        }

        let mut definitions = std::collections::HashMap::new();
        for definition in &storages.definitions {
            if !ALLOWED_STORAGE_TYPES.contains(&definition.fs_type.as_str()) {
                return Err(FloeError::config(format!(
                    "storages.definitions name={} type={} is unsupported (allowed: {})",
                    definition.name,
                    definition.fs_type,
                    ALLOWED_STORAGE_TYPES.join(", ")
                ))
                .into());
            }
            if definition.fs_type == "s3" {
                if definition.bucket.is_none() {
                    return Err(FloeError::config(format!(
                        "storages.definitions name={} requires bucket for type s3",
                        definition.name
                    ))
                    .into());
                }
                if definition.region.is_none() {
                    return Err(FloeError::config(format!(
                        "storages.definitions name={} requires region for type s3",
                        definition.name
                    ))
                    .into());
                }
            }
            if definition.fs_type == "adls" {
                if definition.account.is_none() {
                    return Err(FloeError::config(format!(
                        "storages.definitions name={} requires account for type adls",
                        definition.name
                    ))
                    .into());
                }
                if definition.container.is_none() {
                    return Err(FloeError::config(format!(
                        "storages.definitions name={} requires container for type adls",
                        definition.name
                    ))
                    .into());
                }
            }
            if definition.fs_type == "gcs" && definition.bucket.is_none() {
                return Err(FloeError::config(format!(
                    "storages.definitions name={} requires bucket for type gcs",
                    definition.name
                ))
                .into());
            }
            if definitions
                .insert(definition.name.clone(), definition.clone())
                .is_some()
            {
                return Err(FloeError::config(format!(
                    "storages.definitions name={} is duplicated",
                    definition.name
                ))
                .into());
            }
        }

        if let Some(default_name) = &storages.default {
            if !definitions.contains_key(default_name) {
                return Err(FloeError::config(format!(
                    "storages.default={} does not match any definition",
                    default_name
                ))
                .into());
            }
        } else {
            return Err(FloeError::config(
                "storages.default is required when storages is set".to_string(),
            )
            .into());
        }

        Ok(Self {
            has_config: true,
            default_name: storages.default.clone(),
            definitions,
        })
    }

    fn resolve_name(
        &self,
        entity: &EntityConfig,
        field: &str,
        override_name: Option<&str>,
    ) -> FloeResult<String> {
        if let Some(name) = override_name {
            return Ok(name.to_string());
        }
        if self.has_config {
            let default_name = self.default_name.clone().ok_or_else(|| {
                FloeError::config(format!(
                    "entity.name={} {field} requires storages.default",
                    entity.name
                ))
            })?;
            return Ok(default_name);
        }
        Ok("local".to_string())
    }

    fn validate_reference(&self, entity: &EntityConfig, field: &str, name: &str) -> FloeResult<()> {
        if !self.has_config {
            if name != "local" {
                return Err(FloeError::config(format!(
                    "entity.name={} {field} references unknown storage {} (no storages block)",
                    entity.name, name
                ))
                .into());
            }
            return Ok(());
        }

        let _definition = self.definitions.get(name).ok_or_else(|| {
            FloeError::config(format!(
                "entity.name={} {field} references unknown storage {}",
                entity.name, name
            ))
        })?;

        Ok(())
    }

    fn definition_type(&self, name: &str) -> Option<&str> {
        if !self.has_config {
            if name == "local" {
                return Some("local");
            }
            return None;
        }
        self.definitions
            .get(name)
            .map(|definition| definition.fs_type.as_str())
    }

    fn resolve_report_name(&self, override_name: Option<&str>) -> FloeResult<String> {
        if let Some(name) = override_name {
            return Ok(name.to_string());
        }
        if self.has_config {
            let default_name = self.default_name.clone().ok_or_else(|| {
                FloeError::config("report.storage requires storages.default".to_string())
            })?;
            return Ok(default_name);
        }
        Ok("local".to_string())
    }

    fn validate_report_reference(&self, field: &str, name: &str) -> FloeResult<()> {
        if !self.has_config {
            if name != "local" {
                return Err(FloeError::config(format!(
                    "{field} references unknown storage {} (no storages block)",
                    name
                ))
                .into());
            }
            return Ok(());
        }

        let _definition = self.definitions.get(name).ok_or_else(|| {
            FloeError::config(format!("{field} references unknown storage {}", name))
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod duckdb_tests {
    use super::*;
    use crate::config::{
        DuckDbSinkTargetConfig, PolicyConfig, SchemaConfig, SinkConfig, SinkTarget, SourceConfig,
        WriteMode,
    };

    /// Build a single-entity RootConfig with a local-file DuckDB accepted sink
    /// whose `connection` is set to `connection` (used to exercise the validation
    /// branches that the YAML parser would otherwise normalize away).
    fn duckdb_root(connection: Option<&str>) -> RootConfig {
        let entity = EntityConfig {
            name: "customers".to_string(),
            metadata: None,
            domain: None,
            incremental_mode: IncrementalMode::None,
            state: None,
            source: SourceConfig {
                format: "csv".to_string(),
                path: "in/customers.csv".to_string(),
                storage: None,
                options: None,
                cast_mode: None,
            },
            sink: SinkConfig {
                write_mode: WriteMode::Overwrite,
                accepted: SinkTarget {
                    format: "duckdb".to_string(),
                    path: "out/warehouse.duckdb".to_string(),
                    storage: None,
                    options: None,
                    merge: None,
                    iceberg: None,
                    delta: None,
                    duckdb: Some(DuckDbSinkTargetConfig {
                        table: "customers".to_string(),
                        schema: None,
                        connection: connection.map(str::to_string),
                        token: None,
                    }),
                    partition_by: None,
                    partition_spec: None,
                    write_mode: WriteMode::Overwrite,
                },
                rejected: None,
                archive: None,
            },
            policy: PolicyConfig {
                severity: PolicySeverity::Warn,
            },
            schema: SchemaConfig {
                normalize_columns: None,
                mismatch: None,
                schema_evolution: None,
                primary_key: None,
                unique_keys: None,
                columns: vec![crate::config::ColumnConfig {
                    name: "id".to_string(),
                    source: None,
                    column_type: "string".to_string(),
                    nullable: None,
                    unique: None,
                    width: None,
                    trim: None,
                }],
            },
            pii: None,
        }; // EntityConfig
        RootConfig {
            version: "0.1".to_string(),
            metadata: None,
            storages: None,
            catalogs: None,
            env: None,
            domains: Vec::new(),
            report: None,
            lineage: None,
            entities: vec![entity],
        }
    }

    #[test]
    fn blank_duckdb_connection_rejected() {
        // The YAML parser normalizes blanks to None, but the manifest-reconstruct
        // path deserializes directly into the config types, so validation must still
        // reject a present-but-blank connection rather than silently treating it as a
        // local-file target (which then fails at runtime).
        let err = validate_config(&duckdb_root(Some("   ")))
            .expect_err("blank duckdb connection should be rejected");
        assert!(
            err.to_string().contains("must not be blank"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn omitted_duckdb_connection_is_local_file() {
        // No connection => local-file target, which validates fine.
        validate_config(&duckdb_root(None)).expect("local-file duckdb target should validate");
    }
}
