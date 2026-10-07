use crate::errors::FloeError;
use crate::secret::Secret;
use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use polars::polars_utils::pl_str::PlSmallStr;
use polars::prelude::{
    CsvEncoding, CsvParseOptions, CsvReadOptions, DataType, NullValues, TimeUnit,
};

use crate::FloeResult;

#[derive(Debug)]
pub struct RootConfig {
    pub version: String,
    pub metadata: Option<ProjectMetadata>,
    pub storages: Option<StoragesConfig>,
    pub catalogs: Option<CatalogsConfig>,
    pub env: Option<EnvConfig>,
    pub domains: Vec<DomainConfig>,
    pub report: Option<ReportConfig>,
    pub lineage: Option<LineageConfig>,
    pub entities: Vec<EntityConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageConfig {
    pub url: String,
    pub endpoint: Option<String>,
    pub api_key: Option<String>,
    pub timeout_secs: Option<u64>,
    pub namespace: String,
    pub dataset_namespace: Option<String>,
    pub producer: Option<String>,
    pub max_failures: Option<u32>,
    pub job_name: Option<String>,
}

#[derive(Debug)]
pub struct ProjectMetadata {
    pub project: Option<String>,
    pub description: Option<String>,
    pub owner: Option<String>,
    pub tags: Option<Vec<String>>,
}

#[derive(Debug)]
pub struct EnvConfig {
    pub file: Option<String>,
    pub vars: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct DomainConfig {
    pub name: String,
    pub incoming_dir: String,
    pub resolved_incoming_dir: Option<String>,
}

#[derive(Debug)]
pub struct EntityConfig {
    pub name: String,
    pub metadata: Option<EntityMetadata>,
    pub domain: Option<String>,
    pub incremental_mode: IncrementalMode,
    pub state: Option<EntityStateConfig>,
    pub source: SourceConfig,
    pub sink: SinkConfig,
    pub policy: PolicyConfig,
    pub schema: SchemaConfig,
    pub pii: Option<PiiConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PiiConfig {
    pub columns: Vec<PiiColumnConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PiiColumnConfig {
    pub name: String,
    pub strategy: PiiStrategy,
    pub mask_pattern: Option<String>,
    pub redact_value: Option<String>,
    /// HMAC-SHA256 key for `strategy: hash`. Accepts a plain literal or a
    /// single `${ENV_VAR}` reference resolved at masking time. When absent,
    /// plain SHA-256 is used (backward-compatible but reversible for
    /// low-entropy values).
    #[serde(default)]
    pub key: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PiiStrategy {
    Hash,
    Drop,
    Nullify,
    Redact,
    Mask,
    Tokenize,
}

impl EntityConfig {
    pub fn resolved_incremental_mode(&self) -> IncrementalMode {
        if self.incremental_mode == IncrementalMode::None && self.sink.archive.is_some() {
            IncrementalMode::Archive
        } else {
            self.incremental_mode
        }
    }

    pub fn archive_enabled(&self) -> bool {
        self.resolved_incremental_mode() == IncrementalMode::Archive && self.sink.archive.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IncrementalMode {
    #[default]
    None,
    Archive,
    File,
    Row,
}

impl IncrementalMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Archive => "archive",
            Self::File => "file",
            Self::Row => "row",
        }
    }
}

#[derive(Debug)]
pub struct EntityMetadata {
    pub data_product: Option<String>,
    pub domain: Option<String>,
    pub owner: Option<String>,
    pub description: Option<String>,
    pub tags: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct EntityStateConfig {
    pub path: Option<String>,
}

#[derive(Debug)]
pub struct SourceConfig {
    pub format: String,
    pub path: String,
    pub storage: Option<String>,
    pub options: Option<SourceOptions>,
    pub cast_mode: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SourceOptions {
    pub header: Option<bool>,
    pub separator: Option<String>,
    pub encoding: Option<String>,
    pub null_values: Option<Vec<String>>,
    pub recursive: Option<bool>,
    pub glob: Option<String>,
    pub json_mode: Option<String>,
    pub sheet: Option<String>,
    pub header_row: Option<u64>,
    pub data_row: Option<u64>,
    pub row_tag: Option<String>,
    pub namespace: Option<String>,
    pub value_tag: Option<String>,
}

impl Default for SourceOptions {
    fn default() -> Self {
        Self {
            header: Some(true),
            separator: Some(";".to_string()),
            encoding: Some("UTF8".to_string()),
            // Treat empty CSV fields as nulls by default. Users can override with `null_values: []`.
            null_values: Some(vec!["".to_string()]),
            recursive: Some(false),
            glob: None,
            json_mode: None,
            sheet: None,
            header_row: Some(1),
            data_row: None,
            row_tag: None,
            namespace: None,
            value_tag: None,
        }
    }
}

impl SourceOptions {
    pub fn defaults_for_format(format: &str) -> Self {
        let mut defaults = SourceOptions::default();
        if format == "tsv" {
            defaults.separator = Some("\t".to_string());
        }
        defaults
    }

    pub fn to_csv_parse_options(&self) -> FloeResult<CsvParseOptions> {
        let separator = parse_separator(self.separator.as_deref().unwrap_or(";"))?;
        let encoding = parse_encoding(self.encoding.as_deref())?;
        let null_values = build_null_values(self.null_values.as_ref());
        Ok(CsvParseOptions::default()
            .with_separator(separator)
            .with_encoding(encoding)
            .with_null_values(null_values))
    }

    pub fn to_csv_read_options(&self, input_path: &Path) -> FloeResult<CsvReadOptions> {
        let header = self.header.unwrap_or(true);
        let parse_options = self.to_csv_parse_options()?;

        Ok(CsvReadOptions::default()
            .with_path(Some(input_path))
            .with_has_header(header)
            .with_parse_options(parse_options))
    }
}

fn parse_separator(value: &str) -> FloeResult<u8> {
    let bytes = value.as_bytes();
    if bytes.len() != 1 {
        return Err(
            FloeError::config(format!("separator must be a single byte, got {value:?}")).into(),
        );
    }
    Ok(bytes[0])
}

fn parse_encoding(value: Option<&str>) -> FloeResult<CsvEncoding> {
    let normalized = value
        .unwrap_or("utf8")
        .to_ascii_lowercase()
        .replace(['-', '_'], "");
    match normalized.as_str() {
        "utf8" => Ok(CsvEncoding::Utf8),
        "lossyutf8" => Ok(CsvEncoding::LossyUtf8),
        _ => Err(
            FloeError::config(format!("unsupported encoding: {}", value.unwrap_or("utf8"))).into(),
        ),
    }
}

fn build_null_values(values: Option<&Vec<String>>) -> Option<NullValues> {
    let values = values?;
    if values.is_empty() {
        return None;
    }
    if values.len() == 1 {
        return Some(NullValues::AllColumnsSingle(PlSmallStr::from(
            values[0].as_str(),
        )));
    }
    Some(NullValues::AllColumns(
        values.iter().map(|value| value.as_str().into()).collect(),
    ))
}

#[derive(Debug)]
pub struct SinkConfig {
    pub write_mode: WriteMode,
    pub accepted: SinkTarget,
    pub rejected: Option<SinkTarget>,
    pub archive: Option<ArchiveTarget>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    #[default]
    Overwrite,
    Append,
    MergeScd1,
    MergeScd2,
}

impl WriteMode {
    pub fn as_str(self) -> &'static str {
        match self {
            WriteMode::Overwrite => "overwrite",
            WriteMode::Append => "append",
            WriteMode::MergeScd1 => "merge_scd1",
            WriteMode::MergeScd2 => "merge_scd2",
        }
    }
}

#[derive(Debug)]
pub struct SinkTarget {
    pub format: String,
    pub path: String,
    pub storage: Option<String>,
    pub options: Option<SinkOptions>,
    pub merge: Option<MergeOptionsConfig>,
    pub iceberg: Option<IcebergSinkTargetConfig>,
    pub delta: Option<DeltaSinkTargetConfig>,
    pub duckdb: Option<DuckDbSinkTargetConfig>,
    pub partition_by: Option<Vec<String>>,
    pub partition_spec: Option<Vec<IcebergPartitionFieldConfig>>,
    pub write_mode: WriteMode,
}

pub const DEFAULT_SCD2_CURRENT_FLAG_COLUMN: &str = "__floe_is_current";
pub const DEFAULT_SCD2_VALID_FROM_COLUMN: &str = "__floe_valid_from";
pub const DEFAULT_SCD2_VALID_TO_COLUMN: &str = "__floe_valid_to";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeOptionsConfig {
    pub ignore_columns: Option<Vec<String>>,
    pub compare_columns: Option<Vec<String>>,
    pub scd2: Option<MergeScd2OptionsConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeScd2OptionsConfig {
    pub current_flag_column: Option<String>,
    pub valid_from_column: Option<String>,
    pub valid_to_column: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SinkOptions {
    pub compression: Option<String>,
    pub row_group_size: Option<u64>,
    pub max_size_per_file: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergPartitionFieldConfig {
    pub column: String,
    pub transform: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergSinkTargetConfig {
    pub catalog: Option<String>,
    pub namespace: Option<String>,
    pub table: Option<String>,
    pub location: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaSinkTargetConfig {
    /// Name of the catalog definition to use (falls back to `catalogs.default`).
    pub catalog: Option<String>,
    /// Override the Unity schema for this entity (falls back to the catalog definition's `schema`).
    pub schema: Option<String>,
    /// Override the table name (defaults to the entity name).
    pub table: Option<String>,
}

/// DuckDB accepted-sink configuration.
///
/// A DuckDB sink targets either a **local database file** (via `SinkTarget.path`) or a
/// **MotherDuck** database (via `connection: "md:<db>"`). The two are mutually exclusive.
/// DuckDB cannot read-write a database file over object storage, so remote writes must use
/// MotherDuck; object-store paths are rejected during validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuckDbSinkTargetConfig {
    /// Target table name. May be schema-qualified (`"main.customers"`); otherwise `schema`
    /// (or the default `"main"`) is used.
    pub table: String,
    /// DuckDB schema for the table (default: `"main"`). Ignored if `table` is already qualified.
    pub schema: Option<String>,
    /// MotherDuck connection string (e.g. `"md:analytics"`). When set, the sink writes to
    /// MotherDuck instead of a local file and `SinkTarget.path` is ignored.
    pub connection: Option<String>,
    /// MotherDuck access token. Supports `${ENV}` substitution. Required for MotherDuck unless
    /// the `motherduck_token` environment variable is set in the process. Redacted in
    /// `Debug`/`Display`; the manifest builder additionally drops literal tokens.
    pub token: Option<Secret>,
}

/// True if `connection` is a MotherDuck connection string (`md:` scheme).
///
/// This is a pure string check kept in `config` (always compiled) so config
/// validation and manifest round-tripping can recognize MotherDuck sinks even
/// when the feature-gated `duckdb` writer is not built into the binary.
pub(crate) fn is_motherduck_connection(connection: &str) -> bool {
    connection.trim_start().starts_with("md:")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoragesConfig {
    pub default: Option<String>,
    pub definitions: Vec<StorageDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageDefinition {
    pub name: String,
    pub fs_type: String,
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub account: Option<String>,
    pub container: Option<String>,
    pub prefix: Option<String>,
    pub endpoint: Option<String>,
    pub path_style_access: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogsConfig {
    pub default: Option<String>,
    pub definitions: Vec<CatalogDefinition>,
}

/// Type-specific configuration for a catalog definition.
/// Each variant carries only the fields relevant to that catalog type.
/// Add a new variant here when supporting a new catalog type.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum CatalogTypeConfig {
    Glue {
        region: String,
        database: String,
        /// Create the Glue database if it does not exist (default: true).
        create_database_if_missing: bool,
        /// Allow Floe to take ownership of a Glue table it did not create (default: false).
        allow_takeover: bool,
    },
    Rest {
        /// REST catalog endpoint URI (e.g. `https://<host>/api/catalog`).
        uri: String,
        /// Credential string: `"token:<token>"` or `"client_credentials:<id>:<secret>"`.
        /// Redacted in `Debug`/`Display`.
        credential: Option<Secret>,
        /// Warehouse / catalog identifier (e.g. `"my_catalog"` or `"my_catalog.my_schema"`).
        warehouse: Option<String>,
        /// OAuth2 token endpoint URI (required for Snowflake Polaris).
        oauth2_server_uri: Option<String>,
        /// OAuth2 scope (e.g. `"PRINCIPAL_ROLE:ALL"` for Snowflake).
        scope: Option<String>,
    },
    /// Unity Catalog (Databricks) — post-write Delta table registration via the
    /// Unity Catalog REST API.  Compatible with OSS Unity Catalog as well.
    Unity {
        /// Workspace URL, e.g. "https://my-workspace.azuredatabricks.net".
        host: String,
        /// Unity catalog name (e.g. "my_catalog").
        catalog: String,
        /// Unity schema (database) name (e.g. "my_schema").
        schema: String,
        /// Personal Access Token or value from env-var substitution. Redacted in
        /// `Debug`/`Display`.
        token: Secret,
        /// Create the Unity schema if it does not already exist (default: false).
        create_schema_if_missing: bool,
    },
}

impl CatalogTypeConfig {
    pub fn catalog_type_str(&self) -> &'static str {
        match self {
            Self::Glue { .. } => "glue",
            Self::Rest { .. } => "rest",
            Self::Unity { .. } => "unity",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogDefinition {
    pub name: String,
    pub type_config: CatalogTypeConfig,
    pub warehouse_storage: Option<String>,
    pub warehouse_prefix: Option<String>,
}

#[derive(Debug)]
pub struct ReportConfig {
    pub path: String,
    pub formatter: Option<String>,
    pub storage: Option<String>,
}

#[derive(Debug)]
pub struct ArchiveTarget {
    pub path: String,
    pub storage: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PolicySeverity {
    #[default]
    Warn,
    Reject,
    Abort,
}

impl PolicySeverity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Warn => "warn",
            Self::Reject => "reject",
            Self::Abort => "abort",
        }
    }
}

impl std::fmt::Display for PolicySeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug)]
pub struct PolicyConfig {
    pub severity: PolicySeverity,
}

#[derive(Debug, Serialize)]
pub struct SchemaConfig {
    pub normalize_columns: Option<NormalizeColumnsConfig>,
    pub mismatch: Option<SchemaMismatchConfig>,
    pub schema_evolution: Option<SchemaEvolutionConfig>,
    pub primary_key: Option<Vec<String>>,
    pub unique_keys: Option<Vec<Vec<String>>>,
    pub columns: Vec<ColumnConfig>,
}

impl SchemaConfig {
    pub fn resolved_schema_evolution(&self) -> SchemaEvolutionConfig {
        self.schema_evolution.unwrap_or_default()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NormalizeColumnsConfig {
    pub enabled: Option<bool>,
    pub strategy: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SchemaEvolutionConfig {
    pub mode: SchemaEvolutionMode,
    pub on_incompatible: SchemaEvolutionIncompatibleAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaEvolutionMode {
    #[default]
    Strict,
    AddColumns,
}

impl SchemaEvolutionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            SchemaEvolutionMode::Strict => "strict",
            SchemaEvolutionMode::AddColumns => "add_columns",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SchemaEvolutionIncompatibleAction {
    #[default]
    Fail,
}

impl SchemaEvolutionIncompatibleAction {
    pub fn as_str(self) -> &'static str {
        match self {
            SchemaEvolutionIncompatibleAction::Fail => "fail",
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SchemaMismatchConfig {
    pub missing_columns: Option<String>,
    pub extra_columns: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ColumnConfig {
    pub name: String,
    pub source: Option<String>,
    pub column_type: String,
    pub nullable: Option<bool>,
    pub unique: Option<bool>,
    pub width: Option<u64>,
    pub trim: Option<bool>,
}

impl ColumnConfig {
    pub fn source_or_name(&self) -> &str {
        self.source.as_deref().unwrap_or(self.name.as_str())
    }
}

pub(crate) fn parse_data_type(value: &str) -> FloeResult<DataType> {
    let normalized = value.to_ascii_lowercase().replace(['-', '_'], "");
    match normalized.as_str() {
        "string" | "str" | "text" => Ok(DataType::String),
        "boolean" | "bool" => Ok(DataType::Boolean),
        "int8" => Ok(DataType::Int8),
        "int16" => Ok(DataType::Int16),
        "int32" => Ok(DataType::Int32),
        "int64" | "int" | "integer" | "long" => Ok(DataType::Int64),
        "uint8" => Ok(DataType::UInt8),
        "uint16" => Ok(DataType::UInt16),
        "uint32" => Ok(DataType::UInt32),
        "uint64" => Ok(DataType::UInt64),
        "float32" => Ok(DataType::Float32),
        "float64" | "float" | "double" | "number" | "decimal" => Ok(DataType::Float64),
        "date" => Ok(DataType::Date),
        "datetime" | "timestamp" => Ok(DataType::Datetime(TimeUnit::Milliseconds, None)),
        "time" => Ok(DataType::Time),
        _ => Err(FloeError::config(format!("unsupported column type: {value}")).into()),
    }
}
