use crate::errors::FloeError;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use polars::prelude::{ArrowDataType, DataType, SerReader};
use serde_json::Value as JsonValue;
use tempfile::NamedTempFile;
use url::Url;
use yaml_rust2::yaml::Hash as YamlHash;
use yaml_rust2::{Yaml, YamlEmitter, YamlLoader};

use crate::config;
use crate::{FloeResult, ValidateOptions};

const DEFAULT_SAMPLE_ROWS: usize = 200;
const MINIMAL_CONFIG_YAML: &str = "version: \"0.2\"\nentities: []\n";

#[derive(Debug, Clone)]
pub struct AddEntityOptions {
    pub config_path: PathBuf,
    pub output_path: Option<PathBuf>,
    pub input: String,
    pub format: Option<String>,
    pub name: Option<String>,
    pub domain: Option<String>,
    pub dry_run: bool,
}

#[derive(Debug, Clone)]
pub struct AddEntityOutcome {
    pub entity_name: String,
    pub output_path: PathBuf,
    pub column_count: usize,
    pub format: String,
    pub dry_run: bool,
    pub rendered_yaml: Option<String>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone)]
struct InferredEntity {
    name: String,
    format: String,
    input: String,
    domain: Option<String>,
    source_options: Option<GeneratedSourceOptions>,
    columns: Vec<GeneratedColumn>,
    notes: Vec<String>,
}

#[derive(Debug, Clone)]
struct GeneratedSourceOptions {
    separator: Option<String>,
    json_mode: Option<String>,
}

#[derive(Debug, Clone)]
struct GeneratedColumn {
    name: String,
    source: String,
    column_type: String,
    nullable: bool,
    unique: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonMode {
    Array,
    Ndjson,
}

impl JsonMode {
    fn as_str(self) -> &'static str {
        match self {
            JsonMode::Array => "array",
            JsonMode::Ndjson => "ndjson",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InferredScalarType {
    String,
    Boolean,
    Number,
    Date,
    Time,
    DateTime,
}

#[derive(Debug, Default, Clone)]
struct JsonColumnStats {
    kind: Option<InferredScalarType>,
    non_null_count: usize,
    nullable: bool,
}

pub fn add_entity_to_config(options: AddEntityOptions) -> FloeResult<AddEntityOutcome> {
    let inferred = infer_entity_from_input(
        &options.input,
        options.format.as_deref(),
        options.name.as_deref(),
        options.domain.as_deref(),
    )?;

    let output_path = options
        .output_path
        .clone()
        .unwrap_or_else(|| options.config_path.clone());
    let config_text = load_or_initialize_config_text(&options.config_path)?;

    let updated_yaml = append_entity_yaml(&config_text, &inferred)?;
    if !options.dry_run {
        if let Some(parent) = output_path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
    }
    validate_generated_config(
        &updated_yaml,
        if options.dry_run {
            &options.config_path
        } else {
            &output_path
        },
    )?;

    if !options.dry_run {
        fs::write(&output_path, updated_yaml.as_bytes()).map_err(|err| {
            FloeError::io(format!(
                "failed to write config at {}: {err}",
                output_path.display()
            ))
        })?;
    }

    Ok(AddEntityOutcome {
        entity_name: inferred.name,
        output_path,
        column_count: inferred.columns.len(),
        format: inferred.format,
        dry_run: options.dry_run,
        rendered_yaml: options.dry_run.then_some(updated_yaml),
        notes: inferred.notes,
    })
}

fn infer_entity_from_input(
    input: &str,
    format_override: Option<&str>,
    name_override: Option<&str>,
    domain: Option<&str>,
) -> FloeResult<InferredEntity> {
    let entity_name = match name_override {
        Some(value) => value.to_string(),
        None => derive_entity_name(input)?,
    };
    if entity_name.trim().is_empty() {
        return Err(FloeError::config("entity name must not be empty".to_string()).into());
    }

    let local_input_path = resolve_local_input_path(input)?;
    let format = resolve_add_entity_format(format_override, input, &local_input_path)?;
    let persisted_input_path = normalize_persisted_input_path(input, &local_input_path);
    let (source_options, columns, notes) = match format.as_str() {
        "csv" => infer_csv_columns(&local_input_path)?,
        "json" => infer_json_columns(&local_input_path)?,
        "parquet" => infer_parquet_columns(&local_input_path)?,
        other => {
            return Err(FloeError::config(format!(
                "unsupported add-entity format: {other} (allowed: csv, json, parquet)"
            ))
            .into())
        }
    };

    if columns.is_empty() {
        return Err(FloeError::config(format!(
            "no columns inferred from {}",
            local_input_path.display()
        ))
        .into());
    }

    Ok(InferredEntity {
        name: entity_name,
        format,
        input: persisted_input_path,
        domain: domain.map(ToString::to_string),
        source_options,
        columns,
        notes,
    })
}

fn load_or_initialize_config_text(config_path: &Path) -> FloeResult<String> {
    match fs::read_to_string(config_path) {
        Ok(text) => Ok(text),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            Ok(MINIMAL_CONFIG_YAML.to_string())
        }
        Err(err) => Err(FloeError::io(format!(
            "failed to read config at {}: {err}",
            config_path.display()
        ))
        .into()),
    }
}

fn resolve_add_entity_format(
    format_override: Option<&str>,
    input: &str,
    local_input_path: &Path,
) -> FloeResult<String> {
    if let Some(format) = format_override {
        return Ok(format.to_string());
    }

    infer_format_from_extension(local_input_path).ok_or_else(|| {
        FloeError::config(format!(
            "could not infer add-entity format from input {} (supported extensions: .csv, .json, .parquet); use --format",
            input
        )).into()
    })
}

fn infer_format_from_extension(path: &Path) -> Option<String> {
    let ext = path.extension()?.to_string_lossy().to_ascii_lowercase();
    match ext.as_str() {
        "csv" => Some("csv".to_string()),
        "json" => Some("json".to_string()),
        "parquet" => Some("parquet".to_string()),
        _ => None,
    }
}

fn infer_csv_columns(
    input_path: &Path,
) -> FloeResult<(
    Option<GeneratedSourceOptions>,
    Vec<GeneratedColumn>,
    Vec<String>,
)> {
    let separator = sniff_delimiter(input_path).unwrap_or(',');
    let separator_str = separator.to_string();

    let mut source_options = config::SourceOptions::defaults_for_format("csv");
    source_options.separator = Some(separator.to_string());
    source_options.header = Some(true);

    let reader = source_options
        .to_csv_read_options(input_path)?
        .with_n_rows(Some(DEFAULT_SAMPLE_ROWS))
        .with_infer_schema_length(Some(DEFAULT_SAMPLE_ROWS))
        .try_into_reader_with_file_path(None)
        .map_err(|err| {
            FloeError::io(format!(
                "failed to open csv at {}: {err}",
                input_path.display()
            ))
        })?;
    let df = reader
        .finish()
        .map_err(|err| FloeError::io(format!("csv schema inference failed: {err}")))?;

    let mut columns = Vec::with_capacity(df.width());
    for column in df.get_columns() {
        let name = column.name().as_str().to_string();
        columns.push(GeneratedColumn {
            name: name.clone(),
            source: name,
            column_type: polars_dtype_to_floe_type(column.dtype()),
            nullable: df.height() == 0 || column.null_count() > 0,
            unique: false,
        });
    }

    Ok((
        Some(GeneratedSourceOptions {
            separator: Some(separator_str),
            json_mode: None,
        }),
        columns,
        Vec::new(),
    ))
}

fn infer_parquet_columns(
    input_path: &Path,
) -> FloeResult<(
    Option<GeneratedSourceOptions>,
    Vec<GeneratedColumn>,
    Vec<String>,
)> {
    let file = std::fs::File::open(input_path).map_err(|err| {
        FloeError::io(format!(
            "failed to open parquet at {}: {err}",
            input_path.display()
        ))
    })?;
    let mut reader = polars::prelude::ParquetReader::new(file);
    let schema = reader.schema().map_err(|err| {
        FloeError::io(format!(
            "failed to read parquet schema at {}: {err}",
            input_path.display()
        ))
    })?;

    let columns = schema
        .iter()
        .map(|(name, field)| GeneratedColumn {
            name: name.to_string(),
            source: name.to_string(),
            column_type: arrow_dtype_to_floe_type(field.dtype()),
            nullable: true,
            unique: false,
        })
        .collect();

    Ok((None, columns, Vec::new()))
}

fn infer_json_columns(
    input_path: &Path,
) -> FloeResult<(
    Option<GeneratedSourceOptions>,
    Vec<GeneratedColumn>,
    Vec<String>,
)> {
    let json_mode = detect_json_mode(input_path)?;
    let mut stats_by_key: BTreeMap<String, JsonColumnStats> = BTreeMap::new();
    let mut sampled_rows = 0usize;
    let mut nested_keys = BTreeSet::new();

    match json_mode {
        JsonMode::Array => {
            let content = fs::read_to_string(input_path).map_err(|err| {
                FloeError::io(format!(
                    "failed to read json at {}: {err}",
                    input_path.display()
                ))
            })?;
            let value: JsonValue = serde_json::from_str(&content)
                .map_err(|err| FloeError::io(format!("json parse error: {err}")))?;
            let rows = value
                .as_array()
                .ok_or_else(|| FloeError::config("expected json array at root".to_string()))?;
            for row in rows.iter().take(DEFAULT_SAMPLE_ROWS) {
                let object = row.as_object().ok_or_else(|| {
                    FloeError::config("expected top-level json objects inside array".to_string())
                })?;
                sampled_rows += 1;
                update_json_stats(object, &mut stats_by_key, &mut nested_keys);
            }
        }
        JsonMode::Ndjson => {
            let file = std::fs::File::open(input_path).map_err(|err| {
                FloeError::io(format!(
                    "failed to read json at {}: {err}",
                    input_path.display()
                ))
            })?;
            let reader = BufReader::new(file);
            for (idx, line) in reader.lines().enumerate() {
                if sampled_rows >= DEFAULT_SAMPLE_ROWS {
                    break;
                }
                let line = line.map_err(|err| {
                    FloeError::io(format!(
                        "failed to read json at {}: {err}",
                        input_path.display()
                    ))
                })?;
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let value: JsonValue = serde_json::from_str(line).map_err(|err| {
                    FloeError::io(format!("json parse error at line {}: {err}", idx + 1))
                })?;
                let object = value.as_object().ok_or_else(|| {
                    FloeError::config(format!(
                        "expected top-level json object at line {}",
                        idx + 1
                    ))
                })?;
                sampled_rows += 1;
                update_json_stats(object, &mut stats_by_key, &mut nested_keys);
            }
        }
    }

    if sampled_rows == 0 {
        return Err(FloeError::config(format!(
            "no json objects found in {}",
            input_path.display()
        ))
        .into());
    }

    let mut columns = Vec::with_capacity(stats_by_key.len());
    for (key, stats) in &stats_by_key {
        let nullable = stats.nullable || stats.non_null_count < sampled_rows;
        columns.push(GeneratedColumn {
            name: key.clone(),
            source: key.clone(),
            column_type: stats
                .kind
                .map(inferred_scalar_type_to_floe_type)
                .unwrap_or("string")
                .to_string(),
            nullable,
            unique: false,
        });
    }

    let mut notes = Vec::new();
    notes.push(format!(
        "json inference sampled {} row(s) in {} mode",
        sampled_rows,
        json_mode.as_str()
    ));
    if !nested_keys.is_empty() {
        notes.push(format!(
            "nested JSON values inferred as string for top-level key(s): {}",
            nested_keys.into_iter().collect::<Vec<_>>().join(", ")
        ));
    }

    Ok((
        Some(GeneratedSourceOptions {
            separator: None,
            json_mode: Some(json_mode.as_str().to_string()),
        }),
        columns,
        notes,
    ))
}

fn update_json_stats(
    object: &serde_json::Map<String, JsonValue>,
    stats_by_key: &mut BTreeMap<String, JsonColumnStats>,
    nested_keys: &mut BTreeSet<String>,
) {
    for (key, value) in object {
        let stats = stats_by_key.entry(key.clone()).or_default();
        match json_value_kind(value) {
            None => {
                stats.nullable = true;
            }
            Some(kind) => {
                stats.non_null_count += 1;
                if matches!(value, JsonValue::Array(_) | JsonValue::Object(_)) {
                    nested_keys.insert(key.clone());
                }
                stats.kind = Some(match (stats.kind, kind) {
                    (None, next) => next,
                    (Some(current), next) => merge_json_kinds(current, next),
                });
            }
        }
    }
}

fn json_value_kind(value: &JsonValue) -> Option<InferredScalarType> {
    match value {
        JsonValue::Null => None,
        JsonValue::Bool(_) => Some(InferredScalarType::Boolean),
        JsonValue::Number(_) => Some(InferredScalarType::Number),
        JsonValue::String(value) => Some(infer_string_scalar_type(value)),
        JsonValue::Array(_) | JsonValue::Object(_) => Some(InferredScalarType::String),
    }
}

fn infer_string_scalar_type(value: &str) -> InferredScalarType {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return InferredScalarType::String;
    }
    if looks_like_date_time(trimmed) {
        return InferredScalarType::DateTime;
    }
    if looks_like_date(trimmed) {
        return InferredScalarType::Date;
    }
    if looks_like_time(trimmed) {
        return InferredScalarType::Time;
    }
    InferredScalarType::String
}

fn looks_like_date(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(idx, b)| matches!(idx, 4 | 7) || b.is_ascii_digit())
}

fn looks_like_time(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() == 8 {
        return bytes[2] == b':'
            && bytes[5] == b':'
            && bytes
                .iter()
                .enumerate()
                .all(|(idx, b)| matches!(idx, 2 | 5) || b.is_ascii_digit());
    }
    if bytes.len() == 12 {
        return bytes[2] == b':'
            && bytes[5] == b':'
            && bytes[8] == b'.'
            && bytes
                .iter()
                .enumerate()
                .all(|(idx, b)| matches!(idx, 2 | 5 | 8) || b.is_ascii_digit());
    }
    false
}

fn looks_like_date_time(value: &str) -> bool {
    value.contains('T')
        && (value.ends_with('Z') || value.contains('+') || value.matches(':').count() >= 2)
}

fn merge_json_kinds(current: InferredScalarType, next: InferredScalarType) -> InferredScalarType {
    use InferredScalarType as T;
    match (current, next) {
        (T::String, _) | (_, T::String) => T::String,
        (T::Number, T::Number) => T::Number,
        (T::Boolean, T::Boolean) => T::Boolean,
        (T::Date, T::Date) => T::Date,
        (T::Time, T::Time) => T::Time,
        (T::DateTime, T::DateTime) => T::DateTime,
        (T::Date, T::DateTime) | (T::DateTime, T::Date) => T::DateTime,
        _ => T::String,
    }
}

fn inferred_scalar_type_to_floe_type(kind: InferredScalarType) -> &'static str {
    match kind {
        InferredScalarType::String => "string",
        InferredScalarType::Boolean => "boolean",
        InferredScalarType::Number => "number",
        InferredScalarType::Date => "date",
        InferredScalarType::Time => "time",
        InferredScalarType::DateTime => "datetime",
    }
}

fn detect_json_mode(path: &Path) -> FloeResult<JsonMode> {
    let file = std::fs::File::open(path).map_err(|err| {
        FloeError::io(format!("failed to read json at {}: {err}", path.display()))
    })?;
    let mut reader = BufReader::new(file);
    let mut buf = String::new();
    while reader.read_line(&mut buf)? > 0 {
        let trimmed = buf.trim_start();
        if trimmed.is_empty() {
            buf.clear();
            continue;
        }
        return Ok(if trimmed.starts_with('[') {
            JsonMode::Array
        } else {
            JsonMode::Ndjson
        });
    }
    Err(FloeError::config(format!("json input {} is empty", path.display())).into())
}

fn polars_dtype_to_floe_type(dtype: &DataType) -> String {
    match dtype {
        DataType::Boolean => "boolean".to_string(),
        DataType::Date => "date".to_string(),
        DataType::Time => "time".to_string(),
        DataType::Datetime(_, _) => "datetime".to_string(),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64 => "number".to_string(),
        _ => "string".to_string(),
    }
}

fn arrow_dtype_to_floe_type(dtype: &ArrowDataType) -> String {
    match dtype {
        ArrowDataType::Boolean => "boolean".to_string(),
        ArrowDataType::Date32 | ArrowDataType::Date64 => "date".to_string(),
        ArrowDataType::Time32(_) | ArrowDataType::Time64(_) => "time".to_string(),
        ArrowDataType::Timestamp(_, _) => "datetime".to_string(),
        ArrowDataType::Int8
        | ArrowDataType::Int16
        | ArrowDataType::Int32
        | ArrowDataType::Int64
        | ArrowDataType::UInt8
        | ArrowDataType::UInt16
        | ArrowDataType::UInt32
        | ArrowDataType::UInt64
        | ArrowDataType::Float16
        | ArrowDataType::Float32
        | ArrowDataType::Float64
        | ArrowDataType::Decimal(_, _)
        | ArrowDataType::Decimal256(_, _) => "number".to_string(),
        _ => "string".to_string(),
    }
}

fn append_entity_yaml(config_text: &str, inferred: &InferredEntity) -> FloeResult<String> {
    let mut docs = YamlLoader::load_from_str(config_text)?;
    if docs.len() > 1 {
        return Err(FloeError::config(
            "YAML contains multiple documents; expected one".to_string(),
        )
        .into());
    }
    let mut root = if docs.is_empty() {
        Yaml::Null
    } else {
        docs.swap_remove(0)
    };
    let Yaml::Hash(root_map) = &mut root else {
        return Err(FloeError::config("config root must be a YAML mapping".to_string()).into());
    };

    // 0.3+ entity identity is (domain, name) (#464); older configs compare names only.
    let qualified = root_map
        .get(&yaml_str("version"))
        .and_then(Yaml::as_str)
        .is_some_and(crate::config::qualifies_entity_ids);
    let entities_key = yaml_str("entities");
    let new_entity_value = build_generated_entity_yaml(inferred);

    match root_map.get_mut(&entities_key) {
        Some(value) => {
            let Yaml::Array(entities) = value else {
                return Err(
                    FloeError::config("root.entities must be a YAML sequence".to_string()).into(),
                );
            };
            if entities
                .iter()
                .any(|entity| entity_matches(entity, inferred, qualified))
            {
                return Err(
                    FloeError::config(format!("entity already exists: {}", inferred.name)).into(),
                );
            }
            entities.push(new_entity_value);
        }
        None => {
            root_map.insert(entities_key, Yaml::Array(vec![new_entity_value]));
        }
    }

    let mut rendered = String::new();
    YamlEmitter::new(&mut rendered).dump(&root)?;
    let body = rendered.strip_prefix("---\n").unwrap_or(&rendered);
    Ok(format!("{body}\n"))
}

fn entity_matches(value: &Yaml, inferred: &InferredEntity, qualified: bool) -> bool {
    let field = |key: &str| {
        value
            .as_hash()
            .and_then(|map| map.get(&yaml_str(key)))
            .and_then(Yaml::as_str)
    };
    field("name") == Some(inferred.name.as_str())
        && (!qualified || field("domain") == inferred.domain.as_deref())
}

fn yaml_str(value: impl Into<String>) -> Yaml {
    Yaml::String(value.into())
}

fn build_generated_entity_yaml(inferred: &InferredEntity) -> Yaml {
    let mut entity = YamlHash::new();
    entity.insert(yaml_str("name"), yaml_str(&inferred.name));
    if let Some(domain) = &inferred.domain {
        entity.insert(yaml_str("domain"), yaml_str(domain));
    }

    let mut source = YamlHash::new();
    source.insert(yaml_str("format"), yaml_str(&inferred.format));
    source.insert(yaml_str("path"), yaml_str(&inferred.input));
    let source_options = match inferred.format.as_str() {
        "json" | "csv" => inferred.source_options.as_ref(),
        _ => None,
    };
    if let Some(options) = source_options {
        let mut options_map = YamlHash::new();
        if let Some(separator) = &options.separator {
            options_map.insert(yaml_str("separator"), yaml_str(separator));
        }
        if let Some(json_mode) = &options.json_mode {
            options_map.insert(yaml_str("json_mode"), yaml_str(json_mode));
        }
        source.insert(yaml_str("options"), Yaml::Hash(options_map));
    }
    entity.insert(yaml_str("source"), Yaml::Hash(source));

    let mut accepted = YamlHash::new();
    accepted.insert(yaml_str("format"), yaml_str("parquet"));
    accepted.insert(
        yaml_str("path"),
        yaml_str(format!("out/accepted/{}/", inferred.name)),
    );
    let mut rejected = YamlHash::new();
    rejected.insert(yaml_str("format"), yaml_str("csv"));
    rejected.insert(
        yaml_str("path"),
        yaml_str(format!("out/rejected/{}/", inferred.name)),
    );
    let mut sink = YamlHash::new();
    sink.insert(yaml_str("accepted"), Yaml::Hash(accepted));
    sink.insert(yaml_str("rejected"), Yaml::Hash(rejected));
    entity.insert(yaml_str("sink"), Yaml::Hash(sink));

    let mut policy = YamlHash::new();
    policy.insert(yaml_str("severity"), yaml_str("reject"));
    entity.insert(yaml_str("policy"), Yaml::Hash(policy));

    let mut mismatch = YamlHash::new();
    mismatch.insert(yaml_str("missing_columns"), yaml_str("reject_file"));
    mismatch.insert(yaml_str("extra_columns"), yaml_str("ignore"));
    let mut schema = YamlHash::new();
    schema.insert(yaml_str("mismatch"), Yaml::Hash(mismatch));
    schema.insert(
        yaml_str("columns"),
        Yaml::Array(inferred.columns.iter().map(generated_column_yaml).collect()),
    );
    entity.insert(yaml_str("schema"), Yaml::Hash(schema));

    Yaml::Hash(entity)
}

fn generated_column_yaml(column: &GeneratedColumn) -> Yaml {
    let mut map = YamlHash::new();
    map.insert(yaml_str("name"), yaml_str(&column.name));
    map.insert(yaml_str("source"), yaml_str(&column.source));
    map.insert(yaml_str("type"), yaml_str(&column.column_type));
    map.insert(yaml_str("nullable"), Yaml::Boolean(column.nullable));
    map.insert(yaml_str("unique"), Yaml::Boolean(column.unique));
    Yaml::Hash(map)
}

fn validate_generated_config(yaml: &str, target_path: &Path) -> FloeResult<()> {
    let validation_parent = target_path.parent().unwrap_or_else(|| Path::new("."));
    let mut temp_file =
        NamedTempFile::new_in(validation_parent).or_else(|_| NamedTempFile::new())?;
    use std::io::Write;
    temp_file.write_all(yaml.as_bytes())?;
    crate::validate(temp_file.path(), ValidateOptions::default())
}

fn resolve_local_input_path(input: &str) -> FloeResult<PathBuf> {
    if let Some((scheme, _)) = input.split_once("://") {
        if scheme.eq_ignore_ascii_case("file") {
            let url = Url::parse(input)?;
            return url.to_file_path().map_err(|_| {
                FloeError::config(format!("invalid file:// URI for add-entity input: {input}"))
                    .into()
            });
        }
        return Err(FloeError::config(format!(
            "remote URI inference is not supported yet for add-entity input: {input}"
        ))
        .into());
    }

    let path = PathBuf::from(input);
    let absolute = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()?.join(path)
    };
    let metadata = fs::metadata(&absolute).map_err(|err| {
        FloeError::io(format!(
            "failed to access input at {}: {err}",
            absolute.display()
        ))
    })?;
    if metadata.is_dir() {
        return Err(FloeError::config(format!(
            "add-entity expects a file input for schema inference, got directory: {}",
            absolute.display()
        ))
        .into());
    }
    Ok(absolute)
}

fn normalize_persisted_input_path(input: &str, local_input_path: &Path) -> String {
    if let Some((scheme, _)) = input.split_once("://") {
        if scheme.eq_ignore_ascii_case("file") {
            return local_input_path.display().to_string();
        }
    }
    input.to_string()
}

fn derive_entity_name(input: &str) -> FloeResult<String> {
    let raw = if let Some((scheme, _)) = input.split_once("://") {
        if scheme.eq_ignore_ascii_case("file") {
            let url = Url::parse(input)?;
            let path = url.to_file_path().map_err(|_| {
                FloeError::config(format!("invalid file:// URI for add-entity input: {input}"))
            })?;
            file_name_stem_string(&path)
        } else {
            let trimmed = input.trim_end_matches('/');
            let last = trimmed.rsplit('/').next().unwrap_or(trimmed);
            last.split('?').next().unwrap_or(last).to_string()
        }
    } else {
        file_name_stem_string(Path::new(input))
    };
    let normalized = slugify_entity_name(&raw);
    if normalized.is_empty() {
        return Err(
            FloeError::config(format!("failed to derive entity name from input: {input}")).into(),
        );
    }
    Ok(normalized)
}

fn file_name_stem_string(path: &Path) -> String {
    path.file_stem()
        .or_else(|| path.file_name())
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_else(|| "entity".to_string())
}

fn slugify_entity_name(value: &str) -> String {
    let mut out = String::new();
    let mut last_was_sep = false;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            last_was_sep = false;
        } else if !last_was_sep {
            out.push('_');
            last_was_sep = true;
        }
    }
    out.trim_matches('_').to_string()
}

fn sniff_delimiter(path: &Path) -> Option<char> {
    let file = std::fs::File::open(path).ok()?;
    let reader = BufReader::new(file);
    let mut lines = Vec::new();
    for line in reader.lines().take(12) {
        let line = line.ok()?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        lines.push(trimmed.to_string());
        if lines.len() >= 6 {
            break;
        }
    }
    if lines.is_empty() {
        return None;
    }

    let candidates = [',', ';', '\t', '|'];
    let mut best: Option<(i32, char)> = None;
    for candidate in candidates {
        let counts = lines
            .iter()
            .map(|line| line.chars().filter(|ch| *ch == candidate).count())
            .collect::<Vec<_>>();
        let first = *counts.first().unwrap_or(&0);
        let min = *counts.iter().min().unwrap_or(&0);
        let max = *counts.iter().max().unwrap_or(&0);
        let sum = counts.iter().sum::<usize>();
        if first == 0 || sum == 0 {
            continue;
        }
        let consistency_bonus = if min > 0 && max == min { 10_000 } else { 0 };
        let score = consistency_bonus + (min as i32 * 100) + sum as i32;
        if best
            .map(|(best_score, _)| score > best_score)
            .unwrap_or(true)
        {
            best = Some((score, candidate));
        }
    }
    best.map(|(_, candidate)| candidate)
}
