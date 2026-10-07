use crate::errors::FloeError;
use crate::secret::Secret;
use std::collections::HashMap;
use std::path::Path;

use yaml_rust2::yaml::Hash;
use yaml_rust2::Yaml;

use crate::config::apply_templates_with_vars;
use crate::config::storage::resolve_local_path;
use crate::config::types::{DeltaSinkTargetConfig, DuckDbSinkTargetConfig};
use crate::config::yaml_decode::{
    hash_get, load_yaml, validate_known_keys, yaml_array, yaml_hash, yaml_string,
};
use crate::config::{
    ArchiveTarget, CatalogDefinition, CatalogTypeConfig, CatalogsConfig, ColumnConfig,
    DomainConfig, EntityConfig, EntityMetadata, EntityStateConfig, EnvConfig,
    IcebergPartitionFieldConfig, IcebergSinkTargetConfig, IncrementalMode, LineageConfig,
    MergeOptionsConfig, MergeScd2OptionsConfig, NormalizeColumnsConfig, PiiColumnConfig, PiiConfig,
    PiiStrategy, PolicyConfig, PolicySeverity, ProjectMetadata, ReportConfig, RootConfig,
    SchemaConfig, SchemaEvolutionConfig, SchemaEvolutionIncompatibleAction, SchemaEvolutionMode,
    SchemaMismatchConfig, SinkConfig, SinkOptions, SinkTarget, SourceConfig, SourceOptions,
    StorageDefinition, StoragesConfig, WriteMode,
};
use crate::FloeResult;

pub(crate) fn parse_config(path: &Path) -> FloeResult<RootConfig> {
    parse_config_with_vars(path, &std::collections::HashMap::new())
}

/// Read `env.file` and `env.vars` from a config file without full parsing or
/// template substitution.  Used to seed `VarSources.config` when resolving
/// profile variables so that config-level overrides (both sources) are
/// respected during `${REF}` expansion inside profile variable values.
///
/// Priority matches `build_env_vars`: `env.vars` overwrites `env.file`.
pub fn extract_raw_env_vars(path: &Path) -> FloeResult<HashMap<String, String>> {
    let docs = load_yaml(path)?;
    if docs.is_empty() {
        return Ok(HashMap::new());
    }
    let root = match yaml_hash(&docs[0], "root") {
        Ok(h) => h,
        Err(_) => return Ok(HashMap::new()),
    };
    let env_node = match hash_get(root, "env") {
        Some(n) => n,
        None => return Ok(HashMap::new()),
    };
    let env_hash = match yaml_hash(env_node, "env") {
        Ok(h) => h,
        Err(_) => return Ok(HashMap::new()),
    };

    let config_dir = path.parent().unwrap_or_else(|| Path::new("."));
    let mut vars = HashMap::new();

    // env.file — lower priority, loaded first
    if let Some(file_node) = hash_get(env_hash, "file") {
        if let Ok(file_str) = yaml_string(file_node, "env.file") {
            let file_path = resolve_local_path(config_dir, &file_str);
            if let Ok(file_docs) = load_yaml(&file_path) {
                if let Some(doc) = file_docs.first() {
                    if let Ok(h) = yaml_hash(doc, "env.file") {
                        for (k, v) in h {
                            if let (Ok(key), Ok(val)) =
                                (yaml_string(k, "env.file"), yaml_string(v, "env.file"))
                            {
                                vars.insert(key, val);
                            }
                        }
                    }
                }
            }
        }
    }

    // env.vars — higher priority, overwrites env.file
    if let Some(vars_node) = hash_get(env_hash, "vars") {
        if let Ok(inline) = parse_string_map(vars_node, "env.vars") {
            vars.extend(inline);
        }
    }

    Ok(vars)
}

pub(crate) fn parse_config_with_vars(
    path: &Path,
    profile_vars: &std::collections::HashMap<String, String>,
) -> FloeResult<RootConfig> {
    let docs = load_yaml(path)?;
    if docs.is_empty() {
        return Err(FloeError::config("YAML is empty".to_string()).into());
    }
    if docs.len() > 1 {
        return Err(FloeError::config(
            "YAML contains multiple documents; expected one".to_string(),
        )
        .into());
    }
    let mut config = parse_root(&docs[0])?;
    let config_dir = path.parent().unwrap_or_else(|| Path::new("."));
    apply_templates_with_vars(&mut config, config_dir, profile_vars)?;
    Ok(config)
}

fn parse_root(doc: &Yaml) -> FloeResult<RootConfig> {
    let root = yaml_hash(doc, "root")?;
    validate_known_keys(
        root,
        "root",
        &[
            "version",
            "metadata",
            "storages",
            "filesystems",
            "catalogs",
            "env",
            "domains",
            "report",
            "lineage",
            "entities",
        ],
    )?;
    let version = get_string(root, "version", "root")?;

    let metadata = match hash_get(root, "metadata") {
        Some(value) => Some(parse_project_metadata(value)?),
        None => None,
    };

    let storages = match (hash_get(root, "storages"), hash_get(root, "filesystems")) {
        (Some(_), Some(_)) => {
            return Err(FloeError::config(
                "root.storages and root.filesystems are mutually exclusive".to_string(),
            )
            .into())
        }
        (Some(value), None) => Some(parse_storages(value)?),
        (None, Some(value)) => Some(parse_storages(value)?),
        (None, None) => None,
    };

    let env = match hash_get(root, "env") {
        Some(value) => Some(parse_env(value)?),
        None => None,
    };
    let catalogs = match hash_get(root, "catalogs") {
        Some(value) => Some(parse_catalogs(value)?),
        None => None,
    };

    let domains = match hash_get(root, "domains") {
        Some(value) => parse_domains(value)?,
        None => Vec::new(),
    };

    let report = match hash_get(root, "report") {
        Some(value) => Some(parse_report_config(value)?),
        None => Some(ReportConfig {
            path: "report".to_string(),
            formatter: None,
            storage: None,
        }),
    };
    let lineage = match hash_get(root, "lineage") {
        Some(value) => Some(parse_lineage_config(value)?),
        None => None,
    };
    let entities_yaml = get_array(root, "entities", "root")?;
    let mut entities = Vec::with_capacity(entities_yaml.len());
    for (index, entity_yaml) in entities_yaml.iter().enumerate() {
        let name_hint = entity_name_hint(entity_yaml);
        let entity = parse_entity(entity_yaml).map_err(|err| {
            FloeError::config(format_entity_error(index, name_hint, err.as_ref()))
        })?;
        entities.push(entity);
    }

    Ok(RootConfig {
        version,
        metadata,
        storages,
        catalogs,
        env,
        domains,
        report,
        lineage,
        entities,
    })
}

fn parse_project_metadata(value: &Yaml) -> FloeResult<ProjectMetadata> {
    let hash = yaml_hash(value, "metadata")?;
    Ok(ProjectMetadata {
        project: opt_string(hash, "project", "metadata")?,
        description: opt_string(hash, "description", "metadata")?,
        owner: opt_string(hash, "owner", "metadata")?,
        tags: opt_vec_string(hash, "tags", "metadata")?,
    })
}

fn parse_entity(value: &Yaml) -> FloeResult<EntityConfig> {
    let hash = yaml_hash(value, "entity")?;
    validate_known_keys(
        hash,
        "entity",
        &[
            "name",
            "metadata",
            "domain",
            "incremental_mode",
            "state",
            "source",
            "sink",
            "policy",
            "schema",
            "pii",
        ],
    )?;
    let name = get_string(hash, "name", "entity")?;

    let metadata = match hash_get(hash, "metadata") {
        Some(value) => Some(parse_entity_metadata(value)?),
        None => None,
    };
    let domain = opt_string(hash, "domain", "entity")?;
    let incremental_mode = match opt_string(hash, "incremental_mode", "entity")? {
        Some(value) => parse_incremental_mode(&value, "entity.incremental_mode")?,
        None => IncrementalMode::default(),
    };
    let state = match hash_get(hash, "state") {
        Some(value) => Some(parse_entity_state(value)?),
        None => None,
    };

    let source = parse_source(get_value(hash, "source", "entity")?)?;
    let sink = parse_sink(get_value(hash, "sink", "entity")?)?;
    let policy = parse_policy(get_value(hash, "policy", "entity")?)?;
    let schema = parse_schema(get_value(hash, "schema", "entity")?)?;
    let pii = match hash_get(hash, "pii") {
        Some(value) => Some(parse_pii_config(value)?),
        None => None,
    };

    Ok(EntityConfig {
        name,
        metadata,
        domain,
        incremental_mode,
        state,
        source,
        sink,
        policy,
        schema,
        pii,
    })
}

fn entity_name_hint(value: &Yaml) -> Option<String> {
    let hash = value.as_hash()?;
    let name = hash_get(hash, "name")?;
    match name {
        Yaml::String(value) => Some(value.clone()),
        _ => None,
    }
}

fn format_entity_error(index: usize, name: Option<String>, err: &dyn std::error::Error) -> String {
    match name {
        Some(name) => format!("entities[{index}] (entity.name={name}): {err}"),
        None => format!("entities[{index}]: {err}"),
    }
}

fn parse_entity_metadata(value: &Yaml) -> FloeResult<EntityMetadata> {
    let hash = yaml_hash(value, "entity.metadata")?;
    Ok(EntityMetadata {
        data_product: opt_string(hash, "data_product", "entity.metadata")?,
        domain: opt_string(hash, "domain", "entity.metadata")?,
        owner: opt_string(hash, "owner", "entity.metadata")?,
        description: opt_string(hash, "description", "entity.metadata")?,
        tags: opt_vec_string(hash, "tags", "entity.metadata")?,
    })
}

fn parse_entity_state(value: &Yaml) -> FloeResult<EntityStateConfig> {
    let hash = yaml_hash(value, "entity.state")?;
    validate_known_keys(hash, "entity.state", &["path"])?;
    Ok(EntityStateConfig {
        path: opt_string(hash, "path", "entity.state")?,
    })
}

fn parse_env(value: &Yaml) -> FloeResult<EnvConfig> {
    let hash = yaml_hash(value, "env")?;
    validate_known_keys(hash, "env", &["file", "vars"])?;
    let file = opt_string(hash, "file", "env")?;
    let vars = match hash_get(hash, "vars") {
        Some(value) => parse_string_map(value, "env.vars")?,
        None => HashMap::new(),
    };
    Ok(EnvConfig { file, vars })
}

fn parse_domains(value: &Yaml) -> FloeResult<Vec<DomainConfig>> {
    let array = yaml_array(value, "domains")?;
    let mut domains = Vec::with_capacity(array.len());
    for item in array.iter() {
        let hash = yaml_hash(item, "domains")?;
        validate_known_keys(hash, "domains", &["name", "incoming_dir"])?;
        let name = get_string(hash, "name", "domains")?;
        let incoming_dir = get_string(hash, "incoming_dir", "domains")?;
        domains.push(DomainConfig {
            name,
            incoming_dir,
            resolved_incoming_dir: None,
        });
    }
    Ok(domains)
}

fn parse_string_map(value: &Yaml, context: &str) -> FloeResult<HashMap<String, String>> {
    let hash = yaml_hash(value, context)?;
    let mut map = HashMap::new();
    for (key, val) in hash {
        let key_str = yaml_string(key, context)?;
        let value_str = yaml_string(val, context)?;
        map.insert(key_str, value_str);
    }
    Ok(map)
}

fn parse_incremental_mode(value: &str, ctx: &str) -> FloeResult<IncrementalMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "none" => Ok(IncrementalMode::None),
        "archive" => Ok(IncrementalMode::Archive),
        "file" => Ok(IncrementalMode::File),
        "row" => Ok(IncrementalMode::Row),
        _ => Err(FloeError::config(format!(
            "unsupported value at {ctx}: {value} (allowed: none, archive, file, row)"
        ))
        .into()),
    }
}

fn parse_source(value: &Yaml) -> FloeResult<SourceConfig> {
    let hash = yaml_hash(value, "source")?;
    validate_known_keys(
        hash,
        "source",
        &[
            "format",
            "path",
            "storage",
            "filesystem",
            "options",
            "cast_mode",
        ],
    )?;
    let format = get_string(hash, "format", "source")?;
    let defaults = SourceOptions::defaults_for_format(format.as_str());
    let options = match hash_get(hash, "options") {
        Some(value) => Some(parse_source_options(value, &defaults)?),
        None => Some(defaults),
    };

    let storage = opt_string(hash, "storage", "source")?;
    let filesystem = opt_string(hash, "filesystem", "source")?;
    if storage.is_some() && filesystem.is_some() {
        return Err(FloeError::config(
            "source.storage and source.storage are mutually exclusive".to_string(),
        )
        .into());
    }

    Ok(SourceConfig {
        format,
        path: get_string(hash, "path", "source")?,
        storage: storage.or(filesystem),
        options,
        cast_mode: opt_string(hash, "cast_mode", "source")?.or(Some("strict".to_string())),
    })
}

fn parse_source_options(value: &Yaml, defaults: &SourceOptions) -> FloeResult<SourceOptions> {
    let hash = yaml_hash(value, "source.options")?;
    validate_known_keys(
        hash,
        "source.options",
        &[
            "header",
            "separator",
            "encoding",
            "null_values",
            "recursive",
            "glob",
            "json_mode",
            "sheet",
            "header_row",
            "data_row",
            "row_tag",
            "namespace",
            "value_tag",
        ],
    )?;
    Ok(SourceOptions {
        header: opt_bool(hash, "header", "source.options")?.or(defaults.header),
        separator: opt_string(hash, "separator", "source.options")?.or(defaults.separator.clone()),
        encoding: opt_string(hash, "encoding", "source.options")?.or(defaults.encoding.clone()),
        null_values: opt_vec_string(hash, "null_values", "source.options")?
            .or(defaults.null_values.clone()),
        recursive: opt_bool(hash, "recursive", "source.options")?.or(defaults.recursive),
        glob: opt_string(hash, "glob", "source.options")?.or(defaults.glob.clone()),
        json_mode: opt_string(hash, "json_mode", "source.options")?.or(defaults.json_mode.clone()),
        sheet: opt_string(hash, "sheet", "source.options")?.or(defaults.sheet.clone()),
        header_row: opt_u64(hash, "header_row", "source.options")?.or(defaults.header_row),
        data_row: opt_u64(hash, "data_row", "source.options")?.or(defaults.data_row),
        row_tag: opt_string(hash, "row_tag", "source.options")?.or(defaults.row_tag.clone()),
        namespace: opt_string(hash, "namespace", "source.options")?.or(defaults.namespace.clone()),
        value_tag: opt_string(hash, "value_tag", "source.options")?.or(defaults.value_tag.clone()),
    })
}

fn parse_sink(value: &Yaml) -> FloeResult<SinkConfig> {
    let hash = yaml_hash(value, "sink")?;
    validate_known_keys(
        hash,
        "sink",
        &["write_mode", "accepted", "rejected", "archive"],
    )?;
    let write_mode = match opt_string(hash, "write_mode", "sink")? {
        Some(value) => parse_write_mode(&value, "sink.write_mode")?,
        None => WriteMode::default(),
    };
    let rejected = match hash_get(hash, "rejected") {
        Some(value) => Some(parse_sink_target(
            value,
            "sink.rejected",
            false,
            write_mode,
        )?),
        None => None,
    };
    let archive = match hash_get(hash, "archive") {
        Some(value) => Some(parse_archive_target(value)?),
        None => None,
    };

    Ok(SinkConfig {
        write_mode,
        accepted: parse_sink_target(
            get_value(hash, "accepted", "sink")?,
            "sink.accepted",
            true,
            write_mode,
        )?,
        rejected,
        archive,
    })
}

fn parse_sink_target(
    value: &Yaml,
    ctx: &str,
    allow_options: bool,
    write_mode: WriteMode,
) -> FloeResult<SinkTarget> {
    let hash = yaml_hash(value, ctx)?;
    let mut allowed = vec!["format", "path", "storage", "filesystem"];
    if allow_options {
        allowed.extend([
            "options",
            "merge",
            "iceberg",
            "delta",
            "duckdb",
            "partition_by",
            "partition_spec",
        ]);
    }
    validate_known_keys(hash, ctx, &allowed)?;
    let storage = opt_string(hash, "storage", ctx)?;
    let filesystem = opt_string(hash, "filesystem", ctx)?;
    if storage.is_some() && filesystem.is_some() {
        return Err(FloeError::config(format!(
            "{ctx}.storage and {ctx}.storage are mutually exclusive"
        ))
        .into());
    }
    let options = if allow_options {
        match hash_get(hash, "options") {
            Some(value) => Some(parse_sink_options(value, &format!("{ctx}.options"))?),
            None => None,
        }
    } else {
        None
    };
    let merge = if allow_options {
        match hash_get(hash, "merge") {
            Some(value) => Some(parse_merge_options(value, &format!("{ctx}.merge"))?),
            None => None,
        }
    } else {
        None
    };
    let partition_by = if allow_options {
        opt_vec_string(hash, "partition_by", ctx)?
    } else {
        None
    };
    let iceberg = if allow_options {
        match hash_get(hash, "iceberg") {
            Some(value) => Some(parse_sink_iceberg_options(
                value,
                &format!("{ctx}.iceberg"),
            )?),
            None => None,
        }
    } else {
        None
    };
    let delta = if allow_options {
        match hash_get(hash, "delta") {
            Some(value) => Some(parse_sink_delta_options(value, &format!("{ctx}.delta"))?),
            None => None,
        }
    } else {
        None
    };
    let format = get_string(hash, "format", ctx)?;
    let duckdb = if allow_options {
        match hash_get(hash, "duckdb") {
            Some(value) => {
                // A `duckdb:` block only applies to a DuckDB sink. Reject it for any
                // other format so a misplaced block can't be silently ignored.
                if format != "duckdb" {
                    return Err(FloeError::config(format!(
                        "{ctx}.duckdb is only valid when format is \"duckdb\" (found format={format:?})"
                    )).into());
                }
                Some(parse_sink_duckdb_options(value, &format!("{ctx}.duckdb"))?)
            }
            None => None,
        }
    } else {
        None
    };
    let partition_spec = if allow_options {
        match hash_get(hash, "partition_spec") {
            Some(value) => Some(parse_iceberg_partition_spec(
                value,
                &format!("{ctx}.partition_spec"),
            )?),
            None => None,
        }
    } else {
        None
    };
    // A MotherDuck DuckDB sink is addressed by `duckdb.connection`, not a filesystem path,
    // so `path` is optional in that case. Every other sink (and a local-file DuckDB sink)
    // requires `path`. Gate this on the DuckDB format explicitly rather than the mere
    // presence of a `duckdb.connection` block.
    let is_motherduck_duckdb = format == "duckdb"
        && duckdb
            .as_ref()
            .and_then(|cfg| cfg.connection.as_deref())
            .is_some();
    let path = if is_motherduck_duckdb {
        opt_string(hash, "path", ctx)?.unwrap_or_default()
    } else {
        get_string(hash, "path", ctx)?
    };
    Ok(SinkTarget {
        format,
        path,
        storage: storage.or(filesystem),
        options,
        merge,
        iceberg,
        delta,
        duckdb,
        partition_by,
        partition_spec,
        write_mode,
    })
}

fn parse_write_mode(value: &str, ctx: &str) -> FloeResult<WriteMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "overwrite" => Ok(WriteMode::Overwrite),
        "append" => Ok(WriteMode::Append),
        "merge_scd1" => Ok(WriteMode::MergeScd1),
        "merge_scd2" => Ok(WriteMode::MergeScd2),
        _ => Err(FloeError::config(format!(
            "unsupported value at {ctx}: {value} (allowed: overwrite, append, merge_scd1, merge_scd2)"
        )).into()),
    }
}

fn parse_sink_options(value: &Yaml, ctx: &str) -> FloeResult<SinkOptions> {
    let hash = yaml_hash(value, ctx)?;
    validate_known_keys(
        hash,
        ctx,
        &["compression", "row_group_size", "max_size_per_file"],
    )?;
    Ok(SinkOptions {
        compression: opt_string(hash, "compression", ctx)?,
        row_group_size: opt_u64(hash, "row_group_size", ctx)?,
        max_size_per_file: opt_u64(hash, "max_size_per_file", ctx)?,
    })
}

fn parse_merge_options(value: &Yaml, ctx: &str) -> FloeResult<MergeOptionsConfig> {
    let hash = yaml_hash(value, ctx)?;
    validate_known_keys(hash, ctx, &["ignore_columns", "compare_columns", "scd2"])?;
    let scd2 = match hash_get(hash, "scd2") {
        Some(value) => Some(parse_merge_scd2_options(value, &format!("{ctx}.scd2"))?),
        None => None,
    };
    Ok(MergeOptionsConfig {
        ignore_columns: opt_vec_string(hash, "ignore_columns", ctx)?,
        compare_columns: opt_vec_string(hash, "compare_columns", ctx)?,
        scd2,
    })
}

fn parse_merge_scd2_options(value: &Yaml, ctx: &str) -> FloeResult<MergeScd2OptionsConfig> {
    let hash = yaml_hash(value, ctx)?;
    validate_known_keys(
        hash,
        ctx,
        &[
            "current_flag_column",
            "valid_from_column",
            "valid_to_column",
        ],
    )?;
    Ok(MergeScd2OptionsConfig {
        current_flag_column: opt_string(hash, "current_flag_column", ctx)?,
        valid_from_column: opt_string(hash, "valid_from_column", ctx)?,
        valid_to_column: opt_string(hash, "valid_to_column", ctx)?,
    })
}

fn parse_iceberg_partition_spec(
    value: &Yaml,
    ctx: &str,
) -> FloeResult<Vec<IcebergPartitionFieldConfig>> {
    let items = yaml_array(value, ctx)?;
    let mut fields = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let item_ctx = format!("{ctx}[{index}]");
        let hash = yaml_hash(item, &item_ctx)?;
        validate_known_keys(hash, &item_ctx, &["column", "transform"])?;
        let column = get_string(hash, "column", &item_ctx)?;
        let transform =
            opt_string(hash, "transform", &item_ctx)?.unwrap_or_else(|| "identity".to_string());
        fields.push(IcebergPartitionFieldConfig { column, transform });
    }
    Ok(fields)
}

fn parse_report_config(value: &Yaml) -> FloeResult<ReportConfig> {
    let hash = yaml_hash(value, "report")?;
    validate_known_keys(hash, "report", &["path", "formatter", "storage"])?;
    let path = opt_string(hash, "path", "report")?.unwrap_or_else(|| "report".to_string());
    Ok(ReportConfig {
        path,
        formatter: opt_string(hash, "formatter", "report")?,
        storage: opt_string(hash, "storage", "report")?,
    })
}

fn parse_sink_iceberg_options(value: &Yaml, ctx: &str) -> FloeResult<IcebergSinkTargetConfig> {
    let hash = yaml_hash(value, ctx)?;
    validate_known_keys(hash, ctx, &["catalog", "namespace", "table", "location"])?;
    Ok(IcebergSinkTargetConfig {
        catalog: opt_string(hash, "catalog", ctx)?,
        namespace: opt_string(hash, "namespace", ctx)?,
        table: opt_string(hash, "table", ctx)?,
        location: opt_string(hash, "location", ctx)?,
    })
}

fn parse_sink_delta_options(value: &Yaml, ctx: &str) -> FloeResult<DeltaSinkTargetConfig> {
    let hash = yaml_hash(value, ctx)?;
    validate_known_keys(hash, ctx, &["catalog", "schema", "table"])?;
    Ok(DeltaSinkTargetConfig {
        catalog: opt_string(hash, "catalog", ctx)?,
        schema: opt_string(hash, "schema", ctx)?,
        table: opt_string(hash, "table", ctx)?,
    })
}

fn parse_sink_duckdb_options(value: &Yaml, ctx: &str) -> FloeResult<DuckDbSinkTargetConfig> {
    let hash = yaml_hash(value, ctx)?;
    validate_known_keys(hash, ctx, &["table", "schema", "connection", "token"])?;
    // Normalize blank/whitespace-only `connection` and `token` to `None` so the
    // "is this a MotherDuck target?" check is consistent everywhere (path-optional
    // parsing, validation, and run/manifest resolution). A bare `connection: ""`
    // must behave exactly like an omitted connection — a local-file target — rather
    // than making `path` optional yet failing later at runtime.
    let blank_to_none = |value: Option<String>| value.filter(|s| !s.trim().is_empty());
    Ok(DuckDbSinkTargetConfig {
        table: get_string(hash, "table", ctx)?,
        schema: opt_string(hash, "schema", ctx)?,
        connection: blank_to_none(opt_string(hash, "connection", ctx)?),
        token: blank_to_none(opt_string(hash, "token", ctx)?).map(Secret::from),
    })
}

pub(crate) fn parse_storages(value: &Yaml) -> FloeResult<StoragesConfig> {
    let hash = yaml_hash(value, "storages")?;
    validate_known_keys(hash, "storages", &["default", "definitions"])?;
    let definitions_yaml = match hash_get(hash, "definitions") {
        Some(value) => yaml_array(value, "storages.definitions")?,
        None => {
            return Err(FloeError::config(
                "missing required field storages.definitions".to_string(),
            )
            .into())
        }
    };
    let mut definitions = Vec::with_capacity(definitions_yaml.len());
    for (index, item) in definitions_yaml.iter().enumerate() {
        let definition = parse_storage_definition(item)
            .map_err(|err| FloeError::config(format!("storages.definitions[{index}]: {err}")))?;
        definitions.push(definition);
    }
    Ok(StoragesConfig {
        default: opt_string(hash, "default", "storages")?,
        definitions,
    })
}

fn parse_storage_definition(value: &Yaml) -> FloeResult<StorageDefinition> {
    let hash = yaml_hash(value, "storages.definitions")?;
    validate_known_keys(
        hash,
        "storages.definitions",
        &[
            "name",
            "type",
            "bucket",
            "region",
            "account",
            "container",
            "prefix",
            "endpoint",
            "path_style_access",
        ],
    )?;
    Ok(StorageDefinition {
        name: get_string(hash, "name", "storages.definitions")?,
        fs_type: get_string(hash, "type", "storages.definitions")?,
        bucket: opt_string(hash, "bucket", "storages.definitions")?,
        region: opt_string(hash, "region", "storages.definitions")?,
        account: opt_string(hash, "account", "storages.definitions")?,
        container: opt_string(hash, "container", "storages.definitions")?,
        prefix: opt_string(hash, "prefix", "storages.definitions")?,
        endpoint: opt_string(hash, "endpoint", "storages.definitions")?,
        path_style_access: opt_bool(hash, "path_style_access", "storages.definitions")?,
    })
}

pub(crate) fn parse_catalogs_with_context(
    value: &Yaml,
    context: &str,
) -> FloeResult<CatalogsConfig> {
    let definitions_context = format!("{context}.definitions");
    let hash = yaml_hash(value, context)?;
    validate_known_keys(hash, context, &["default", "definitions"])?;
    let definitions_yaml = match hash_get(hash, "definitions") {
        Some(value) => yaml_array(value, &definitions_context)?,
        None => {
            return Err(
                FloeError::config(format!("missing required field {definitions_context}")).into(),
            )
        }
    };
    let mut definitions = Vec::with_capacity(definitions_yaml.len());
    for (index, item) in definitions_yaml.iter().enumerate() {
        let definition = parse_catalog_definition(item, &definitions_context)
            .map_err(|err| FloeError::config(format!("{definitions_context}[{index}]: {err}")))?;
        definitions.push(definition);
    }
    Ok(CatalogsConfig {
        default: opt_string(hash, "default", context)?,
        definitions,
    })
}

fn parse_catalogs(value: &Yaml) -> FloeResult<CatalogsConfig> {
    parse_catalogs_with_context(value, "catalogs")
}

fn parse_catalog_definition(value: &Yaml, context: &str) -> FloeResult<CatalogDefinition> {
    let hash = yaml_hash(value, context)?;
    validate_known_keys(
        hash,
        context,
        &[
            "name",
            "type",
            // Glue-specific:
            "region",
            "database",
            "create_database_if_missing",
            "allow_takeover",
            // REST-specific:
            "uri",
            "credential",
            "warehouse",
            "oauth2_server_uri",
            "scope",
            // Shared:
            "warehouse_storage",
            "warehouse_prefix",
            "host",
            "catalog",
            "schema",
            "token",
            "create_schema_if_missing",
        ],
    )?;
    let name = get_string(hash, "name", context)?;
    let catalog_type = get_string(hash, "type", context)?;
    let type_config = parse_catalog_type_config(&name, &catalog_type, hash, context)?;
    Ok(CatalogDefinition {
        name,
        type_config,
        warehouse_storage: opt_string(hash, "warehouse_storage", context)?,
        warehouse_prefix: opt_string(hash, "warehouse_prefix", context)?,
    })
}

fn parse_catalog_type_config(
    name: &str,
    catalog_type: &str,
    hash: &yaml_rust2::yaml::Hash,
    context: &str,
) -> FloeResult<CatalogTypeConfig> {
    match catalog_type {
        "glue" => Ok(CatalogTypeConfig::Glue {
            region: get_string(hash, "region", context)?,
            database: get_string(hash, "database", context)?,
            create_database_if_missing: opt_bool(hash, "create_database_if_missing", context)?
                .unwrap_or(true),
            allow_takeover: opt_bool(hash, "allow_takeover", context)?.unwrap_or(false),
        }),
        "rest" => Ok(CatalogTypeConfig::Rest {
            uri: get_string(hash, "uri", context)?,
            credential: opt_string(hash, "credential", context)?.map(Secret::from),
            warehouse: opt_string(hash, "warehouse", context)?,
            oauth2_server_uri: opt_string(hash, "oauth2_server_uri", context)?,
            scope: opt_string(hash, "scope", context)?,
        }),
        "unity" => Ok(CatalogTypeConfig::Unity {
            host: get_string(hash, "host", context)?,
            catalog: get_string(hash, "catalog", context)?,
            schema: get_string(hash, "schema", context)?,
            token: Secret::from(get_string(hash, "token", context)?),
            create_schema_if_missing: opt_bool(hash, "create_schema_if_missing", context)?
                .unwrap_or(false),
        }),
        other => Err(crate::errors::FloeError::config(format!(
            "{context} name={name} has unsupported type={other} (supported: glue, rest, unity)"
        ))
        .into()),
    }
}

fn parse_archive_target(value: &Yaml) -> FloeResult<ArchiveTarget> {
    let hash = yaml_hash(value, "sink.archive")?;
    validate_known_keys(hash, "sink.archive", &["path", "storage"])?;
    let path = opt_string(hash, "path", "sink.archive")?.unwrap_or_else(|| "archive".to_string());
    Ok(ArchiveTarget {
        path,
        storage: opt_string(hash, "storage", "sink.archive")?,
    })
}

fn parse_policy(value: &Yaml) -> FloeResult<PolicyConfig> {
    let hash = yaml_hash(value, "policy")?;
    validate_known_keys(hash, "policy", &["severity"])?;
    let severity_str = get_string(hash, "severity", "policy")?;
    let severity = match severity_str.as_str() {
        "warn" => PolicySeverity::Warn,
        "reject" => PolicySeverity::Reject,
        "abort" => PolicySeverity::Abort,
        other => {
            return Err(FloeError::config(format!(
                "policy.severity={other} is unsupported (allowed: warn, reject, abort)"
            ))
            .into())
        }
    };
    Ok(PolicyConfig { severity })
}

fn parse_schema(value: &Yaml) -> FloeResult<SchemaConfig> {
    let hash = yaml_hash(value, "schema")?;
    validate_known_keys(
        hash,
        "schema",
        &[
            "normalize_columns",
            "mismatch",
            "schema_evolution",
            "primary_key",
            "unique_keys",
            "columns",
        ],
    )?;
    let normalize_columns = match hash_get(hash, "normalize_columns") {
        Some(value) => Some(parse_normalize_columns(value)?),
        None => None,
    };
    let mismatch = match hash_get(hash, "mismatch") {
        Some(value) => Some(parse_mismatch(value)?),
        None => None,
    };
    let schema_evolution = match hash_get(hash, "schema_evolution") {
        Some(value) => Some(parse_schema_evolution(value)?),
        None => None,
    };
    let primary_key = opt_vec_string(hash, "primary_key", "schema")?;
    let unique_keys = opt_vec_vec_string(hash, "unique_keys", "schema")?;
    let columns_yaml = get_array(hash, "columns", "schema")?;

    let mut columns = Vec::with_capacity(columns_yaml.len());
    for (index, column_yaml) in columns_yaml.iter().enumerate() {
        let column = parse_column(column_yaml)
            .map_err(|err| FloeError::config(format!("schema.columns[{index}]: {err}")))?;
        columns.push(column);
    }

    Ok(SchemaConfig {
        normalize_columns,
        mismatch,
        schema_evolution,
        primary_key,
        unique_keys,
        columns,
    })
}

fn parse_normalize_columns(value: &Yaml) -> FloeResult<NormalizeColumnsConfig> {
    let hash = yaml_hash(value, "schema.normalize_columns")?;
    validate_known_keys(hash, "schema.normalize_columns", &["enabled", "strategy"])?;
    Ok(NormalizeColumnsConfig {
        enabled: opt_bool(hash, "enabled", "schema.normalize_columns")?,
        strategy: opt_string(hash, "strategy", "schema.normalize_columns")?,
    })
}

fn parse_mismatch(value: &Yaml) -> FloeResult<SchemaMismatchConfig> {
    let hash = yaml_hash(value, "schema.mismatch")?;
    validate_known_keys(
        hash,
        "schema.mismatch",
        &["missing_columns", "extra_columns"],
    )?;
    Ok(SchemaMismatchConfig {
        missing_columns: opt_string(hash, "missing_columns", "schema.mismatch")?,
        extra_columns: opt_string(hash, "extra_columns", "schema.mismatch")?,
    })
}

fn parse_schema_evolution(value: &Yaml) -> FloeResult<SchemaEvolutionConfig> {
    let hash = yaml_hash(value, "schema.schema_evolution")?;
    validate_known_keys(
        hash,
        "schema.schema_evolution",
        &["mode", "on_incompatible"],
    )?;
    let mode = match opt_string(hash, "mode", "schema.schema_evolution")? {
        Some(value) => parse_schema_evolution_mode(&value, "schema.schema_evolution.mode")?,
        None => SchemaEvolutionMode::Strict,
    };
    let on_incompatible = match opt_string(hash, "on_incompatible", "schema.schema_evolution")? {
        Some(value) => parse_schema_evolution_incompatible_action(
            &value,
            "schema.schema_evolution.on_incompatible",
        )?,
        None => SchemaEvolutionIncompatibleAction::Fail,
    };
    Ok(SchemaEvolutionConfig {
        mode,
        on_incompatible,
    })
}

fn parse_schema_evolution_mode(value: &str, ctx: &str) -> FloeResult<SchemaEvolutionMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "strict" => Ok(SchemaEvolutionMode::Strict),
        "add_columns" => Ok(SchemaEvolutionMode::AddColumns),
        _ => Err(FloeError::config(format!(
            "unsupported value at {ctx}: {value} (allowed: strict, add_columns)"
        ))
        .into()),
    }
}

fn parse_schema_evolution_incompatible_action(
    value: &str,
    ctx: &str,
) -> FloeResult<SchemaEvolutionIncompatibleAction> {
    match value.trim().to_ascii_lowercase().as_str() {
        "fail" => Ok(SchemaEvolutionIncompatibleAction::Fail),
        _ => Err(FloeError::config(format!(
            "unsupported value at {ctx}: {value} (allowed: fail)"
        ))
        .into()),
    }
}

fn parse_column(value: &Yaml) -> FloeResult<ColumnConfig> {
    let hash = yaml_hash(value, "schema.columns")?;
    validate_known_keys(
        hash,
        "schema.columns",
        &[
            "name", "source", "type", "nullable", "unique", "width", "trim",
        ],
    )?;
    let name = get_string(hash, "name", "schema.columns")?;
    let source = opt_string(hash, "source", "schema.columns")?;
    Ok(ColumnConfig {
        name: name.clone(),
        source,
        column_type: get_string(hash, "type", "schema.columns")?,
        nullable: opt_bool(hash, "nullable", "schema.columns")?,
        unique: opt_bool(hash, "unique", "schema.columns")?,
        width: opt_u64(hash, "width", "schema.columns")?,
        trim: opt_bool(hash, "trim", "schema.columns")?,
    })
}

fn get_value<'a>(hash: &'a Hash, key: &str, ctx: &str) -> FloeResult<&'a Yaml> {
    hash_get(hash, key)
        .ok_or_else(|| FloeError::config(format!("missing required field {ctx}.{key}")).into())
}

fn get_string(hash: &Hash, key: &str, ctx: &str) -> FloeResult<String> {
    let value = get_value(hash, key, ctx)?;
    yaml_string(value, &format!("{ctx}.{key}"))
}

fn get_array<'a>(hash: &'a Hash, key: &str, ctx: &str) -> FloeResult<&'a Vec<Yaml>> {
    let value = get_value(hash, key, ctx)?;
    yaml_array(value, &format!("{ctx}.{key}"))
}

fn opt_string(hash: &Hash, key: &str, ctx: &str) -> FloeResult<Option<String>> {
    match hash_get(hash, key) {
        None | Some(Yaml::Null) | Some(Yaml::BadValue) => Ok(None),
        Some(value) => Ok(Some(yaml_string(value, &format!("{ctx}.{key}"))?)),
    }
}

fn opt_vec_string(hash: &Hash, key: &str, ctx: &str) -> FloeResult<Option<Vec<String>>> {
    let value = match hash_get(hash, key) {
        None | Some(Yaml::Null) | Some(Yaml::BadValue) => return Ok(None),
        Some(value) => value,
    };
    let list = yaml_array(value, &format!("{ctx}.{key}"))?;
    let mut values = Vec::with_capacity(list.len());
    for (index, item) in list.iter().enumerate() {
        let item_ctx = format!("{ctx}.{key}[{index}]");
        values.push(yaml_string(item, &item_ctx)?);
    }
    Ok(Some(values))
}

fn opt_vec_vec_string(hash: &Hash, key: &str, ctx: &str) -> FloeResult<Option<Vec<Vec<String>>>> {
    let value = match hash_get(hash, key) {
        None | Some(Yaml::Null) | Some(Yaml::BadValue) => return Ok(None),
        Some(value) => value,
    };
    let list = yaml_array(value, &format!("{ctx}.{key}"))?;
    let mut values = Vec::with_capacity(list.len());
    for (index, item) in list.iter().enumerate() {
        let item_ctx = format!("{ctx}.{key}[{index}]");
        let item_array = yaml_array(item, &item_ctx)?;
        let mut tuple = Vec::with_capacity(item_array.len());
        for (inner_idx, inner_item) in item_array.iter().enumerate() {
            let inner_ctx = format!("{item_ctx}[{inner_idx}]");
            tuple.push(yaml_string(inner_item, &inner_ctx)?);
        }
        values.push(tuple);
    }
    Ok(Some(values))
}

fn opt_bool(hash: &Hash, key: &str, ctx: &str) -> FloeResult<Option<bool>> {
    match hash_get(hash, key) {
        None | Some(Yaml::Null) | Some(Yaml::BadValue) => Ok(None),
        Some(value) => match value {
            Yaml::Boolean(value) => Ok(Some(*value)),
            _ => Err(FloeError::config(format!("expected boolean at {ctx}.{key}")).into()),
        },
    }
}

fn opt_u64(hash: &Hash, key: &str, ctx: &str) -> FloeResult<Option<u64>> {
    match hash_get(hash, key) {
        None | Some(Yaml::Null) | Some(Yaml::BadValue) => Ok(None),
        Some(value) => match value {
            Yaml::Integer(raw) => {
                if *raw < 0 {
                    return Err(FloeError::config(format!(
                        "expected positive integer at {ctx}.{key}"
                    ))
                    .into());
                }
                Ok(Some(*raw as u64))
            }
            _ => Err(FloeError::config(format!("expected integer at {ctx}.{key}")).into()),
        },
    }
}

fn opt_u32(hash: &Hash, key: &str, ctx: &str) -> FloeResult<Option<u32>> {
    match opt_u64(hash, key, ctx)? {
        None => Ok(None),
        Some(v) => {
            if v > u32::MAX as u64 {
                return Err(FloeError::config(format!(
                    "value at {ctx}.{key} exceeds maximum allowed value"
                ))
                .into());
            }
            Ok(Some(v as u32))
        }
    }
}

fn parse_pii_config(value: &Yaml) -> FloeResult<PiiConfig> {
    let hash = yaml_hash(value, "pii")?;
    validate_known_keys(hash, "pii", &["columns"])?;
    let columns_yaml = get_array(hash, "columns", "pii")?;
    let mut columns = Vec::with_capacity(columns_yaml.len());
    for (index, col_yaml) in columns_yaml.iter().enumerate() {
        let col = parse_pii_column(col_yaml)
            .map_err(|err| FloeError::config(format!("pii.columns[{index}]: {err}")))?;
        columns.push(col);
    }
    Ok(PiiConfig { columns })
}

fn parse_pii_column(value: &Yaml) -> FloeResult<PiiColumnConfig> {
    let hash = yaml_hash(value, "pii.columns")?;
    validate_known_keys(
        hash,
        "pii.columns",
        &["name", "strategy", "mask_pattern", "redact_value", "key"],
    )?;
    let name = get_string(hash, "name", "pii.columns")?;
    let strategy_str = get_string(hash, "strategy", "pii.columns")?;
    let strategy = match strategy_str.trim().to_ascii_lowercase().as_str() {
        "hash" => PiiStrategy::Hash,
        "drop" => PiiStrategy::Drop,
        "nullify" => PiiStrategy::Nullify,
        "redact" => PiiStrategy::Redact,
        "mask" => PiiStrategy::Mask,
        "tokenize" => PiiStrategy::Tokenize,
        other => {
            return Err(FloeError::config(format!(
                "pii.columns[name={name}].strategy={other} is unsupported (allowed: hash, drop, nullify, redact, mask)"
            )).into())
        }
    };
    Ok(PiiColumnConfig {
        name,
        strategy,
        mask_pattern: opt_string(hash, "mask_pattern", "pii.columns")?,
        redact_value: opt_string(hash, "redact_value", "pii.columns")?,
        key: opt_string(hash, "key", "pii.columns")?,
    })
}

pub(crate) fn parse_lineage_config(value: &Yaml) -> FloeResult<LineageConfig> {
    let hash = yaml_hash(value, "lineage")?;
    validate_known_keys(
        hash,
        "lineage",
        &[
            "url",
            "endpoint",
            "api_key",
            "timeout_secs",
            "namespace",
            "dataset_namespace",
            "producer",
            "max_failures",
            "job_name",
        ],
    )?;
    Ok(LineageConfig {
        url: get_string(hash, "url", "lineage")?,
        endpoint: opt_string(hash, "endpoint", "lineage")?,
        api_key: opt_string(hash, "api_key", "lineage")?,
        timeout_secs: opt_u64(hash, "timeout_secs", "lineage")?,
        namespace: get_string(hash, "namespace", "lineage")?,
        dataset_namespace: opt_string(hash, "dataset_namespace", "lineage")?,
        producer: opt_string(hash, "producer", "lineage")?,
        max_failures: opt_u32(hash, "max_failures", "lineage")?,
        job_name: opt_string(hash, "job_name", "lineage")?,
    })
}
