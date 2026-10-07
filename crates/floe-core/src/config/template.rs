use crate::errors::FloeError;
use std::collections::HashMap;
use std::path::Path;

use yaml_rust2::yaml::Hash;

use crate::config::storage::resolve_local_path;
use crate::config::yaml_decode::{load_yaml, yaml_hash, yaml_string};
use crate::config::{EnvConfig, RootConfig};
use crate::FloeResult;

pub fn apply_templates_with_vars(
    config: &mut RootConfig,
    config_dir: &Path,
    profile_vars: &HashMap<String, String>,
) -> FloeResult<()> {
    let vars = build_env_vars(config_dir, config.env.as_ref(), profile_vars)?;
    let mut domain_lookup = HashMap::new();
    for domain in config.domains.iter_mut() {
        let resolved =
            replace_placeholders(&domain.incoming_dir, &vars, "domains.incoming_dir", None)?;
        domain.resolved_incoming_dir = Some(resolved.clone());
        if domain_lookup
            .insert(domain.name.clone(), resolved)
            .is_some()
        {
            return Err(FloeError::config(format!("duplicate domain name {}", domain.name)).into());
        }
    }

    if let Some(report) = config.report.as_mut() {
        report.path = replace_placeholders(&report.path, &vars, "report.path", None)?;
    }

    if let Some(lineage) = config.lineage.as_mut() {
        lineage.url = replace_placeholders(&lineage.url, &vars, "lineage.url", None)?;
        replace_lineage_field(&mut lineage.endpoint, &vars, "endpoint")?;
        if let Some(api_key) = lineage.api_key.as_mut() {
            *api_key = replace_placeholders(api_key, &vars, "lineage.api_key", None)?;
        }
        replace_lineage_field(&mut lineage.dataset_namespace, &vars, "dataset_namespace")?;
    }

    if let Some(storages) = config.storages.as_mut() {
        for definition in storages.definitions.iter_mut() {
            let name = definition.name.clone();
            replace_storage_definition_field(&mut definition.bucket, &vars, &name, "bucket")?;
            replace_storage_definition_field(&mut definition.region, &vars, &name, "region")?;
            replace_storage_definition_field(&mut definition.account, &vars, &name, "account")?;
            replace_storage_definition_field(&mut definition.container, &vars, &name, "container")?;
            replace_storage_definition_field(&mut definition.prefix, &vars, &name, "prefix")?;
            replace_storage_definition_field(&mut definition.endpoint, &vars, &name, "endpoint")?;
        }
    }

    for entity in config.entities.iter_mut() {
        let mut context_vars = vars.clone();
        if let Some(domain_name) = entity.domain.as_ref() {
            let incoming_dir = domain_lookup.get(domain_name).ok_or_else(|| {
                FloeError::config(format!(
                    "entity.name={} references unknown domain {}",
                    entity.name, domain_name
                ))
            })?;
            context_vars.insert("domain.incoming_dir".to_string(), incoming_dir.clone());
        }

        entity.source.path = replace_placeholders(
            &entity.source.path,
            &context_vars,
            "entities.source.path",
            Some(&entity.name),
        )?;
        entity.sink.accepted.path = replace_placeholders(
            &entity.sink.accepted.path,
            &context_vars,
            "entities.sink.accepted.path",
            Some(&entity.name),
        )?;
        if let Some(rejected) = entity.sink.rejected.as_mut() {
            rejected.path = replace_placeholders(
                &rejected.path,
                &context_vars,
                "entities.sink.rejected.path",
                Some(&entity.name),
            )?;
        }
        if let Some(archived) = entity.sink.archive.as_mut() {
            archived.path = replace_placeholders(
                &archived.path,
                &context_vars,
                "entities.sink.archive.path",
                Some(&entity.name),
            )?;
        }
    }

    Ok(())
}

fn replace_lineage_field(
    value: &mut Option<String>,
    vars: &HashMap<String, String>,
    field_name: &str,
) -> FloeResult<()> {
    if let Some(current) = value.as_mut() {
        let field = format!("lineage.{field_name}");
        *current = replace_placeholders(current, vars, &field, None)?;
    }
    Ok(())
}

fn replace_storage_definition_field(
    value: &mut Option<String>,
    vars: &HashMap<String, String>,
    definition_name: &str,
    field_name: &str,
) -> FloeResult<()> {
    if let Some(current) = value.as_mut() {
        let field = format!("storages.definitions.{definition_name}.{field_name}");
        *current = replace_placeholders(current, vars, &field, None)?;
    }
    Ok(())
}

fn build_env_vars(
    config_dir: &Path,
    env: Option<&EnvConfig>,
    profile_vars: &HashMap<String, String>,
) -> FloeResult<HashMap<String, String>> {
    // Lowest priority: profile variables
    let mut vars: HashMap<String, String> = profile_vars.clone();
    let env = match env {
        Some(env) => env,
        None => return Ok(vars),
    };
    // Then env file (overwrites profile)
    if let Some(file) = env.file.as_ref() {
        let path = resolve_local_path(config_dir, file);
        let file_vars = load_env_file(&path)?;
        vars.extend(file_vars);
    }
    // Highest priority: inline env.vars (overwrites both)
    vars.extend(env.vars.clone());
    Ok(vars)
}

fn load_env_file(path: &Path) -> FloeResult<HashMap<String, String>> {
    let docs = load_yaml(path)?;
    if docs.is_empty() {
        return Err(FloeError::config(format!("env file {} is empty", path.display())).into());
    }
    let hash = yaml_hash(&docs[0], "env.file")?;
    extract_string_map(hash, "env.file")
}

fn extract_string_map(hash: &Hash, context: &str) -> FloeResult<HashMap<String, String>> {
    let mut map = HashMap::new();
    for (key, value) in hash {
        let key_str = yaml_string(key, context)?;
        let value_str = yaml_string(value, context)?;
        map.insert(key_str, value_str);
    }
    Ok(map)
}

fn replace_placeholders(
    value: &str,
    vars: &HashMap<String, String>,
    field: &str,
    entity: Option<&str>,
) -> FloeResult<String> {
    let mut result = String::new();
    let mut rest = value;
    while let Some(start) = rest.find("{{") {
        result.push_str(&rest[..start]);
        rest = &rest[start + 2..];
        let end = rest.find("}}").ok_or_else(|| {
            FloeError::config(format!(
                "{}{} missing closing '}}'",
                entity_prefix(entity),
                field
            ))
        })?;
        let key = rest[..end].trim();
        if key.is_empty() {
            return Err(FloeError::config(format!(
                "{}{} empty placeholder",
                entity_prefix(entity),
                field
            ))
            .into());
        }
        let replacement = vars.get(key).ok_or_else(|| {
            FloeError::config(format!(
                "{}{} references unknown variable {}",
                entity_prefix(entity),
                field,
                key
            ))
        })?;
        result.push_str(replacement);
        rest = &rest[end + 2..];
    }
    result.push_str(rest);
    Ok(result)
}

fn entity_prefix(entity: Option<&str>) -> String {
    match entity {
        Some(name) => format!("entity.name={} ", name),
        None => String::new(),
    }
}
