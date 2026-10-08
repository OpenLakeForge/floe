use crate::errors::FloeError;
use std::path::Path;

use yaml_rust2::yaml::Hash;
use yaml_rust2::{Yaml, YamlLoader};

use crate::FloeResult;

pub(crate) fn load_yaml(path: &Path) -> FloeResult<Vec<Yaml>> {
    let contents = std::fs::read_to_string(path)?;
    let docs = YamlLoader::load_from_str(&contents)?;
    Ok(docs)
}

pub(crate) fn yaml_hash<'a>(value: &'a Yaml, ctx: &str) -> FloeResult<&'a Hash> {
    match value {
        Yaml::Hash(hash) => Ok(hash),
        _ => Err(FloeError::config(format!("expected map at {ctx}")).into()),
    }
}

pub(crate) fn yaml_array<'a>(value: &'a Yaml, ctx: &str) -> FloeResult<&'a Vec<Yaml>> {
    match value {
        Yaml::Array(values) => Ok(values),
        _ => Err(FloeError::config(format!("expected array at {ctx}")).into()),
    }
}

pub(crate) fn yaml_string(value: &Yaml, ctx: &str) -> FloeResult<String> {
    match value {
        Yaml::String(value) => Ok(value.clone()),
        _ => Err(FloeError::config(format!("expected string at {ctx}")).into()),
    }
}

#[allow(dead_code)]
pub(crate) fn yaml_number(value: &Yaml) -> Result<f64, ()> {
    match value {
        Yaml::Integer(raw) => Ok(*raw as f64),
        Yaml::Real(raw) => raw.parse::<f64>().map_err(|_| ()),
        _ => Err(()),
    }
}

pub(crate) fn merge_yaml(base: &Yaml, overlay: &Yaml) -> Yaml {
    match (base, overlay) {
        (Yaml::Hash(base), Yaml::Hash(overlay)) => {
            let mut merged = base.clone();
            for (key, value) in overlay {
                let value = match base.get(key) {
                    Some(base_value) => merge_yaml(base_value, value),
                    None => value.clone(),
                };
                merged.insert(key.clone(), value);
            }
            Yaml::Hash(merged)
        }
        _ => overlay.clone(),
    }
}

pub(crate) fn hash_get<'a>(hash: &'a Hash, key: &str) -> Option<&'a Yaml> {
    hash.get(&Yaml::String(key.to_string()))
}

pub(crate) fn validate_known_keys(hash: &Hash, ctx: &str, allowed: &[&str]) -> FloeResult<()> {
    for key in hash.keys() {
        let key = match key {
            Yaml::String(value) => value.as_str(),
            _ => return Err(FloeError::config(format!("expected string key at {ctx}")).into()),
        };
        if !allowed.contains(&key) {
            return Err(FloeError::config(format!("unknown field {ctx}.{key}")).into());
        }
    }
    Ok(())
}
