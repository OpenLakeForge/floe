use std::path::{Path, PathBuf};

use floe_core::config::{
    ConfigBase, RootConfig, StorageDefinition, StorageResolver, StoragesConfig,
};
use floe_core::FloeResult;

fn base_root(definition: StorageDefinition) -> RootConfig {
    RootConfig {
        version: "0.1".to_string(),
        metadata: None,
        storages: Some(StoragesConfig {
            default: Some(definition.name.clone()),
            definitions: vec![definition],
        }),
        catalogs: None,
        env: None,
        domains: Vec::new(),
        report: None,
        lineage: None,
        entities: Vec::new(),
        config_files: Vec::new(),
    }
}

#[test]
fn local_base_resolves_relative_paths() -> FloeResult<()> {
    let config = RootConfig {
        version: "0.1".to_string(),
        metadata: None,
        storages: None,
        catalogs: None,
        env: None,
        domains: Vec::new(),
        report: None,
        lineage: None,
        entities: Vec::new(),
        config_files: Vec::new(),
    };
    let resolver = StorageResolver::from_path(&config, Path::new("/tmp/config.yml"))?;
    let resolved = resolver.resolve_path("entity", "source.path", None, "data/file.csv")?;
    assert_eq!(
        resolved.local_path,
        Some(PathBuf::from("/tmp/data/file.csv"))
    );
    Ok(())
}

#[test]
fn remote_s3_base_resolves_relative_paths() -> FloeResult<()> {
    let definition = StorageDefinition {
        name: "s3_raw".to_string(),
        fs_type: "s3".to_string(),
        bucket: Some("my-bucket".to_string()),
        region: None,
        account: None,
        container: None,
        prefix: None,
        endpoint: None,
        path_style_access: None,
    };
    let config = base_root(definition);
    let base =
        ConfigBase::remote_from_uri(PathBuf::from("/tmp"), "s3://my-bucket/configs/demo.yml")?;
    let resolver = StorageResolver::new(&config, base)?;
    let resolved = resolver.resolve_path("entity", "source.path", None, "in/orders")?;
    assert_eq!(resolved.uri, "s3://my-bucket/configs/in/orders");
    Ok(())
}

#[test]
fn remote_gcs_base_resolves_relative_paths() -> FloeResult<()> {
    let definition = StorageDefinition {
        name: "gcs_raw".to_string(),
        fs_type: "gcs".to_string(),
        bucket: Some("my-bucket".to_string()),
        region: None,
        account: None,
        container: None,
        prefix: None,
        endpoint: None,
        path_style_access: None,
    };
    let config = base_root(definition);
    let base =
        ConfigBase::remote_from_uri(PathBuf::from("/tmp"), "gs://my-bucket/configs/demo.yml")?;
    let resolver = StorageResolver::new(&config, base)?;
    let resolved = resolver.resolve_path("entity", "source.path", None, "in/orders")?;
    assert_eq!(resolved.uri, "gs://my-bucket/configs/in/orders");
    Ok(())
}

#[test]
fn remote_adls_base_resolves_relative_paths() -> FloeResult<()> {
    let definition = StorageDefinition {
        name: "adls_raw".to_string(),
        fs_type: "adls".to_string(),
        bucket: None,
        region: None,
        account: Some("acct".to_string()),
        container: Some("cont".to_string()),
        prefix: None,
        endpoint: None,
        path_style_access: None,
    };
    let config = base_root(definition);
    let base = ConfigBase::remote_from_uri(
        PathBuf::from("/tmp"),
        "abfs://cont@acct.dfs.core.windows.net/configs/demo.yml",
    )?;
    let resolver = StorageResolver::new(&config, base)?;
    let resolved = resolver.resolve_path("entity", "source.path", None, "in/orders")?;
    assert_eq!(
        resolved.uri,
        "abfs://cont@acct.dfs.core.windows.net/configs/in/orders"
    );
    Ok(())
}

#[test]
fn remote_base_respects_explicit_prefix() -> FloeResult<()> {
    let definition = StorageDefinition {
        name: "s3_raw".to_string(),
        fs_type: "s3".to_string(),
        bucket: Some("my-bucket".to_string()),
        region: None,
        account: None,
        container: None,
        prefix: Some("lakehouse".to_string()),
        endpoint: None,
        path_style_access: None,
    };
    let config = base_root(definition);
    let base =
        ConfigBase::remote_from_uri(PathBuf::from("/tmp"), "s3://my-bucket/configs/demo.yml")?;
    let resolver = StorageResolver::new(&config, base)?;
    let resolved = resolver.resolve_path("entity", "source.path", None, "in/orders")?;
    assert_eq!(resolved.uri, "s3://my-bucket/lakehouse/in/orders");
    Ok(())
}
