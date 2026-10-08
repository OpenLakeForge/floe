use floe_core::config;
use floe_core::config::StorageResolver;
use floe_core::FloeResult;

fn base_root() -> config::RootConfig {
    config::RootConfig {
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
    }
}

#[test]
fn adls_uri_with_prefix_is_built() -> FloeResult<()> {
    let mut config = base_root();
    config.storages = Some(config::StoragesConfig {
        default: Some("adls".to_string()),
        definitions: vec![config::StorageDefinition {
            name: "adls".to_string(),
            fs_type: "adls".to_string(),
            bucket: None,
            region: None,
            account: Some("acct".to_string()),
            container: Some("cont".to_string()),
            prefix: Some("prefix".to_string()),
            endpoint: None,
            path_style_access: None,
        }],
    });

    let resolver = StorageResolver::from_path(&config, std::path::Path::new("./config.yml"))?;
    let resolved = resolver.resolve_path("entity", "source.storage", None, "data/file.csv")?;
    assert_eq!(
        resolved.uri,
        "abfs://cont@acct.dfs.core.windows.net/prefix/data/file.csv"
    );
    Ok(())
}

#[test]
fn adls_uri_without_prefix_is_built() -> FloeResult<()> {
    let mut config = base_root();
    config.storages = Some(config::StoragesConfig {
        default: Some("adls".to_string()),
        definitions: vec![config::StorageDefinition {
            name: "adls".to_string(),
            fs_type: "adls".to_string(),
            bucket: None,
            region: None,
            account: Some("acct".to_string()),
            container: Some("cont".to_string()),
            prefix: None,
            endpoint: None,
            path_style_access: None,
        }],
    });

    let resolver = StorageResolver::from_path(&config, std::path::Path::new("./config.yml"))?;
    let resolved = resolver.resolve_path("entity", "source.storage", None, "data/file.csv")?;
    assert_eq!(
        resolved.uri,
        "abfs://cont@acct.dfs.core.windows.net/data/file.csv"
    );
    Ok(())
}

#[test]
fn abfss_uri_in_path_resolves_to_canonical_abfs() -> FloeResult<()> {
    let mut config = base_root();
    config.storages = Some(config::StoragesConfig {
        default: Some("adls".to_string()),
        definitions: vec![config::StorageDefinition {
            name: "adls".to_string(),
            fs_type: "adls".to_string(),
            bucket: None,
            region: None,
            account: Some("acct".to_string()),
            container: Some("cont".to_string()),
            prefix: None,
            endpoint: None,
            path_style_access: None,
        }],
    });

    // The secure abfss:// spelling (copied from the Azure portal/Databricks) must be
    // accepted and folded to the canonical abfs:// Floe uses internally.
    let resolver = StorageResolver::from_path(&config, std::path::Path::new("./config.yml"))?;
    let resolved = resolver.resolve_path(
        "entity",
        "source.storage",
        None,
        "abfss://cont@acct.dfs.core.windows.net/data/file.csv",
    )?;
    assert_eq!(
        resolved.uri,
        "abfs://cont@acct.dfs.core.windows.net/data/file.csv"
    );
    Ok(())
}
