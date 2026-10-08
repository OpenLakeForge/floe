use std::path::PathBuf;

use floe_core::config::{
    resolve_local_path, RootConfig, StorageDefinition, StorageResolver, StoragesConfig,
};

fn config_with_default_local() -> RootConfig {
    RootConfig {
        version: "0.1".to_string(),
        metadata: None,
        storages: Some(StoragesConfig {
            default: Some("local".to_string()),
            definitions: vec![StorageDefinition {
                name: "local".to_string(),
                fs_type: "local".to_string(),
                bucket: None,
                region: None,
                account: None,
                container: None,
                prefix: None,
                endpoint: None,
                path_style_access: None,
            }],
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
fn resolve_local_path_normalizes_dot_segments_without_canonicalizing() {
    let config_dir = PathBuf::from("/tmp/floe/config");
    let resolved = resolve_local_path(&config_dir, "./in/../out/data.csv");
    assert_eq!(resolved, PathBuf::from("/tmp/floe/config/out/data.csv"));
}

#[test]
fn storage_resolver_local_paths_are_normalized() {
    let config = config_with_default_local();
    let resolver = StorageResolver::from_path(&config, &PathBuf::from("/tmp/floe/conf/./cfg.yml"))
        .expect("resolver");

    let resolved = resolver
        .resolve_path("orders", "source.path", None, "./in/../in/data.csv")
        .expect("resolve local source");
    assert_eq!(
        resolved.local_path.as_ref().expect("local path"),
        &PathBuf::from("/tmp/floe/conf/in/data.csv")
    );
    assert_eq!(resolved.uri, "local:///tmp/floe/conf/in/data.csv");
}
