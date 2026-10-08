use floe_core::config::{
    ConfigBase, RootConfig, StorageDefinition, StorageResolver, StoragesConfig,
};
use std::path::PathBuf;

fn resolver_with_definitions(defs: Vec<StorageDefinition>) -> StorageResolver {
    let default = defs.first().map(|d| d.name.clone()).unwrap_or_default();
    let config = RootConfig {
        version: "0.1".to_string(),
        metadata: None,
        storages: Some(StoragesConfig {
            default: Some(default),
            definitions: defs,
        }),
        catalogs: None,
        env: None,
        domains: vec![],
        report: None,
        lineage: None,
        entities: vec![],
        config_files: Vec::new(),
    };
    let base = ConfigBase::local_from_path(&PathBuf::from("/tmp/config.yml"));
    StorageResolver::new(&config, base).expect("resolver")
}

fn s3_def(name: &str, bucket: &str) -> StorageDefinition {
    StorageDefinition {
        name: name.to_string(),
        fs_type: "s3".to_string(),
        bucket: Some(bucket.to_string()),
        region: None,
        endpoint: None,
        path_style_access: None,
        account: None,
        container: None,
        prefix: None,
    }
}

fn gcs_def(name: &str, bucket: &str) -> StorageDefinition {
    StorageDefinition {
        name: name.to_string(),
        fs_type: "gcs".to_string(),
        bucket: Some(bucket.to_string()),
        region: None,
        endpoint: None,
        path_style_access: None,
        account: None,
        container: None,
        prefix: None,
    }
}

fn adls_def(name: &str, container: &str, account: &str) -> StorageDefinition {
    StorageDefinition {
        name: name.to_string(),
        fs_type: "adls".to_string(),
        bucket: None,
        region: None,
        endpoint: None,
        path_style_access: None,
        account: Some(account.to_string()),
        container: Some(container.to_string()),
        prefix: None,
    }
}

#[test]
fn find_s3_definition_by_uri_with_path() {
    let resolver = resolver_with_definitions(vec![s3_def("reports", "my-reports")]);
    assert_eq!(
        resolver.find_definition_name_for_uri("s3://my-reports/floe/run123/"),
        Some("reports".to_string())
    );
}

#[test]
fn find_s3_definition_by_uri_exact_bucket() {
    let resolver = resolver_with_definitions(vec![s3_def("reports", "my-reports")]);
    assert_eq!(
        resolver.find_definition_name_for_uri("s3://my-reports"),
        Some("reports".to_string())
    );
}

#[test]
fn find_gcs_definition_by_uri() {
    let resolver = resolver_with_definitions(vec![gcs_def("gcs_store", "gcs-bucket")]);
    assert_eq!(
        resolver.find_definition_name_for_uri("gs://gcs-bucket/reports/"),
        Some("gcs_store".to_string())
    );
}

#[test]
fn find_adls_definition_by_uri() {
    let resolver =
        resolver_with_definitions(vec![adls_def("adls_store", "mycontainer", "myaccount")]);
    assert_eq!(
        resolver.find_definition_name_for_uri(
            "abfs://mycontainer@myaccount.dfs.core.windows.net/reports/run1/"
        ),
        Some("adls_store".to_string())
    );
}

#[test]
fn adls_account_prefix_does_not_match() {
    // "acct" must not match a URI containing "acct2" as the account.
    let resolver = resolver_with_definitions(vec![adls_def("adls_store", "cont", "acct")]);
    assert_eq!(
        resolver.find_definition_name_for_uri("abfs://cont@acct2.dfs.core.windows.net/reports/"),
        None
    );
}

#[test]
fn find_definition_returns_none_when_no_match() {
    let resolver = resolver_with_definitions(vec![s3_def("other", "different-bucket")]);
    assert_eq!(
        resolver.find_definition_name_for_uri("s3://my-reports/path/"),
        None
    );
}

#[test]
fn register_definition_makes_it_discoverable() {
    let mut resolver = resolver_with_definitions(vec![s3_def("existing", "existing-bucket")]);
    let new_def = s3_def("__report_s3__", "new-bucket");
    resolver.register_definition(new_def);
    assert_eq!(
        resolver.find_definition_name_for_uri("s3://new-bucket/reports/"),
        Some("__report_s3__".to_string())
    );
    assert!(resolver.definition("__report_s3__").is_some());
}
