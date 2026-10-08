use std::path::PathBuf;

use floe_core::config::{
    ReportConfig, RootConfig, StorageDefinition, StorageResolver, StoragesConfig,
};
use floe_core::io::storage::Target;
use floe_core::report::ReportWriter;

fn build_config(definition: StorageDefinition, report: ReportConfig) -> RootConfig {
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
        report: Some(report),
        lineage: None,
        entities: Vec::new(),
        config_files: Vec::new(),
    }
}

fn resolver_for(config: &RootConfig) -> StorageResolver {
    let config_path = PathBuf::from("/tmp/floe-config.yml");
    StorageResolver::from_path(config, &config_path).expect("storage resolver")
}

#[test]
fn report_base_resolves_under_prefix() {
    let definition = StorageDefinition {
        name: "s3_test".to_string(),
        fs_type: "s3".to_string(),
        bucket: Some("my-bucket".to_string()),
        region: Some("eu-west-1".to_string()),
        account: None,
        container: None,
        prefix: Some("lakehouse".to_string()),
        endpoint: None,
        path_style_access: None,
    };
    let report = ReportConfig {
        path: "report".to_string(),
        formatter: None,
        storage: None,
    };
    let config = build_config(definition, report);
    let resolver = resolver_for(&config);
    let resolved = resolver
        .resolve_report_path(None, "report")
        .expect("resolve report path");
    assert_eq!(resolved.uri, "s3://my-bucket/lakehouse/report");
}

#[test]
fn report_uri_builds_with_run_layout() {
    let definition = StorageDefinition {
        name: "gcs_test".to_string(),
        fs_type: "gcs".to_string(),
        bucket: Some("my-bucket".to_string()),
        region: None,
        account: None,
        container: None,
        prefix: Some("lakehouse/report".to_string()),
        endpoint: None,
        path_style_access: None,
    };
    let report = ReportConfig {
        path: "reports".to_string(),
        formatter: None,
        storage: None,
    };
    let config = build_config(definition, report);
    let resolver = resolver_for(&config);
    let resolved = resolver
        .resolve_report_path(None, "reports")
        .expect("resolve report path");
    let target = Target::from_resolved(&resolved).expect("target");
    let relative = ReportWriter::report_relative_path("2026-01-19T10-23-45Z", "customer");
    let uri = target.join_relative(&relative);
    assert_eq!(
        uri,
        "gs://my-bucket/lakehouse/report/reports/run_2026-01-19T10-23-45Z/customer/run.json"
    );
}

#[test]
fn report_uri_normalizes_prefix_and_path() {
    let definition = StorageDefinition {
        name: "adls_test".to_string(),
        fs_type: "adls".to_string(),
        bucket: None,
        region: None,
        account: Some("acct".to_string()),
        container: Some("container".to_string()),
        prefix: Some("/lakehouse/".to_string()),
        endpoint: None,
        path_style_access: None,
    };
    let report = ReportConfig {
        path: "/report/".to_string(),
        formatter: None,
        storage: None,
    };
    let config = build_config(definition, report);
    let resolver = resolver_for(&config);
    let resolved = resolver
        .resolve_report_path(None, "/report/")
        .expect("resolve report path");
    assert_eq!(
        resolved.uri,
        "abfs://container@acct.dfs.core.windows.net/lakehouse/report/"
    );
}

#[test]
fn local_report_paths_and_uris_are_normalized() {
    let config = RootConfig {
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
        report: Some(ReportConfig {
            path: "./reports/../reports_out/./base".to_string(),
            formatter: None,
            storage: None,
        }),
        lineage: None,
        entities: Vec::new(),
        config_files: Vec::new(),
    };
    let resolver = resolver_for(&config);
    let resolved = resolver
        .resolve_report_path(None, "./reports/../reports_out/./base")
        .expect("resolve report path");
    assert_eq!(
        resolved.local_path.as_ref().expect("local path"),
        &PathBuf::from("/tmp/reports_out/base")
    );
    assert_eq!(resolved.uri, "local:///tmp/reports_out/base");

    let target = Target::from_resolved(&resolved).expect("target");
    let report_file = target.join_relative(&ReportWriter::report_relative_path("run1", "orders"));
    assert_eq!(
        report_file,
        "/tmp/reports_out/base/run_run1/orders/run.json"
    );
    assert!(!report_file.contains("/./"));
    assert!(!report_file.contains("/../"));
}
