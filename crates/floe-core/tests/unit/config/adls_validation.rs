use floe_core::config;

fn base_entity() -> config::EntityConfig {
    config::EntityConfig {
        name: "customer".to_string(),
        id: "customer".to_string(),
        metadata: None,
        domain: None,
        incremental_mode: config::IncrementalMode::None,
        state: None,
        source: config::SourceConfig {
            format: "csv".to_string(),
            path: "in.csv".to_string(),
            storage: None,
            options: None,
            cast_mode: None,
        },
        sink: config::SinkConfig {
            write_mode: config::WriteMode::Overwrite,
            accepted: config::SinkTarget {
                format: "parquet".to_string(),
                path: "out.parquet".to_string(),
                storage: None,
                options: None,
                merge: None,
                iceberg: None,
                write_mode: config::WriteMode::Overwrite,
                partition_by: None,
                partition_spec: None,
                delta: None,
                duckdb: None,
            },
            rejected: None,
            archive: None,
        },
        policy: config::PolicyConfig {
            severity: config::PolicySeverity::Warn,
        },
        schema: config::SchemaConfig {
            normalize_columns: None,
            mismatch: None,
            schema_evolution: None,
            primary_key: None,
            unique_keys: None,
            columns: Vec::new(),
        },
        pii: None,
    }
}

#[test]
fn adls_missing_required_fields_errors() {
    let mut config = config::RootConfig {
        version: "0.1".to_string(),
        metadata: None,
        storages: Some(config::StoragesConfig {
            default: Some("adls".to_string()),
            definitions: vec![config::StorageDefinition {
                name: "adls".to_string(),
                fs_type: "adls".to_string(),
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
        entities: vec![base_entity()],
    };

    let err = floe_core::validate_config_for_tests(&config).expect_err("expected error");
    assert!(err.to_string().contains("requires account"));

    config.storages.as_mut().unwrap().definitions[0].account = Some("acct".to_string());
    let err = floe_core::validate_config_for_tests(&config).expect_err("expected error");
    assert!(err.to_string().contains("requires container"));
}

#[test]
fn adls_referenced_errors_until_implemented() {
    let mut config = config::RootConfig {
        version: "0.1".to_string(),
        metadata: None,
        storages: Some(config::StoragesConfig {
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
        }),
        catalogs: None,
        env: None,
        domains: Vec::new(),
        report: None,
        lineage: None,
        entities: vec![base_entity()],
    };

    config.entities[0].source.storage = Some("adls".to_string());
    floe_core::validate_config_for_tests(&config).expect("valid config");

    config.entities[0].source.storage = None;
    config.entities[0].sink.accepted.storage = Some("adls".to_string());
    floe_core::validate_config_for_tests(&config).expect("valid config");
}
