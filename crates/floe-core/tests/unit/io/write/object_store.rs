use floe_core::{
    config,
    io::storage::object_store::{delta_store_config, iceberg_store_config},
    io::storage::Target,
    FloeResult,
};
use iceberg::io::{CLIENT_REGION, S3_REGION};

fn sample_config() -> config::RootConfig {
    config::RootConfig {
        version: "0.1".to_string(),
        metadata: None,
        storages: Some(config::StoragesConfig {
            default: Some("s3_raw".to_string()),
            definitions: vec![config::StorageDefinition {
                name: "s3_raw".to_string(),
                fs_type: "s3".to_string(),
                bucket: Some("my-bucket".to_string()),
                region: Some("eu-west-1".to_string()),
                account: None,
                container: None,
                prefix: Some("data".to_string()),
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
fn delta_store_config_builds_s3_url_and_options() -> FloeResult<()> {
    let config = sample_config();
    let resolver =
        config::StorageResolver::from_path(&config, std::path::Path::new("./config.yml"))?;
    let resolved = resolver.resolve_path("orders", "sink.accepted.path", None, "delta/orders")?;
    let target = Target::from_resolved(&resolved)?;
    let entity = config::EntityConfig {
        name: "orders".to_string(),
        id: "orders".to_string(),
        metadata: None,
        domain: None,
        incremental_mode: config::IncrementalMode::None,
        state: None,
        source: config::SourceConfig {
            format: "csv".to_string(),
            path: "in".to_string(),
            storage: None,
            options: None,
            cast_mode: None,
        },
        sink: config::SinkConfig {
            write_mode: config::WriteMode::Overwrite,
            accepted: config::SinkTarget {
                format: "delta".to_string(),
                path: "delta/orders".to_string(),
                storage: None,
                options: None,
                merge: None,
                iceberg: None,
                partition_by: None,
                partition_spec: None,
                delta: None,
                duckdb: None,
                write_mode: config::WriteMode::Overwrite,
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
    };

    let store = delta_store_config(&target, &resolver, &entity)?;
    assert_eq!(store.table_url.as_str(), "s3://my-bucket/data/delta/orders");
    assert_eq!(
        store.storage_options.get("region").map(String::as_str),
        Some("eu-west-1")
    );
    assert_eq!(
        store.storage_options.get("bucket").map(String::as_str),
        Some("my-bucket")
    );
    Ok(())
}

#[test]
fn iceberg_store_config_builds_s3_warehouse_and_region_props() -> FloeResult<()> {
    let config = sample_config();
    let resolver =
        config::StorageResolver::from_path(&config, std::path::Path::new("./config.yml"))?;
    let resolved = resolver.resolve_path("orders", "sink.accepted.path", None, "iceberg/orders")?;
    let target = Target::from_resolved(&resolved)?;
    let entity = config::EntityConfig {
        name: "orders".to_string(),
        id: "orders".to_string(),
        metadata: None,
        domain: None,
        incremental_mode: config::IncrementalMode::None,
        state: None,
        source: config::SourceConfig {
            format: "csv".to_string(),
            path: "in".to_string(),
            storage: None,
            options: None,
            cast_mode: None,
        },
        sink: config::SinkConfig {
            write_mode: config::WriteMode::Overwrite,
            accepted: config::SinkTarget {
                format: "iceberg".to_string(),
                path: "iceberg/orders".to_string(),
                storage: None,
                options: None,
                merge: None,
                iceberg: None,
                partition_by: None,
                partition_spec: None,
                delta: None,
                duckdb: None,
                write_mode: config::WriteMode::Overwrite,
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
    };

    let store = iceberg_store_config(&target, &resolver, &entity)?;
    assert_eq!(
        store.warehouse_location,
        "s3://my-bucket/data/iceberg/orders"
    );
    assert_eq!(
        store.file_io_props.get(S3_REGION).map(String::as_str),
        Some("eu-west-1")
    );
    assert_eq!(
        store.file_io_props.get(CLIENT_REGION).map(String::as_str),
        Some("eu-west-1")
    );
    Ok(())
}

#[test]
fn delta_store_config_builds_local_url() -> FloeResult<()> {
    let config = config::RootConfig {
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
    let temp_dir = tempfile::TempDir::new()?;
    let resolver = config::StorageResolver::from_path(&config, temp_dir.path())?;
    let resolved = resolver.resolve_path("orders", "sink.accepted.path", None, "delta/orders")?;
    let target = Target::from_resolved(&resolved)?;
    let entity = config::EntityConfig {
        name: "orders".to_string(),
        id: "orders".to_string(),
        metadata: None,
        domain: None,
        incremental_mode: config::IncrementalMode::None,
        state: None,
        source: config::SourceConfig {
            format: "csv".to_string(),
            path: "in".to_string(),
            storage: None,
            options: None,
            cast_mode: None,
        },
        sink: config::SinkConfig {
            write_mode: config::WriteMode::Overwrite,
            accepted: config::SinkTarget {
                format: "delta".to_string(),
                path: "delta/orders".to_string(),
                storage: None,
                options: None,
                merge: None,
                iceberg: None,
                partition_by: None,
                partition_spec: None,
                delta: None,
                duckdb: None,
                write_mode: config::WriteMode::Overwrite,
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
    };
    let store = delta_store_config(&target, &resolver, &entity)?;
    assert_eq!(store.storage_options.len(), 0);
    assert_eq!(store.table_url.scheme(), "file");
    Ok(())
}

#[test]
fn iceberg_store_config_builds_local_warehouse_without_props() -> FloeResult<()> {
    let config = config::RootConfig {
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
    let temp_dir = tempfile::TempDir::new()?;
    let resolver = config::StorageResolver::from_path(&config, temp_dir.path())?;
    let resolved = resolver.resolve_path("orders", "sink.accepted.path", None, "iceberg/orders")?;
    let target = Target::from_resolved(&resolved)?;
    let entity = config::EntityConfig {
        name: "orders".to_string(),
        id: "orders".to_string(),
        metadata: None,
        domain: None,
        incremental_mode: config::IncrementalMode::None,
        state: None,
        source: config::SourceConfig {
            format: "csv".to_string(),
            path: "in".to_string(),
            storage: None,
            options: None,
            cast_mode: None,
        },
        sink: config::SinkConfig {
            write_mode: config::WriteMode::Overwrite,
            accepted: config::SinkTarget {
                format: "iceberg".to_string(),
                path: "iceberg/orders".to_string(),
                storage: None,
                options: None,
                merge: None,
                iceberg: None,
                partition_by: None,
                partition_spec: None,
                delta: None,
                duckdb: None,
                write_mode: config::WriteMode::Overwrite,
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
    };

    let store = iceberg_store_config(&target, &resolver, &entity)?;
    assert!(store.warehouse_location.ends_with("/iceberg/orders"));
    assert!(store.file_io_props.is_empty());
    Ok(())
}

#[test]
fn delta_store_config_builds_adls_url_and_options() -> FloeResult<()> {
    let config = config::RootConfig {
        version: "0.1".to_string(),
        metadata: None,
        storages: Some(config::StoragesConfig {
            default: Some("adls_raw".to_string()),
            definitions: vec![config::StorageDefinition {
                name: "adls_raw".to_string(),
                fs_type: "adls".to_string(),
                bucket: None,
                region: None,
                account: Some("account".to_string()),
                container: Some("container".to_string()),
                prefix: Some("data".to_string()),
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
    };
    let resolver =
        config::StorageResolver::from_path(&config, std::path::Path::new("./config.yml"))?;
    let resolved = resolver.resolve_path("orders", "sink.accepted.path", None, "delta/orders")?;
    let target = Target::from_resolved(&resolved)?;
    let entity = config::EntityConfig {
        name: "orders".to_string(),
        id: "orders".to_string(),
        metadata: None,
        domain: None,
        incremental_mode: config::IncrementalMode::None,
        state: None,
        source: config::SourceConfig {
            format: "csv".to_string(),
            path: "in".to_string(),
            storage: None,
            options: None,
            cast_mode: None,
        },
        sink: config::SinkConfig {
            write_mode: config::WriteMode::Overwrite,
            accepted: config::SinkTarget {
                format: "delta".to_string(),
                path: "delta/orders".to_string(),
                storage: None,
                options: None,
                merge: None,
                iceberg: None,
                partition_by: None,
                partition_spec: None,
                delta: None,
                duckdb: None,
                write_mode: config::WriteMode::Overwrite,
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
    };

    let store = delta_store_config(&target, &resolver, &entity)?;
    assert_eq!(
        store.table_url.as_str(),
        "abfs://container@account.dfs.core.windows.net/data/delta/orders"
    );
    assert_eq!(
        store
            .storage_options
            .get("azure_storage_account_name")
            .map(String::as_str),
        Some("account")
    );
    assert_eq!(
        store
            .storage_options
            .get("azure_container_name")
            .map(String::as_str),
        Some("container")
    );
    Ok(())
}

#[test]
fn iceberg_store_config_builds_gcs_warehouse_without_props() -> FloeResult<()> {
    let config = config::RootConfig {
        version: "0.1".to_string(),
        metadata: None,
        storages: Some(config::StoragesConfig {
            default: Some("gcs_raw".to_string()),
            definitions: vec![config::StorageDefinition {
                name: "gcs_raw".to_string(),
                fs_type: "gcs".to_string(),
                bucket: Some("my-bucket".to_string()),
                region: None,
                account: None,
                container: None,
                prefix: Some("data".to_string()),
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
    };
    let resolver =
        config::StorageResolver::from_path(&config, std::path::Path::new("./config.yml"))?;
    let resolved = resolver.resolve_path("orders", "sink.accepted.path", None, "iceberg/orders")?;
    let target = Target::from_resolved(&resolved)?;
    let entity = config::EntityConfig {
        name: "orders".to_string(),
        id: "orders".to_string(),
        metadata: None,
        domain: None,
        incremental_mode: config::IncrementalMode::None,
        state: None,
        source: config::SourceConfig {
            format: "csv".to_string(),
            path: "in".to_string(),
            storage: None,
            options: None,
            cast_mode: None,
        },
        sink: config::SinkConfig {
            write_mode: config::WriteMode::Overwrite,
            accepted: config::SinkTarget {
                format: "iceberg".to_string(),
                path: "iceberg/orders".to_string(),
                storage: None,
                options: None,
                merge: None,
                iceberg: None,
                partition_by: None,
                partition_spec: None,
                delta: None,
                duckdb: None,
                write_mode: config::WriteMode::Overwrite,
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
    };

    let store = iceberg_store_config(&target, &resolver, &entity)?;
    assert_eq!(
        store.warehouse_location,
        "gs://my-bucket/data/iceberg/orders"
    );
    assert!(store.file_io_props.is_empty());
    Ok(())
}

#[test]
fn iceberg_store_config_builds_adls_target() -> FloeResult<()> {
    let config = config::RootConfig {
        version: "0.1".to_string(),
        metadata: None,
        storages: Some(config::StoragesConfig {
            default: Some("adls_raw".to_string()),
            definitions: vec![config::StorageDefinition {
                name: "adls_raw".to_string(),
                fs_type: "adls".to_string(),
                bucket: None,
                region: None,
                account: Some("account".to_string()),
                container: Some("container".to_string()),
                prefix: Some("data".to_string()),
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
    };
    let resolver =
        config::StorageResolver::from_path(&config, std::path::Path::new("./config.yml"))?;
    let resolved = resolver.resolve_path("orders", "sink.accepted.path", None, "iceberg/orders")?;
    let target = Target::from_resolved(&resolved)?;
    let entity = config::EntityConfig {
        name: "orders".to_string(),
        id: "orders".to_string(),
        metadata: None,
        domain: None,
        incremental_mode: config::IncrementalMode::None,
        state: None,
        source: config::SourceConfig {
            format: "csv".to_string(),
            path: "in".to_string(),
            storage: None,
            options: None,
            cast_mode: None,
        },
        sink: config::SinkConfig {
            write_mode: config::WriteMode::Overwrite,
            accepted: config::SinkTarget {
                format: "iceberg".to_string(),
                path: "iceberg/orders".to_string(),
                storage: None,
                options: None,
                merge: None,
                iceberg: None,
                partition_by: None,
                partition_spec: None,
                delta: None,
                duckdb: None,
                write_mode: config::WriteMode::Overwrite,
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
    };

    // Clear credential env vars so the test is deterministic regardless of the
    // runner environment; we only assert the account-name key here.
    std::env::remove_var("AZURE_STORAGE_ACCOUNT_KEY");
    std::env::remove_var("AZURE_STORAGE_SAS_TOKEN");

    let store = iceberg_store_config(&target, &resolver, &entity)?;
    assert!(
        store
            .warehouse_location
            .starts_with("abfss://container@account.dfs.core.windows.net/data/iceberg/orders"),
        "warehouse_location should be the abfss URI: {}",
        store.warehouse_location
    );
    assert_eq!(
        store
            .file_io_props
            .get("adls.account-name")
            .map(String::as_str),
        Some("account")
    );
    // filesystem is derived from the URI path by iceberg-storage-opendal, not passed as a prop
    assert!(!store.file_io_props.contains_key("filesystem"));
    Ok(())
}

#[test]
fn delta_store_config_builds_gcs_url() -> FloeResult<()> {
    let config = config::RootConfig {
        version: "0.1".to_string(),
        metadata: None,
        storages: Some(config::StoragesConfig {
            default: Some("gcs_raw".to_string()),
            definitions: vec![config::StorageDefinition {
                name: "gcs_raw".to_string(),
                fs_type: "gcs".to_string(),
                bucket: Some("my-bucket".to_string()),
                region: None,
                account: None,
                container: None,
                prefix: Some("data".to_string()),
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
    };
    let resolver =
        config::StorageResolver::from_path(&config, std::path::Path::new("./config.yml"))?;
    let resolved = resolver.resolve_path("orders", "sink.accepted.path", None, "delta/orders")?;
    let target = Target::from_resolved(&resolved)?;
    let entity = config::EntityConfig {
        name: "orders".to_string(),
        id: "orders".to_string(),
        metadata: None,
        domain: None,
        incremental_mode: config::IncrementalMode::None,
        state: None,
        source: config::SourceConfig {
            format: "csv".to_string(),
            path: "in".to_string(),
            storage: None,
            options: None,
            cast_mode: None,
        },
        sink: config::SinkConfig {
            write_mode: config::WriteMode::Overwrite,
            accepted: config::SinkTarget {
                format: "delta".to_string(),
                path: "delta/orders".to_string(),
                storage: None,
                options: None,
                merge: None,
                iceberg: None,
                partition_by: None,
                partition_spec: None,
                delta: None,
                duckdb: None,
                write_mode: config::WriteMode::Overwrite,
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
    };

    let store = delta_store_config(&target, &resolver, &entity)?;
    assert_eq!(store.table_url.as_str(), "gs://my-bucket/data/delta/orders");
    assert!(store.storage_options.is_empty());
    Ok(())
}
