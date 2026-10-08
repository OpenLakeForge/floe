use floe_core::config;

fn base_root() -> config::RootConfig {
    config::RootConfig {
        version: "0.1".to_string(),
        metadata: None,
        storages: Some(config::StoragesConfig {
            default: Some("s3_out".to_string()),
            definitions: vec![
                config::StorageDefinition {
                    name: "s3_out".to_string(),
                    fs_type: "s3".to_string(),
                    bucket: Some("data-bucket".to_string()),
                    region: Some("us-east-1".to_string()),
                    account: None,
                    container: None,
                    prefix: Some("accepted".to_string()),
                    endpoint: None,
                    path_style_access: None,
                },
                config::StorageDefinition {
                    name: "s3_wh".to_string(),
                    fs_type: "s3".to_string(),
                    bucket: Some("warehouse-bucket".to_string()),
                    region: Some("us-east-1".to_string()),
                    account: None,
                    container: None,
                    prefix: Some("warehouse".to_string()),
                    endpoint: None,
                    path_style_access: None,
                },
            ],
        }),
        catalogs: Some(config::CatalogsConfig {
            default: Some("glue_main".to_string()),
            definitions: vec![config::CatalogDefinition {
                name: "glue_main".to_string(),
                type_config: config::CatalogTypeConfig::Glue {
                    region: "us-east-1".to_string(),
                    database: "lakehouse".to_string(),
                    create_database_if_missing: true,
                    allow_takeover: false,
                },
                warehouse_storage: Some("s3_wh".to_string()),
                warehouse_prefix: Some("iceberg".to_string()),
            }],
        }),
        env: None,
        domains: Vec::new(),
        report: None,
        lineage: None,
        entities: Vec::new(),
        config_files: Vec::new(),
    }
}

fn entity() -> config::EntityConfig {
    config::EntityConfig {
        name: "Customer Orders".to_string(),
        id: "Customer Orders".to_string(),
        metadata: None,
        domain: Some("Sales Ops".to_string()),
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
            write_mode: config::WriteMode::Append,
            accepted: config::SinkTarget {
                format: "iceberg".to_string(),
                path: "customer/orders".to_string(),
                storage: Some("s3_out".to_string()),
                options: None,
                merge: None,
                iceberg: Some(config::IcebergSinkTargetConfig {
                    catalog: Some("glue_main".to_string()),
                    namespace: None,
                    table: None,
                    location: None,
                }),
                delta: None,
                duckdb: None,
                partition_by: None,
                partition_spec: None,
                write_mode: config::WriteMode::Append,
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
fn catalog_resolver_derives_glue_identity_and_warehouse_location() {
    let root = base_root();
    let resolver = config::StorageResolver::from_path(&root, std::path::Path::new("./config.yml"))
        .expect("storage resolver");
    let catalogs = config::CatalogResolver::new(&root).expect("catalog resolver");
    let entity = entity();

    let resolved = catalogs
        .resolve_iceberg_target(&resolver, &entity, &entity.sink.accepted)
        .expect("resolve")
        .expect("glue target");

    assert_eq!(resolved.catalog_name, "glue_main");
    assert!(
        matches!(resolved.type_config, config::CatalogTypeConfig::Glue { ref database, .. } if database == "lakehouse")
    );
    assert_eq!(resolved.namespace, "sales_ops");
    assert_eq!(resolved.table, "customer_orders");
    assert_eq!(
        resolved.table_location.uri,
        "s3://warehouse-bucket/warehouse/iceberg/sales_ops/customer_orders"
    );
}

#[test]
fn catalog_resolver_rest_derives_namespace_and_location() {
    let mut root = base_root();
    root.catalogs = Some(config::CatalogsConfig {
        default: Some("unity_main".to_string()),
        definitions: vec![config::CatalogDefinition {
            name: "unity_main".to_string(),
            type_config: config::CatalogTypeConfig::Rest {
                uri: "https://adb-123.azuredatabricks.net/api/2.1/unity-catalog/iceberg"
                    .to_string(),
                credential: Some(floe_core::Secret::from("token:my_token")),
                warehouse: Some("my_catalog.my_schema".to_string()),
                oauth2_server_uri: None,
                scope: None,
            },
            warehouse_storage: Some("s3_wh".to_string()),
            warehouse_prefix: Some("iceberg".to_string()),
        }],
    });
    let resolver = config::StorageResolver::from_path(&root, std::path::Path::new("./config.yml"))
        .expect("storage resolver");
    let catalogs = config::CatalogResolver::new(&root).expect("catalog resolver");
    let mut entity = entity();
    entity.sink.accepted.iceberg = Some(config::IcebergSinkTargetConfig {
        catalog: Some("unity_main".to_string()),
        namespace: None,
        table: None,
        location: None,
    });

    let resolved = catalogs
        .resolve_iceberg_target(&resolver, &entity, &entity.sink.accepted)
        .expect("resolve")
        .expect("rest target");

    assert_eq!(resolved.catalog_name, "unity_main");
    assert!(matches!(
        resolved.type_config,
        config::CatalogTypeConfig::Rest { ref uri, .. } if uri == "https://adb-123.azuredatabricks.net/api/2.1/unity-catalog/iceberg"
    ));
    // namespace falls back to entity domain "Sales Ops" normalized
    assert_eq!(resolved.namespace, "sales_ops");
    assert_eq!(resolved.table, "customer_orders");
    assert_eq!(
        resolved.table_location.uri,
        "s3://warehouse-bucket/warehouse/iceberg/sales_ops/customer_orders"
    );
}

#[test]
fn catalog_resolver_rest_uses_default_namespace_fallback() {
    let mut root = base_root();
    root.catalogs = Some(config::CatalogsConfig {
        default: Some("polaris_main".to_string()),
        definitions: vec![config::CatalogDefinition {
            name: "polaris_main".to_string(),
            type_config: config::CatalogTypeConfig::Rest {
                uri: "https://my-polaris.example.com/api/catalog".to_string(),
                credential: None,
                warehouse: Some("my_catalog.my_schema".to_string()),
                oauth2_server_uri: None,
                scope: None,
            },
            warehouse_storage: Some("s3_wh".to_string()),
            warehouse_prefix: None,
        }],
    });
    let resolver = config::StorageResolver::from_path(&root, std::path::Path::new("./config.yml"))
        .expect("storage resolver");
    let catalogs = config::CatalogResolver::new(&root).expect("catalog resolver");
    // Entity with no domain or namespace — should fall back to "default", not the warehouse value
    // (warehouse is a catalog/bucket identifier, not a namespace).
    let mut entity = entity();
    entity.domain = None;
    entity.sink.accepted.iceberg = Some(config::IcebergSinkTargetConfig {
        catalog: Some("polaris_main".to_string()),
        namespace: None,
        table: None,
        location: None,
    });

    let resolved = catalogs
        .resolve_iceberg_target(&resolver, &entity, &entity.sink.accepted)
        .expect("resolve")
        .expect("rest target");

    assert_eq!(resolved.namespace, "default");
    assert_eq!(resolved.table, "customer_orders");
}

#[test]
fn catalog_resolver_honors_explicit_location_override() {
    let root = base_root();
    let resolver = config::StorageResolver::from_path(&root, std::path::Path::new("./config.yml"))
        .expect("storage resolver");
    let catalogs = config::CatalogResolver::new(&root).expect("catalog resolver");
    let mut entity = entity();
    entity.sink.accepted.iceberg = Some(config::IcebergSinkTargetConfig {
        catalog: Some("glue_main".to_string()),
        namespace: Some("billing".to_string()),
        table: Some("invoice_events".to_string()),
        location: Some("custom/location".to_string()),
    });

    let resolved = catalogs
        .resolve_iceberg_target(&resolver, &entity, &entity.sink.accepted)
        .expect("resolve")
        .expect("glue target");

    assert_eq!(resolved.namespace, "billing");
    assert_eq!(resolved.table, "invoice_events");
    assert_eq!(
        resolved.table_location.uri,
        "s3://warehouse-bucket/warehouse/custom/location"
    );
}

// ---------------------------------------------------------------------------
// Unity Catalog / Delta tests
// ---------------------------------------------------------------------------

fn unity_root() -> config::RootConfig {
    config::RootConfig {
        version: "0.1".to_string(),
        metadata: None,
        storages: Some(config::StoragesConfig {
            default: Some("s3_out".to_string()),
            definitions: vec![config::StorageDefinition {
                name: "s3_out".to_string(),
                fs_type: "s3".to_string(),
                bucket: Some("data-bucket".to_string()),
                region: Some("us-east-1".to_string()),
                account: None,
                container: None,
                prefix: Some("accepted".to_string()),
                endpoint: None,
                path_style_access: None,
            }],
        }),
        catalogs: Some(config::CatalogsConfig {
            default: Some("databricks".to_string()),
            definitions: vec![config::CatalogDefinition {
                name: "databricks".to_string(),
                type_config: config::CatalogTypeConfig::Unity {
                    host: "https://my-workspace.azuredatabricks.net".to_string(),
                    catalog: "my_catalog".to_string(),
                    schema: "my_schema".to_string(),
                    token: floe_core::Secret::from("dapi-token"),
                    create_schema_if_missing: false,
                },
                warehouse_storage: None,
                warehouse_prefix: None,
            }],
        }),
        env: None,
        domains: Vec::new(),
        report: None,
        lineage: None,
        entities: Vec::new(),
        config_files: Vec::new(),
    }
}

fn delta_entity() -> config::EntityConfig {
    config::EntityConfig {
        name: "orders".to_string(),
        id: "orders".to_string(),
        metadata: None,
        domain: Some("sales".to_string()),
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
            write_mode: config::WriteMode::Append,
            accepted: config::SinkTarget {
                format: "delta".to_string(),
                path: "warehouse/orders".to_string(),
                storage: Some("s3_out".to_string()),
                options: None,
                merge: None,
                iceberg: None,
                delta: Some(config::DeltaSinkTargetConfig {
                    catalog: Some("databricks".to_string()),
                    schema: None,
                    table: None,
                }),
                duckdb: None,
                partition_by: None,
                partition_spec: None,
                write_mode: config::WriteMode::Append,
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
fn resolve_delta_target_returns_none_when_no_delta_config() {
    let root = unity_root();
    let catalogs = config::CatalogResolver::new(&root).expect("catalog resolver");
    let entity = entity(); // entity with iceberg config, no delta

    let resolved = catalogs
        .resolve_delta_target(&entity, &entity.sink.accepted)
        .expect("resolve");

    assert!(resolved.is_none());
}

#[test]
fn resolve_delta_target_uses_entity_name_as_default_table() {
    let root = unity_root();
    let catalogs = config::CatalogResolver::new(&root).expect("catalog resolver");
    let entity = delta_entity();

    let resolved = catalogs
        .resolve_delta_target(&entity, &entity.sink.accepted)
        .expect("resolve")
        .expect("unity target");

    assert_eq!(resolved.catalog_name, "databricks");
    assert_eq!(resolved.table, "orders");
}

#[test]
fn resolve_delta_target_uses_domain_as_default_schema() {
    let root = unity_root();
    let catalogs = config::CatalogResolver::new(&root).expect("catalog resolver");
    let entity = delta_entity();

    let resolved = catalogs
        .resolve_delta_target(&entity, &entity.sink.accepted)
        .expect("resolve")
        .expect("unity target");

    assert_eq!(resolved.schema, "sales");
}

#[test]
fn resolve_delta_target_honors_explicit_table_override() {
    let root = unity_root();
    let catalogs = config::CatalogResolver::new(&root).expect("catalog resolver");
    let mut entity = delta_entity();
    entity.sink.accepted.delta = Some(config::DeltaSinkTargetConfig {
        catalog: Some("databricks".to_string()),
        schema: Some("billing".to_string()),
        table: Some("invoice_events".to_string()),
    });

    let resolved = catalogs
        .resolve_delta_target(&entity, &entity.sink.accepted)
        .expect("resolve")
        .expect("unity target");

    assert_eq!(resolved.schema, "billing");
    assert_eq!(resolved.table, "invoice_events");
}

#[test]
fn validate_config_rejects_unity_catalog_for_iceberg_sink() {
    let mut root = unity_root();
    let mut entity = entity(); // iceberg entity
    entity.sink.accepted.iceberg = Some(config::IcebergSinkTargetConfig {
        catalog: Some("databricks".to_string()),
        namespace: None,
        table: None,
        location: None,
    });
    root.entities.push(entity);

    let err =
        floe_core::validate_config_for_tests(&root).expect_err("should reject unity for iceberg");
    assert!(
        err.to_string().contains("unity"),
        "error should mention unity: {err}"
    );
}

#[test]
fn validate_config_rejects_local_storage_for_unity_catalog() {
    let mut root = unity_root();
    root.storages = Some(config::StoragesConfig {
        default: Some("local_out".to_string()),
        definitions: vec![config::StorageDefinition {
            name: "local_out".to_string(),
            fs_type: "local".to_string(),
            bucket: None,
            region: None,
            account: None,
            container: None,
            prefix: None,
            endpoint: None,
            path_style_access: None,
        }],
    });
    let mut entity = delta_entity();
    entity.sink.accepted.storage = Some("local_out".to_string());
    root.entities.push(entity);

    let err = floe_core::validate_config_for_tests(&root).expect_err("should reject local storage");
    assert!(
        err.to_string().contains("local"),
        "error should mention local storage: {err}"
    );
}

#[test]
fn validate_config_accepts_s3_storage_for_unity_catalog() {
    let mut root = unity_root();
    let entity = delta_entity();
    root.entities.push(entity);

    floe_core::validate_config_for_tests(&root).expect("s3 + unity catalog should be valid");
}

#[test]
fn validate_config_rejects_delta_block_on_non_delta_format() {
    let mut root = unity_root();
    let mut entity = delta_entity();
    // Switch format to parquet but keep the delta catalog block — should be rejected.
    entity.sink.accepted.format = "parquet".to_string();
    root.entities.push(entity);

    let err = floe_core::validate_config_for_tests(&root)
        .expect_err("delta block on non-delta format should be rejected");
    assert!(
        err.to_string().contains("format=delta"),
        "error should mention format=delta: {err}"
    );
}
