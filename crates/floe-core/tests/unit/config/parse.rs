use floe_core::config::{
    IncrementalMode, SchemaEvolutionIncompatibleAction, SchemaEvolutionMode, WriteMode,
};
use floe_core::{load_config, validate_config_for_tests};

use super::super::common::write_temp_config;

#[test]
fn parse_config_loads_report_and_defaults() {
    let yaml = r#"
version: "0.1"
report:
  path: "/tmp/reports"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
      rejected:
        format: "csv"
        path: "/tmp/rejected"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
          nullable: false
          unique: true
"#;
    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");

    assert_eq!(config.report.as_ref().unwrap().path, "/tmp/reports");
    assert_eq!(config.entities.len(), 1);
    let entity = &config.entities[0];
    assert_eq!(entity.source.cast_mode.as_deref(), Some("strict"));
    let options = entity.source.options.as_ref().expect("options");
    assert_eq!(options.header, Some(true));
    assert_eq!(options.separator.as_deref(), Some(";"));
    assert_eq!(options.encoding.as_deref(), Some("UTF8"));
    assert_eq!(entity.sink.write_mode, WriteMode::Overwrite);
    assert_eq!(entity.sink.accepted.write_mode, WriteMode::Overwrite);
    assert_eq!(
        entity.sink.rejected.as_ref().unwrap().write_mode,
        WriteMode::Overwrite
    );
    assert_eq!(entity.sink.write_mode, WriteMode::Overwrite);
    let schema_evolution = entity.schema.resolved_schema_evolution();
    assert_eq!(schema_evolution.mode, SchemaEvolutionMode::Strict);
    assert_eq!(
        schema_evolution.on_incompatible,
        SchemaEvolutionIncompatibleAction::Fail
    );
}

#[test]
fn parse_config_supports_version_0_2_and_schema_evolution() {
    let yaml = r#"
version: "0.2"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "delta"
        path: "/tmp/out"
    policy:
      severity: "warn"
    schema:
      schema_evolution:
        mode: "add_columns"
        on_incompatible: "fail"
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");

    assert_eq!(config.version, "0.2");
    let schema_evolution = config.entities[0]
        .schema
        .schema_evolution
        .expect("schema evolution");
    assert_eq!(schema_evolution.mode, SchemaEvolutionMode::AddColumns);
    assert_eq!(
        schema_evolution.on_incompatible,
        SchemaEvolutionIncompatibleAction::Fail
    );
}

#[test]
fn parse_config_supports_higher_version_with_schema_evolution() {
    let yaml = r#"
version: "0.3"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "delta"
        path: "/tmp/out"
    policy:
      severity: "warn"
    schema:
      schema_evolution:
        mode: "add_columns"
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");

    assert_eq!(config.version, "0.3");
    assert_eq!(
        config.entities[0]
            .schema
            .schema_evolution
            .expect("schema evolution")
            .mode,
        SchemaEvolutionMode::AddColumns
    );
}

#[test]
fn parse_config_allows_v03_domain_without_incoming_dir() {
    let yaml = r#"
version: "0.3"
domains:
  - name: sales
entities:
  - name: orders
    domain: sales
    source:
      format: csv
      path: /tmp/input.csv
    sink:
      accepted:
        format: parquet
        path: /tmp/output
    policy:
      severity: warn
    schema:
      columns:
        - name: id
          type: string
"#;
    let config = load_config(&write_temp_config(yaml)).expect("parse config");

    assert_eq!(config.domains.len(), 1);
    assert_eq!(config.domains[0].name, "sales");
}

#[test]
fn parse_config_requires_domain_incoming_dir_in_v02() {
    let yaml = r#"
version: "0.2"
domains:
  - name: sales
entities:
  - name: orders
    source:
      format: csv
      path: /tmp/input.csv
    sink:
      accepted:
        format: parquet
        path: /tmp/output
    policy:
      severity: warn
    schema:
      columns:
        - name: id
          type: string
"#;

    let err = load_config(&write_temp_config(yaml)).expect_err("incoming_dir required in v0.2");
    assert_eq!(
        err.to_string(),
        "missing required field domains.incoming_dir"
    );
}

#[test]
fn parse_config_supports_sink_level_append_write_mode() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "append"
      accepted:
        format: "parquet"
        path: "/tmp/out"
      rejected:
        format: "csv"
        path: "/tmp/rejected"
    policy:
      severity: "reject"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");
    let entity = config.entities.first().expect("entity");
    assert_eq!(entity.sink.write_mode, WriteMode::Append);
    assert_eq!(entity.sink.accepted.write_mode, WriteMode::Append);
    assert_eq!(
        entity.sink.rejected.as_ref().unwrap().write_mode,
        WriteMode::Append
    );
    assert_eq!(entity.sink.write_mode, WriteMode::Append);
}

#[test]
fn parse_config_defaults_incremental_mode_to_none() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");

    assert_eq!(config.entities[0].incremental_mode, IncrementalMode::None);
}

#[test]
fn parse_config_supports_incremental_modes() {
    for (raw, expected) in [
        ("none", IncrementalMode::None),
        ("archive", IncrementalMode::Archive),
        ("file", IncrementalMode::File),
        ("row", IncrementalMode::Row),
    ] {
        let yaml = format!(
            r#"
version: "0.1"
entities:
  - name: "customer"
    incremental_mode: "{raw}"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#
        );
        let path = write_temp_config(&yaml);
        let config = load_config(&path).expect("parse config");

        assert_eq!(config.entities[0].incremental_mode, expected, "mode {raw}");
    }
}

#[test]
fn parse_config_supports_entity_state_path_override() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "customer"
    incremental_mode: "file"
    state:
      path: "custom/state/customer.json"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");

    assert_eq!(
        config.entities[0]
            .state
            .as_ref()
            .and_then(|state| state.path.as_deref()),
        Some("custom/state/customer.json")
    );
}

#[test]
fn parse_config_maps_legacy_sink_archive_to_resolved_archive_mode() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
      archive:
        path: "/tmp/archive"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");

    assert_eq!(config.entities[0].incremental_mode, IncrementalMode::None);
    assert_eq!(
        config.entities[0].resolved_incremental_mode(),
        IncrementalMode::Archive
    );
    assert!(config.entities[0].archive_enabled());
}

#[test]
fn parse_config_rejects_empty_entity_state_path_during_validation() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "customer"
    incremental_mode: "file"
    state:
      path: ""
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");
    let err =
        validate_config_for_tests(&config).expect_err("empty state.path should fail validation");

    assert!(err
        .to_string()
        .contains("entity.state.path must not be empty"));
}

#[test]
fn parse_config_supports_sink_level_merge_scd1_write_mode() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "merge_scd1"
      accepted:
        format: "delta"
        path: "/tmp/out"
      rejected:
        format: "csv"
        path: "/tmp/rejected"
    policy:
      severity: "reject"
    schema:
      primary_key: ["customer_id"]
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");
    let entity = config.entities.first().expect("entity");
    assert_eq!(entity.sink.write_mode, WriteMode::MergeScd1);
    assert_eq!(entity.sink.accepted.write_mode, WriteMode::MergeScd1);
    assert_eq!(
        entity.sink.rejected.as_ref().unwrap().write_mode,
        WriteMode::MergeScd1
    );
    assert_eq!(entity.sink.write_mode, WriteMode::MergeScd1);
}

#[test]
fn parse_config_supports_sink_level_merge_scd2_write_mode() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "merge_scd2"
      accepted:
        format: "delta"
        path: "/tmp/out"
      rejected:
        format: "csv"
        path: "/tmp/rejected"
    policy:
      severity: "reject"
    schema:
      primary_key: ["customer_id"]
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");
    let entity = config.entities.first().expect("entity");
    assert_eq!(entity.sink.write_mode, WriteMode::MergeScd2);
    assert_eq!(entity.sink.accepted.write_mode, WriteMode::MergeScd2);
    assert_eq!(
        entity.sink.rejected.as_ref().unwrap().write_mode,
        WriteMode::MergeScd2
    );
    assert_eq!(entity.sink.write_mode, WriteMode::MergeScd2);
}

#[test]
fn parse_config_supports_delta_merge_options_block() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "merge_scd2"
      accepted:
        format: "delta"
        path: "/tmp/out"
        merge:
          ignore_columns: ["ingested_at", "load_ts"]
          compare_columns: ["name", "address", "status"]
          scd2:
            current_flag_column: "__is_current"
            valid_from_column: "__valid_from"
            valid_to_column: "__valid_to"
      rejected:
        format: "csv"
        path: "/tmp/rejected"
    policy:
      severity: "reject"
    schema:
      primary_key: ["customer_id"]
      columns:
        - name: "customer_id"
          type: "string"
        - name: "name"
          type: "string"
        - name: "address"
          type: "string"
        - name: "status"
          type: "string"
        - name: "ingested_at"
          type: "datetime"
        - name: "load_ts"
          type: "datetime"
"#;
    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");
    let entity = config.entities.first().expect("entity");
    let merge = entity
        .sink
        .accepted
        .merge
        .as_ref()
        .expect("sink.accepted.merge");

    assert_eq!(
        merge.ignore_columns.as_ref().expect("ignore columns"),
        &vec!["ingested_at".to_string(), "load_ts".to_string()]
    );
    assert_eq!(
        merge.compare_columns.as_ref().expect("compare columns"),
        &vec![
            "name".to_string(),
            "address".to_string(),
            "status".to_string()
        ]
    );
    let scd2 = merge.scd2.as_ref().expect("scd2 options");
    assert_eq!(scd2.current_flag_column.as_deref(), Some("__is_current"));
    assert_eq!(scd2.valid_from_column.as_deref(), Some("__valid_from"));
    assert_eq!(scd2.valid_to_column.as_deref(), Some("__valid_to"));
}

#[test]
fn parse_config_defaults_column_source_to_name() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "users"
    source:
      format: "json"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
      rejected:
        format: "csv"
        path: "/tmp/rejected"
    policy:
      severity: "reject"
    schema:
      columns:
        - name: "user_first_name"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");
    let column = &config.entities[0].schema.columns[0];
    assert_eq!(column.name, "user_first_name");
    assert_eq!(column.source_or_name(), "user_first_name");
    assert!(column.source.is_none());
}

#[test]
fn parse_config_preserves_column_source() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "users"
    source:
      format: "json"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
      rejected:
        format: "csv"
        path: "/tmp/rejected"
    policy:
      severity: "reject"
    schema:
      columns:
        - name: "user_first_name"
          source: "user.names[0]"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");
    let column = &config.entities[0].schema.columns[0];
    assert_eq!(column.name, "user_first_name");
    assert_eq!(column.source.as_deref(), Some("user.names[0]"));
    assert_eq!(column.source_or_name(), "user.names[0]");
}

#[test]
fn parse_config_supports_schema_primary_key_and_unique_keys() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "users"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
      rejected:
        format: "csv"
        path: "/tmp/rejected"
    policy:
      severity: "reject"
    schema:
      primary_key: ["id", "country"]
      unique_keys:
        - ["id", "country"]
        - ["email"]
      columns:
        - name: "id"
          type: "string"
        - name: "country"
          type: "string"
        - name: "email"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");
    let schema = &config.entities[0].schema;
    assert_eq!(
        schema.primary_key.as_ref().expect("primary key"),
        &vec!["id".to_string(), "country".to_string()]
    );
    assert_eq!(
        schema.unique_keys.as_ref().expect("unique keys"),
        &vec![
            vec!["id".to_string(), "country".to_string()],
            vec!["email".to_string()]
        ]
    );
}

#[test]
fn parse_config_prefers_schema_unique_keys_over_legacy_column_unique_flags() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "users"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
      rejected:
        format: "csv"
        path: "/tmp/rejected"
    policy:
      severity: "reject"
    schema:
      unique_keys:
        - ["id", "country"]
      columns:
        - name: "id"
          type: "string"
          unique: true
        - name: "country"
          type: "string"
        - name: "email"
          type: "string"
          unique: true
"#;
    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");
    let schema = &config.entities[0].schema;
    let constraints = floe_core::check::resolve_schema_unique_keys(schema);
    assert_eq!(
        constraints,
        vec![vec!["id".to_string(), "country".to_string()]]
    );
}

#[test]
fn parse_config_supports_sink_partitioning_and_file_size_knobs() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "events_delta"
    source:
      format: "csv"
      path: "/tmp/input.csv"
    sink:
      accepted:
        format: "delta"
        path: "/tmp/out_delta"
        partition_by: ["event_date", "country"]
      rejected:
        format: "csv"
        path: "/tmp/rejected"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "event_date"
          type: "date"
        - name: "country"
          type: "string"
  - name: "events_iceberg"
    source:
      format: "csv"
      path: "/tmp/input.csv"
    sink:
      accepted:
        format: "iceberg"
        path: "/tmp/out_iceberg"
        options:
          max_size_per_file: 268435456
        partition_spec:
          - column: "event_date"
            transform: "day"
          - column: "country"
      rejected:
        format: "csv"
        path: "/tmp/rejected"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "event_date"
          type: "date"
        - name: "country"
          type: "string"
"#;

    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");

    let delta = &config.entities[0];
    assert_eq!(
        delta
            .sink
            .accepted
            .partition_by
            .as_ref()
            .expect("partition_by"),
        &vec!["event_date".to_string(), "country".to_string()]
    );
    assert!(delta.sink.accepted.partition_spec.is_none());

    let iceberg = &config.entities[1];
    assert_eq!(
        iceberg
            .sink
            .accepted
            .options
            .as_ref()
            .expect("options")
            .max_size_per_file,
        Some(268435456)
    );
    let spec = iceberg
        .sink
        .accepted
        .partition_spec
        .as_ref()
        .expect("partition_spec");
    assert_eq!(spec.len(), 2);
    assert_eq!(spec[0].column, "event_date");
    assert_eq!(spec[0].transform, "day");
    assert_eq!(spec[1].column, "country");
    assert_eq!(spec[1].transform, "identity");
}

#[test]
fn parse_config_supports_catalogs_and_iceberg_catalog_binding() {
    let yaml = r#"
version: "0.1"
storages:
  default: "s3_out"
  definitions:
    - name: "s3_out"
      type: "s3"
      bucket: "data-bucket"
      region: "us-east-1"
      prefix: "accepted"
catalogs:
  default: "glue_main"
  definitions:
    - name: "glue_main"
      type: "glue"
      region: "us-east-1"
      database: "lakehouse"
      warehouse_storage: "s3_out"
      warehouse_prefix: "iceberg"
domains:
  - name: "sales"
    incoming_dir: "/tmp/incoming/sales"
entities:
  - name: "orders"
    domain: "sales"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "iceberg"
        path: "orders_table"
        iceberg:
          catalog: "glue_main"
          namespace: "sales_ops"
          table: "orders_fact"
          location: "custom/orders"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "number"
"#;

    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");
    let catalogs = config.catalogs.as_ref().expect("catalogs");
    assert_eq!(catalogs.default.as_deref(), Some("glue_main"));
    assert_eq!(catalogs.definitions.len(), 1);
    assert!(matches!(
        catalogs.definitions[0].type_config,
        floe_core::config::CatalogTypeConfig::Glue { ref database, .. } if database == "lakehouse"
    ));
    assert_eq!(
        catalogs.definitions[0].warehouse_prefix.as_deref(),
        Some("iceberg")
    );

    let iceberg = config.entities[0]
        .sink
        .accepted
        .iceberg
        .as_ref()
        .expect("iceberg options");
    assert_eq!(iceberg.catalog.as_deref(), Some("glue_main"));
    assert_eq!(iceberg.namespace.as_deref(), Some("sales_ops"));
    assert_eq!(iceberg.table.as_deref(), Some("orders_fact"));
    assert_eq!(iceberg.location.as_deref(), Some("custom/orders"));
}

#[test]
fn parse_config_supports_rest_catalog() {
    let yaml = r#"
version: "0.1"
storages:
  default: "s3_out"
  definitions:
    - name: "s3_out"
      type: "s3"
      bucket: "data-bucket"
      region: "us-east-1"
      prefix: "accepted"
catalogs:
  default: "unity_main"
  definitions:
    - name: "unity_main"
      type: "rest"
      uri: "https://adb-123.azuredatabricks.net/api/2.1/unity-catalog/iceberg"
      credential: "token:my_token"
      warehouse: "my_catalog.my_schema"
    - name: "snowflake_main"
      type: "rest"
      uri: "https://account.snowflakecomputing.com/polaris/api/catalog"
      credential: "client_credentials:client_id:client_secret"
      warehouse: "my_catalog"
      oauth2_server_uri: "https://account.snowflakecomputing.com/oauth/token"
      scope: "PRINCIPAL_ROLE:ALL"
      warehouse_storage: "s3_out"
      warehouse_prefix: "iceberg"
domains:
  - name: "sales"
    incoming_dir: "/tmp/incoming"
entities:
  - name: "orders"
    domain: "sales"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "iceberg"
        path: "orders_table"
        storage: "s3_out"
        iceberg:
          catalog: "unity_main"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "number"
"#;

    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");
    let catalogs = config.catalogs.as_ref().expect("catalogs");
    assert_eq!(catalogs.default.as_deref(), Some("unity_main"));
    assert_eq!(catalogs.definitions.len(), 2);

    let unity = &catalogs.definitions[0];
    assert_eq!(unity.name, "unity_main");
    assert!(matches!(
        &unity.type_config,
        floe_core::config::CatalogTypeConfig::Rest { uri, credential, warehouse, oauth2_server_uri, scope }
        if uri == "https://adb-123.azuredatabricks.net/api/2.1/unity-catalog/iceberg"
            && credential.as_ref().map(floe_core::Secret::expose) == Some("token:my_token")
            && warehouse.as_deref() == Some("my_catalog.my_schema")
            && oauth2_server_uri.is_none()
            && scope.is_none()
    ));

    let snowflake = &catalogs.definitions[1];
    assert_eq!(snowflake.name, "snowflake_main");
    assert!(matches!(
        &snowflake.type_config,
        floe_core::config::CatalogTypeConfig::Rest { oauth2_server_uri, scope, .. }
        if oauth2_server_uri.as_deref() == Some("https://account.snowflakecomputing.com/oauth/token")
            && scope.as_deref() == Some("PRINCIPAL_ROLE:ALL")
    ));
    assert_eq!(snowflake.warehouse_storage.as_deref(), Some("s3_out"));
    assert_eq!(snowflake.warehouse_prefix.as_deref(), Some("iceberg"));
}

#[test]
fn parse_config_rest_catalog_warehouse_storage_satisfies_binding_without_cloud_accepted_storage() {
    // When a REST catalog has warehouse_storage, the entity's accepted storage doesn't
    // need to be cloud-backed — the catalog's warehouse_storage is the actual table root.
    let yaml = r#"
version: "0.1"
storages:
  default: "local_staging"
  definitions:
    - name: "local_staging"
      type: "local"
    - name: "s3_wh"
      type: "s3"
      bucket: "iceberg-bucket"
      region: "us-east-1"
      prefix: "tables"
catalogs:
  default: "unity_main"
  definitions:
    - name: "unity_main"
      type: "rest"
      uri: "https://adb-123.azuredatabricks.net/api/2.1/unity-catalog/iceberg"
      credential: "token:my_pat"
      warehouse: "my_catalog.my_schema"
      warehouse_storage: "s3_wh"
domains:
  - name: "sales"
    incoming_dir: "/tmp/incoming"
entities:
  - name: "orders"
    domain: "sales"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "iceberg"
        path: "orders_table"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "number"
"#;

    let path = write_temp_config(yaml);
    let config = load_config(&path).expect(
        "REST catalog with warehouse_storage should validate when accepted storage is local",
    );
    assert_eq!(config.entities[0].name, "orders");
}
