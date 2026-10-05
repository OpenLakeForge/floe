use floe_core::{validate, ValidateOptions};

use super::super::common::write_temp_config;

fn assert_validation_error(contents: &str, expected_parts: &[&str]) {
    let path = write_temp_config(contents);
    let err = validate(&path, ValidateOptions::default()).expect_err("expected error");
    let message = err.to_string();
    for part in expected_parts {
        assert!(
            message.contains(part),
            "expected error to contain {part:?}, got: {message}"
        );
    }
}

fn assert_validation_ok(contents: &str) {
    let path = write_temp_config(contents);
    validate(&path, ValidateOptions::default()).expect("expected config to be valid");
}

fn base_entity(name: &str) -> String {
    format!(
        r#"  - name: "{name}"
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
"#
    )
}

fn base_config(entities_yaml: &str) -> String {
    base_config_with_version("0.1", entities_yaml)
}

fn base_config_with_version(version: &str, entities_yaml: &str) -> String {
    format!(
        r#"version: "{version}"
report:
  path: "/tmp/reports"
entities:
{entities_yaml}"#
    )
}

fn config_with_storages(storages_yaml: &str, entities_yaml: &str) -> String {
    format!(
        r#"version: "0.1"
storages:
{storages_yaml}
report:
  path: "/tmp/reports"
entities:
{entities_yaml}"#
    )
}

fn config_with_storages_and_catalogs(
    storages_yaml: &str,
    catalogs_yaml: &str,
    entities_yaml: &str,
) -> String {
    format!(
        r#"version: "0.1"
storages:
{storages_yaml}
catalogs:
{catalogs_yaml}
report:
  path: "/tmp/reports"
entities:
{entities_yaml}"#
    )
}

#[test]
fn missing_report_path_defaults() {
    let yaml = format!(
        r#"version: "0.1"
report: {{}}
entities:
{}"#,
        base_entity("customer")
    );
    assert_validation_ok(&yaml);
}

#[test]
fn report_section_optional() {
    let yaml = format!(
        r#"version: "0.1"
entities:
{}"#,
        base_entity("customer")
    );
    assert_validation_ok(&yaml);
}

#[test]
fn remote_report_path_rejected_when_storage_is_local() {
    let yaml = format!(
        r#"version: "0.1"
report:
  path: "s3://floe-demo-bucket/reports"
entities:
{}"#,
        base_entity("customer")
    );
    assert_validation_error(
        &yaml,
        &["report.path must be a local path (got s3://floe-demo-bucket/reports)"],
    );
}

#[test]
fn remote_report_path_rejected_for_gcs() {
    let yaml = format!(
        r#"version: "0.1"
report:
  path: "gs://floe-demo-bucket/reports"
entities:
{}"#,
        base_entity("customer")
    );
    assert_validation_error(
        &yaml,
        &["report.path must be a local path (got gs://floe-demo-bucket/reports)"],
    );
}

#[test]
fn supported_config_versions_are_valid() {
    for version in &["0.2", "0.3"] {
        assert_validation_ok(&base_config_with_version(version, &base_entity("customer")));
    }
}

#[test]
fn malformed_config_version_errors() {
    assert_validation_error(
        &base_config_with_version("abc", &base_entity("customer")),
        &["root.version=abc", "invalid", "major.minor"],
    );
}

#[test]
fn config_version_below_minimum_errors() {
    assert_validation_error(
        &base_config_with_version("0.0", &base_entity("customer")),
        &[
            "root.version=0.0",
            "unsupported",
            "minimum supported version",
            "0.1",
        ],
    );
}

#[test]
fn empty_entities_errors() {
    let yaml = r#"version: "0.1"
report:
  path: "/tmp/reports"
entities: []
"#;
    assert_validation_error(yaml, &["entities", "empty"]);
}

#[test]
fn entity_missing_name_errors() {
    let yaml = r#"version: "0.1"
report:
  path: "/tmp/reports"
entities:
  - source:
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
    assert_validation_error(yaml, &["entity.name", "entities[0]"]);
}

#[test]
fn invalid_source_format_errors() {
    let entity = r#"  - name: "customer"
    source:
      format: "toml"
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
    let yaml = base_config(entity);
    assert_validation_error(&yaml, &["entity.name=customer", "source.format", "toml"]);
}

#[test]
fn empty_entity_state_path_errors() {
    let entity = r#"  - name: "customer"
    state:
      path: ""
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
"#;
    let yaml = base_config(entity);
    assert_validation_error(
        &yaml,
        &[
            "entity.name=customer",
            "entity.state.path",
            "must not be empty",
        ],
    );
}

#[test]
fn unsupported_incremental_mode_errors() {
    let entity = r#"  - name: "customer"
    incremental_mode: "batch"
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
    let yaml = base_config(entity);
    assert_validation_error(
        &yaml,
        &[
            "entity.incremental_mode",
            "batch",
            "allowed: none, archive, file, row",
        ],
    );
}

#[test]
fn archive_incremental_mode_requires_archive_sink() {
    let entity = r#"  - name: "customer"
    incremental_mode: "archive"
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
    let yaml = base_config(entity);
    assert_validation_error(
        &yaml,
        &[
            "entity.name=customer",
            "incremental_mode=archive requires sink.archive",
        ],
    );
}

#[test]
fn sink_archive_conflicts_with_file_incremental_mode() {
    let entity = r#"  - name: "customer"
    incremental_mode: "file"
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
    let yaml = base_config(entity);
    assert_validation_error(
        &yaml,
        &[
            "entity.name=customer",
            "sink.archive conflicts with incremental_mode=file",
        ],
    );
}

#[test]
fn xml_source_requires_row_tag() {
    let entity = r#"  - name: "customer"
    source:
      format: "xml"
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
    let yaml = base_config(entity);
    assert_validation_error(
        &yaml,
        &[
            "entity.name=customer",
            "source.options.row_tag",
            "xml input",
        ],
    );
}

#[test]
fn xml_source_invalid_selector_errors() {
    let entity = r#"  - name: "customer"
    source:
      format: "xml"
      path: "/tmp/input"
      options:
        row_tag: "row"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          source: "user..id"
          type: "string"
"#;
    let yaml = base_config(entity);
    assert_validation_error(
        &yaml,
        &[
            "entity.name=customer",
            "schema.columns[0].source=user..id",
            "empty token",
        ],
    );
}

#[test]
fn delta_partition_by_validates_schema_columns() {
    let entity = r#"  - name: "orders"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "delta"
        path: "/tmp/out_delta"
        partition_by: ["order_date", "country"]
      rejected:
        format: "csv"
        path: "/tmp/rejected"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "order_id"
          type: "string"
        - name: "order_date"
          type: "date"
        - name: "country"
          type: "string"
"#;
    assert_validation_ok(&base_config(entity));
}

#[test]
fn delta_partition_by_unknown_column_errors() {
    let entity = r#"  - name: "orders"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "delta"
        path: "/tmp/out_delta"
        partition_by: ["missing_col"]
      rejected:
        format: "csv"
        path: "/tmp/rejected"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "order_id"
          type: "string"
"#;
    assert_validation_error(
        &base_config(entity),
        &[
            "entity.name=orders",
            "sink.accepted.partition_by[0]=missing_col",
            "unknown schema column",
        ],
    );
}

#[test]
fn schema_evolution_add_columns_requires_delta_sink() {
    let entity = r#"  - name: "orders"
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
      schema_evolution:
        mode: "add_columns"
      columns:
        - name: "order_id"
          type: "string"
"#;
    assert_validation_error(
        &base_config_with_version("0.2", entity),
        &[
            "entity.name=orders",
            "schema.schema_evolution.mode=add_columns",
            "sink.accepted.format=delta",
        ],
    );
}

#[test]
fn schema_evolution_is_rejected_for_version_0_1() {
    let entity = r#"  - name: "orders"
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
        mode: "strict"
      columns:
        - name: "order_id"
          type: "string"
"#;
    assert_validation_error(
        &base_config(entity),
        &[
            "entity.name=orders",
            "schema.schema_evolution",
            "root.version >= \"0.2\"",
        ],
    );
}

#[test]
fn schema_evolution_add_columns_is_valid_for_delta() {
    let entity = r#"  - name: "orders"
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
        - name: "order_id"
          type: "string"
"#;
    for version in &["0.2", "0.3"] {
        assert_validation_ok(&base_config_with_version(version, entity));
    }
}

#[test]
fn iceberg_partition_spec_validates_supported_transform_subset() {
    let entity = r#"  - name: "orders"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "iceberg"
        path: "/tmp/out_iceberg"
        partition_spec:
          - column: "order_date"
            transform: "day"
          - column: "country"
            transform: "identity"
      rejected:
        format: "csv"
        path: "/tmp/rejected"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "order_date"
          type: "date"
        - name: "country"
          type: "string"
"#;
    assert_validation_ok(&base_config(entity));
}

#[test]
fn iceberg_partition_spec_unsupported_transform_errors() {
    let entity = r#"  - name: "orders"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "iceberg"
        path: "/tmp/out_iceberg"
        partition_spec:
          - column: "order_date"
            transform: "bucket[16]"
      rejected:
        format: "csv"
        path: "/tmp/rejected"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "order_date"
          type: "date"
"#;
    assert_validation_error(
        &base_config(entity),
        &[
            "entity.name=orders",
            "partition_spec[0].transform=bucket[16]",
            "unsupported",
        ],
    );
}

#[test]
fn accepted_partition_by_and_partition_spec_are_mutually_exclusive() {
    let entity = r#"  - name: "orders"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "iceberg"
        path: "/tmp/out_iceberg"
        partition_by: ["country"]
        partition_spec:
          - column: "country"
      rejected:
        format: "csv"
        path: "/tmp/rejected"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "country"
          type: "string"
"#;
    assert_validation_error(
        &base_config(entity),
        &[
            "entity.name=orders",
            "partition_by",
            "partition_spec",
            "mutually exclusive",
        ],
    );
}

#[test]
fn fixed_width_requires_column_widths() {
    let entity = r#"  - name: "customer"
    source:
      format: "fixed"
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
    let yaml = base_config(entity);
    assert_validation_error(&yaml, &["schema.columns[0].width", "source.format=fixed"]);
}

#[test]
fn json_source_defaults_to_array_mode() {
    let entity = r#"  - name: "customer"
    source:
      format: "json"
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
    let yaml = base_config(entity);
    assert_validation_ok(&yaml);
}

#[test]
fn json_source_rejects_unknown_mode() {
    let entity = r#"  - name: "customer"
    source:
      format: "json"
      path: "/tmp/input"
      options:
        json_mode: "lines"
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
    let yaml = base_config(entity);
    assert_validation_error(
        &yaml,
        &[
            "entity.name=customer",
            "source.options.json_mode=lines",
            "array",
            "ndjson",
        ],
    );
}

#[test]
fn json_source_invalid_selector_errors() {
    let yaml = r#"version: "0.1"
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
        - name: "user_name"
          source: "user..name"
          type: "string"
"#;
    assert_validation_error(yaml, &["schema.columns[0].source", "invalid"]);
}

#[test]
fn invalid_cast_mode_errors() {
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
      cast_mode: "unsafe"
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
    let yaml = base_config(entity);
    assert_validation_error(
        &yaml,
        &["entity.name=customer", "source.cast_mode", "unsafe"],
    );
}

#[test]
fn invalid_normalize_strategy_errors() {
    let entity = r#"  - name: "customer"
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
      normalize_columns:
        enabled: true
        strategy: "odd_case"
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let yaml = base_config(entity);
    assert_validation_error(
        &yaml,
        &[
            "entity.name=customer",
            "schema.normalize_columns.strategy",
            "odd_case",
        ],
    );
}

#[test]
fn invalid_column_type_errors() {
    let entity = r#"  - name: "customer"
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
          type: "uuid"
"#;
    let yaml = base_config(entity);
    assert_validation_error(
        &yaml,
        &[
            "entity.name=customer",
            "schema.columns[0].type",
            "uuid",
            "customer_id",
        ],
    );
}

#[test]
fn invalid_policy_severity_errors() {
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
    policy:
      severity: "block"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let yaml = base_config(entity);
    assert_validation_error(&yaml, &["entity.name=customer", "policy.severity", "block"]);
}

#[test]
fn duplicate_entity_names_error() {
    let entities = format!("{}{}", base_entity("customer"), base_entity("customer"));
    let yaml = base_config(&entities);
    assert_validation_error(&yaml, &["entity.name=customer", "duplicated"]);
}

#[test]
fn v03_dot_in_entity_name_errors_but_v02_allows_it() {
    let entity = base_entity("sales.orders");
    assert_validation_error(
        &base_config_with_version("0.3", &entity),
        &["entity.name=sales.orders", "must not contain '.'"],
    );
    assert_validation_ok(&base_config_with_version("0.2", &entity));
}

#[test]
fn missing_sink_accepted_path_errors() {
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let yaml = base_config(entity);
    assert_validation_error(&yaml, &["entity.name=customer", "sink.accepted.path"]);
}

#[test]
fn rejected_format_errors() {
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
      rejected:
        format: "parquet"
        path: "/tmp/rejected"
    policy:
      severity: "reject"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let yaml = base_config(entity);
    assert_validation_error(
        &yaml,
        &["entity.name=customer", "sink.rejected.format", "parquet"],
    );
}

#[test]
fn iceberg_accepted_format_is_valid() {
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "iceberg"
        path: "/tmp/out"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let yaml = base_config(entity);
    assert_validation_ok(&yaml);
}

#[test]
fn parquet_sink_options_are_valid() {
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
        options:
          compression: "gzip"
          row_group_size: 1000
          max_size_per_file: 268435456
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let yaml = base_config(entity);
    assert_validation_ok(&yaml);
}

#[test]
fn parquet_sink_invalid_compression_errors() {
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
        options:
          compression: "fast"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let yaml = base_config(entity);
    assert_validation_error(
        &yaml,
        &[
            "entity.name=customer",
            "sink.accepted.options.compression",
            "fast",
        ],
    );
}

#[test]
fn parquet_sink_row_group_size_must_be_positive() {
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
        options:
          row_group_size: 0
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let yaml = base_config(entity);
    assert_validation_error(
        &yaml,
        &[
            "entity.name=customer",
            "sink.accepted.options.row_group_size",
        ],
    );
}

#[test]
fn parquet_sink_max_size_per_file_must_be_positive() {
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
        options:
          max_size_per_file: 0
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let yaml = base_config(entity);
    assert_validation_error(
        &yaml,
        &[
            "entity.name=customer",
            "sink.accepted.options.max_size_per_file",
        ],
    );
}

#[test]
fn unknown_root_field_errors() {
    let yaml = r#"version: "0.1"
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
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
extra_root: "oops"
"#;
    assert_validation_error(yaml, &["unknown field root.extra_root"]);
}

#[test]
fn unknown_entity_field_errors() {
    let entity = r#"  - name: "customer"
    tune: "fast"
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
    let yaml = base_config(entity);
    assert_validation_error(
        &yaml,
        &["entity.name=customer", "unknown field entity.tune"],
    );
}

#[test]
fn unknown_nested_field_errors() {
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
      options:
        header: true
        sep: ";"
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
    let yaml = base_config(entity);
    assert_validation_error(
        &yaml,
        &["entity.name=customer", "unknown field source.options.sep"],
    );
}

#[test]
fn metadata_allows_extra_fields() {
    let yaml = r#"version: "0.1"
metadata:
  project: "demo"
  owner: "data"
  extra_key: "free-form"
report:
  path: "/tmp/reports"
entities:
  - name: "customer"
    metadata:
      domain: "sales"
      extra_note: "ok"
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
    assert_validation_ok(yaml);
}

#[test]
fn metadata_project_is_optional() {
    let yaml = r#"version: "0.1"
metadata:
  owner: "data"
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
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    assert_validation_ok(yaml);
}

#[test]
fn storages_optional_defaults_to_local() {
    let yaml = base_config(&base_entity("customer"));
    assert_validation_ok(&yaml);
}

#[test]
fn storages_local_defined_and_used() {
    let storages = r#"  default: "local"
  definitions:
    - name: "local"
      type: "local"
"#;
    let yaml = config_with_storages(storages, &base_entity("customer"));
    assert_validation_ok(&yaml);
}

#[test]
fn source_storage_override_works() {
    let storages = r#"  default: "local"
  definitions:
    - name: "local"
      type: "local"
    - name: "local_alt"
      type: "local"
"#;
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
      storage: "local_alt"
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
    let yaml = config_with_storages(storages, entity);
    assert_validation_ok(&yaml);
}

#[test]
fn sink_accepted_storage_override_works() {
    let storages = r#"  default: "local"
  definitions:
    - name: "local"
      type: "local"
    - name: "local_alt"
      type: "local"
"#;
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
        storage: "local_alt"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let yaml = config_with_storages(storages, entity);
    assert_validation_ok(&yaml);
}

#[test]
fn missing_storage_reference_errors() {
    let storages = r#"  default: "local"
  definitions:
    - name: "local"
      type: "local"
"#;
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
      storage: "missing"
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
    let yaml = config_with_storages(storages, entity);
    assert_validation_error(
        &yaml,
        &["entity.name=customer", "source.storage", "missing"],
    );
}

#[test]
fn s3_storage_reference_ok() {
    let storages = r#"  default: "local"
  definitions:
    - name: "local"
      type: "local"
    - name: "s3_raw"
      type: "s3"
      bucket: "demo"
      region: "us-east-1"
"#;
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
      storage: "s3_raw"
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
    let yaml = config_with_storages(storages, entity);
    assert_validation_ok(&yaml);
}

#[test]
fn parquet_source_allows_s3_storage() {
    let storages = r#"  default: "local"
  definitions:
    - name: "local"
      type: "local"
    - name: "s3_raw"
      type: "s3"
      bucket: "demo"
      region: "us-east-1"
"#;
    let entity = r#"  - name: "customer"
    source:
      format: "parquet"
      path: "/tmp/input"
      storage: "s3_raw"
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
    let yaml = config_with_storages(storages, entity);
    assert_validation_ok(&yaml);
}

#[test]
fn sink_level_append_write_mode_is_valid() {
    let yaml = r#"version: "0.1"
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
    assert_validation_ok(yaml);
}

#[test]
fn sink_level_write_mode_rejects_unknown_value() {
    let yaml = r#"version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "merge"
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
    assert_validation_error(
        yaml,
        &[
            "sink.write_mode",
            "merge",
            "overwrite",
            "append",
            "merge_scd1",
            "merge_scd2",
        ],
    );
}

#[test]
fn sink_level_merge_scd1_write_mode_is_valid_for_delta_with_primary_key() {
    let yaml = r#"version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "merge_scd1"
      accepted:
        format: "delta"
        path: "/tmp/out_delta"
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
    assert_validation_ok(yaml);
}

#[test]
fn sink_level_merge_scd1_requires_delta_sink() {
    let yaml = r#"version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "merge_scd1"
      accepted:
        format: "parquet"
        path: "/tmp/out"
    policy:
      severity: "warn"
    schema:
      primary_key: ["customer_id"]
      columns:
        - name: "customer_id"
          type: "string"
"#;
    assert_validation_error(
        yaml,
        &[
            "entity.name=customer",
            "sink.write_mode=merge_scd1",
            "sink.accepted.format=parquet",
        ],
    );
}

#[test]
fn sink_level_merge_scd1_requires_primary_key() {
    let yaml = r#"version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "merge_scd1"
      accepted:
        format: "delta"
        path: "/tmp/out_delta"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    assert_validation_error(
        yaml,
        &[
            "entity.name=customer",
            "sink.write_mode=merge_scd1",
            "schema.primary_key",
        ],
    );
}

#[test]
fn sink_level_merge_scd2_write_mode_is_valid_for_delta_with_primary_key() {
    let yaml = r#"version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "merge_scd2"
      accepted:
        format: "delta"
        path: "/tmp/out_delta"
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
    assert_validation_ok(yaml);
}

#[test]
fn sink_level_merge_scd2_requires_delta_sink() {
    let yaml = r#"version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "merge_scd2"
      accepted:
        format: "parquet"
        path: "/tmp/out"
    policy:
      severity: "warn"
    schema:
      primary_key: ["customer_id"]
      columns:
        - name: "customer_id"
          type: "string"
"#;
    assert_validation_error(
        yaml,
        &[
            "entity.name=customer",
            "sink.write_mode=merge_scd2",
            "sink.accepted.format=parquet",
        ],
    );
}

#[test]
fn sink_level_merge_scd2_requires_primary_key() {
    let yaml = r#"version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "merge_scd2"
      accepted:
        format: "delta"
        path: "/tmp/out_delta"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    assert_validation_error(
        yaml,
        &[
            "entity.name=customer",
            "sink.write_mode=merge_scd2",
            "schema.primary_key",
        ],
    );
}

#[test]
fn sink_level_merge_options_are_valid_for_delta_merge_scd2() {
    let yaml = r#"version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "merge_scd2"
      accepted:
        format: "delta"
        path: "/tmp/out_delta"
        merge:
          ignore_columns: ["ingested_at", "load_ts"]
          compare_columns: ["name", "status"]
          scd2:
            current_flag_column: "__is_current"
            valid_from_column: "__valid_from"
            valid_to_column: "__valid_to"
    policy:
      severity: "warn"
    schema:
      primary_key: ["customer_id"]
      columns:
        - name: "customer_id"
          type: "string"
        - name: "name"
          type: "string"
        - name: "status"
          type: "string"
        - name: "ingested_at"
          type: "datetime"
        - name: "load_ts"
          type: "datetime"
"#;
    assert_validation_ok(yaml);
}

#[test]
fn sink_level_merge_options_require_merge_write_mode() {
    let yaml = r#"version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "append"
      accepted:
        format: "delta"
        path: "/tmp/out_delta"
        merge:
          ignore_columns: ["name"]
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
        - name: "name"
          type: "string"
"#;
    assert_validation_error(
        yaml,
        &[
            "entity.name=customer",
            "sink.accepted.merge",
            "sink.write_mode=merge_scd1 or merge_scd2",
        ],
    );
}

#[test]
fn sink_level_merge_options_require_delta_sink() {
    let yaml = r#"version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "merge_scd1"
      accepted:
        format: "parquet"
        path: "/tmp/out"
        merge:
          ignore_columns: ["name"]
    policy:
      severity: "warn"
    schema:
      primary_key: ["customer_id"]
      columns:
        - name: "customer_id"
          type: "string"
        - name: "name"
          type: "string"
"#;
    assert_validation_error(
        yaml,
        &[
            "entity.name=customer",
            "sink.write_mode=merge_scd1",
            "sink.accepted.format=parquet",
        ],
    );
}

#[test]
fn sink_level_merge_ignore_columns_cannot_reference_primary_key() {
    let yaml = r#"version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "merge_scd1"
      accepted:
        format: "delta"
        path: "/tmp/out_delta"
        merge:
          ignore_columns: ["customer_id"]
    policy:
      severity: "warn"
    schema:
      primary_key: ["customer_id"]
      columns:
        - name: "customer_id"
          type: "string"
        - name: "name"
          type: "string"
"#;
    assert_validation_error(
        yaml,
        &[
            "entity.name=customer",
            "sink.accepted.merge.ignore_columns[0]=customer_id",
            "schema.primary_key",
        ],
    );
}

#[test]
fn sink_level_merge_compare_columns_cannot_reference_primary_key() {
    let yaml = r#"version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "merge_scd2"
      accepted:
        format: "delta"
        path: "/tmp/out_delta"
        merge:
          compare_columns: ["customer_id", "name"]
    policy:
      severity: "warn"
    schema:
      primary_key: ["customer_id"]
      columns:
        - name: "customer_id"
          type: "string"
        - name: "name"
          type: "string"
"#;
    assert_validation_error(
        yaml,
        &[
            "entity.name=customer",
            "sink.accepted.merge.compare_columns[0]=customer_id",
            "schema.primary_key",
        ],
    );
}

#[test]
fn sink_level_merge_compare_columns_must_reference_schema_columns() {
    let yaml = r#"version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "merge_scd2"
      accepted:
        format: "delta"
        path: "/tmp/out_delta"
        merge:
          compare_columns: ["missing_col"]
    policy:
      severity: "warn"
    schema:
      primary_key: ["customer_id"]
      columns:
        - name: "customer_id"
          type: "string"
        - name: "name"
          type: "string"
"#;
    assert_validation_error(
        yaml,
        &[
            "entity.name=customer",
            "sink.accepted.merge.compare_columns[0]=missing_col",
            "unknown schema column",
        ],
    );
}

#[test]
fn sink_level_merge_scd2_system_column_names_must_be_unique_and_not_business_columns() {
    let yaml = r#"version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "merge_scd2"
      accepted:
        format: "delta"
        path: "/tmp/out_delta"
        merge:
          scd2:
            current_flag_column: "status"
            valid_from_column: "__shared"
            valid_to_column: "__shared"
    policy:
      severity: "warn"
    schema:
      primary_key: ["customer_id"]
      columns:
        - name: "customer_id"
          type: "string"
        - name: "status"
          type: "string"
"#;
    assert_validation_error(
        yaml,
        &[
            "entity.name=customer",
            "sink.accepted.merge.scd2.current_flag_column=status",
            "collides with schema column name",
        ],
    );
}

#[test]
fn sink_level_merge_scd2_system_column_names_must_not_collide_with_normalized_business_columns() {
    let yaml = r#"version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "merge_scd2"
      accepted:
        format: "delta"
        path: "/tmp/out_delta"
        merge:
          scd2:
            current_flag_column: "order_id"
    policy:
      severity: "warn"
    schema:
      primary_key: ["customer_id"]
      normalize_columns:
        enabled: true
        strategy: "snake_case"
      columns:
        - name: "customer_id"
          type: "string"
        - name: "Order ID"
          type: "string"
"#;
    assert_validation_error(
        yaml,
        &[
            "entity.name=customer",
            "sink.accepted.merge.scd2.current_flag_column=order_id",
            "collides with schema column name",
        ],
    );
}

#[test]
fn sink_level_merge_scd2_system_column_names_must_be_unique() {
    let yaml = r#"version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "merge_scd2"
      accepted:
        format: "delta"
        path: "/tmp/out_delta"
        merge:
          scd2:
            current_flag_column: "__shared"
            valid_from_column: "__shared"
            valid_to_column: "__valid_to"
    policy:
      severity: "warn"
    schema:
      primary_key: ["customer_id"]
      columns:
        - name: "customer_id"
          type: "string"
"#;
    assert_validation_error(
        yaml,
        &[
            "entity.name=customer",
            "sink.accepted.merge.scd2 column names must be unique",
        ],
    );
}

#[test]
fn sink_level_merge_scd2_options_require_merge_scd2_mode() {
    let yaml = r#"version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "merge_scd1"
      accepted:
        format: "delta"
        path: "/tmp/out_delta"
        merge:
          scd2:
            current_flag_column: "__is_current"
    policy:
      severity: "warn"
    schema:
      primary_key: ["customer_id"]
      columns:
        - name: "customer_id"
          type: "string"
"#;
    assert_validation_error(
        yaml,
        &[
            "entity.name=customer",
            "sink.accepted.merge.scd2",
            "sink.write_mode=merge_scd2",
        ],
    );
}

#[test]
fn iceberg_accepted_sink_is_valid_on_local_storage() {
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "iceberg"
        path: "/tmp/out/customer_iceberg"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let yaml = base_config(entity);
    assert_validation_ok(&yaml);
}

#[test]
fn iceberg_accepted_sink_is_valid_on_s3_storage() {
    let storages = r#"  default: "local_fs"
  definitions:
    - name: "local_fs"
      type: "local"
    - name: "s3_out"
      type: "s3"
      bucket: "demo-bucket"
      region: "us-east-1"
"#;
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
      storage: "local_fs"
    sink:
      accepted:
        format: "iceberg"
        path: "customer_iceberg"
        storage: "s3_out"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let yaml = config_with_storages(storages, entity);
    assert_validation_ok(&yaml);
}

#[test]
fn iceberg_accepted_sink_is_valid_on_gcs_storage() {
    let storages = r#"  default: "local_fs"
  definitions:
    - name: "local_fs"
      type: "local"
    - name: "gcs_out"
      type: "gcs"
      bucket: "demo-bucket"
"#;
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
      storage: "local_fs"
    sink:
      accepted:
        format: "iceberg"
        path: "customer_iceberg"
        storage: "gcs_out"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let yaml = config_with_storages(storages, entity);
    assert_validation_ok(&yaml);
}

#[test]
fn iceberg_accepted_sink_accepts_adls_storage() {
    let storages = r#"  default: "local_fs"
  definitions:
    - name: "local_fs"
      type: "local"
    - name: "adls_out"
      type: "adls"
      account: "demo"
      container: "raw"
"#;
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
      storage: "local_fs"
    sink:
      accepted:
        format: "iceberg"
        path: "customer_iceberg"
        storage: "adls_out"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let yaml = config_with_storages(storages, entity);
    assert_validation_ok(&yaml);
}

#[test]
fn iceberg_glue_catalog_binding_validates_on_s3() {
    let storages = r#"  default: "local_fs"
  definitions:
    - name: "local_fs"
      type: "local"
    - name: "s3_out"
      type: "s3"
      bucket: "demo-bucket"
      region: "us-east-1"
      prefix: "accepted"
"#;
    let catalogs = r#"  default: "glue_main"
  definitions:
    - name: "glue_main"
      type: "glue"
      region: "us-east-1"
      database: "lakehouse"
      warehouse_storage: "s3_out"
      warehouse_prefix: "iceberg"
"#;
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
      storage: "local_fs"
    sink:
      accepted:
        format: "iceberg"
        path: "customer_iceberg"
        storage: "s3_out"
        iceberg:
          catalog: "glue_main"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    assert_validation_ok(&config_with_storages_and_catalogs(
        storages, catalogs, entity,
    ));
}

#[test]
fn iceberg_glue_catalog_binding_requires_s3_sink_storage() {
    let storages = r#"  default: "local_fs"
  definitions:
    - name: "local_fs"
      type: "local"
"#;
    let catalogs = r#"  definitions:
    - name: "glue_main"
      type: "glue"
      region: "us-east-1"
      database: "lakehouse"
"#;
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
      storage: "local_fs"
    sink:
      accepted:
        format: "iceberg"
        path: "/tmp/out"
        storage: "local_fs"
        iceberg:
          catalog: "glue_main"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    assert_validation_error(
        &config_with_storages_and_catalogs(storages, catalogs, entity),
        &[
            "sink.accepted.iceberg.catalog",
            "requires sink.accepted storage type s3",
            "local",
        ],
    );
}

#[test]
fn iceberg_glue_catalog_binding_rejects_unknown_catalog_reference() {
    let storages = r#"  default: "s3_out"
  definitions:
    - name: "s3_out"
      type: "s3"
      bucket: "demo-bucket"
      region: "us-east-1"
"#;
    let catalogs = r#"  definitions:
    - name: "glue_other"
      type: "glue"
      region: "us-east-1"
      database: "lakehouse"
"#;
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "iceberg"
        path: "customer_iceberg"
        storage: "s3_out"
        iceberg:
          catalog: "glue_main"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    assert_validation_error(
        &config_with_storages_and_catalogs(storages, catalogs, entity),
        &[
            "sink.accepted.iceberg.catalog",
            "unknown catalog",
            "glue_main",
        ],
    );
}

#[test]
fn schema_unique_keys_reject_unknown_columns() {
    let entity = r#"  - name: "customer"
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
        - ["customer_id", "missing_col"]
      columns:
        - name: "customer_id"
          type: "string"
"#;
    assert_validation_error(
        &base_config(entity),
        &[
            "entity.name=customer",
            "schema.unique_keys[0][1]=missing_col",
            "unknown schema column",
        ],
    );
}

#[test]
fn schema_unique_keys_reject_duplicate_columns_inside_constraint() {
    let entity = r#"  - name: "customer"
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
        - ["customer_id", "customer_id"]
      columns:
        - name: "customer_id"
          type: "string"
"#;
    assert_validation_error(
        &base_config(entity),
        &[
            "entity.name=customer",
            "schema.unique_keys[0] has duplicate column customer_id",
        ],
    );
}

#[test]
fn schema_unique_keys_reject_empty_constraint() {
    let entity = r#"  - name: "customer"
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
        - []
      columns:
        - name: "customer_id"
          type: "string"
"#;
    assert_validation_error(
        &base_config(entity),
        &[
            "entity.name=customer",
            "schema.unique_keys[0] must not be empty",
        ],
    );
}

#[test]
fn schema_primary_key_rejects_nullable_true_column() {
    let entity = r#"  - name: "customer"
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
      primary_key: ["customer_id"]
      columns:
        - name: "customer_id"
          type: "string"
          nullable: true
"#;
    assert_validation_error(
        &base_config(entity),
        &[
            "entity.name=customer",
            "schema.primary_key column customer_id cannot set nullable=true",
        ],
    );
}

#[test]
fn schema_unique_keys_with_legacy_column_unique_flags_is_valid() {
    let entity = r#"  - name: "customer"
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
        - ["customer_id", "country"]
      columns:
        - name: "customer_id"
          type: "string"
          unique: true
        - name: "country"
          type: "string"
"#;
    assert_validation_ok(&base_config(entity));
}
