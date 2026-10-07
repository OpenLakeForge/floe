use floe_core::config::LineageConfig;
use floe_core::{load_config_with_profile_overrides, validate, ValidateOptions};

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

#[test]
fn lineage_url_required() {
    let config = r#"version: "0.1"
report:
  path: "/tmp/reports"
lineage:
  namespace: "my-ns"
entities:
  - name: "orders"
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
        - name: "id"
          type: "string"
"#;
    assert_validation_error(config, &["url"]);
}

#[test]
fn lineage_namespace_required() {
    let config = r#"version: "0.1"
report:
  path: "/tmp/reports"
lineage:
  url: "http://marquez:5000"
entities:
  - name: "orders"
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
        - name: "id"
          type: "string"
"#;
    assert_validation_error(config, &["namespace"]);
}

#[test]
fn lineage_valid_config_parses() {
    let config = r#"version: "0.1"
report:
  path: "/tmp/reports"
lineage:
  url: "http://marquez:5000"
  endpoint: "/api/v1/openlineage/lineage"
  namespace: "my-namespace"
  dataset_namespace: "iceberg.prod"
  timeout_secs: 10
  producer: "https://github.com/myorg/floe"
entities:
  - name: "orders"
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
        - name: "id"
          type: "string"
"#;
    let path = write_temp_config(config);
    validate(&path, ValidateOptions::default()).expect("valid lineage config");
    let parsed = load_config_with_profile_overrides(
        &path,
        &std::collections::HashMap::new(),
        None,
        None,
        None,
    )
    .expect("lineage config should parse");
    let lineage = parsed.lineage.expect("lineage should be present");
    assert_eq!(
        lineage.endpoint.as_deref(),
        Some("/api/v1/openlineage/lineage")
    );
    assert_eq!(lineage.dataset_namespace.as_deref(), Some("iceberg.prod"));
}

#[test]
fn lineage_max_failures_zero_rejected() {
    let config = r#"version: "0.1"
report:
  path: "/tmp/reports"
lineage:
  url: "http://marquez:5000"
  namespace: "my-namespace"
  max_failures: 0
entities:
  - name: "orders"
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
        - name: "id"
          type: "string"
"#;
    assert_validation_error(config, &["max_failures", "at least 1"]);
}

#[test]
fn lineage_max_failures_valid() {
    let config = r#"version: "0.1"
report:
  path: "/tmp/reports"
lineage:
  url: "http://marquez:5000"
  namespace: "my-namespace"
  max_failures: 5
entities:
  - name: "orders"
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
        - name: "id"
          type: "string"
"#;
    let path = write_temp_config(config);
    validate(&path, ValidateOptions::default()).expect("max_failures: 5 should be valid");
}

// Regression test for issue #316: load_config_with_profile_overrides must merge
// a lineage block that exists only in the profile, not the base config.
#[test]
fn profile_lineage_overrides_empty_config() {
    let base = r#"version: "0.1"
report:
  path: "/tmp/reports"
entities:
  - name: "orders"
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
        - name: "id"
          type: "string"
"#;
    let path = write_temp_config(base);
    let profile_lineage = LineageConfig {
        url: "http://localhost:5000".to_string(),
        endpoint: None,
        namespace: "floe-demo-local".to_string(),
        dataset_namespace: None,
        api_key: None,
        timeout_secs: Some(2),
        producer: None,
        max_failures: None,
        job_name: None,
    };
    let config = load_config_with_profile_overrides(
        &path,
        &std::collections::HashMap::new(),
        None,
        None,
        Some(&profile_lineage),
    )
    .expect("config with profile lineage override should parse");

    let lineage = config.lineage.expect("lineage should be set from profile");
    assert_eq!(lineage.url, "http://localhost:5000");
    assert_eq!(lineage.namespace, "floe-demo-local");
}
