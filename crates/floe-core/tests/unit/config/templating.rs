use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use floe_core::{load_config, load_config_with_profile_vars};

fn temp_dir(prefix: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    path.push(format!("{prefix}-{nanos}"));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

fn write_file(path: &Path, contents: &str) -> PathBuf {
    fs::write(path, contents).expect("write file");
    path.to_path_buf()
}

fn write_config(dir: &Path, contents: &str) -> PathBuf {
    let path = dir.join("config.yml");
    write_file(&path, contents)
}

fn write_env(dir: &Path, contents: &str) -> PathBuf {
    let path = dir.join("env.yml");
    write_file(&path, contents)
}

#[test]
fn profile_vars_apply_to_storage_definition_fields() {
    let root = temp_dir("floe-storage-vars");
    let config_yaml = format!(
        r#"version: "0.1"
storages:
  default: "lakehouse_bronze"
  definitions:
    - name: "lakehouse_bronze"
      type: "s3"
      bucket: "{{{{BRONZE_BUCKET}}}}"
      region: "{{{{STORAGE_REGION}}}}"
      prefix: "{{{{STORAGE_PREFIX}}}}"
      endpoint: "https://{{{{STORAGE_ENDPOINT}}}}"
entities:
  - name: "orders"
    source:
      format: "csv"
      path: "{root}/in/orders.csv"
    sink:
      accepted:
        format: "parquet"
        storage: "lakehouse_bronze"
        path: "sales/orders"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
        root = root.display(),
    );
    let config_path = write_config(&root, &config_yaml);
    let profile_vars = [
        ("BRONZE_BUCKET".to_string(), "lakehouse-bronze".to_string()),
        ("STORAGE_REGION".to_string(), "us-east-1".to_string()),
        ("STORAGE_PREFIX".to_string(), "bronze/root".to_string()),
        (
            "STORAGE_ENDPOINT".to_string(),
            "s3.local.example".to_string(),
        ),
    ]
    .into_iter()
    .collect();

    let parsed = load_config_with_profile_vars(&config_path, &profile_vars).expect("parse config");
    let definition = &parsed.storages.as_ref().expect("storages").definitions[0];

    assert_eq!(definition.bucket.as_deref(), Some("lakehouse-bronze"));
    assert_eq!(definition.region.as_deref(), Some("us-east-1"));
    assert_eq!(definition.prefix.as_deref(), Some("bronze/root"));
    assert_eq!(
        definition.endpoint.as_deref(),
        Some("https://s3.local.example")
    );
}

#[test]
fn env_vars_apply_to_lineage_endpoint_and_dataset_namespace() {
    let root = temp_dir("floe-lineage-vars");
    let config_yaml = format!(
        r#"version: "0.1"
env:
  vars:
    LINEAGE_ENDPOINT: "api/v1/openlineage/lineage"
    DATASET_NAMESPACE: "iceberg.prod"
lineage:
  url: "http://openmetadata:8585"
  endpoint: "{{{{LINEAGE_ENDPOINT}}}}"
  namespace: "dagster"
  dataset_namespace: "{{{{DATASET_NAMESPACE}}}}"
entities:
  - name: "orders"
    source:
      format: "csv"
      path: "{root}/in/orders.csv"
    sink:
      accepted:
        format: "parquet"
        path: "{root}/out/orders"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
        root = root.display(),
    );
    let config_path = write_config(&root, &config_yaml);
    let parsed = load_config(&config_path).expect("parse config");
    let lineage = parsed.lineage.expect("lineage");

    assert_eq!(
        lineage.endpoint.as_deref(),
        Some("api/v1/openlineage/lineage")
    );
    assert_eq!(lineage.dataset_namespace.as_deref(), Some("iceberg.prod"));
}

#[test]
fn unresolved_lineage_placeholder_errors() {
    let root = temp_dir("floe-lineage-unresolved");
    let config_yaml = format!(
        r#"version: "0.1"
lineage:
  url: "http://openmetadata:8585"
  endpoint: "{{{{OPENLINEAGE_ENDPOINT}}}}"
  namespace: "dagster"
entities:
  - name: "orders"
    source:
      format: "csv"
      path: "{root}/in/orders.csv"
    sink:
      accepted:
        format: "parquet"
        path: "{root}/out/orders"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
        root = root.display(),
    );
    let config_path = write_config(&root, &config_yaml);

    let err = load_config(&config_path).expect_err("placeholder error");
    assert_eq!(
        err.to_string(),
        "lineage.endpoint references unknown variable OPENLINEAGE_ENDPOINT"
    );
}

#[test]
fn env_file_vars_apply_to_source_path() {
    let root = temp_dir("floe-env-file");
    let env_path = write_env(&root, "incoming_path: \"/data/incoming\"");
    let config_yaml = format!(
        r#"version: "0.1"
env:
  file: "{env_path}"
report:
  path: "{root}/out"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "{{{{incoming_path}}}}/customer"
    sink:
      accepted:
        format: "parquet"
        path: "{root}/out/accepted"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
        env_path = env_path.display(),
        root = root.display(),
    );
    let config_path = write_config(&root, &config_yaml);

    let parsed = load_config(&config_path).expect("parse config");
    assert_eq!(parsed.entities[0].source.path, "/data/incoming/customer");
}

#[test]
fn env_vars_override_env_file() {
    let root = temp_dir("floe-env-override");
    let env_path = write_env(&root, "incoming_path: \"/data/incoming\"");
    let config_yaml = format!(
        r#"version: "0.1"
env:
  file: "{env_path}"
  vars:
    incoming_path: "/override"
report:
  path: "{root}/out"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "{{{{incoming_path}}}}/customer"
    sink:
      accepted:
        format: "parquet"
        path: "{root}/out/accepted"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
        env_path = env_path.display(),
        root = root.display(),
    );
    let config_path = write_config(&root, &config_yaml);

    let parsed = load_config(&config_path).expect("parse config");
    assert_eq!(parsed.entities[0].source.path, "/override/customer");
}

#[test]
fn domain_incoming_dir_uses_globals() {
    let root = temp_dir("floe-domain");
    let config_yaml = format!(
        r#"version: "0.1"
env:
  vars:
    base_path: "/data/base"
domains:
  - name: "sales"
    incoming_dir: "{{{{base_path}}}}/sales"
report:
  path: "{root}/out"
entities:
  - name: "customer"
    domain: "sales"
    source:
      format: "csv"
      path: "{{{{domain.incoming_dir}}}}/customer"
    sink:
      accepted:
        format: "parquet"
        path: "{root}/out/accepted"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
        root = root.display(),
    );
    let config_path = write_config(&root, &config_yaml);

    let parsed = load_config(&config_path).expect("parse config");
    let domain = parsed
        .domains
        .iter()
        .find(|domain| domain.name == "sales")
        .expect("domain found");
    assert_eq!(
        domain.resolved_incoming_dir.as_deref(),
        Some("/data/base/sales")
    );
    assert_eq!(parsed.entities[0].source.path, "/data/base/sales/customer");
}

#[test]
fn unresolved_placeholder_errors_with_entity_context() {
    let root = temp_dir("floe-unresolved");
    let config_yaml = format!(
        r#"version: "0.1"
report:
  path: "{root}/out"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "{{{{missing}}}}/customer"
    sink:
      accepted:
        format: "parquet"
        path: "{root}/out/accepted"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
        root = root.display(),
    );
    let config_path = write_config(&root, &config_yaml);

    let err = load_config(&config_path).expect_err("placeholder error");
    let msg = err.to_string();
    assert!(msg.contains("entity.name=customer"));
    assert!(msg.contains("entities.source.path references unknown variable missing"));
}

#[test]
fn unknown_domain_errors() {
    let root = temp_dir("floe-unknown-domain");
    let config_yaml = format!(
        r#"version: "0.1"
report:
  path: "{root}/out"
entities:
  - name: "customer"
    domain: "missing"
    source:
      format: "csv"
      path: "/tmp"
    sink:
      accepted:
        format: "parquet"
        path: "{root}/out/accepted"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
        root = root.display(),
    );
    let config_path = write_config(&root, &config_yaml);

    let err = load_config(&config_path).expect_err("domain error");
    let msg = err.to_string();
    assert!(msg.contains("entity.name=customer references unknown domain missing"));
}

#[test]
fn duplicate_domain_names_error() {
    let root = temp_dir("floe-duplicate-domain");
    let config_yaml = format!(
        r#"version: "0.1"
domains:
  - name: "sales"
    incoming_dir: "/data/sales"
  - name: "sales"
    incoming_dir: "/data/sales_v2"
report:
  path: "{root}/out"
entities:
  - name: "customer"
    domain: "sales"
    source:
      format: "csv"
      path: "/tmp"
    sink:
      accepted:
        format: "parquet"
        path: "{root}/out/accepted"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
        root = root.display(),
    );
    let config_path = write_config(&root, &config_yaml);

    let err = load_config(&config_path).expect_err("duplicate domain error");
    let msg = err.to_string();
    assert!(msg.contains("duplicate domain name sales"));
}
