use floe_core::config::PolicySeverity;
use floe_core::load_config;

use super::super::common::write_temp_config;

const DEFAULTS: &str = r#"
domains:
  - name: "sales"
    incoming_dir: "/tmp/incoming/sales"
    defaults:
      metadata:
        owner: "sales-team"
        tags: ["sales", "silver"]
      sink:
        accepted:
          format: "parquet"
          path: "/tmp/out/default"
        rejected:
          format: "csv"
          path: "/tmp/rejected/default"
      policy:
        severity: "reject"
      schema:
        normalize_columns:
          enabled: true
          strategy: "snake_case"
        primary_key: ["default_id"]
        columns:
          - name: "default_id"
            type: "string"
"#;

fn config_with(version: &str, entity: &str) -> String {
    format!("version: \"{version}\"\n{DEFAULTS}entities:\n{entity}")
}

const ORDERS_SOURCE: &str = r#"  - name: "orders"
    domain: "sales"
    source:
      format: "csv"
      path: "/tmp/input"
"#;

#[test]
fn entity_without_sink_inherits_domain_default_sink() {
    let entity = format!(
        "{ORDERS_SOURCE}    schema:\n      columns:\n        - name: \"order_id\"\n          type: \"string\"\n"
    );
    let path = write_temp_config(&config_with("0.3", &entity));
    let config = load_config(&path).expect("parse config");

    let entity = &config.entities[0];
    assert_eq!(entity.sink.accepted.format, "parquet");
    assert_eq!(entity.sink.accepted.path, "/tmp/out/default");
    assert_eq!(
        entity.sink.rejected.as_ref().expect("rejected").path,
        "/tmp/rejected/default"
    );
}

#[test]
fn domain_defaults_deep_merge_maps_and_entity_wins() {
    let entity = format!(
        "{ORDERS_SOURCE}    sink:\n      accepted:\n        path: \"/tmp/out/orders\"\n    policy:\n      severity: \"warn\"\n    schema:\n      columns:\n        - name: \"order_id\"\n          type: \"string\"\n"
    );
    let path = write_temp_config(&config_with("0.3", &entity));
    let config = load_config(&path).expect("parse config");

    let entity = &config.entities[0];
    assert_eq!(entity.sink.accepted.format, "parquet");
    assert_eq!(entity.sink.accepted.path, "/tmp/out/orders");
    assert_eq!(entity.policy.severity, PolicySeverity::Warn);
    let normalize = entity
        .schema
        .normalize_columns
        .as_ref()
        .expect("normalize_columns");
    assert_eq!(normalize.strategy.as_deref(), Some("snake_case"));
}

#[test]
fn domain_defaults_lists_are_replaced_not_appended() {
    let entity = format!(
        "{ORDERS_SOURCE}    metadata:\n      tags: [\"orders\"]\n    schema:\n      columns:\n        - name: \"order_id\"\n          type: \"string\"\n"
    );
    let path = write_temp_config(&config_with("0.3", &entity));
    let config = load_config(&path).expect("parse config");

    let metadata = config.entities[0].metadata.as_ref().expect("metadata");
    assert_eq!(metadata.tags, Some(vec!["orders".to_string()]));
    assert_eq!(metadata.owner.as_deref(), Some("sales-team"));
}

#[test]
fn domain_defaults_never_inherit_columns_or_primary_key() {
    let entity = format!(
        "{ORDERS_SOURCE}    schema:\n      columns:\n        - name: \"order_id\"\n          type: \"string\"\n"
    );
    let path = write_temp_config(&config_with("0.3", &entity));
    let config = load_config(&path).expect("parse config");

    let schema = &config.entities[0].schema;
    let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["order_id"]);
    assert_eq!(schema.primary_key, None);
}

#[test]
fn error_on_merged_field_names_the_entity() {
    let config = config_with(
        "0.3",
        &format!("{ORDERS_SOURCE}    schema:\n      columns: []\n"),
    )
    .replace("severity: \"reject\"", "severity: \"bogus\"");
    let path = write_temp_config(&config);
    let err = load_config(&path).expect_err("invalid merged severity");

    assert_eq!(
        err.to_string(),
        "entities[0] (entity.name=orders): policy.severity=bogus is unsupported (allowed: warn, reject, abort)"
    );
}

#[test]
fn domain_defaults_require_config_version_0_3() {
    let entity = format!(
        "{ORDERS_SOURCE}    schema:\n      columns:\n        - name: \"order_id\"\n          type: \"string\"\n"
    );
    let path = write_temp_config(&config_with("0.2", &entity));
    let err = load_config(&path).expect_err("defaults on 0.2");

    assert_eq!(
        err.to_string(),
        "domains.defaults requires root.version >= \"0.3\""
    );
}

#[test]
fn domain_default_path_resolves_entity_and_domain_name() {
    let entity = format!(
        "{ORDERS_SOURCE}    schema:\n      columns:\n        - name: \"order_id\"\n          type: \"string\"\n"
    );
    let config = config_with("0.3", &entity)
        .replace("    incoming_dir: \"/tmp/incoming/sales\"\n", "")
        .replace("/tmp/out/default", "{{domain.name}}/{{entity.name}}");
    let path = write_temp_config(&config);
    let config = load_config(&path).expect("parse config");

    assert_eq!(config.entities[0].sink.accepted.path, "sales/orders");
}
