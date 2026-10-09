use floe_core::load_config;

use super::super::common::write_temp_config;

const SOURCES: &str = r#"version: "0.3"
sources:
  - name: "crm"
    format: "csv"
    path: "/tmp/bronze/crm/{{ resource }}"
    options:
      separator: ";"
    cast_mode: "coerce"
  - name: "erp"
    format: "parquet"
    path: "/tmp/bronze/erp/{{resource}}"
entities:
"#;

fn config_with(source: &str) -> String {
    format!(
        r#"{SOURCES}  - name: "orders"
    source: {source}
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out/orders"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "order_id"
          type: "string"
"#
    )
}

#[test]
fn source_ref_resolves_resource_into_path() {
    let path = write_temp_config(&config_with("{ ref: crm, resource: crm_orders }"));
    let config = load_config(&path).expect("parse config");

    let source = &config.entities[0].source;
    assert_eq!(source.path, "/tmp/bronze/crm/crm_orders");
    assert_eq!(source.format, "csv");
    assert_eq!(source.cast_mode.as_deref(), Some("coerce"));
    let options = source.options.as_ref().expect("options");
    assert_eq!(options.separator.as_deref(), Some(";"));
}

#[test]
fn source_resource_defaults_to_entity_name() {
    let path = write_temp_config(&config_with("{ ref: erp }"));
    let config = load_config(&path).expect("parse config");

    assert_eq!(config.entities[0].source.path, "/tmp/bronze/erp/orders");
}

#[test]
fn entity_source_fields_override_referenced_source() {
    let path = write_temp_config(&config_with(
        "{ ref: crm, cast_mode: strict, options: { header: false }, path: \"/tmp/x/{{resource}}.csv\" }",
    ));
    let config = load_config(&path).expect("parse config");

    let source = &config.entities[0].source;
    assert_eq!(source.path, "/tmp/x/orders.csv");
    assert_eq!(source.cast_mode.as_deref(), Some("strict"));
    let options = source.options.as_ref().expect("options");
    assert_eq!(options.header, Some(false));
    assert_eq!(options.separator.as_deref(), Some(";"));
}

#[test]
fn unknown_source_ref_lists_available_sources() {
    let path = write_temp_config(&config_with("{ ref: billing }"));
    let err = load_config(&path).expect_err("unknown ref");

    assert_eq!(
        err.to_string(),
        "entities[0] (entity.name=orders): source.ref=billing is not a declared source (available: crm, erp)"
    );
}

#[test]
fn sources_require_config_version_0_3() {
    let config = config_with("{ ref: crm }").replace("version: \"0.3\"", "version: \"0.2\"");
    let path = write_temp_config(&config);
    let err = load_config(&path).expect_err("sources on 0.2");

    assert_eq!(
        err.to_string(),
        "root.sources requires root.version >= \"0.3\""
    );
}

#[test]
fn unknown_field_in_source_is_rejected() {
    let config = config_with("{ ref: crm }").replace("cast_mode: \"coerce\"", "bogus: 1");
    let path = write_temp_config(&config);
    let err = load_config(&path).expect_err("unknown source field");

    assert_eq!(err.to_string(), "unknown field sources.bogus");
}

#[test]
fn source_ref_resolves_in_included_entity_files() {
    let dir = tempfile::tempdir().expect("tempdir");
    let write = |rel: &str, contents: &str| {
        let path = dir.path().join(rel);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        std::fs::write(path, contents).expect("write");
    };
    write(
        "floe.yml",
        &SOURCES.replace(
            "entities:\n",
            "include:\n  domains: [\"silver/*/_domain.yml\"]\n",
        ),
    );
    write(
        "silver/sales/_domain.yml",
        "name: \"sales\"\nincoming_dir: \"/tmp/incoming/sales\"\n",
    );
    write(
        "silver/sales/orders.yml",
        r#"name: "orders"
source: { ref: crm }
sink:
  accepted:
    format: "parquet"
    path: "/tmp/out/orders"
policy:
  severity: "warn"
schema:
  columns:
    - name: "order_id"
      type: "string"
"#,
    );
    let config = load_config(&dir.path().join("floe.yml")).expect("parse config");

    assert_eq!(config.entities[0].source.path, "/tmp/bronze/crm/orders");
    assert_eq!(config.entities[0].source.format, "csv");
}

#[test]
fn resource_resolves_in_path_inherited_from_domain_defaults() {
    let config = r#"version: "0.3"
sources:
  - name: "crm"
    format: "csv"
domains:
  - name: "sales"
    incoming_dir: "/tmp/incoming/sales"
    defaults:
      source:
        path: "/tmp/{{resource}}"
entities:
  - name: "orders"
    domain: "sales"
    source: { ref: crm, resource: crm_orders }
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out/orders"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "order_id"
          type: "string"
"#;
    let path = write_temp_config(config);
    let config = load_config(&path).expect("parse config");

    assert_eq!(config.entities[0].source.path, "/tmp/crm_orders");
}
