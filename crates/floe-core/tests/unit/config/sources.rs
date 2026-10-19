use floe_core::load_config;
use floe_core::FloeError;

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
    assert_eq!(source.source_ref.as_deref(), Some("crm"));
    assert_eq!(source.resource.as_deref(), Some("crm_orders"));
    assert_eq!(source.cast_mode.as_deref(), Some("coerce"));
    let options = source.options.as_ref().expect("options");
    assert_eq!(options.separator.as_deref(), Some(";"));
}

#[test]
fn source_resource_defaults_to_entity_name() {
    let path = write_temp_config(&config_with("{ ref: erp }"));
    let config = load_config(&path).expect("parse config");

    let source = &config.entities[0].source;
    assert_eq!(source.path, "/tmp/bronze/erp/orders");
    assert_eq!(source.source_ref.as_deref(), Some("erp"));
    assert_eq!(source.resource.as_deref(), Some("orders"));
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
fn source_resource_must_be_a_string() {
    let path = write_temp_config(&config_with("{ ref: crm, resource: 12 }"));
    let err = load_config(&path).expect_err("numeric resource");

    assert_eq!(
        err.to_string(),
        "entities[0] (entity.name=orders): expected string at source.resource"
    );
}

#[test]
fn inline_source_has_no_ref_metadata() {
    let path = write_temp_config(&config_with("{ format: csv, path: /tmp/orders.csv }"));
    let config = load_config(&path).expect("parse config");

    assert_eq!(config.entities[0].source.source_ref, None);
    assert_eq!(config.entities[0].source.resource, None);
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

#[test]
fn include_sources_matches_inline_sources_and_records_the_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let sources = SOURCES
        .strip_prefix("version: \"0.3\"\n")
        .expect("version")
        .split_once("entities:\n")
        .expect("entities")
        .0;
    std::fs::write(dir.path().join("sources.yml"), sources).expect("write sources");

    let inline_config = config_with("{ ref: crm, resource: crm_orders }");
    let inline_path = write_temp_config(&inline_config);
    let entities = inline_config.split_once("entities:\n").expect("entities").1;
    let include =
        format!("version: \"0.3\"\ninclude:\n  sources: sources.yml\nentities:\n{entities}");
    let include_path = dir.path().join("floe.yml");
    std::fs::write(&include_path, include).expect("write project");

    let mut inline = load_config(&inline_path).expect("inline sources");
    let mut included = load_config(&include_path).expect("included sources");
    assert!(included
        .config_files
        .contains(&dir.path().join("sources.yml")));
    included.config_files.clear();
    inline.config_files.clear();
    assert_eq!(format!("{included:?}"), format!("{inline:?}"));
}

#[test]
fn inline_sources_and_include_sources_are_mutually_exclusive_when_empty() {
    let path = write_temp_config(
        "version: \"0.3\"\ninclude:\n  sources: sources.yml\nsources: []\nentities: []\n",
    );
    let err = load_config(&path).expect_err("conflicting source declarations");

    assert_eq!(
        err.to_string(),
        "root.sources and include.sources are mutually exclusive"
    );
}

#[test]
fn included_sources_require_config_version_0_3() {
    let path =
        write_temp_config("version: \"0.2\"\ninclude:\n  sources: sources.yml\nentities: []\n");
    let err = load_config(&path).expect_err("included sources on 0.2");

    assert_eq!(
        err.to_string(),
        "root.include requires root.version >= \"0.3\""
    );
}

#[test]
fn duplicate_included_source_names_report_the_source_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let source_path = dir.path().join("sources.yml");
    std::fs::write(
        &source_path,
        "sources:\n  - name: crm\n    format: csv\n  - name: crm\n    format: parquet\n",
    )
    .expect("write sources");
    let config_path = dir.path().join("floe.yml");
    std::fs::write(
        &config_path,
        "version: \"0.3\"\ninclude:\n  sources: sources.yml\nentities: []\n",
    )
    .expect("write project");

    let err = load_config(&config_path).expect_err("duplicate source name");

    assert_eq!(
        err.to_string(),
        format!("{}: duplicate source name crm", source_path.display())
    );
}

#[test]
fn invalid_included_sources_report_the_source_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let source_path = dir.path().join("sources.yml");
    std::fs::write(&source_path, "sources: {}\n").expect("write sources");
    let config_path = dir.path().join("floe.yml");
    std::fs::write(
        &config_path,
        "version: \"0.3\"\ninclude:\n  sources: sources.yml\nentities: []\n",
    )
    .expect("write project");

    let err = load_config(&config_path).expect_err("invalid sources shape");

    assert_eq!(
        err.to_string(),
        format!("{}: expected array at sources", source_path.display())
    );
}

#[test]
fn entity_error_from_included_source_names_the_source_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let source_path = dir.path().join("sources.yml");
    std::fs::write(
        &source_path,
        "sources:\n  - name: crm\n    format: csv\n    options: { bogus: 1 }\n",
    )
    .expect("write sources");
    let entities = config_with("{ ref: crm }");
    let entities = entities.split_once("entities:\n").expect("entities").1;
    let config_path = dir.path().join("floe.yml");
    std::fs::write(
        &config_path,
        format!("version: \"0.3\"\ninclude:\n  sources: sources.yml\nentities:\n{entities}"),
    )
    .expect("write project");

    let err = load_config(&config_path).expect_err("bad source option");

    assert_eq!(
        err.to_string(),
        format!(
            "inherits from {}: entities[0] (entity.name=orders): unknown field source.options.bogus",
            source_path.display()
        )
    );
}

#[test]
fn inline_source_error_is_unchanged() {
    let path = write_temp_config(&config_with("{ ref: crm, options: { bogus: 1 } }"));
    let err = load_config(&path).expect_err("bad source option");

    assert_eq!(
        err.to_string(),
        "entities[0] (entity.name=orders): unknown field source.options.bogus"
    );
}

#[test]
fn missing_included_sources_preserve_the_io_error_and_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = dir.path().join("floe.yml");
    std::fs::write(
        &config_path,
        "version: \"0.3\"\ninclude:\n  sources: sources.yml\nentities: []\n",
    )
    .expect("write project");
    let source_path = dir.path().join("sources.yml");

    let err = load_config(&config_path).expect_err("missing sources file");

    assert!(err.to_string().contains(&source_path.display().to_string()));
    assert!(matches!(
        err.downcast_ref::<FloeError>(),
        Some(FloeError::Io {
            path: Some(path), ..
        }) if path == &source_path.display().to_string()
    ));
}
