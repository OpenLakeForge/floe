use std::fs;
use std::path::{Path, PathBuf};

use floe_core::config::PolicySeverity;
use floe_core::{add_entity_to_config, load_config, validate, AddEntityOptions, ValidateOptions};
use polars::prelude::{DataFrame, NamedFrom, ParquetWriter, Series};
use tempfile::tempdir;
use url::Url;

fn write_file(path: &std::path::Path, contents: &str) {
    fs::write(path, contents).expect("write test file");
}

fn write_parquet(path: &Path) -> PathBuf {
    let mut df = DataFrame::new(vec![
        Series::new("employee_id".into(), vec![1_i64, 2_i64]).into(),
        Series::new("active".into(), vec![true, false]).into(),
    ])
    .expect("dataframe");
    let file = fs::File::create(path).expect("create parquet");
    ParquetWriter::new(file)
        .finish(&mut df)
        .expect("write parquet");
    path.to_path_buf()
}

fn base_config_yaml() -> &'static str {
    r#"
version: "0.2"
domains:
  - name: "sales"
    incoming_dir: "./in/sales"
entities: []
"#
}

#[test]
fn add_entity_infers_csv_and_appends_defaults() {
    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("config.yml");
    let input_path = dir.path().join("customers.csv");

    write_file(&config_path, base_config_yaml());
    write_file(
        &input_path,
        "id,name,active\n1,Alice,true\n2,Bob,false\n3,,true\n",
    );

    let outcome = add_entity_to_config(AddEntityOptions {
        config_path: config_path.clone(),
        output_path: None,
        input: input_path.display().to_string(),
        format: Some("csv".to_string()),
        name: Some("customers".to_string()),
        domain: Some("sales".to_string()),
        dry_run: false,
    })
    .expect("add entity");

    assert_eq!(outcome.entity_name, "customers");
    assert_eq!(outcome.column_count, 3);
    assert!(!outcome.dry_run);

    validate(&config_path, ValidateOptions::default()).expect("validate updated config");
    let config = load_config(&config_path).expect("load updated config");
    let entity = config.entities.first().expect("entity");

    assert_eq!(entity.name, "customers");
    assert_eq!(entity.domain.as_deref(), Some("sales"));
    assert_eq!(entity.source.format, "csv");
    assert_eq!(entity.source.path, input_path.display().to_string());
    let source_options = entity.source.options.as_ref().expect("source options");
    assert_eq!(source_options.separator.as_deref(), Some(","));

    assert_eq!(entity.sink.accepted.format, "parquet");
    assert_eq!(entity.sink.accepted.path, "out/accepted/customers/");
    assert_eq!(
        entity.sink.rejected.as_ref().expect("rejected").path,
        "out/rejected/customers/"
    );
    assert_eq!(entity.policy.severity, PolicySeverity::Reject);
    let mismatch = entity.schema.mismatch.as_ref().expect("mismatch");
    assert_eq!(mismatch.missing_columns.as_deref(), Some("reject_file"));
    assert_eq!(mismatch.extra_columns.as_deref(), Some("ignore"));

    let id_col = entity
        .schema
        .columns
        .iter()
        .find(|column| column.name == "id")
        .expect("id column");
    assert_eq!(id_col.source.as_deref(), Some("id"));
    assert_eq!(id_col.column_type, "number");

    let active_col = entity
        .schema
        .columns
        .iter()
        .find(|column| column.name == "active")
        .expect("active column");
    assert_eq!(active_col.source.as_deref(), Some("active"));
    assert_eq!(active_col.column_type, "boolean");
}

#[test]
fn add_entity_infers_json_top_level_keys_only() {
    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("config.yml");
    let input_path = dir.path().join("orders.json");

    write_file(&config_path, base_config_yaml());
    write_file(
        &input_path,
        r#"[
  {"id": 1, "meta": {"source": "api"}, "tags": ["new"], "created_at": "2026-01-01T10:00:00Z"},
  {"id": 2, "meta": null, "tags": ["vip"], "created_at": "2026-01-02T10:00:00Z"}
]"#,
    );

    let outcome = add_entity_to_config(AddEntityOptions {
        config_path: config_path.clone(),
        output_path: None,
        input: input_path.display().to_string(),
        format: Some("json".to_string()),
        name: None,
        domain: None,
        dry_run: false,
    })
    .expect("add entity");

    assert_eq!(outcome.entity_name, "orders");
    assert!(outcome
        .notes
        .iter()
        .any(|note| note.contains("nested JSON values inferred as string")));

    validate(&config_path, ValidateOptions::default()).expect("validate updated config");
    let config = load_config(&config_path).expect("load updated config");
    let entity = config.entities.first().expect("entity");

    assert_eq!(entity.name, "orders");
    assert_eq!(entity.source.format, "json");
    let source_options = entity.source.options.as_ref().expect("source options");
    assert_eq!(source_options.json_mode.as_deref(), Some("array"));

    let created_at = entity
        .schema
        .columns
        .iter()
        .find(|column| column.name == "created_at")
        .expect("created_at column");
    assert_eq!(created_at.source.as_deref(), Some("created_at"));
    assert_eq!(created_at.column_type, "datetime");

    let meta = entity
        .schema
        .columns
        .iter()
        .find(|column| column.name == "meta")
        .expect("meta column");
    assert_eq!(meta.source.as_deref(), Some("meta"));
    assert_eq!(meta.column_type, "string");
    assert_eq!(meta.nullable, Some(true));

    let tags = entity
        .schema
        .columns
        .iter()
        .find(|column| column.name == "tags")
        .expect("tags column");
    assert_eq!(tags.source.as_deref(), Some("tags"));
    assert_eq!(tags.column_type, "string");
}

#[test]
fn add_entity_normalizes_file_uri_before_persisting_source_path() {
    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("config.yml");
    let input_path = dir.path().join("customers.csv");

    write_file(&config_path, base_config_yaml());
    write_file(&input_path, "id,name\n1,Alice\n");

    let input_uri = Url::from_file_path(&input_path)
        .expect("file url")
        .to_string();

    add_entity_to_config(AddEntityOptions {
        config_path: config_path.clone(),
        output_path: None,
        input: input_uri,
        format: Some("csv".to_string()),
        name: Some("customers".to_string()),
        domain: None,
        dry_run: false,
    })
    .expect("add entity");

    let config = load_config(&config_path).expect("load updated config");
    let entity = config.entities.first().expect("entity");
    assert_eq!(entity.source.path, input_path.display().to_string());
    assert!(!entity.source.path.starts_with("file://"));
}

#[test]
fn add_entity_bootstraps_missing_config_file() {
    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("new.yml");
    let input_path = dir.path().join("orders.csv");

    write_file(&input_path, "id,total\n1,10.1\n");
    assert!(!config_path.exists());

    let outcome = add_entity_to_config(AddEntityOptions {
        config_path: config_path.clone(),
        output_path: None,
        input: input_path.display().to_string(),
        format: None,
        name: None,
        domain: None,
        dry_run: false,
    })
    .expect("bootstrap config and add entity");

    assert!(config_path.exists());
    assert_eq!(outcome.entity_name, "orders");
    assert_eq!(outcome.format, "csv");
    validate(&config_path, ValidateOptions::default()).expect("validate bootstrapped config");
}

#[test]
fn add_entity_infers_name_from_file_uri_stem_by_default() {
    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("config.yml");
    let input_path = dir.path().join("order_events.json");
    write_file(&config_path, "version: \"0.2\"\nentities: []\n");
    write_file(&input_path, "[{\"id\":1}]");

    let input_uri = Url::from_file_path(&input_path)
        .expect("file url")
        .to_string();
    let outcome = add_entity_to_config(AddEntityOptions {
        config_path: config_path.clone(),
        output_path: None,
        input: input_uri,
        format: None,
        name: None,
        domain: None,
        dry_run: false,
    })
    .expect("add entity");

    assert_eq!(outcome.entity_name, "order_events");
    let config = load_config(&config_path).expect("load config");
    assert_eq!(config.entities[0].name, "order_events");
}

#[test]
fn add_entity_infers_format_from_extension_csv_json_and_parquet() {
    let dir = tempdir().expect("tempdir");

    let csv_config = dir.path().join("csv.yml");
    let csv_input = dir.path().join("orders.csv");
    write_file(&csv_input, "id\n1\n");
    add_entity_to_config(AddEntityOptions {
        config_path: csv_config.clone(),
        output_path: None,
        input: csv_input.display().to_string(),
        format: None,
        name: None,
        domain: None,
        dry_run: false,
    })
    .expect("csv inferred");
    assert_eq!(
        load_config(&csv_config).expect("csv cfg").entities[0]
            .source
            .format,
        "csv"
    );

    let json_config = dir.path().join("json.yml");
    let json_input = dir.path().join("events.json");
    write_file(&json_input, "[{\"id\":1}]");
    add_entity_to_config(AddEntityOptions {
        config_path: json_config.clone(),
        output_path: None,
        input: json_input.display().to_string(),
        format: None,
        name: None,
        domain: None,
        dry_run: false,
    })
    .expect("json inferred");
    assert_eq!(
        load_config(&json_config).expect("json cfg").entities[0]
            .source
            .format,
        "json"
    );

    let parquet_config = dir.path().join("parquet.yml");
    let parquet_input = write_parquet(&dir.path().join("employees.parquet"));
    add_entity_to_config(AddEntityOptions {
        config_path: parquet_config.clone(),
        output_path: None,
        input: parquet_input.display().to_string(),
        format: None,
        name: None,
        domain: None,
        dry_run: false,
    })
    .expect("parquet inferred");
    assert_eq!(
        load_config(&parquet_config).expect("parquet cfg").entities[0]
            .source
            .format,
        "parquet"
    );
}

#[test]
fn add_entity_explicit_name_and_format_override_inferred_values() {
    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("config.yml");
    let input_path = dir.path().join("strange_name.data");

    write_file(&input_path, "id,name\n1,Alice\n");

    let outcome = add_entity_to_config(AddEntityOptions {
        config_path: config_path.clone(),
        output_path: None,
        input: input_path.display().to_string(),
        format: Some("csv".to_string()),
        name: Some("customers_override".to_string()),
        domain: None,
        dry_run: false,
    })
    .expect("add entity");

    assert_eq!(outcome.entity_name, "customers_override");
    assert_eq!(outcome.format, "csv");
    let config = load_config(&config_path).expect("load config");
    let entity = &config.entities[0];
    assert_eq!(entity.name, "customers_override");
    assert_eq!(entity.source.format, "csv");
}

#[test]
fn add_entity_unknown_extension_requires_explicit_format() {
    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("config.yml");
    let input_path = dir.path().join("payload.unknown");

    write_file(&input_path, "some,data\n");

    let err = add_entity_to_config(AddEntityOptions {
        config_path,
        output_path: None,
        input: input_path.display().to_string(),
        format: None,
        name: None,
        domain: None,
        dry_run: false,
    })
    .expect_err("unknown extension should require --format");

    assert!(err.to_string().contains("use --format"));
}

#[test]
fn add_entity_rejects_multi_document_config() {
    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("config.yml");
    let input_path = dir.path().join("customers.csv");

    let multi_doc = format!(
        "{}---\nversion: \"0.2\"\nentities: []\n",
        base_config_yaml()
    );
    write_file(&config_path, &multi_doc);
    write_file(&input_path, "id,name\n1,Alice\n");
    let original = fs::read_to_string(&config_path).expect("read config");

    let err = add_entity_to_config(AddEntityOptions {
        config_path: config_path.clone(),
        output_path: None,
        input: input_path.display().to_string(),
        format: Some("csv".to_string()),
        name: Some("customers".to_string()),
        domain: None,
        dry_run: false,
    })
    .expect_err("multi-document config should be rejected");

    assert_eq!(
        err.to_string(),
        "YAML contains multiple documents; expected one"
    );
    let after = fs::read_to_string(&config_path).expect("read config after failure");
    assert_eq!(after, original, "config file must not be modified");
}

#[test]
fn add_entity_v03_allows_same_name_in_another_domain() {
    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("config.yml");
    let input_path = dir.path().join("accounts.csv");
    write_file(
        &config_path,
        r#"
version: "0.3"
domains:
  - name: "sales"
    incoming_dir: "./in/sales"
  - name: "finance"
    incoming_dir: "./in/finance"
entities: []
"#,
    );
    write_file(&input_path, "id\n1\n");
    let add = |domain: &str| {
        add_entity_to_config(AddEntityOptions {
            config_path: config_path.clone(),
            output_path: None,
            input: input_path.display().to_string(),
            format: Some("csv".to_string()),
            name: Some("accounts".to_string()),
            domain: Some(domain.to_string()),
            dry_run: false,
        })
    };

    add("sales").expect("add sales.accounts");
    add("finance").expect("add finance.accounts");
    let err = add("sales").expect_err("sales.accounts exists").to_string();
    assert!(err.contains("entity already exists: accounts"), "{err}");

    let config = load_config(&config_path).expect("load updated config");
    let ids: Vec<&str> = config.entities.iter().map(|e| e.id.as_str()).collect();
    assert_eq!(ids, vec!["sales.accounts", "finance.accounts"]);
}
