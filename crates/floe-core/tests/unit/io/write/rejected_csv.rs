use std::fs;
use std::path::Path;

use floe_core::io::format::RejectedWriteRequest;
use floe_core::io::write::parts;
use floe_core::{config, io, FloeResult};
use polars::prelude::{DataFrame, NamedFrom, Series};

fn sample_entity() -> config::EntityConfig {
    config::EntityConfig {
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
                format: "parquet".to_string(),
                path: "out/accepted".to_string(),
                storage: None,
                options: None,
                merge: None,
                iceberg: None,
                write_mode: config::WriteMode::Overwrite,
                partition_by: None,
                partition_spec: None,
                delta: None,
                duckdb: None,
            },
            rejected: Some(config::SinkTarget {
                format: "csv".to_string(),
                path: "out/rejected".to_string(),
                storage: None,
                options: None,
                merge: None,
                iceberg: None,
                write_mode: config::WriteMode::Overwrite,
                partition_by: None,
                partition_spec: None,
                delta: None,
                duckdb: None,
            }),
            archive: None,
        },
        policy: config::PolicyConfig {
            severity: config::PolicySeverity::Reject,
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

fn sample_resolver(config_path: &Path) -> FloeResult<config::StorageResolver> {
    let root_config = config::RootConfig {
        version: "0.1".to_string(),
        metadata: None,
        storages: None,
        catalogs: None,
        env: None,
        domains: Vec::new(),
        report: None,
        lineage: None,
        entities: Vec::new(),
    };
    config::StorageResolver::from_path(&root_config, config_path)
}

fn make_rejected_df(value: &str) -> DataFrame {
    DataFrame::new(vec![
        Series::new("id".into(), [value]).into(),
        Series::new("__floe_row_index".into(), [0i64]).into(),
        Series::new("__floe_errors".into(), ["[{\"rule\":\"test\"}]"]).into(),
    ])
    .expect("build rejected df")
}

fn write_rejected_part(
    target: &io::storage::Target,
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
    cloud: &mut io::storage::CloudClient,
    mode: config::WriteMode,
    source_stem: &str,
    value: &str,
) -> FloeResult<String> {
    let adapter = io::format::rejected_sink_adapter("csv")?;
    let mut df = make_rejected_df(value);
    adapter.write_rejected(RejectedWriteRequest {
        target,
        df: &mut df,
        source_stem,
        temp_dir: None,
        cloud,
        resolver,
        entity,
        mode,
    })
}

#[test]
fn rejected_csv_append_writes_additional_dataset_parts() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let rejected_dir = temp_dir.path().join("out/rejected/orders");
    let target = io::storage::Target::Local {
        storage: "local".to_string(),
        uri: rejected_dir.display().to_string(),
        base_path: rejected_dir.display().to_string(),
    };
    let resolver = sample_resolver(temp_dir.path())?;
    let entity = sample_entity();
    let mut cloud = io::storage::CloudClient::new();

    let first_path = write_rejected_part(
        &target,
        &resolver,
        &entity,
        &mut cloud,
        config::WriteMode::Append,
        "first",
        "alpha",
    )?;
    let second_path = write_rejected_part(
        &target,
        &resolver,
        &entity,
        &mut cloud,
        config::WriteMode::Append,
        "second",
        "beta",
    )?;

    let first_path = first_path.trim_start_matches("local://");
    let second_path = second_path.trim_start_matches("local://");
    let first_file = Path::new(first_path)
        .file_name()
        .and_then(|name| name.to_str())
        .expect("first part file");
    let second_file = Path::new(second_path)
        .file_name()
        .and_then(|name| name.to_str())
        .expect("second part file");

    assert_ne!(first_file, second_file);
    assert!(parts::is_part_filename(first_file, "csv"));
    assert!(parts::is_part_filename(second_file, "csv"));
    assert!(Path::new(first_path).exists());
    assert!(Path::new(second_path).exists());
    assert!(!rejected_dir.join("first_rejected.csv").exists());
    assert!(!rejected_dir.join("second_rejected.csv").exists());

    let first_contents = fs::read_to_string(first_path)?;
    let second_contents = fs::read_to_string(second_path)?;
    assert!(first_contents.contains("alpha"));
    assert!(second_contents.contains("beta"));
    Ok(())
}

#[test]
fn rejected_csv_overwrite_resets_dataset_parts() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let rejected_dir = temp_dir.path().join("out/rejected/orders");
    let target = io::storage::Target::Local {
        storage: "local".to_string(),
        uri: rejected_dir.display().to_string(),
        base_path: rejected_dir.display().to_string(),
    };
    let resolver = sample_resolver(temp_dir.path())?;
    let entity = sample_entity();
    let mut cloud = io::storage::CloudClient::new();

    let _ = write_rejected_part(
        &target,
        &resolver,
        &entity,
        &mut cloud,
        config::WriteMode::Append,
        "first",
        "alpha",
    )?;
    let _ = write_rejected_part(
        &target,
        &resolver,
        &entity,
        &mut cloud,
        config::WriteMode::Append,
        "second",
        "beta",
    )?;

    let overwrite_path = write_rejected_part(
        &target,
        &resolver,
        &entity,
        &mut cloud,
        config::WriteMode::Overwrite,
        "third",
        "gamma",
    )?;

    assert!(overwrite_path.ends_with("part-00000.csv"));
    assert!(rejected_dir.join("part-00000.csv").exists());
    assert!(!rejected_dir.join("part-00001.csv").exists());

    let part_files = fs::read_dir(&rejected_dir)?
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("csv"))
        .count();
    assert_eq!(part_files, 1);

    let overwritten_contents = fs::read_to_string(rejected_dir.join("part-00000.csv"))?;
    assert!(overwritten_contents.contains("gamma"));
    assert!(!overwritten_contents.contains("alpha"));
    assert!(!overwritten_contents.contains("beta"));
    Ok(())
}
