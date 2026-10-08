use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use floe_core::io::storage::inputs::{resolve_inputs, ResolveInputsMode};
use floe_core::io::storage::{ObjectRef, StorageClient, Target};
use floe_core::{config, io, report, FloeResult};

struct MockStorageClient {
    keys: Vec<String>,
    contents: HashMap<String, String>,
}

impl MockStorageClient {
    fn new(keys: Vec<String>) -> Self {
        let mut contents = HashMap::new();
        for key in &keys {
            contents.insert(key.clone(), format!("data for {key}"));
        }
        Self { keys, contents }
    }
}

impl StorageClient for MockStorageClient {
    fn list(&self, _prefix: &str) -> FloeResult<Vec<ObjectRef>> {
        Ok(self
            .keys
            .iter()
            .map(|key| ObjectRef {
                uri: format!("s3://bucket/{key}"),
                key: key.clone(),
                last_modified: None,
                size: None,
            })
            .collect())
    }

    fn download_to_temp(&self, uri: &str, temp_dir: &Path) -> FloeResult<PathBuf> {
        let key = uri.strip_prefix("s3://bucket/").unwrap_or(uri).to_string();
        let dest = temp_dir.join(key.replace('/', "_"));
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let content = self
            .contents
            .get(&key)
            .cloned()
            .unwrap_or_else(|| "missing".to_string());
        std::fs::write(&dest, content)?;
        Ok(dest)
    }

    fn upload_from_path(&self, _local_path: &Path, _uri: &str) -> FloeResult<()> {
        Ok(())
    }

    fn resolve_uri(&self, path: &str) -> FloeResult<String> {
        Ok(format!("s3://bucket/{path}"))
    }

    fn copy_object(&self, _src_uri: &str, _dst_uri: &str) -> FloeResult<()> {
        Ok(())
    }

    fn delete_object(&self, _uri: &str) -> FloeResult<()> {
        Ok(())
    }

    fn exists(&self, _uri: &str) -> FloeResult<bool> {
        Ok(false)
    }
}

struct ListOnlyStorageClient {
    keys: Vec<String>,
    list_prefixes: Arc<Mutex<Vec<String>>>,
}

impl ListOnlyStorageClient {
    fn new(keys: Vec<String>) -> Self {
        Self {
            keys,
            list_prefixes: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn list_prefixes(&self) -> Vec<String> {
        self.list_prefixes
            .lock()
            .expect("list prefixes lock")
            .clone()
    }
}

impl StorageClient for ListOnlyStorageClient {
    fn list(&self, prefix: &str) -> FloeResult<Vec<ObjectRef>> {
        self.list_prefixes
            .lock()
            .expect("list prefixes lock")
            .push(prefix.to_string());
        Ok(self
            .keys
            .iter()
            .filter(|key| prefix.is_empty() || key.starts_with(prefix))
            .map(|key| ObjectRef {
                uri: format!("s3://bucket/{key}"),
                key: key.clone(),
                last_modified: None,
                size: None,
            })
            .collect())
    }

    fn download_to_temp(&self, _uri: &str, _temp_dir: &Path) -> FloeResult<PathBuf> {
        panic!("download_to_temp should not be called in list-only mode");
    }

    fn upload_from_path(&self, _local_path: &Path, _uri: &str) -> FloeResult<()> {
        Ok(())
    }

    fn resolve_uri(&self, path: &str) -> FloeResult<String> {
        Ok(format!("s3://bucket/{path}"))
    }

    fn copy_object(&self, _src_uri: &str, _dst_uri: &str) -> FloeResult<()> {
        Ok(())
    }

    fn delete_object(&self, _uri: &str) -> FloeResult<()> {
        Ok(())
    }

    fn exists(&self, _uri: &str) -> FloeResult<bool> {
        Ok(false)
    }
}

struct MockAdapter;

impl io::format::InputAdapter for MockAdapter {
    fn format(&self) -> &'static str {
        "csv"
    }

    fn suffixes(&self) -> FloeResult<Vec<String>> {
        Ok(vec![".csv".to_string()])
    }

    fn read_input_columns(
        &self,
        _entity: &config::EntityConfig,
        _input_file: &io::format::LocalInputFile,
        _columns: &[config::ColumnConfig],
    ) -> Result<Vec<String>, io::format::FileReadError> {
        Ok(vec!["id".to_string()])
    }

    fn read_inputs(
        &self,
        _entity: &config::EntityConfig,
        _files: &[io::format::LocalInputFile],
        _columns: &[config::ColumnConfig],
        _normalize_strategy: Option<&str>,
        _collect_raw: bool,
    ) -> FloeResult<Vec<io::format::ReadInput>> {
        Ok(Vec::new())
    }
}

struct MockParquetAdapter;

impl io::format::InputAdapter for MockParquetAdapter {
    fn format(&self) -> &'static str {
        "parquet"
    }

    fn suffixes(&self) -> FloeResult<Vec<String>> {
        Ok(vec![".parquet".to_string()])
    }

    fn read_input_columns(
        &self,
        _entity: &config::EntityConfig,
        _input_file: &io::format::LocalInputFile,
        _columns: &[config::ColumnConfig],
    ) -> Result<Vec<String>, io::format::FileReadError> {
        Ok(vec!["id".to_string()])
    }

    fn read_inputs(
        &self,
        _entity: &config::EntityConfig,
        _files: &[io::format::LocalInputFile],
        _columns: &[config::ColumnConfig],
        _normalize_strategy: Option<&str>,
        _collect_raw: bool,
    ) -> FloeResult<Vec<io::format::ReadInput>> {
        Ok(Vec::new())
    }
}

fn mock_entity(name: &str) -> config::EntityConfig {
    config::EntityConfig {
        name: name.to_string(),
        id: name.to_string(),
        metadata: None,
        domain: None,
        incremental_mode: config::IncrementalMode::None,
        state: None,
        source: config::SourceConfig {
            format: "csv".to_string(),
            path: "unused".to_string(),
            storage: None,
            options: None,
            cast_mode: None,
        },
        sink: config::SinkConfig {
            write_mode: config::WriteMode::Overwrite,
            accepted: config::SinkTarget {
                format: "csv".to_string(),
                path: "out".to_string(),
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
fn resolve_inputs_s3_filters_and_downloads() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let keys = vec![
        "data/b.csv".to_string(),
        "data/a.csv".to_string(),
        "data/ignore.txt".to_string(),
    ];
    let client = MockStorageClient::new(keys);
    let adapter = MockAdapter;
    let entity = mock_entity("orders");
    let target = Target::S3 {
        storage: "s3_raw".to_string(),
        uri: "s3://bucket/data".to_string(),
        bucket: "bucket".to_string(),
        base_key: "data".to_string(),
    };

    let resolved = resolve_inputs(
        Path::new("."),
        &entity,
        &adapter,
        &target,
        ResolveInputsMode::Download,
        Some(temp_dir.path()),
        Some(&client),
    )?;

    assert_eq!(resolved.mode, report::ResolvedInputMode::Directory);
    assert_eq!(resolved.files.len(), 2);
    assert_eq!(resolved.listed.len(), 2);
    assert!(resolved.files[0].source_uri.ends_with("data/a.csv"));
    assert!(resolved.files[1].source_uri.ends_with("data/b.csv"));
    Ok(())
}

fn s3_target(base_key: &str) -> Target {
    let uri = if base_key.is_empty() {
        "s3://bucket".to_string()
    } else {
        format!("s3://bucket/{base_key}")
    };
    Target::S3 {
        storage: "s3_raw".to_string(),
        uri,
        bucket: "bucket".to_string(),
        base_key: base_key.to_string(),
    }
}

#[test]
fn resolve_inputs_s3_cloud_glob_suffix_filters_and_uses_derived_prefix() -> FloeResult<()> {
    let keys = vec![
        "data/sales_2024.json".to_string(),
        "data/sales_q1.csv".to_string(),
        "data/sales_2024.csv".to_string(),
        "data/inventory_2024.csv".to_string(),
    ];
    let client = ListOnlyStorageClient::new(keys);
    let adapter = MockAdapter;
    let entity = mock_entity("orders");
    let target = s3_target("data/sales_*.csv");

    let resolved = resolve_inputs(
        Path::new("."),
        &entity,
        &adapter,
        &target,
        ResolveInputsMode::ListOnly,
        None,
        Some(&client),
    )?;

    assert_eq!(client.list_prefixes(), vec!["data/sales_".to_string()]);
    assert_eq!(resolved.files.len(), 0);
    assert_eq!(
        resolved.listed,
        vec![
            "s3://bucket/data/sales_2024.csv".to_string(),
            "s3://bucket/data/sales_q1.csv".to_string(),
        ]
    );
    Ok(())
}

#[test]
fn resolve_inputs_s3_cloud_glob_nested_segment_wildcard() -> FloeResult<()> {
    let keys = vec![
        "data/us/sales.csv".to_string(),
        "data/eu/sales.csv".to_string(),
        "data/us/archive/sales.csv".to_string(),
        "data/sales.csv".to_string(),
    ];
    let client = ListOnlyStorageClient::new(keys);
    let adapter = MockAdapter;
    let entity = mock_entity("orders");
    let target = s3_target("data/*/sales.csv");

    let resolved = resolve_inputs(
        Path::new("."),
        &entity,
        &adapter,
        &target,
        ResolveInputsMode::ListOnly,
        None,
        Some(&client),
    )?;

    assert_eq!(client.list_prefixes(), vec!["data/".to_string()]);
    assert_eq!(
        resolved.listed,
        vec![
            "s3://bucket/data/eu/sales.csv".to_string(),
            "s3://bucket/data/us/sales.csv".to_string(),
        ]
    );
    Ok(())
}

#[test]
fn resolve_inputs_s3_cloud_glob_rejects_missing_prefix() {
    let client = ListOnlyStorageClient::new(vec!["data/a.csv".to_string()]);
    let adapter = MockAdapter;
    let entity = mock_entity("orders");
    let target = s3_target("*.csv");

    let err = resolve_inputs(
        Path::new("."),
        &entity,
        &adapter,
        &target,
        ResolveInputsMode::ListOnly,
        None,
        Some(&client),
    )
    .expect_err("glob without literal prefix should error");

    let msg = err.to_string();
    assert!(msg.contains("invalid cloud source path"));
    assert!(msg.contains("non-empty literal prefix"));
    assert!(client.list_prefixes().is_empty());
}

#[test]
fn resolve_inputs_s3_cloud_glob_rejects_invalid_pattern() {
    let client = ListOnlyStorageClient::new(vec!["data/a.csv".to_string()]);
    let adapter = MockAdapter;
    let entity = mock_entity("orders");
    let target = s3_target("data/*.[");

    let err = resolve_inputs(
        Path::new("."),
        &entity,
        &adapter,
        &target,
        ResolveInputsMode::ListOnly,
        None,
        Some(&client),
    )
    .expect_err("invalid glob should error");

    let msg = err.to_string();
    assert!(msg.contains("invalid cloud source path"));
    assert!(msg.contains("invalid cloud glob pattern"));
    assert!(client.list_prefixes().is_empty());
}

#[test]
fn resolve_inputs_s3_cloud_literal_brackets_are_treated_as_literal_path() -> FloeResult<()> {
    let keys = vec![
        "data/report[2024].csv".to_string(),
        "data/report22024.csv".to_string(),
    ];
    let client = ListOnlyStorageClient::new(keys);
    let adapter = MockAdapter;
    let entity = mock_entity("orders");
    let target = s3_target("data/report[2024].csv");

    let resolved = resolve_inputs(
        Path::new("."),
        &entity,
        &adapter,
        &target,
        ResolveInputsMode::ListOnly,
        None,
        Some(&client),
    )?;

    assert_eq!(
        client.list_prefixes(),
        vec!["data/report[2024].csv".to_string()]
    );
    assert_eq!(resolved.listed, vec!["s3://bucket/data/report[2024].csv"]);
    Ok(())
}

#[test]
fn resolve_inputs_s3_cloud_glob_results_stably_sorted() -> FloeResult<()> {
    let keys = vec![
        "root/b.csv".to_string(),
        "root/a.csv".to_string(),
        "root/c.csv".to_string(),
    ];
    let client = ListOnlyStorageClient::new(keys);
    let adapter = MockAdapter;
    let entity = mock_entity("orders");
    let target = s3_target("root/*.csv");

    let resolved = resolve_inputs(
        Path::new("."),
        &entity,
        &adapter,
        &target,
        ResolveInputsMode::ListOnly,
        None,
        Some(&client),
    )?;

    assert_eq!(client.list_prefixes(), vec!["root/".to_string()]);
    assert_eq!(
        resolved.listed,
        vec![
            "s3://bucket/root/a.csv".to_string(),
            "s3://bucket/root/b.csv".to_string(),
            "s3://bucket/root/c.csv".to_string(),
        ]
    );
    Ok(())
}

#[test]
fn resolve_inputs_local_normalizes_resolved_file_paths() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let config_dir = temp_dir.path().join("cfg");
    let input_dir = temp_dir.path().join("in");
    std::fs::create_dir_all(&config_dir)?;
    std::fs::create_dir_all(&input_dir)?;
    let input_file = input_dir.join("data.csv");
    std::fs::write(&input_file, "id,name\n1,alice\n")?;

    let adapter = MockAdapter;
    let mut entity = mock_entity("orders");
    entity.source.path = "../in/./data.csv".to_string();
    let target = Target::Local {
        storage: "local".to_string(),
        uri: "local://unused".to_string(),
        base_path: ".".to_string(),
    };

    let resolved = resolve_inputs(
        &config_dir,
        &entity,
        &adapter,
        &target,
        ResolveInputsMode::Download,
        None,
        None,
    )?;

    assert_eq!(resolved.files.len(), 1);
    let normalized = temp_dir.path().join("in/data.csv");
    assert_eq!(
        resolved.files[0].source_uri,
        normalized.display().to_string()
    );
    assert_eq!(resolved.listed, vec![normalized.display().to_string()]);
    assert!(!resolved.files[0].source_uri.contains("/./"));
    assert!(!resolved.files[0].source_uri.contains("/../"));
    Ok(())
}

#[test]
fn resolve_inputs_s3_filters_parquet_objects() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let keys = vec![
        "data/b.parquet".to_string(),
        "data/a.parquet".to_string(),
        "data/ignore.csv".to_string(),
    ];
    let client = MockStorageClient::new(keys);
    let adapter = MockParquetAdapter;
    let entity = mock_entity("orders");
    let target = Target::S3 {
        storage: "s3_raw".to_string(),
        uri: "s3://bucket/data".to_string(),
        bucket: "bucket".to_string(),
        base_key: "data".to_string(),
    };

    let resolved = resolve_inputs(
        Path::new("."),
        &entity,
        &adapter,
        &target,
        ResolveInputsMode::Download,
        Some(temp_dir.path()),
        Some(&client),
    )?;

    assert_eq!(resolved.mode, report::ResolvedInputMode::Directory);
    assert_eq!(resolved.files.len(), 2);
    assert_eq!(resolved.listed.len(), 2);
    assert!(resolved.files[0].source_uri.ends_with("data/a.parquet"));
    assert!(resolved.files[1].source_uri.ends_with("data/b.parquet"));
    Ok(())
}

#[test]
fn resolve_inputs_s3_list_only_does_not_download() -> FloeResult<()> {
    let keys = vec![
        "data/b.csv".to_string(),
        "data/a.csv".to_string(),
        "data/ignore.txt".to_string(),
    ];
    let client = ListOnlyStorageClient::new(keys);
    let adapter = MockAdapter;
    let entity = mock_entity("orders");
    let target = Target::S3 {
        storage: "s3_raw".to_string(),
        uri: "s3://bucket/data".to_string(),
        bucket: "bucket".to_string(),
        base_key: "data".to_string(),
    };

    let resolved = resolve_inputs(
        Path::new("."),
        &entity,
        &adapter,
        &target,
        ResolveInputsMode::ListOnly,
        None,
        Some(&client),
    )?;

    assert_eq!(resolved.mode, report::ResolvedInputMode::Directory);
    assert_eq!(resolved.files.len(), 0);
    assert_eq!(resolved.listed.len(), 2);
    assert!(resolved.listed[0].ends_with("data/a.csv"));
    assert!(resolved.listed[1].ends_with("data/b.csv"));
    Ok(())
}
