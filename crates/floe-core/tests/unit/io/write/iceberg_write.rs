use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use arrow::array::Int64Array;
use floe_core::io::storage::Target;
use floe_core::io::write::iceberg::write_iceberg_table;
use floe_core::{config, FloeResult};
use futures::TryStreamExt;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::spec::Transform;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use polars::prelude::{df, DataFrame, DataType, NamedFrom, Series, TimeUnit};

#[test]
fn write_iceberg_table_append_creates_new_snapshot_and_metadata_version() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let table_path = temp_dir.path().join("iceberg_table");
    let config = empty_root_config();
    let resolver = config::StorageResolver::from_path(&config, temp_dir.path())?;
    let target = resolve_local_target(&resolver, &table_path)?;
    let entity = build_entity(&table_path, config::WriteMode::Append, Vec::new(), None);

    let mut df_first = df!(
        "id" => &[1i64, 2, 3],
        "name" => &["a", "b", "c"]
    )?;
    let out1 = write_iceberg_table(&mut df_first, &target, &entity, config::WriteMode::Append)?;
    assert_eq!(out1.parts_written, 1);
    assert!(out1.snapshot_id.is_some());

    let snapshot1 = current_snapshot_id(&table_path)?;
    let version1 = out1.table_version.unwrap_or(-1);
    assert_eq!(snapshot1, out1.snapshot_id.unwrap());

    let mut df_second = df!(
        "id" => &[4i64, 5],
        "name" => &["d", "e"]
    )?;
    let out2 = write_iceberg_table(&mut df_second, &target, &entity, config::WriteMode::Append)?;
    assert_eq!(out2.parts_written, 1);
    assert!(out2.snapshot_id.is_some());
    assert_ne!(out2.snapshot_id, out1.snapshot_id);
    assert!(out2.table_version.unwrap_or(-1) > version1);

    let mut rows = scan_i64_column(&table_path, "id")?;
    rows.sort_unstable();
    assert_eq!(rows, vec![1, 2, 3, 4, 5]);
    assert!(metadata_json_count(&table_path)? >= 3);

    Ok(())
}

#[test]
fn write_iceberg_table_overwrite_replaces_logical_contents() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let table_path = temp_dir.path().join("iceberg_table");
    let config = empty_root_config();
    let resolver = config::StorageResolver::from_path(&config, temp_dir.path())?;
    let target = resolve_local_target(&resolver, &table_path)?;
    let entity = build_entity(&table_path, config::WriteMode::Overwrite, Vec::new(), None);

    let mut df_first = df!(
        "id" => &[10i64, 20, 30],
        "name" => &["a", "b", "c"]
    )?;
    let out1 = write_iceberg_table(
        &mut df_first,
        &target,
        &entity,
        config::WriteMode::Overwrite,
    )?;
    assert!(out1.snapshot_id.is_some());

    let mut df_second = df!(
        "id" => &[40i64, 50],
        "name" => &["d", "e"]
    )?;
    let out2 = write_iceberg_table(
        &mut df_second,
        &target,
        &entity,
        config::WriteMode::Overwrite,
    )?;
    assert!(out2.snapshot_id.is_some());

    let rows = scan_i64_column(&table_path, "id")?;
    assert_eq!(rows, vec![40, 50]);
    Ok(())
}

#[test]
fn write_iceberg_table_rejects_unsupported_dtype() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let table_path = temp_dir.path().join("iceberg_table");
    let config = empty_root_config();
    let resolver = config::StorageResolver::from_path(&config, temp_dir.path())?;
    let target = resolve_local_target(&resolver, &table_path)?;
    let entity = build_entity(&table_path, config::WriteMode::Overwrite, Vec::new(), None);

    let payload = Series::new("payload".into(), &[Some(&b"ab"[..]), Some(&b"cd"[..])]);
    let mut df = DataFrame::new(vec![payload.into()])?;
    let err = write_iceberg_table(&mut df, &target, &entity, config::WriteMode::Overwrite)
        .expect_err("binary dtype should be rejected");
    let msg = err.to_string();
    assert!(msg.contains("iceberg sink supports scalar types only"));
    assert!(msg.contains("payload"));

    Ok(())
}

#[test]
fn write_iceberg_table_empty_dataframe_creates_table_without_snapshot() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let table_path = temp_dir.path().join("iceberg_table");
    let config = empty_root_config();
    let resolver = config::StorageResolver::from_path(&config, temp_dir.path())?;
    let target = resolve_local_target(&resolver, &table_path)?;
    let entity = build_entity(&table_path, config::WriteMode::Overwrite, Vec::new(), None);

    let mut df = df!(
        "id" => Vec::<i64>::new(),
        "name" => Vec::<String>::new()
    )?;
    let out = write_iceberg_table(&mut df, &target, &entity, config::WriteMode::Overwrite)?;
    assert_eq!(out.parts_written, 0);
    assert!(out.snapshot_id.is_none());
    assert_eq!(out.files_written, Some(0));
    assert_eq!(out.metrics.total_bytes_written, Some(0));
    assert_eq!(out.metrics.avg_file_size_mb, None);
    assert_eq!(out.metrics.small_files_count, Some(0));
    assert!(table_path.join("metadata").exists());
    assert!(!table_path.join("data").exists());
    assert_eq!(metadata_json_count(&table_path)?, 1);

    Ok(())
}

#[test]
fn write_iceberg_table_local_metrics_count_data_files_not_metadata() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let table_path = temp_dir.path().join("iceberg_table");
    let config = empty_root_config();
    let resolver = config::StorageResolver::from_path(&config, temp_dir.path())?;
    let target = resolve_local_target(&resolver, &table_path)?;
    let entity = build_entity(&table_path, config::WriteMode::Overwrite, Vec::new(), None);

    let mut df = df!(
        "id" => &[1i64, 2, 3, 4, 5],
        "name" => &["a", "b", "c", "d", "e"]
    )?;
    let out = write_iceberg_table(&mut df, &target, &entity, config::WriteMode::Overwrite)?;

    let (data_file_count, data_total_bytes) = collect_file_stats(&table_path.join("data"))?;
    let (_metadata_file_count, metadata_total_bytes) =
        collect_file_stats(&table_path.join("metadata"))?;

    assert_eq!(out.files_written, Some(data_file_count));
    assert_eq!(out.parts_written, data_file_count);
    assert_eq!(out.metrics.total_bytes_written, Some(data_total_bytes));
    assert!(out.metrics.avg_file_size_mb.is_some());
    assert!(out.metrics.small_files_count.is_some());
    assert!(metadata_total_bytes > 0);
    assert!(data_total_bytes < data_total_bytes + metadata_total_bytes);
    if let Some(avg_mb) = out.metrics.avg_file_size_mb {
        let expected_avg_mb = data_total_bytes as f64 / data_file_count as f64 / (1024.0 * 1024.0);
        assert!((avg_mb - expected_avg_mb).abs() < 1e-12);
    }

    Ok(())
}

#[test]
fn write_iceberg_table_public_helper_rejects_catalog_mode_without_runtime_context() -> FloeResult<()>
{
    let mut df = df!(
        "id" => &[1i64],
        "name" => &["alice"]
    )?;
    let mut entity = build_entity(
        Path::new("/tmp/unused"),
        config::WriteMode::Append,
        Vec::new(),
        None,
    );
    entity.sink.accepted.storage = Some("s3_out".to_string());
    entity.sink.accepted.iceberg = Some(config::IcebergSinkTargetConfig {
        catalog: Some("glue_main".to_string()),
        namespace: None,
        table: None,
        location: None,
    });
    let target = Target::S3 {
        storage: "s3_out".to_string(),
        uri: "s3://bucket/accepted/orders".to_string(),
        bucket: "bucket".to_string(),
        base_key: "accepted/orders".to_string(),
    };

    let err = write_iceberg_table(&mut df, &target, &entity, config::WriteMode::Append)
        .expect_err("catalog mode should require runtime context");
    assert!(err.to_string().contains("runtime catalog context"));
    Ok(())
}

#[test]
fn write_iceberg_table_supports_i8_i16_by_upcasting_to_iceberg_int() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let table_path = temp_dir.path().join("iceberg_table");
    let config = empty_root_config();
    let resolver = config::StorageResolver::from_path(&config, temp_dir.path())?;
    let target = resolve_local_target(&resolver, &table_path)?;
    let entity = build_entity(&table_path, config::WriteMode::Overwrite, Vec::new(), None);

    let mut df = df!(
        "tiny" => &[1_i8, 2_i8, 3_i8],
        "small" => &[10_i16, 20_i16, 30_i16]
    )?;
    let out = write_iceberg_table(&mut df, &target, &entity, config::WriteMode::Overwrite)?;
    assert_eq!(out.parts_written, 1);
    assert!(out.snapshot_id.is_some());

    Ok(())
}

#[test]
fn write_iceberg_table_append_without_schema_keeps_nullability_stable() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let table_path = temp_dir.path().join("iceberg_table");
    let config = empty_root_config();
    let resolver = config::StorageResolver::from_path(&config, temp_dir.path())?;
    let target = resolve_local_target(&resolver, &table_path)?;
    let entity = build_entity(&table_path, config::WriteMode::Append, Vec::new(), None);

    let mut df_first = df!(
        "id" => &[1_i64, 2_i64],
        "name" => &["alice", "bob"]
    )?;
    write_iceberg_table(&mut df_first, &target, &entity, config::WriteMode::Append)?;

    let mut df_second = df!(
        "id" => &[3_i64, 4_i64],
        "name" => &[Some("charlie"), None]
    )?;
    let out = write_iceberg_table(&mut df_second, &target, &entity, config::WriteMode::Append)?;
    assert_eq!(out.parts_written, 1);
    assert!(out.snapshot_id.is_some());

    let mut ids = scan_i64_column(&table_path, "id")?;
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3, 4]);

    Ok(())
}

#[test]
fn write_iceberg_table_applies_partition_spec_transforms_to_table_metadata_and_layout(
) -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let table_path = temp_dir.path().join("iceberg_table");
    let config = empty_root_config();
    let resolver = config::StorageResolver::from_path(&config, temp_dir.path())?;
    let target = resolve_local_target(&resolver, &table_path)?;
    let mut entity = build_entity(
        &table_path,
        config::WriteMode::Overwrite,
        vec![
            column("id", "int"),
            column("ts_year", "timestamp"),
            column("ts_month", "timestamp"),
            column("ts_day", "timestamp"),
            column("ts_hour", "timestamp"),
            column("name", "string"),
        ],
        None,
    );
    entity.sink.accepted.partition_spec = Some(vec![
        config::IcebergPartitionFieldConfig {
            column: "id".to_string(),
            transform: "identity".to_string(),
        },
        config::IcebergPartitionFieldConfig {
            column: "ts_year".to_string(),
            transform: "year".to_string(),
        },
        config::IcebergPartitionFieldConfig {
            column: "ts_month".to_string(),
            transform: "month".to_string(),
        },
        config::IcebergPartitionFieldConfig {
            column: "ts_day".to_string(),
            transform: "day".to_string(),
        },
        config::IcebergPartitionFieldConfig {
            column: "ts_hour".to_string(),
            transform: "hour".to_string(),
        },
    ]);

    let ts_raw = Series::new(
        "ts".into(),
        &[1_706_847_000_000_000_i64, 1_706_850_600_000_000_i64],
    );
    let ts = ts_raw.cast(&DataType::Datetime(TimeUnit::Microseconds, None))?;
    let mut ts_year = ts.clone();
    ts_year.rename("ts_year".into());
    let mut ts_month = ts.clone();
    ts_month.rename("ts_month".into());
    let mut ts_day = ts.clone();
    ts_day.rename("ts_day".into());
    let mut ts_hour = ts.clone();
    ts_hour.rename("ts_hour".into());
    let mut df = DataFrame::new(vec![
        Series::new("id".into(), &[1_i64, 2_i64]).into(),
        ts_year.into(),
        ts_month.into(),
        ts_day.into(),
        ts_hour.into(),
        Series::new("name".into(), &["alice", "bob"]).into(),
    ])?;

    let out = write_iceberg_table(&mut df, &target, &entity, config::WriteMode::Overwrite)?;
    assert_eq!(out.parts_written, 2);
    assert!(out.snapshot_id.is_some());

    let runtime = test_runtime()?;
    runtime.block_on(async {
        let table = load_table(&table_path).await?;
        let spec = table.metadata().default_partition_spec();
        let fields = spec.fields();
        assert_eq!(fields.len(), 5);
        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[0].transform, Transform::Identity);
        assert_eq!(fields[1].name, "ts_year_year");
        assert_eq!(fields[1].transform, Transform::Year);
        assert_eq!(fields[2].name, "ts_month_month");
        assert_eq!(fields[2].transform, Transform::Month);
        assert_eq!(fields[3].name, "ts_day_day");
        assert_eq!(fields[3].transform, Transform::Day);
        assert_eq!(fields[4].name, "ts_hour_hour");
        assert_eq!(fields[4].transform, Transform::Hour);
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })?;

    let data_dirs = fs::read_dir(table_path.join("data"))?
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .collect::<Vec<_>>();
    assert!(data_dirs.iter().any(|name| name.starts_with("id=")));

    Ok(())
}

#[test]
fn write_iceberg_table_rejects_unsupported_partition_transform_at_runtime() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let table_path = temp_dir.path().join("iceberg_table");
    let config = empty_root_config();
    let resolver = config::StorageResolver::from_path(&config, temp_dir.path())?;
    let target = resolve_local_target(&resolver, &table_path)?;
    let mut entity = build_entity(
        &table_path,
        config::WriteMode::Overwrite,
        vec![column("id", "int")],
        None,
    );
    entity.sink.accepted.partition_spec = Some(vec![config::IcebergPartitionFieldConfig {
        column: "id".to_string(),
        transform: "bucket[16]".to_string(),
    }]);

    let mut df = df!("id" => &[1_i64, 2_i64])?;
    let err = write_iceberg_table(&mut df, &target, &entity, config::WriteMode::Overwrite)
        .expect_err("unsupported runtime transform should error");
    let msg = err.to_string();
    assert!(msg.contains("unsupported runtime transform"));
    assert!(msg.contains("bucket[16]"));
    Ok(())
}

#[test]
fn write_iceberg_table_rejects_missing_partition_column_at_runtime() -> FloeResult<()> {
    let temp_dir = tempfile::TempDir::new()?;
    let table_path = temp_dir.path().join("iceberg_table");
    let config = empty_root_config();
    let resolver = config::StorageResolver::from_path(&config, temp_dir.path())?;
    let target = resolve_local_target(&resolver, &table_path)?;
    let mut entity = build_entity(
        &table_path,
        config::WriteMode::Overwrite,
        vec![column("id", "int")],
        None,
    );
    entity.sink.accepted.partition_spec = Some(vec![config::IcebergPartitionFieldConfig {
        column: "missing".to_string(),
        transform: "identity".to_string(),
    }]);

    let mut df = df!("id" => &[1_i64, 2_i64])?;
    let err = write_iceberg_table(&mut df, &target, &entity, config::WriteMode::Overwrite)
        .expect_err("missing runtime partition column should error");
    let msg = err.to_string();
    assert!(msg.contains("partition_spec column missing"));
    assert!(msg.contains("runtime schema"));
    Ok(())
}

fn empty_root_config() -> config::RootConfig {
    config::RootConfig {
        version: "0.1".to_string(),
        metadata: None,
        storages: None,
        catalogs: None,
        env: None,
        domains: Vec::new(),
        report: None,
        lineage: None,
        entities: Vec::new(),
    }
}

fn resolve_local_target(
    resolver: &config::StorageResolver,
    table_path: &Path,
) -> FloeResult<Target> {
    let resolved = resolver.resolve_path(
        "orders",
        "sink.accepted.path",
        None,
        table_path.to_str().unwrap(),
    )?;
    Target::from_resolved(&resolved)
}

fn build_entity(
    table_path: &Path,
    write_mode: config::WriteMode,
    columns: Vec<config::ColumnConfig>,
    normalize_columns: Option<config::NormalizeColumnsConfig>,
) -> config::EntityConfig {
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
            write_mode,
            accepted: config::SinkTarget {
                format: "iceberg".to_string(),
                path: table_path.display().to_string(),
                storage: None,
                options: None,
                merge: None,
                iceberg: None,
                write_mode,
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
            normalize_columns,
            mismatch: None,
            schema_evolution: None,
            primary_key: None,
            unique_keys: None,
            columns,
        },
        pii: None,
    }
}

fn column(name: &str, column_type: &str) -> config::ColumnConfig {
    config::ColumnConfig {
        name: name.to_string(),
        source: None,
        column_type: column_type.to_string(),
        nullable: Some(true),
        unique: None,
        width: None,
        trim: None,
    }
}

fn metadata_json_count(table_path: &Path) -> FloeResult<usize> {
    let metadata_dir = table_path.join("metadata");
    if !metadata_dir.exists() {
        return Ok(0);
    }
    let mut count = 0;
    for entry in fs::read_dir(metadata_dir)? {
        let entry = entry?;
        if entry
            .path()
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| name.ends_with(".metadata.json"))
            .unwrap_or(false)
        {
            count += 1;
        }
    }
    Ok(count)
}

fn current_snapshot_id(table_path: &Path) -> FloeResult<i64> {
    let runtime = test_runtime()?;
    runtime.block_on(async {
        let table = load_table(table_path).await?;
        table
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .ok_or_else(|| {
                floe_core::FloeError::run("missing current iceberg snapshot".to_string()).into()
            })
    })
}

fn scan_i64_column(table_path: &Path, column: &str) -> FloeResult<Vec<i64>> {
    let runtime = test_runtime()?;
    runtime.block_on(async {
        let table = load_table(table_path).await?;
        let mut stream = table
            .scan()
            .build()
            .map_err(map_iceberg_err("iceberg scan build failed"))?
            .to_arrow()
            .await
            .map_err(map_iceberg_err("iceberg scan to_arrow failed"))?;
        let mut values = Vec::new();
        while let Some(batch) = stream
            .try_next()
            .await
            .map_err(map_iceberg_err("iceberg scan read failed"))?
        {
            let idx = batch.schema().index_of(column).map_err(|err| {
                floe_core::FloeError::run(format!("missing column in scan batch: {err}"))
            })?;
            let arr = batch
                .column(idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| floe_core::FloeError::run("expected Int64Array".to_string()))?;
            for i in 0..arr.len() {
                values.push(arr.value(i));
            }
        }
        Ok(values)
    })
}

async fn load_table(table_path: &Path) -> FloeResult<iceberg::table::Table> {
    let metadata_location = latest_metadata_location(table_path)?
        .ok_or_else(|| floe_core::FloeError::run("missing iceberg metadata file".to_string()))?;
    // LocalFsStorageFactory is required so that register_table can read the
    // metadata JSON from disk and the subsequent scan reads Parquet files from disk.
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "floe_test",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                table_path.display().to_string(),
            )]),
        )
        .await
        .map_err(map_iceberg_err("iceberg test catalog init failed"))?;
    let namespace = NamespaceIdent::new("floe".to_string());
    if !catalog
        .namespace_exists(&namespace)
        .await
        .map_err(map_iceberg_err("iceberg test namespace exists failed"))?
    {
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .map_err(map_iceberg_err("iceberg test namespace create failed"))?;
    }
    let ident = TableIdent::new(namespace, "orders".to_string());
    catalog
        .register_table(&ident, metadata_location)
        .await
        .map_err(map_iceberg_err("iceberg test register table failed"))
}

fn latest_metadata_location(table_path: &Path) -> FloeResult<Option<String>> {
    let metadata_dir = table_path.join("metadata");
    if !metadata_dir.exists() {
        return Ok(None);
    }
    let mut files = Vec::new();
    for entry in fs::read_dir(metadata_dir)? {
        let entry = entry?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !name.ends_with(".metadata.json") {
            continue;
        }
        files.push(path);
    }
    files.sort();
    Ok(files.last().map(|p| p.display().to_string()))
}

fn test_runtime() -> FloeResult<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| {
            floe_core::FloeError::run(format!("iceberg test runtime init failed: {err}")).into()
        })
}

fn map_iceberg_err(
    context: &'static str,
) -> impl FnOnce(iceberg::Error) -> Box<dyn std::error::Error + Send + Sync> {
    move |err| floe_core::FloeError::run(format!("{context}: {err}")).into()
}

fn collect_file_stats(dir: &Path) -> FloeResult<(u64, u64)> {
    if !dir.exists() {
        return Ok((0, 0));
    }
    let mut files = 0_u64;
    let mut total_bytes = 0_u64;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let entry_path = entry.path();
            let metadata = entry.metadata()?;
            if metadata.is_dir() {
                stack.push(entry_path);
                continue;
            }
            if metadata.is_file() {
                files += 1;
                total_bytes += metadata.len();
            }
        }
    }
    Ok((files, total_bytes))
}
