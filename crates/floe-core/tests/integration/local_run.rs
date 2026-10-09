use std::fs;
use std::path::{Path, PathBuf};

use floe_core::{load_config, run, validate_config_for_tests, RunOptions};
use polars::prelude::{ParquetReader, SerReader};

fn write_csv(dir: &Path, name: &str, contents: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, contents).expect("write csv");
    path
}

fn write_config(dir: &Path, contents: &str) -> PathBuf {
    let path = dir.join("config.yml");
    fs::write(&path, contents).expect("write config");
    path
}

#[test]
fn local_run_writes_outputs_and_report() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer");
    let rejected_dir = root.join("out/rejected/customer");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "data.csv", "id;name\n1;alice\n2;bob\n");

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "{input_dir}"
    sink:
      accepted:
        format: "parquet"
        path: "{accepted_dir}"
      rejected:
        format: "csv"
        path: "{rejected_dir}"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
        - name: "name"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
        rejected_dir = rejected_dir.display(),
    );
    let config_path = write_config(root, &yaml);

    let outcome = run(
        &config_path,
        RunOptions {
            profile: None,
            run_id: Some("it-run".to_string()),
            entities: Vec::new(),
            dry_run: false,
            full_refresh: false,
        },
    )
    .expect("run config");

    assert_eq!(outcome.run_id, "it-run");

    let mut parquet_files = Vec::new();
    if accepted_dir.exists() {
        for entry in fs::read_dir(&accepted_dir).expect("read accepted dir") {
            let entry = entry.expect("read entry");
            if entry.path().extension().and_then(|ext| ext.to_str()) == Some("parquet") {
                parquet_files.push(entry.path());
            }
        }
    }
    assert!(
        !parquet_files.is_empty(),
        "expected parquet output in {}",
        accepted_dir.display()
    );

    let report_path = report_dir.join("run_it-run/customer/run.json");
    assert!(
        report_path.exists(),
        "expected report at {}",
        report_path.display()
    );

    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(report.sink.accepted.format, "parquet");
    assert_eq!(
        report.accepted_output.files_written.expect("files_written") as usize,
        parquet_files.len()
    );
    assert_eq!(
        report.accepted_output.parts_written as usize,
        parquet_files.len()
    );
    assert!(report.accepted_output.total_bytes_written.is_some());
    assert!(report.accepted_output.avg_file_size_mb.is_some());
    assert!(report.accepted_output.small_files_count.is_some());
}

#[test]
fn local_run_accepts_v03_domain_without_incoming_dir() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_path = write_csv(root, "orders.csv", "id\n1\n");
    let accepted_dir = root.join("out/accepted");
    let report_dir = root.join("report");
    let yaml = format!(
        r#"version: "0.3"
domains:
  - name: sales
entities:
  - name: orders
    domain: sales
    source:
      format: csv
      path: "{input_path}"
    sink:
      accepted:
        format: parquet
        path: "{accepted_dir}"
    policy:
      severity: warn
    schema:
      columns:
        - name: id
          type: string
report:
  path: "{report_dir}"
"#,
        input_path = input_path.display(),
        accepted_dir = accepted_dir.display(),
        report_dir = report_dir.display(),
    );
    let config_path = write_config(root, &yaml);
    let config = load_config(&config_path).expect("load config");
    validate_config_for_tests(&config).expect("validate config");

    let outcome = run(
        &config_path,
        RunOptions {
            profile: None,
            run_id: Some("v03-domain".to_string()),
            entities: Vec::new(),
            dry_run: false,
            full_refresh: false,
        },
    )
    .expect("run config");

    assert_eq!(
        outcome.summary.run.status,
        floe_core::report::RunStatus::Success
    );
    assert_eq!(outcome.summary.results.accepted_total, 1);
    assert_eq!(outcome.summary.results.rejected_total, 0);
    assert!(accepted_dir.exists());
}

#[test]
fn local_run_parquet_metrics_track_chunked_output_files() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    let mut csv = String::from("id;name\n");
    for i in 0..200 {
        csv.push_str(&format!("{i};user_{i}\n"));
    }
    write_csv(&input_dir, "data.csv", &csv);

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "{input_dir}"
    sink:
      accepted:
        format: "parquet"
        path: "{accepted_dir}"
        options:
          max_size_per_file: 256
      rejected:
        format: "csv"
        path: "{rejected_dir}"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
        - name: "name"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
        rejected_dir = root.join("out/rejected/customer").display(),
    );
    let config_path = write_config(root, &yaml);

    let outcome = run(
        &config_path,
        RunOptions {
            profile: None,
            run_id: Some("it-run-parquet-metrics".to_string()),
            entities: Vec::new(),
            dry_run: false,
            full_refresh: false,
        },
    )
    .expect("run config");

    let parquet_files = fs::read_dir(&accepted_dir)
        .expect("read accepted dir")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("parquet"))
        .count();
    assert!(parquet_files > 1, "expected chunked parquet output");

    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(
        report.accepted_output.files_written.expect("files_written") as usize,
        parquet_files
    );
    assert_eq!(report.accepted_output.parts_written as usize, parquet_files);
    let total_bytes = report
        .accepted_output
        .total_bytes_written
        .expect("total_bytes_written");
    assert!(total_bytes > 0);
    assert!(
        report
            .accepted_output
            .avg_file_size_mb
            .expect("avg_file_size_mb")
            > 0.0
    );
    assert!(report.accepted_output.small_files_count.is_some());
}

#[test]
fn streaming_parquet_writes_preserve_row_counts_and_size_bound() {
    // Forces multiple chunked writes through LazyFrame::sink_parquet (the
    // new_streaming engine) within a single buffer flush, and verifies the
    // round trip: sum of rows on disk == input rows, and each part stays
    // within a generous multiple of `max_size_per_file`. Locks down the
    // streaming-engine swap against the previous eager-writer baseline.
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    let mut csv = String::from("id;name\n");
    for i in 0..5000 {
        csv.push_str(&format!("{i};user_{i}\n"));
    }
    write_csv(&input_dir, "data.csv", &csv);

    let max_size_per_file: u64 = 4096;
    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "{input_dir}"
    sink:
      accepted:
        format: "parquet"
        path: "{accepted_dir}"
        options:
          max_size_per_file: {max_size_per_file}
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
        - name: "name"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
        max_size_per_file = max_size_per_file,
    );
    let config_path = write_config(root, &yaml);

    let outcome = run(
        &config_path,
        RunOptions {
            profile: None,
            run_id: Some("it-streaming-parquet".to_string()),
            entities: Vec::new(),
            dry_run: false,
            full_refresh: false,
        },
    )
    .expect("run config");

    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(report.results.accepted_total, 5000);
    assert!(
        report.accepted_output.parts_written > 1,
        "expected the streaming engine to emit multiple part files for a small max_size_per_file"
    );

    let mut part_paths: Vec<PathBuf> = fs::read_dir(&accepted_dir)
        .expect("read accepted dir")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("parquet"))
        .collect();
    part_paths.sort();

    let mut total_rows = 0_usize;
    for part_path in &part_paths {
        let file = fs::File::open(part_path).expect("open part parquet");
        let df = ParquetReader::new(file)
            .finish()
            .expect("read part parquet");
        total_rows += df.height();
        // Allow generous slack for Parquet metadata + footer overhead.
        let size = fs::metadata(part_path).expect("part metadata").len();
        assert!(
            size <= max_size_per_file * 4,
            "part {part_path:?} size {size} exceeds 4x max_size_per_file ({max_size_per_file})"
        );
    }
    assert_eq!(total_rows, 5000);
}
