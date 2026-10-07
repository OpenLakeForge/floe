use floe_core::config::{
    ColumnConfig, EntityConfig, IcebergSinkTargetConfig, IncrementalMode, LineageConfig,
    PolicyConfig, PolicySeverity, SchemaConfig, SinkConfig, SinkTarget, SourceConfig, WriteMode,
};
use floe_core::lineage::OpenLineageObserver;
use floe_core::run::events::{RunEvent, RunObserver};
use serde_json::json;

fn make_config(server_url: &str, max_failures: Option<u32>) -> LineageConfig {
    LineageConfig {
        url: server_url.to_string(),
        endpoint: None,
        api_key: None,
        timeout_secs: Some(2),
        namespace: "test-ns".to_string(),
        dataset_namespace: None,
        producer: None,
        max_failures,
        job_name: None,
    }
}

#[test]
fn custom_endpoint_is_used_for_posts() {
    let mut server = mockito::Server::new();
    let mock = server
        .mock("POST", "/api/v1/openlineage/lineage")
        .with_status(200)
        .expect(1)
        .create();

    let mut config = make_config(&format!("{}/", server.url()), None);
    config.endpoint = Some("/api/v1/openlineage/lineage".to_string());
    let obs = OpenLineageObserver::new(&config, &[], "config.yml").unwrap();

    obs.on_event(run_started_event());
    mock.assert();
}

fn run_started_event() -> RunEvent {
    RunEvent::RunStarted {
        run_id: "test-run-1".to_string(),
        config: "config.yml".to_string(),
        report_base: None,
        ts_ms: 1_000_000,
    }
}

// EntityStarted is used in tests where we need to trigger a POST without the
// RunStarted reset side-effect, matching the realistic per-run event sequence.
fn entity_started_event() -> RunEvent {
    RunEvent::EntityStarted {
        run_id: "test-run-1".to_string(),
        name: "orders".to_string(),
        ts_ms: 1_001_000,
    }
}

// Circuit opens after max_failures consecutive failures and stops hitting the server.
#[test]
fn circuit_opens_after_threshold() {
    let mut server = mockito::Server::new();
    let mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(500)
        // 1 failure call × 3 retry attempts = 3 server hits, then circuit opens.
        .expect(3)
        .create();

    let config = make_config(&server.url(), Some(1));
    let obs = OpenLineageObserver::new(&config, &[], "config.yml").unwrap();

    // RunStarted: resets circuit state, then POSTs once → all 3 retries fail → circuit opens.
    obs.on_event(run_started_event());
    assert!(
        obs.is_circuit_open(),
        "circuit should be open after max_failures"
    );

    // Subsequent events in this run should not hit the server (circuit is open).
    obs.on_event(entity_started_event());
    obs.on_event(entity_started_event());

    mock.assert();
}

// A successful attempt resets the consecutive failure counter.
#[test]
fn success_resets_failure_counter() {
    let mut server = mockito::Server::new();
    // RunStarted + one EntityStarted = 2 successful POSTs.
    let _mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(200)
        .expect(2)
        .create();

    let config = make_config(&server.url(), Some(3));
    let obs = OpenLineageObserver::new(&config, &[], "config.yml").unwrap();

    obs.on_event(run_started_event());
    obs.on_event(entity_started_event());

    assert!(
        !obs.is_circuit_open(),
        "circuit should remain closed on success"
    );
    assert_eq!(
        obs.consecutive_failures(),
        0,
        "failures should be reset on success"
    );
}

// A 4xx response (non-retryable) is counted as one failure without retrying.
#[test]
fn non_retryable_error_counts_without_retry() {
    let mut server = mockito::Server::new();
    // Exactly 1 request expected — no retries for 401.
    let mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(401)
        .expect(1)
        .create();

    let config = make_config(&server.url(), Some(3));
    let obs = OpenLineageObserver::new(&config, &[], "config.yml").unwrap();

    obs.on_event(run_started_event());

    assert_eq!(obs.consecutive_failures(), 1);
    assert!(
        !obs.is_circuit_open(),
        "one 4xx should not open the circuit (threshold is 3)"
    );
    mock.assert();
}

// A 429 (Too Many Requests) is retryable and triggers all retry attempts.
#[test]
fn rate_limit_response_is_retried() {
    let mut server = mockito::Server::new();
    // 3 retry attempts expected for a 429.
    let mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(429)
        .expect(3)
        .create();

    let config = make_config(&server.url(), Some(5));
    let obs = OpenLineageObserver::new(&config, &[], "config.yml").unwrap();

    obs.on_event(run_started_event());

    assert_eq!(obs.consecutive_failures(), 1);
    mock.assert();
}

// After a failure then a success within the same run, the circuit stays closed.
#[test]
fn circuit_stays_closed_after_recovery() {
    let mut server = mockito::Server::new();

    // RunStarted: 500 (all 3 retry attempts fail), failure counter = 1.
    let fail_mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(500)
        .expect(3)
        .create();

    let config = make_config(&server.url(), Some(3));
    let obs = OpenLineageObserver::new(&config, &[], "config.yml").unwrap();

    obs.on_event(run_started_event());
    assert_eq!(obs.consecutive_failures(), 1);
    fail_mock.assert();

    // EntityStarted: 200 → failure counter resets to 0.
    let success_mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(200)
        .expect(1)
        .create();

    obs.on_event(entity_started_event());
    assert_eq!(
        obs.consecutive_failures(),
        0,
        "failure counter resets on success"
    );
    assert!(!obs.is_circuit_open());
    success_mock.assert();
}

fn make_entity(
    name: &str,
    source_path: &str,
    accepted_path: &str,
    rejected_path: Option<&str>,
) -> EntityConfig {
    EntityConfig {
        name: name.to_string(),
        metadata: None,
        domain: None,
        incremental_mode: IncrementalMode::None,
        state: None,
        source: SourceConfig {
            format: "csv".to_string(),
            path: source_path.to_string(),
            storage: None,
            options: None,
            cast_mode: None,
        },
        sink: SinkConfig {
            write_mode: WriteMode::Overwrite,
            accepted: SinkTarget {
                format: "parquet".to_string(),
                path: accepted_path.to_string(),
                storage: None,
                options: None,
                merge: None,
                iceberg: None,
                delta: None,
                duckdb: None,
                partition_by: None,
                partition_spec: None,
                write_mode: WriteMode::Overwrite,
            },
            rejected: rejected_path.map(|p| SinkTarget {
                format: "csv".to_string(),
                path: p.to_string(),
                storage: None,
                options: None,
                merge: None,
                iceberg: None,
                delta: None,
                duckdb: None,
                partition_by: None,
                partition_spec: None,
                write_mode: WriteMode::Overwrite,
            }),
            archive: None,
        },
        policy: PolicyConfig {
            severity: PolicySeverity::Warn,
        },
        schema: SchemaConfig {
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

fn entity_finished_event(name: &str, status: &str) -> RunEvent {
    RunEvent::EntityFinished {
        run_id: "test-run-1".to_string(),
        name: name.to_string(),
        status: status.to_string(),
        files: 2,
        files_skipped: 0,
        rows: 100,
        accepted: 90,
        rejected: 10,
        warnings: 1,
        errors: 0,
        ts_ms: 1_002_000,
    }
}

// COMPLETE entity event uses actual storage paths as dataset namespace/name.
#[test]
fn entity_complete_event_has_source_input_and_accepted_output() {
    let mut server = mockito::Server::new();

    // EntityStarted (START) + EntityFinished (COMPLETE) = 2 posts.
    let _start_mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(200)
        .expect(1)
        .create();

    let _complete_mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::AllOf(vec![
            mockito::Matcher::PartialJson(json!({
                "eventType": "COMPLETE",
                "inputs": [{ "namespace": "file", "name": "/data/in/" }],
                "outputs": [{ "namespace": "file", "name": "/data/out/" }]
            })),
        ]))
        .with_status(200)
        .expect(1)
        .create();

    let entity = make_entity("orders", "/data/in/", "/data/out/", None);
    let config = make_config(&server.url(), None);
    let obs = OpenLineageObserver::new(&config, &[entity], "config.yml").unwrap();

    obs.on_event(entity_started_event());
    obs.on_event(entity_finished_event("orders", "success"));

    _start_mock.assert();
    _complete_mock.assert();
}

// COMPLETE entity event with a rejected sink includes both accepted and rejected in outputs.
#[test]
fn entity_complete_event_includes_rejected_output_when_configured() {
    let mut server = mockito::Server::new();

    let _start_mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(200)
        .expect(1)
        .create();

    let _complete_mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::AllOf(vec![
            mockito::Matcher::PartialJson(json!({
                "eventType": "COMPLETE",
                "inputs": [{ "namespace": "file", "name": "/data/in/" }],
                "outputs": [
                    { "namespace": "file", "name": "/data/out/" },
                    { "namespace": "file", "name": "/data/rejected/" }
                ]
            })),
        ]))
        .with_status(200)
        .expect(1)
        .create();

    let entity = make_entity("orders", "/data/in/", "/data/out/", Some("/data/rejected/"));
    let config = make_config(&server.url(), None);
    let obs = OpenLineageObserver::new(&config, &[entity], "config.yml").unwrap();

    obs.on_event(entity_started_event());
    obs.on_event(entity_finished_event("orders", "success"));

    _start_mock.assert();
    _complete_mock.assert();
}

// Source dataset uses the actual storage path as namespace/name (no symlinks).
#[test]
fn source_dataset_uses_storage_namespace_and_name() {
    let mut server = mockito::Server::new();

    let _start_mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(200)
        .expect(1)
        .create();

    let _complete_mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::PartialJson(json!({
            "eventType": "COMPLETE",
            "inputs": [{
                "namespace": "s3://my-bucket",
                "name": "data/raw/"
            }]
        })))
        .with_status(200)
        .expect(1)
        .create();

    let entity = make_entity(
        "orders",
        "s3://my-bucket/data/raw/",
        "s3://my-bucket/data/out/",
        None,
    );
    let config = make_config(&server.url(), None);
    let obs = OpenLineageObserver::new(&config, &[entity], "config.yml").unwrap();

    obs.on_event(entity_started_event());
    obs.on_event(entity_finished_event("orders", "success"));

    _start_mock.assert();
    _complete_mock.assert();
}

// Accepted output uses the actual storage path as namespace/name (no symlinks).
#[test]
fn accepted_dataset_uses_storage_namespace_and_name() {
    let mut server = mockito::Server::new();

    let _start_mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(200)
        .expect(1)
        .create();

    let _complete_mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::PartialJson(json!({
            "eventType": "COMPLETE",
            "outputs": [{
                "namespace": "s3://my-bucket",
                "name": "data/out/"
            }]
        })))
        .with_status(200)
        .expect(1)
        .create();

    let entity = make_entity(
        "orders",
        "s3://my-bucket/data/raw/",
        "s3://my-bucket/data/out/",
        None,
    );
    let config = make_config(&server.url(), None);
    let obs = OpenLineageObserver::new(&config, &[entity], "config.yml").unwrap();

    obs.on_event(entity_started_event());
    obs.on_event(entity_finished_event("orders", "success"));

    _start_mock.assert();
    _complete_mock.assert();
}

// Schema facet is placed on the accepted output dataset, not on the source input.
#[test]
fn accepted_dataset_has_schema_facet() {
    let mut server = mockito::Server::new();

    let _start_mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(200)
        .expect(1)
        .create();

    let _complete_mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::PartialJson(json!({
            "eventType": "COMPLETE",
            "outputs": [{ "name": "/data/out/", "facets": { "schema": {} } }]
        })))
        .with_status(200)
        .expect(1)
        .create();

    let entity = make_entity("orders", "/data/in/", "/data/out/", None);
    let config = make_config(&server.url(), None);
    let obs = OpenLineageObserver::new(&config, &[entity], "config.yml").unwrap();

    obs.on_event(entity_started_event());
    obs.on_event(entity_finished_event("orders", "success"));

    _start_mock.assert();
    _complete_mock.assert();
}

// Accepted output dataQualityMetrics reflects accepted rows only (invalidCount=0).
#[test]
fn accepted_dq_reflects_accepted_rows_only() {
    let mut server = mockito::Server::new();

    let _start_mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(200)
        .expect(1)
        .create();

    // entity_finished_event has accepted=90, rejected=10, rows=100.
    let _complete_mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::PartialJson(json!({
            "eventType": "COMPLETE",
            "outputs": [{
                "facets": {
                    "dataQualityMetrics": {
                        "rowCount": 90_u64,
                        "validCount": 90_u64,
                        "invalidCount": 0_u64
                    }
                }
            }]
        })))
        .with_status(200)
        .expect(1)
        .create();

    let entity = make_entity("orders", "/data/in/", "/data/out/", None);
    let config = make_config(&server.url(), None);
    let obs = OpenLineageObserver::new(&config, &[entity], "config.yml").unwrap();

    obs.on_event(entity_started_event());
    obs.on_event(entity_finished_event("orders", "success"));

    _start_mock.assert();
    _complete_mock.assert();
}

// Rejected output dataQualityMetrics reflects rejected rows only (validCount=0).
#[test]
fn rejected_dq_reflects_rejected_rows_only() {
    let mut server = mockito::Server::new();

    let _start_mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(200)
        .expect(1)
        .create();

    // entity_finished_event has accepted=90, rejected=10, rows=100.
    let _complete_mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::PartialJson(json!({
            "eventType": "COMPLETE",
            "outputs": [
                {},
                {
                    "namespace": "file",
                    "name": "/data/rejected/",
                    "facets": {
                        "dataQualityMetrics": {
                            "rowCount": 10_u64,
                            "validCount": 0_u64,
                            "invalidCount": 10_u64
                        }
                    }
                }
            ]
        })))
        .with_status(200)
        .expect(1)
        .create();

    let entity = make_entity("orders", "/data/in/", "/data/out/", Some("/data/rejected/"));
    let config = make_config(&server.url(), None);
    let obs = OpenLineageObserver::new(&config, &[entity], "config.yml").unwrap();

    obs.on_event(entity_started_event());
    obs.on_event(entity_finished_event("orders", "success"));

    _start_mock.assert();
    _complete_mock.assert();
}

// floeQualityRun no longer carries redundant accepted/rejected counts (those are in per-dataset DQ).
#[test]
fn floe_quality_run_has_no_accepted_rejected_keys() {
    let mut server = mockito::Server::new();

    // START event
    let _start_mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::PartialJson(
            json!({ "eventType": "START" }),
        ))
        .with_status(200)
        .expect(1)
        .create();

    // Expected COMPLETE structure: floeQualityRun has entity/files/rows but NOT accepted/rejected.
    // Created before _bad_mock so it has lower LIFO priority — only matched if _bad_mock doesn't.
    let _complete_mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::PartialJson(json!({
            "eventType": "COMPLETE",
            "outputs": [{
                "facets": {
                    "floeQualityRun": { "entity": "orders", "rows": 100_u64, "files": 2_u64 }
                }
            }]
        })))
        .with_status(200)
        .expect(1)
        .create();

    // Highest LIFO priority: matches if "accepted" key is erroneously present in floeQualityRun.
    // expect(0) — the test fails if this mock is ever matched.
    let _bad_mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::AllOf(vec![
            mockito::Matcher::PartialJson(json!({ "eventType": "COMPLETE" })),
            mockito::Matcher::PartialJson(json!({
                "outputs": [{ "facets": { "floeQualityRun": { "accepted": 90_u64 } } }]
            })),
        ]))
        .with_status(500)
        .expect(0)
        .create();

    let entity = make_entity("orders", "/data/in/", "/data/out/", None);
    let config = make_config(&server.url(), None);
    let obs = OpenLineageObserver::new(&config, &[entity], "config.yml").unwrap();

    obs.on_event(entity_started_event());
    obs.on_event(entity_finished_event("orders", "success"));

    _start_mock.assert();
    _complete_mock.assert();
    _bad_mock.assert();
}

// S3 URIs split into bucket namespace and slash-free path name.
#[test]
fn split_storage_uri_s3() {
    let mut server = mockito::Server::new();

    let _start_mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(200)
        .expect(1)
        .create();

    let _complete_mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::PartialJson(json!({
            "eventType": "COMPLETE",
            "inputs": [{
                "namespace": "s3://my-bucket",
                "name": "data/raw/"
            }]
        })))
        .with_status(200)
        .expect(1)
        .create();

    let entity = make_entity(
        "orders",
        "s3://my-bucket/data/raw/",
        "s3://my-bucket/data/out/",
        None,
    );
    let config = make_config(&server.url(), None);
    let obs = OpenLineageObserver::new(&config, &[entity], "config.yml").unwrap();

    obs.on_event(entity_started_event());
    obs.on_event(entity_finished_event("orders", "success"));

    _start_mock.assert();
    _complete_mock.assert();
}

// ADLS URIs split at the first path separator after the authority component.
#[test]
fn split_storage_uri_adls() {
    let mut server = mockito::Server::new();

    let _start_mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(200)
        .expect(1)
        .create();

    let _complete_mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::PartialJson(json!({
            "eventType": "COMPLETE",
            "inputs": [{
                "namespace": "abfss://container@acct.dfs.core.windows.net",
                "name": "raw/"
            }]
        })))
        .with_status(200)
        .expect(1)
        .create();

    let entity = make_entity(
        "orders",
        "abfss://container@acct.dfs.core.windows.net/raw/",
        "abfss://container@acct.dfs.core.windows.net/out/",
        None,
    );
    let config = make_config(&server.url(), None);
    let obs = OpenLineageObserver::new(&config, &[entity], "config.yml").unwrap();

    obs.on_event(entity_started_event());
    obs.on_event(entity_finished_event("orders", "success"));

    _start_mock.assert();
    _complete_mock.assert();
}

// abfs:// URIs are preserved as-is in the namespace (not normalised to abfss://).
#[test]
fn split_storage_uri_abfs_preserved() {
    let mut server = mockito::Server::new();

    let _start_mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(200)
        .expect(1)
        .create();

    let _complete_mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::PartialJson(json!({
            "eventType": "COMPLETE",
            "inputs": [{
                "namespace": "abfs://container@acct.dfs.core.windows.net",
                "name": "raw/"
            }]
        })))
        .with_status(200)
        .expect(1)
        .create();

    let entity = make_entity(
        "orders",
        "abfs://container@acct.dfs.core.windows.net/raw/",
        "abfs://container@acct.dfs.core.windows.net/out/",
        None,
    );
    let config = make_config(&server.url(), None);
    let obs = OpenLineageObserver::new(&config, &[entity], "config.yml").unwrap();

    obs.on_event(entity_started_event());
    obs.on_event(entity_finished_event("orders", "success"));

    _start_mock.assert();
    _complete_mock.assert();
}

// RunStarted uses the config file stem as a stable job name, not the run UUID.
#[test]
fn run_started_uses_stable_job_name_derived_from_config_path() {
    let mut server = mockito::Server::new();

    let _mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::PartialJson(json!({
            "eventType": "START",
            "job": { "namespace": "test-ns", "name": "orders" }
        })))
        .with_status(200)
        .expect(1)
        .create();

    let config = make_config(&server.url(), None);
    let obs = OpenLineageObserver::new(&config, &[], "pipelines/orders.yml").unwrap();

    obs.on_event(run_started_event());
    _mock.assert();
}

// lineage.job_name overrides the config-path-derived stable name.
#[test]
fn run_job_name_override_via_config_field() {
    let mut server = mockito::Server::new();

    let _mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::PartialJson(json!({
            "eventType": "START",
            "job": { "namespace": "test-ns", "name": "my-pipeline" }
        })))
        .with_status(200)
        .expect(1)
        .create();

    let mut config = make_config(&server.url(), None);
    config.job_name = Some("my-pipeline".to_string());
    let obs = OpenLineageObserver::new(&config, &[], "orders.yml").unwrap();

    obs.on_event(run_started_event());
    _mock.assert();
}

fn make_column(name: &str, column_type: &str, source: Option<&str>) -> ColumnConfig {
    ColumnConfig {
        name: name.to_string(),
        source: source.map(|s| s.to_string()),
        column_type: column_type.to_string(),
        nullable: None,
        unique: None,
        width: None,
        trim: None,
    }
}

// columnLineage facet maps each output column to the source dataset's storage coordinates.
#[test]
fn accepted_dataset_has_column_lineage_facet() {
    let mut server = mockito::Server::new();

    let _start_mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(200)
        .expect(1)
        .create();

    let _complete_mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::PartialJson(json!({
            "eventType": "COMPLETE",
            "outputs": [{
                "facets": {
                    "columnLineage": {
                        "fields": {
                            "order_id": {
                                "inputFields": [{
                                    "namespace": "file",
                                    "name": "/data/in/",
                                    "field": "order_id"
                                }]
                            }
                        }
                    }
                }
            }]
        })))
        .with_status(200)
        .expect(1)
        .create();

    let mut entity = make_entity("orders", "/data/in/", "/data/out/", None);
    entity.schema.columns = vec![make_column("order_id", "string", None)];
    let config = make_config(&server.url(), None);
    let obs = OpenLineageObserver::new(&config, &[entity], "config.yml").unwrap();

    obs.on_event(entity_started_event());
    obs.on_event(entity_finished_event("orders", "success"));

    _start_mock.assert();
    _complete_mock.assert();
}

// columnLineage facet uses the source field when set instead of the output column name.
#[test]
fn column_lineage_uses_source_field_when_set() {
    let mut server = mockito::Server::new();

    let _start_mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(200)
        .expect(1)
        .create();

    let _complete_mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::PartialJson(json!({
            "eventType": "COMPLETE",
            "outputs": [{
                "facets": {
                    "columnLineage": {
                        "fields": {
                            "amount": {
                                "inputFields": [{
                                    "namespace": "file",
                                    "name": "/data/in/",
                                    "field": "raw_amt"
                                }]
                            }
                        }
                    }
                }
            }]
        })))
        .with_status(200)
        .expect(1)
        .create();

    let mut entity = make_entity("orders", "/data/in/", "/data/out/", None);
    entity.schema.columns = vec![make_column("amount", "float64", Some("raw_amt"))];
    let config = make_config(&server.url(), None);
    let obs = OpenLineageObserver::new(&config, &[entity], "config.yml").unwrap();

    obs.on_event(entity_started_event());
    obs.on_event(entity_finished_event("orders", "success"));

    _start_mock.assert();
    _complete_mock.assert();
}

// Circuit state is reset at the start of each new run (RunStarted) so a
// recovered endpoint is retried in subsequent runs within the same process.
#[test]
fn circuit_resets_on_new_run_started() {
    let mut server = mockito::Server::new();

    // Run 1: all events fail → circuit opens.
    let fail_mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(500)
        .expect(3)
        .create();

    let config = make_config(&server.url(), Some(1));
    let obs = OpenLineageObserver::new(&config, &[], "config.yml").unwrap();

    obs.on_event(run_started_event());
    assert!(obs.is_circuit_open());
    fail_mock.assert();

    // Run 2: RunStarted resets the circuit; endpoint now returns 200.
    let success_mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(200)
        .expect(1)
        .create();

    obs.on_event(RunEvent::RunStarted {
        run_id: "test-run-2".to_string(),
        config: "config.yml".to_string(),
        report_base: None,
        ts_ms: 2_000_000,
    });
    assert!(
        !obs.is_circuit_open(),
        "circuit should reset for the new run"
    );
    assert_eq!(obs.consecutive_failures(), 0);
    success_mock.assert();
}

// RunStarted emits a run_id that is a valid UUID v4 (not a timestamp string).
#[test]
fn run_started_emits_valid_uuid_run_id() {
    let mut server = mockito::Server::new();

    let _mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::Regex(
            r#""runId"\s*:\s*"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}""#
                .to_string(),
        ))
        .with_status(200)
        .expect(1)
        .create();

    let config = make_config(&server.url(), None);
    let obs = OpenLineageObserver::new(&config, &[], "config.yml").unwrap();

    obs.on_event(run_started_event());
    _mock.assert();
}

// EntityStarted emits a run_id that is a valid UUID v4 (not a derived string).
#[test]
fn entity_started_emits_valid_uuid_run_id() {
    let mut server = mockito::Server::new();

    let _mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::Regex(
            r#""runId"\s*:\s*"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}""#
                .to_string(),
        ))
        .with_status(200)
        .expect(1)
        .create();

    let config = make_config(&server.url(), None);
    let obs = OpenLineageObserver::new(&config, &[], "config.yml").unwrap();

    obs.on_event(entity_started_event());
    _mock.assert();
}

// Entity job name is the entity name alone, not namespace.entity_name.
#[test]
fn entity_job_name_is_entity_name_without_namespace_prefix() {
    let mut server = mockito::Server::new();

    let _start_mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(200)
        .expect(1)
        .create();

    let _complete_mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::PartialJson(json!({
            "eventType": "COMPLETE",
            "job": { "namespace": "test-ns", "name": "orders" }
        })))
        .with_status(200)
        .expect(1)
        .create();

    let entity = make_entity("orders", "/data/in/", "/data/out/", None);
    let config = make_config(&server.url(), None);
    let obs = OpenLineageObserver::new(&config, &[entity], "config.yml").unwrap();

    obs.on_event(entity_started_event());
    obs.on_event(entity_finished_event("orders", "success"));

    _start_mock.assert();
    _complete_mock.assert();
}

// Iceberg sink: accepted output uses OL config namespace + iceberg namespace.table as name.
#[test]
fn iceberg_accepted_uses_catalog_namespace_and_table_name() {
    let mut server = mockito::Server::new();

    let _start_mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(200)
        .expect(1)
        .create();

    let _complete_mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::PartialJson(json!({
            "eventType": "COMPLETE",
            "inputs": [{ "namespace": "s3://iceberg-data", "name": "bronze/sales/customers" }],
            "job": { "namespace": "test-ns" },
            "outputs": [{ "namespace": "iceberg.prod", "name": "sales_dev.customers" }]
        })))
        .with_status(200)
        .expect(1)
        .create();

    let mut entity = make_entity(
        "customers",
        "s3://iceberg-data/bronze/sales/customers",
        "s3://warehouse/silver/customers",
        None,
    );
    entity.sink.accepted.iceberg = Some(IcebergSinkTargetConfig {
        catalog: None,
        namespace: Some("sales_dev".to_string()),
        table: Some("customers".to_string()),
        location: None,
    });

    let mut config = make_config(&server.url(), None);
    config.dataset_namespace = Some("iceberg.prod".to_string());
    let obs = OpenLineageObserver::new(&config, &[entity], "config.yml").unwrap();

    obs.on_event(RunEvent::EntityStarted {
        run_id: "test-run-1".to_string(),
        name: "customers".to_string(),
        ts_ms: 1_001_000,
    });
    obs.on_event(RunEvent::EntityFinished {
        run_id: "test-run-1".to_string(),
        name: "customers".to_string(),
        status: "success".to_string(),
        files: 1,
        files_skipped: 0,
        rows: 50,
        accepted: 50,
        rejected: 0,
        warnings: 0,
        errors: 0,
        ts_ms: 1_002_000,
    });

    _start_mock.assert();
    _complete_mock.assert();
}

#[test]
fn iceberg_accepted_dataset_namespace_defaults_to_job_namespace() {
    let mut server = mockito::Server::new();
    let _start_mock = server
        .mock("POST", "/api/v1/lineage")
        .with_status(200)
        .expect(1)
        .create();
    let _complete_mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::PartialJson(json!({
            "eventType": "COMPLETE",
            "job": { "namespace": "test-ns" },
            "outputs": [{ "namespace": "test-ns", "name": "sales_dev.customers" }]
        })))
        .with_status(200)
        .expect(1)
        .create();

    let mut entity = make_entity(
        "customers",
        "s3://iceberg-data/bronze/sales/customers",
        "s3://warehouse/silver/customers",
        None,
    );
    entity.sink.accepted.iceberg = Some(IcebergSinkTargetConfig {
        catalog: None,
        namespace: Some("sales_dev".to_string()),
        table: Some("customers".to_string()),
        location: None,
    });

    let config = make_config(&server.url(), None);
    let obs = OpenLineageObserver::new(&config, &[entity], "config.yml").unwrap();
    obs.on_event(entity_started_event());
    obs.on_event(entity_finished_event("customers", "success"));
    _start_mock.assert();
    _complete_mock.assert();
}

// Sole owner of the process-wide OPENLINEAGE_API_KEY variable. The crate has no
// serial-test harness, so all three auth paths are asserted inside one test that
// controls the variable's lifecycle; no other test reads it, and every other lineage
// test either sets an explicit api_key (which wins over the env regardless) or does not
// match on request headers, so this cannot race them.
#[test]
fn api_key_resolution_over_the_wire() {
    std::env::remove_var("OPENLINEAGE_API_KEY");

    // No config api_key and no env var => request carries no Authorization header.
    {
        let mut server = mockito::Server::new();
        let mock = server
            .mock("POST", "/api/v1/lineage")
            .match_header("authorization", mockito::Matcher::Missing)
            .with_status(200)
            .expect(1)
            .create();
        let config = make_config(&server.url(), None);
        let obs = OpenLineageObserver::new(&config, &[], "config.yml").unwrap();
        obs.on_event(run_started_event());
        mock.assert();
    }

    // Manifest replay with api_key omitted + env var set => Bearer token from the env.
    {
        let mut server = mockito::Server::new();
        let mock = server
            .mock("POST", "/api/v1/lineage")
            .match_header("authorization", "Bearer env-replay-token")
            .with_status(200)
            .expect(1)
            .create();
        let manifest = format!(
            r#"{{"spec_version":"0.3","report_base_uri":"file:///tmp","entities":[],"lineage":{{"url":"{}","namespace":"test-ns"}}}}"#,
            server.url()
        );
        std::env::set_var("OPENLINEAGE_API_KEY", "env-replay-token");
        let (config, _) =
            floe_core::config_from_manifest_json(&manifest).expect("reconstruct manifest config");
        let lineage = config.lineage.expect("manifest lineage block");
        let obs = OpenLineageObserver::new(&lineage, &[], "").unwrap();
        obs.on_event(run_started_event());
        mock.assert();
    }

    // Explicit config api_key wins even when the env var is present (backward compatible).
    {
        let mut server = mockito::Server::new();
        let mock = server
            .mock("POST", "/api/v1/lineage")
            .match_header("authorization", "Bearer cfg-token")
            .with_status(200)
            .expect(1)
            .create();
        let mut config = make_config(&server.url(), None);
        config.api_key = Some("cfg-token".to_string());
        let obs = OpenLineageObserver::new(&config, &[], "config.yml").unwrap();
        obs.on_event(run_started_event());
        mock.assert();
    }

    std::env::remove_var("OPENLINEAGE_API_KEY");
}

// ---- Manifest-replay dataset lineage (issue #455) ----
// build_observer_from_manifest_json must feed the manifest's entities and their
// resolved cloud URIs to the observer so replay emits non-empty inputs/outputs.

fn replay_entity(name: &str, source_uri: &str, accepted_uri: &str) -> serde_json::Value {
    json!({
        "name": name,
        "source": { "format": "csv", "storage": "bronze", "uri": source_uri, "path": source_uri },
        "sinks": {
            "accepted": {
                "format": "parquet",
                "storage": "warehouse",
                "uri": accepted_uri,
                "path": accepted_uri
            }
        },
        "schema": { "columns": [], "primary_key": [], "unique_keys": [] }
    })
}

fn replay_manifest(server_url: &str, entities: serde_json::Value) -> String {
    json!({
        "spec_version": "0.3",
        "report_base_uri": "file:///tmp",
        "entities": entities,
        "lineage": { "url": server_url, "namespace": "test-ns" }
    })
    .to_string()
}

fn entity_started(name: &str) -> RunEvent {
    RunEvent::EntityStarted {
        run_id: "test-run-1".to_string(),
        name: name.to_string(),
        ts_ms: 1_001_000,
    }
}

// Replaying a manifest with an entity emits the entity's source input and
// accepted output — the regression from issue #455. With entities dropped
// (the previous `&[]`), inputs/outputs would be empty and the subset match on
// inputs[0]/outputs[0] would fail.
#[test]
fn manifest_replay_passes_entities_to_observer() {
    let mut server = mockito::Server::new();

    let start_mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::PartialJson(
            json!({ "eventType": "START" }),
        ))
        .with_status(200)
        .expect(1)
        .create();

    let complete_mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::PartialJson(json!({
            "eventType": "COMPLETE",
            "inputs": [{
                "namespace": "s3://lakehouse-bronze",
                "name": "sales/customer_health/support_tickets"
            }],
            "outputs": [{
                "namespace": "s3://lakehouse-warehouse",
                "name": "silver/support_tickets"
            }]
        })))
        .with_status(200)
        .expect(1)
        .create();

    let manifest = replay_manifest(
        &server.url(),
        json!([replay_entity(
            "support_tickets",
            "s3://lakehouse-bronze/sales/customer_health/support_tickets",
            "s3://lakehouse-warehouse/silver/support_tickets",
        )]),
    );

    let obs = floe_core::lineage::build_observer_from_manifest_json(&manifest, "")
        .expect("observer builds from manifest")
        .expect("manifest has a lineage block => Some observer");

    obs.on_event(entity_started("support_tickets"));
    obs.on_event(entity_finished_event("support_tickets", "success"));

    start_mock.assert();
    complete_mock.assert();
}

// Default manifest path mode records raw config paths in `path` and the resolved
// cloud identity in `uri`. Lineage must report the cloud dataset from `uri`, not
// a `file` namespace derived from the raw path (PR #456 review).
#[test]
fn manifest_replay_default_path_mode_uses_resolved_cloud_uri() {
    let mut server = mockito::Server::new();

    let start_mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::PartialJson(
            json!({ "eventType": "START" }),
        ))
        .with_status(200)
        .expect(1)
        .create();

    let complete_mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::PartialJson(json!({
            "eventType": "COMPLETE",
            "inputs": [{ "namespace": "s3://lakehouse-bronze", "name": "sales/orders" }],
            "outputs": [{ "namespace": "s3://lakehouse-warehouse", "name": "silver/orders" }]
        })))
        .with_status(200)
        .expect(1)
        .create();

    // path = raw config paths (default mode); uri = resolved cloud identities.
    let entity = json!({
        "name": "orders",
        "source": {
            "format": "csv",
            "storage": "bronze",
            "uri": "s3://lakehouse-bronze/sales/orders",
            "path": "sales/orders"
        },
        "sinks": {
            "accepted": {
                "format": "parquet",
                "storage": "warehouse",
                "uri": "s3://lakehouse-warehouse/silver/orders",
                "path": "silver/orders"
            }
        },
        "schema": { "columns": [], "primary_key": [], "unique_keys": [] }
    });
    let manifest = replay_manifest(&server.url(), json!([entity]));

    let obs = floe_core::lineage::build_observer_from_manifest_json(&manifest, "")
        .expect("observer builds from manifest")
        .expect("manifest has a lineage block => Some observer");

    obs.on_event(entity_started("orders"));
    obs.on_event(entity_finished_event("orders", "success"));

    start_mock.assert();
    complete_mock.assert();
}

// Firing events for only the selected entity (as `--entities orders` does)
// still carries that entity's dataset context, because the observer holds all
// manifest entities keyed by name.
#[test]
fn manifest_replay_entity_selection_keeps_dataset_context() {
    let mut server = mockito::Server::new();

    let start_mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::PartialJson(
            json!({ "eventType": "START" }),
        ))
        .with_status(200)
        .expect(1)
        .create();

    let complete_mock = server
        .mock("POST", "/api/v1/lineage")
        .match_body(mockito::Matcher::PartialJson(json!({
            "eventType": "COMPLETE",
            "inputs": [{ "namespace": "s3://lakehouse-bronze", "name": "sales/orders" }],
            "outputs": [{ "namespace": "s3://lakehouse-warehouse", "name": "silver/orders" }]
        })))
        .with_status(200)
        .expect(1)
        .create();

    let manifest = replay_manifest(
        &server.url(),
        json!([
            replay_entity(
                "orders",
                "s3://lakehouse-bronze/sales/orders",
                "s3://lakehouse-warehouse/silver/orders",
            ),
            replay_entity(
                "customers",
                "s3://lakehouse-bronze/sales/customers",
                "s3://lakehouse-warehouse/silver/customers",
            )
        ]),
    );

    let obs = floe_core::lineage::build_observer_from_manifest_json(&manifest, "")
        .expect("observer builds from manifest")
        .expect("manifest has a lineage block => Some observer");

    // Only "orders" runs; "customers" is not selected.
    obs.on_event(entity_started("orders"));
    obs.on_event(entity_finished_event("orders", "success"));

    start_mock.assert();
    complete_mock.assert();
}

// A manifest without a lineage block yields no observer (lineage disabled),
// rather than erroring.
#[test]
fn manifest_replay_without_lineage_block_yields_no_observer() {
    let manifest = json!({
        "spec_version": "0.3",
        "report_base_uri": "file:///tmp",
        "entities": []
    })
    .to_string();

    let obs = floe_core::lineage::build_observer_from_manifest_json(&manifest, "")
        .expect("no error when lineage block is absent");

    assert!(
        obs.is_none(),
        "manifest without lineage block => no observer"
    );
}
