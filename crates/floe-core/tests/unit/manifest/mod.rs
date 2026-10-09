pub mod runtime;

use floe_core::{
    build_common_manifest_json, config_from_manifest_json, load_config, parse_profile_from_str,
    resolve_config_location, ManifestOptions, PathMode, RuntimeEnv,
};
// `validate_config_for_tests` is only exercised by the duckdb-gated MotherDuck
// round-trip assertion below; importing it unconditionally would warn (and fail the
// lean `-D warnings` clippy job) on builds without the `duckdb` feature.
#[cfg(feature = "duckdb")]
use floe_core::validate_config_for_tests;
use serde_json::Value;
use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn manifest_uses_local_uri_for_local_config() {
    // Pass a canonical path so the expected value is independent of the test's cwd.
    let config_path = std::fs::canonicalize(repo_root().join("example/config.yml"))
        .expect("canonicalize config path");
    let expected_config_uri = format!("local://{}", config_path.display());

    let config_location = resolve_config_location(
        config_path
            .to_str()
            .expect("example/config.yml path should be valid UTF-8"),
    )
    .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");

    let payload = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        None,
        &ManifestOptions::default(),
    )
    .expect("build common manifest");
    let value: Value = serde_json::from_str(&payload).expect("manifest is valid json");

    assert_eq!(value["schema"], "floe.manifest.v1");
    assert_eq!(value["config_uri"], expected_config_uri);
}

#[test]
fn manifest_omits_domain_incoming_dir_when_unset() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let config_path = temp_dir.path().join("config.yml");
    std::fs::write(
        &config_path,
        "version: \"0.3\"\ndomains:\n  - name: sales\nentities:\n  - name: orders\n    domain: sales\n    source:\n      format: csv\n      path: ./in/orders.csv\n    sink:\n      accepted:\n        format: parquet\n        path: ./out/orders\n    policy:\n      severity: warn\n    schema:\n      columns:\n        - name: id\n          type: string\n",
    )
    .expect("write config");

    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let payload = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        None,
        &ManifestOptions::default(),
    )
    .expect("build manifest");
    let value: Value = serde_json::from_str(&payload).expect("manifest is valid json");

    assert_eq!(value["domains"], serde_json::json!([{"name": "sales"}]));
}

#[test]
fn manifest_cli_config_uri_is_host_absolute_canonical() {
    // Under `cli` runtime every local path is recorded host-absolute and canonicalized: a
    // lexical `..` bounce in the typed path is collapsed, so config_uri is a clean absolute
    // URI that resolves on the generating host (the definitive "all paths absolute" rule).
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let cfg_dir = temp_dir.path().join("cfg");
    std::fs::create_dir_all(&cfg_dir).expect("cfg dir");
    let config_path = cfg_dir.join("config.yml");
    std::fs::write(
        &config_path,
        "version: \"0.1\"\nentities:\n  - name: orders\n    source:\n      format: csv\n      path: ./in/orders\n    sink:\n      write_mode: overwrite\n      accepted:\n        format: parquet\n        path: ./out/orders\n    policy:\n      severity: warn\n    schema:\n      columns:\n        - name: id\n          type: string\n",
    )
    .expect("write config");

    // <cfg_dir>/../cfg/config.yml — cli canonicalization resolves this to
    // <cfg_dir>/config.yml, dropping the "/../".
    let bouncy = format!("{}/../cfg/config.yml", cfg_dir.display());
    let loc = resolve_config_location(&bouncy).expect("resolve config location");
    let config = load_config(&loc.path).expect("load config");
    let payload = build_common_manifest_json(&loc, &config, &[], None, &ManifestOptions::default())
        .expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    let uri = value["config_uri"].as_str().expect("config_uri string");
    assert!(
        uri.starts_with("local:///"),
        "cli config_uri must be host-absolute: {uri}"
    );
    assert!(
        !uri.contains("/../"),
        "cli config_uri must be canonicalized (no `..`): {uri}"
    );
    assert!(value["manifest_id"].as_str().unwrap().starts_with("mfv1-"));
    // Default (no runtime override) auto-detects; in the test env that is `cli`.
    assert_eq!(value["runtime_env"], "cli");
    assert!(
        value.get("work_root").is_none(),
        "cli must not record work_root"
    );
}

#[test]
fn manifest_image_runtime_records_runtime_env_and_work_root() {
    // `image` runtime records runtime_env/work_root so a remote runner knows how to resolve
    // local paths. An absolute config path is already portable, so it is preserved unchanged
    // (the relative -> /work re-rooting is covered by the runtime module's unit tests and the
    // CLI `--runtime image` reproducibility test).
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let cfg_dir = temp_dir.path().join("cfg");
    std::fs::create_dir_all(&cfg_dir).expect("cfg dir");
    let config_path = cfg_dir.join("config.yml");
    std::fs::write(
        &config_path,
        "version: \"0.1\"\nentities:\n  - name: orders\n    source:\n      format: csv\n      path: ./in/orders\n    sink:\n      write_mode: overwrite\n      accepted:\n        format: parquet\n        path: ./out/orders\n    policy:\n      severity: warn\n    schema:\n      columns:\n        - name: id\n          type: string\n",
    )
    .expect("write config");

    let loc = resolve_config_location(config_path.to_str().expect("utf8")).expect("resolve");
    let config = load_config(&loc.path).expect("load config");
    let opts = ManifestOptions {
        runtime_env: RuntimeEnv::Image,
        ..ManifestOptions::default()
    };
    let payload = build_common_manifest_json(&loc, &config, &[], None, &opts).expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    assert_eq!(value["runtime_env"], "image");
    assert_eq!(value["work_root"], "/work");
    let uri = value["config_uri"].as_str().expect("config_uri string");
    assert!(uri.starts_with("local://"), "config_uri: {uri}");
}

#[test]
fn manifest_id_is_stable_for_same_config() {
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(
        config_path
            .to_str()
            .expect("example/config.yml path should be valid UTF-8"),
    )
    .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");

    let first = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        None,
        &ManifestOptions::default(),
    )
    .expect("manifest");
    let second = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        None,
        &ManifestOptions::default(),
    )
    .expect("manifest");

    let first_value: Value = serde_json::from_str(&first).expect("valid json");
    let second_value: Value = serde_json::from_str(&second).expect("valid json");

    let first_id = first_value["manifest_id"]
        .as_str()
        .expect("manifest_id should be string");
    let second_id = second_value["manifest_id"]
        .as_str()
        .expect("manifest_id should be string");

    assert!(first_id.starts_with("mfv1-"));
    assert_eq!(first_id.len(), 21);
    assert_eq!(first_id, second_id);
}

#[test]
fn manifest_local_uris_are_normalized() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let cfg_dir = root.join("cfg");
    std::fs::create_dir_all(&cfg_dir).expect("cfg dir");
    let config_path = cfg_dir.join("manifest.yml");

    let yaml = r#"version: "0.1"
report:
  path: "./report/../report_out/./base"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "./data/../input/./customer.csv"
    sink:
      accepted:
        format: "parquet"
        path: "./out/../out_norm/./accepted/customer"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#;
    std::fs::write(&config_path, yaml).expect("write config");

    let config_location = resolve_config_location(config_path.to_str().expect("config path utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let payload = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        None,
        &ManifestOptions::default(),
    )
    .expect("build common manifest");
    let value: Value = serde_json::from_str(&payload).expect("manifest json");

    let cfg_base = std::fs::canonicalize(&cfg_dir).expect("canonicalize cfg dir");
    assert_eq!(
        value["report_base_uri"],
        format!("local://{}/report_out/base", cfg_base.display())
    );
    let entity = &value["entities"][0];
    assert_eq!(
        entity["source"]["uri"],
        format!("local://{}/input/customer.csv", cfg_base.display())
    );
    assert_eq!(
        entity["accepted_sink_uri"],
        format!("local://{}/out_norm/accepted/customer", cfg_base.display())
    );
}

#[test]
fn manifest_without_profile_has_local_runner() {
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let payload = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        None,
        &ManifestOptions::default(),
    )
    .expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    assert_eq!(value["runners"]["default"], "local");
    assert_eq!(
        value["runners"]["definitions"]["local"]["type"],
        "local_process"
    );
}

#[test]
fn manifest_with_local_profile_has_local_runner() {
    let profile_yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: dev
execution:
  runner:
    type: local
"#;
    let profile = parse_profile_from_str(profile_yaml).expect("parse profile");
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let payload = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        Some(&profile),
        &ManifestOptions::default(),
    )
    .expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    assert_eq!(value["runners"]["default"], "local");
    assert_eq!(
        value["runners"]["definitions"]["local"]["type"],
        "local_process"
    );
}

#[test]
fn manifest_with_kubernetes_profile_has_kubernetes_runner() {
    let profile_yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: prod-k8s
execution:
  runner:
    type: kubernetes_job
"#;
    let profile = parse_profile_from_str(profile_yaml).expect("parse profile");
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let payload = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        Some(&profile),
        &ManifestOptions::default(),
    )
    .expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    assert_eq!(value["runners"]["default"], "default");
    assert_eq!(
        value["runners"]["definitions"]["default"]["type"],
        "kubernetes_job"
    );
}

#[test]
fn manifest_with_kubernetes_profile_serializes_k8_runner_fields() {
    let profile_yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: prod-k8s
execution:
  runner:
    type: kubernetes_job
    image: my-registry/floe:latest
    namespace: floe-prod
    service_account: floe-sa
    command: floe
    args:
      - run
      - -c
      - /config/config.yml
    timeout_seconds: 3600
    ttl_seconds_after_finished: 600
    poll_interval_seconds: 15
    env:
      FLOE_ENV: prod
    resources:
      cpu: "500m"
      memory_mb: 512
    secrets:
      - name: DB_PASSWORD
        secret_name: floe-db-secret
        key: password
"#;
    let profile = parse_profile_from_str(profile_yaml).expect("parse profile");
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let payload = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        Some(&profile),
        &ManifestOptions::default(),
    )
    .expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    let runner = &value["runners"]["definitions"]["default"];
    assert_eq!(runner["type"], "kubernetes_job");
    assert_eq!(runner["image"], "my-registry/floe:latest");
    assert_eq!(runner["namespace"], "floe-prod");
    assert_eq!(runner["service_account"], "floe-sa");
    assert_eq!(runner["command"], "floe");
    assert_eq!(
        runner["args"],
        serde_json::json!(["run", "-c", "/config/config.yml"])
    );
    assert_eq!(runner["timeout_seconds"], 3600);
    assert_eq!(runner["ttl_seconds_after_finished"], 600);
    assert_eq!(runner["poll_interval_seconds"], 15);
    assert_eq!(runner["env"]["FLOE_ENV"], "prod");
    assert_eq!(runner["resources"]["cpu"], "500m");
    assert_eq!(runner["resources"]["memory_mb"], 512);
    assert_eq!(
        runner["secrets"],
        serde_json::json!([{
            "name": "DB_PASSWORD",
            "secret_name": "floe-db-secret",
            "key": "password"
        }])
    );
}

#[test]
fn manifest_with_databricks_profile_serializes_runner_fields() {
    let profile_yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: prod-dbx
execution:
  runner:
    type: databricks_job
    workspace_url: https://adb-1234.5.azuredatabricks.net
    existing_cluster_id: 1111-222222-abc123
    config_uri: dbfs:/floe/configs/prod.yml
    python_file_uri: dbfs:/floe/bin/floe_entry.py
    command: floe
    args:
      - run
      - -c
      - dbfs:/floe/configs/prod.yml
    poll_interval_seconds: 12
    timeout_seconds: 1800
    auth:
      service_principal_oauth_ref: env://DATABRICKS_TOKEN
    env_parameters:
      FLOE_ENV: prod
"#;
    let profile = parse_profile_from_str(profile_yaml).expect("parse profile");
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let payload = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        Some(&profile),
        &ManifestOptions::default(),
    )
    .expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    let runner = &value["runners"]["definitions"]["default"];
    assert_eq!(value["runners"]["default"], "default");
    assert_eq!(runner["type"], "databricks_job");
    assert_eq!(
        runner["workspace_url"],
        "https://adb-1234.5.azuredatabricks.net"
    );
    assert_eq!(runner["existing_cluster_id"], "1111-222222-abc123");
    assert_eq!(runner["config_uri"], "dbfs:/floe/configs/prod.yml");
    assert_eq!(runner["python_file_uri"], "dbfs:/floe/bin/floe_entry.py");
    assert_eq!(runner["job_name"], "floe-{domain}-{env}");
    assert_eq!(
        runner["auth"]["service_principal_oauth_ref"],
        "env://DATABRICKS_TOKEN"
    );
}

#[test]
fn manifest_deterministic_mode_produces_stable_output() {
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let opts = ManifestOptions {
        deterministic: true,
        ..ManifestOptions::default()
    };

    let first =
        build_common_manifest_json(&config_location, &config, &[], None, &opts).expect("manifest");
    let second =
        build_common_manifest_json(&config_location, &config, &[], None, &opts).expect("manifest");

    assert_eq!(
        first, second,
        "deterministic manifests must be byte-identical"
    );
}

#[test]
fn manifest_deterministic_mode_sets_timestamp_to_zero() {
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let opts = ManifestOptions {
        deterministic: true,
        ..ManifestOptions::default()
    };

    let payload =
        build_common_manifest_json(&config_location, &config, &[], None, &opts).expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    assert_eq!(value["generated_at_ts_ms"], 0);
}

#[test]
fn manifest_config_checksum_is_populated() {
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");

    let payload = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        None,
        &ManifestOptions::default(),
    )
    .expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    let checksum = value["config_checksum"]
        .as_str()
        .expect("config_checksum should be present");
    assert!(
        checksum.starts_with("sha256:"),
        "config_checksum should start with 'sha256:'"
    );
}

#[test]
fn manifest_revision_is_present_and_stable() {
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");

    let first = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        None,
        &ManifestOptions::default(),
    )
    .expect("manifest");
    let second = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        None,
        &ManifestOptions::default(),
    )
    .expect("manifest");

    let first_value: Value = serde_json::from_str(&first).expect("valid json");
    let second_value: Value = serde_json::from_str(&second).expect("valid json");

    let first_rev = first_value["manifest_revision"]
        .as_str()
        .expect("manifest_revision should be present");
    let second_rev = second_value["manifest_revision"]
        .as_str()
        .expect("manifest_revision should be present");

    assert!(
        first_rev.starts_with("sha256:"),
        "revision should be a sha256 hash"
    );
    assert_eq!(
        first_rev, second_rev,
        "manifest_revision must be stable across calls"
    );
}

#[test]
fn manifest_name_is_stored_when_provided() {
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let opts = ManifestOptions {
        manifest_name: Some("sales.prod".to_string()),
        ..ManifestOptions::default()
    };

    let payload =
        build_common_manifest_json(&config_location, &config, &[], None, &opts).expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    assert_eq!(value["manifest_name"], "sales.prod");
}

#[test]
fn manifest_manifest_uri_renders_placeholder() {
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let opts = ManifestOptions {
        manifest_uri: Some("s3://my-bucket/manifests/example.json".to_string()),
        ..ManifestOptions::default()
    };

    let payload =
        build_common_manifest_json(&config_location, &config, &[], None, &opts).expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    let base_args = value["execution"]["base_args"]
        .as_array()
        .expect("base_args should be array");
    let args_strs: Vec<&str> = base_args
        .iter()
        .map(|v| v.as_str().expect("string"))
        .collect();

    assert!(
        args_strs.contains(&"s3://my-bucket/manifests/example.json"),
        "manifest URI should be rendered into base_args"
    );
    assert!(
        !args_strs.contains(&"{manifest_uri}"),
        "placeholder should be replaced"
    );
}

#[test]
fn manifest_default_domain_applied_when_entity_has_none() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let cfg_dir = root.join("cfg");
    std::fs::create_dir_all(&cfg_dir).expect("cfg dir");
    let config_path = cfg_dir.join("config.yml");

    let yaml = r#"version: "0.1"
entities:
  - name: "orders"
    source:
      format: "csv"
      path: "./in/orders.csv"
    sink:
      accepted:
        format: "parquet"
        path: "./out/accepted/orders"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#;
    std::fs::write(&config_path, yaml).expect("write config");

    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let opts = ManifestOptions {
        default_domain: Some("sales".to_string()),
        ..ManifestOptions::default()
    };

    let payload =
        build_common_manifest_json(&config_location, &config, &[], None, &opts).expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    let entity = &value["entities"][0];
    assert_eq!(entity["domain"], "sales");
    assert_eq!(entity["group_name"], "sales");
    assert_eq!(
        entity["asset_key"],
        serde_json::json!(["sales", "orders"]),
        "asset_key should be prefixed with default domain"
    );
}

#[test]
fn manifest_orchestration_absent_when_no_profile() {
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");

    let payload = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        None,
        &ManifestOptions::default(),
    )
    .expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    assert!(
        value["execution"]["orchestration"].is_null(),
        "orchestration should be absent when no profile is provided"
    );
}

#[test]
fn manifest_orchestration_absent_when_profile_has_none() {
    let profile_yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: prod
execution:
  runner:
    type: local
"#;
    let profile = parse_profile_from_str(profile_yaml).expect("parse profile");
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");

    let payload = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        Some(&profile),
        &ManifestOptions::default(),
    )
    .expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    assert!(
        value["execution"]["orchestration"].is_null(),
        "orchestration should be absent when profile has no orchestration"
    );
}

#[test]
fn manifest_orchestration_strategy_sequential_from_profile() {
    let profile_yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: prod
execution:
  runner:
    type: kubernetes_job
  orchestration:
    strategy: sequential
"#;
    let profile = parse_profile_from_str(profile_yaml).expect("parse profile");
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");

    let payload = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        Some(&profile),
        &ManifestOptions::default(),
    )
    .expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    assert_eq!(
        value["execution"]["orchestration"]["strategy"],
        "sequential"
    );
    assert!(value["execution"]["orchestration"]["max_concurrent_entities"].is_null());
}

#[test]
fn manifest_orchestration_max_concurrent_from_profile() {
    let profile_yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: prod
execution:
  runner:
    type: kubernetes_job
  orchestration:
    max_concurrent_entities: 2
"#;
    let profile = parse_profile_from_str(profile_yaml).expect("parse profile");
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");

    let payload = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        Some(&profile),
        &ManifestOptions::default(),
    )
    .expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    assert_eq!(
        value["execution"]["orchestration"]["max_concurrent_entities"],
        2
    );
}

#[test]
fn manifest_orchestration_both_fields_from_profile() {
    let profile_yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: prod
execution:
  runner:
    type: local
  orchestration:
    max_concurrent_entities: 1
    strategy: sequential
"#;
    let profile = parse_profile_from_str(profile_yaml).expect("parse profile");
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");

    let payload = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        Some(&profile),
        &ManifestOptions::default(),
    )
    .expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    let orch = &value["execution"]["orchestration"];
    assert_eq!(orch["max_concurrent_entities"], 1);
    assert_eq!(orch["strategy"], "sequential");
}

#[test]
fn manifest_round_trip_preserves_duckdb_sink_block() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let cfg_dir = root.join("cfg");
    std::fs::create_dir_all(&cfg_dir).expect("cfg dir");
    let config_path = cfg_dir.join("config.yml");

    let yaml = r#"version: "0.1"
entities:
  - name: "customers"
    source:
      format: "csv"
      path: "./in/customers.csv"
    sink:
      write_mode: "merge_scd1"
      accepted:
        format: "duckdb"
        path: "./out/warehouse.duckdb"
        duckdb:
          table: "customers"
          schema: "main"
    policy:
      severity: "warn"
    schema:
      primary_key: ["id"]
      columns:
        - name: "id"
          type: "string"
"#;
    std::fs::write(&config_path, yaml).expect("write config");

    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");

    let payload = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        None,
        &ManifestOptions::default(),
    )
    .expect("build manifest");

    // The duckdb block must be serialized into the manifest...
    let value: Value = serde_json::from_str(&payload).expect("valid json");
    let accepted = &value["entities"][0]["sinks"]["accepted"];
    assert_eq!(accepted["format"], "duckdb");
    assert_eq!(accepted["duckdb"]["table"], "customers");
    assert_eq!(accepted["duckdb"]["schema"], "main");

    // ...and survive reconstruction so manifest replay can address the table.
    let (reconstructed, _base) =
        config_from_manifest_json(&payload).expect("reconstruct config from manifest");
    let duckdb = reconstructed.entities[0]
        .sink
        .accepted
        .duckdb
        .as_ref()
        .expect("duckdb block should survive manifest round-trip");
    assert_eq!(duckdb.table, "customers");
    assert_eq!(duckdb.schema.as_deref(), Some("main"));
}

#[test]
fn manifest_motherduck_sink_records_connection_uri() {
    // A MotherDuck DuckDB sink has no filesystem path; the manifest must record the
    // `md:<database>` connection string as the accepted sink URI/path so orchestrators
    // reading the manifest see the real target instead of "" or the config directory.
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let cfg_dir = root.join("cfg");
    std::fs::create_dir_all(&cfg_dir).expect("cfg dir");
    let config_path = cfg_dir.join("config.yml");

    let yaml = r#"version: "0.1"
entities:
  - name: "customers"
    source:
      format: "csv"
      path: "./in/customers.csv"
    sink:
      accepted:
        format: "duckdb"
        duckdb:
          connection: "md:analytics"
          table: "customers"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#;
    std::fs::write(&config_path, yaml).expect("write config");

    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");

    let payload = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        None,
        &ManifestOptions::default(),
    )
    .expect("build manifest");

    let value: Value = serde_json::from_str(&payload).expect("valid json");
    let entity = &value["entities"][0];
    let accepted = &entity["sinks"]["accepted"];
    assert_eq!(accepted["uri"], "md:analytics");
    assert_eq!(accepted["path"], "md:analytics");
    assert_eq!(accepted["storage"], "motherduck");
    assert_eq!(entity["accepted_sink_uri"], "md:analytics");

    // Round-trips back into a config that still addresses MotherDuck.
    let (reconstructed, _base) =
        config_from_manifest_json(&payload).expect("reconstruct config from manifest");
    let accepted = &reconstructed.entities[0].sink.accepted;
    let duckdb = accepted
        .duckdb
        .as_ref()
        .expect("duckdb block should survive round-trip");
    assert_eq!(duckdb.connection.as_deref(), Some("md:analytics"));
    // The synthetic "motherduck" storage placeholder must reconstruct to an unset
    // storage; otherwise `validate_duckdb_sink` rejects the MotherDuck target.
    assert_eq!(accepted.storage, None);
    // The reconstructed config must pass validation, proving the manifest round-trip
    // does not turn a valid MotherDuck config into an invalid one. Validation routes
    // the duckdb sink through the feature-gated `sink_format` registry, so it only
    // succeeds when the `duckdb` feature is compiled in; under the lean default build
    // the registry intentionally rejects it. Gate the assertion accordingly while the
    // manifest round-trip above still runs (and is verified) on every build.
    #[cfg(feature = "duckdb")]
    validate_config_for_tests(&reconstructed).expect("reconstructed MotherDuck config is valid");
}

#[test]
fn manifest_redacts_literal_motherduck_token() {
    // A literal MotherDuck token must never be written into the manifest: manifests
    // are orchestration/replay artifacts that may be persisted and shared. A `${ENV}`
    // reference is non-secret and is preserved so replay can re-expand it; a literal
    // secret is dropped entirely.
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let cfg_dir = root.join("cfg");
    std::fs::create_dir_all(&cfg_dir).expect("cfg dir");

    let literal_yaml = r#"version: "0.1"
entities:
  - name: "customers"
    source:
      format: "csv"
      path: "./in/customers.csv"
    sink:
      accepted:
        format: "duckdb"
        duckdb:
          connection: "md:analytics"
          table: "customers"
          token: "super-secret-literal-token"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#;
    let literal_path = cfg_dir.join("literal.yml");
    std::fs::write(&literal_path, literal_yaml).expect("write config");
    let location =
        resolve_config_location(literal_path.to_str().expect("utf8")).expect("resolve location");
    let config = load_config(&location.path).expect("load config");
    let payload =
        build_common_manifest_json(&location, &config, &[], None, &ManifestOptions::default())
            .expect("build manifest");

    // The raw secret must not appear anywhere in the serialized manifest.
    assert!(
        !payload.contains("super-secret-literal-token"),
        "literal MotherDuck token leaked into manifest JSON"
    );
    let value: Value = serde_json::from_str(&payload).expect("valid json");
    let duckdb = &value["entities"][0]["sinks"]["accepted"]["duckdb"];
    assert!(
        duckdb.get("token").map(|t| t.is_null()).unwrap_or(true),
        "literal token field must be redacted, got {duckdb:?}"
    );

    // An ${ENV} reference is non-secret and must be preserved verbatim.
    let env_yaml = r#"version: "0.1"
entities:
  - name: "customers"
    source:
      format: "csv"
      path: "./in/customers.csv"
    sink:
      accepted:
        format: "duckdb"
        duckdb:
          connection: "md:analytics"
          table: "customers"
          token: "${MOTHERDUCK_TOKEN}"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#;
    let env_path = cfg_dir.join("env.yml");
    std::fs::write(&env_path, env_yaml).expect("write config");
    let location =
        resolve_config_location(env_path.to_str().expect("utf8")).expect("resolve location");
    let config = load_config(&location.path).expect("load config");
    let payload =
        build_common_manifest_json(&location, &config, &[], None, &ManifestOptions::default())
            .expect("build manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");
    assert_eq!(
        value["entities"][0]["sinks"]["accepted"]["duckdb"]["token"], "${MOTHERDUCK_TOKEN}",
        "non-secret ${{ENV}} placeholder must be preserved for replay"
    );

    // A mixed/malformed token that merely contains `${` (rejected by
    // expand_env_token at connect time) carries literal secret material and must
    // be redacted, not preserved.
    let mixed_yaml = r#"version: "0.1"
entities:
  - name: "customers"
    source:
      format: "csv"
      path: "./in/customers.csv"
    sink:
      accepted:
        format: "duckdb"
        duckdb:
          connection: "md:analytics"
          table: "customers"
          token: "tok-secret-${MOTHERDUCK_TOKEN}"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#;
    let mixed_path = cfg_dir.join("mixed.yml");
    std::fs::write(&mixed_path, mixed_yaml).expect("write config");
    let location =
        resolve_config_location(mixed_path.to_str().expect("utf8")).expect("resolve location");
    let config = load_config(&location.path).expect("load config");
    let payload =
        build_common_manifest_json(&location, &config, &[], None, &ManifestOptions::default())
            .expect("build manifest");
    assert!(
        !payload.contains("tok-secret-"),
        "mixed/malformed token leaked into manifest JSON"
    );
    let value: Value = serde_json::from_str(&payload).expect("valid json");
    let duckdb = &value["entities"][0]["sinks"]["accepted"]["duckdb"];
    assert!(
        duckdb.get("token").map(|t| t.is_null()).unwrap_or(true),
        "mixed/malformed token must be redacted, got {duckdb:?}"
    );
}

#[test]
fn manifest_real_storage_named_motherduck_survives_roundtrip() {
    // The "motherduck" storage value is only a synthetic placeholder for MotherDuck
    // DuckDB sinks. A real, user-defined storage definition that happens to be named
    // "motherduck" on a non-DuckDB sink must be preserved verbatim on reconstruct,
    // not silently dropped to the default/local storage.
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let cfg_dir = root.join("cfg");
    std::fs::create_dir_all(&cfg_dir).expect("cfg dir");
    let config_path = cfg_dir.join("config.yml");

    let yaml = r#"version: "0.1"
storages:
  default: "local"
  definitions:
    - name: "local"
      type: "local"
    - name: "motherduck"
      type: "s3"
      bucket: "demo"
      region: "us-east-1"
entities:
  - name: "events"
    source:
      format: "csv"
      path: "./in/events.csv"
    sink:
      accepted:
        format: "parquet"
        storage: "motherduck"
        path: "s3://demo/out/events"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#;
    std::fs::write(&config_path, yaml).expect("write config");

    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");

    let payload = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        None,
        &ManifestOptions::default(),
    )
    .expect("build manifest");

    let value: Value = serde_json::from_str(&payload).expect("valid json");
    assert_eq!(
        value["entities"][0]["sinks"]["accepted"]["storage"],
        "motherduck"
    );

    let (reconstructed, _base) =
        config_from_manifest_json(&payload).expect("reconstruct config from manifest");
    let accepted = &reconstructed.entities[0].sink.accepted;
    // Non-DuckDB sink: the real storage name must NOT be treated as the synthetic
    // placeholder, so it is preserved rather than dropped to None.
    assert_eq!(accepted.storage.as_deref(), Some("motherduck"));
}

#[test]
fn manifest_embeds_config_level_storages_for_replay() {
    // Storages defined at config level (not in a profile) must be embedded in the generated
    // manifest so `floe run --manifest` is self-contained and can resolve named storages.
    // Regression for #425 ("no storages block").
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let cfg_dir = temp_dir.path().join("cfg");
    std::fs::create_dir_all(&cfg_dir).expect("cfg dir");
    let config_path = cfg_dir.join("config.yml");

    let yaml = r#"version: "0.1"
storages:
  default: "local"
  definitions:
    - name: "local"
      type: "local"
    - name: "lakehouse_bronze"
      type: "s3"
      bucket: "example-bronze"
      region: "eu-west-1"
entities:
  - name: "accounts"
    source:
      format: "csv"
      storage: "local"
      path: "./in/accounts.csv"
    sink:
      accepted:
        format: "parquet"
        storage: "lakehouse_bronze"
        path: "sales/accounts"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#;
    std::fs::write(&config_path, yaml).expect("write config");

    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let opts = ManifestOptions {
        path_mode: PathMode::ResolvedUri,
        ..ManifestOptions::default()
    };

    let payload =
        build_common_manifest_json(&config_location, &config, &[], None, &opts).expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    // The manifest must carry the config-level storages even without a profile.
    let definitions = value["storages"]["definitions"]
        .as_array()
        .expect("storages.definitions should be present in the manifest");
    assert!(
        definitions
            .iter()
            .any(|d| d["name"] == "lakehouse_bronze" && d["bucket"] == "example-bronze"),
        "config-level storage definition must be embedded in the manifest: {value:#}"
    );

    // ...and survive reconstruction so manifest replay can resolve the named storage.
    let (reconstructed, _base) =
        config_from_manifest_json(&payload).expect("reconstruct config from manifest");
    let storages = reconstructed
        .storages
        .expect("storages should be populated after manifest round-trip");
    let bronze = storages
        .definitions
        .iter()
        .find(|d| d.name == "lakehouse_bronze")
        .expect("lakehouse_bronze definition should survive round-trip");
    assert_eq!(bronze.bucket.as_deref(), Some("example-bronze"));
    assert_eq!(bronze.region.as_deref(), Some("eu-west-1"));
}

#[test]
fn manifest_reconstruct_rejects_malformed_storages_block() {
    // A present-but-malformed storages block must surface a clear error rather than being
    // silently dropped (which previously resurfaced downstream as the confusing
    // "no storages block" failure). Regression for #425.
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let payload = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        None,
        &ManifestOptions::default(),
    )
    .expect("manifest");

    let mut value: Value = serde_json::from_str(&payload).expect("valid json");
    // A bare string is not a valid storages object.
    value["storages"] = Value::String("totally-not-a-storages-block".into());
    let corrupted = serde_json::to_string(&value).expect("reserialize");

    let err =
        config_from_manifest_json(&corrupted).expect_err("malformed storages block must error");
    let msg = err.to_string();
    assert!(
        msg.contains("storages") && msg.contains("malformed"),
        "expected a clear 'malformed storages' error, got: {msg}"
    );
}

#[test]
fn manifest_path_mode_resolved_uri_sets_path_from_uri() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let cfg_dir = root.join("cfg");
    std::fs::create_dir_all(&cfg_dir).expect("cfg dir");
    let in_dir = cfg_dir.join("in");
    std::fs::create_dir_all(&in_dir).expect("in dir");
    std::fs::write(in_dir.join("data.csv"), "id\n1\n").expect("write csv");
    let config_path = cfg_dir.join("config.yml");

    let yaml = r#"version: "0.1"
entities:
  - name: "data"
    source:
      format: "csv"
      path: "./in/data.csv"
    sink:
      accepted:
        format: "parquet"
        path: "./out/accepted/data"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#;
    std::fs::write(&config_path, yaml).expect("write config");

    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let opts = ManifestOptions {
        path_mode: PathMode::ResolvedUri,
        ..ManifestOptions::default()
    };

    let payload =
        build_common_manifest_json(&config_location, &config, &[], None, &opts).expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    let entity = &value["entities"][0];
    let source_path = entity["source"]["path"].as_str().expect("source.path");
    let source_uri = entity["source"]["uri"].as_str().expect("source.uri");

    assert!(
        entity["source"]["resolved"].as_bool().unwrap_or(false),
        "source should be resolved for a local path"
    );
    // Local URIs have the local:// scheme stripped so the StorageResolver receives
    // a plain filesystem path rather than an unrecognised local:// prefix.
    let expected_path = source_uri.strip_prefix("local://").unwrap_or(source_uri);
    assert_eq!(
        source_path, expected_path,
        "path should equal the filesystem path (local:// prefix stripped) when path_mode=resolved-uri"
    );
}

fn write_project_file(dir: &std::path::Path, rel: &str, contents: &str) {
    let path = dir.join(rel);
    std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
    std::fs::write(path, contents).expect("write");
}

fn project_manifest(config_path: &std::path::Path) -> String {
    let loc = resolve_config_location(config_path.to_str().expect("utf8")).expect("resolve");
    let config = load_config(&loc.path).expect("load config");
    let opts = ManifestOptions {
        deterministic: true,
        ..ManifestOptions::default()
    };
    build_common_manifest_json(&loc, &config, &[], None, &opts).expect("manifest")
}

const PROJECT_ENTITY: &str = "name: orders\nsource:\n  format: csv\n  path: /tmp/in/orders\nsink:\n  accepted:\n    format: parquet\n    path: /tmp/out/orders\npolicy:\n  severity: warn\nschema:\n  columns:\n    - name: id\n      type: string\n";

#[test]
fn manifest_id_covers_every_included_config_file() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let dir = temp_dir.path();
    let config_path = dir.join("floe.yml");
    write_project_file(
        dir,
        "floe.yml",
        "version: \"0.3\"\ninclude:\n  domains: [\"silver/*/_domain.yml\"]\n",
    );
    write_project_file(
        dir,
        "silver/sales/_domain.yml",
        "name: sales\nincoming_dir: /tmp/incoming/sales\n",
    );
    write_project_file(dir, "silver/sales/orders.yml", PROJECT_ENTITY);

    let before = project_manifest(&config_path);
    assert_eq!(before, project_manifest(&config_path));
    let value: Value = serde_json::from_str(&before).expect("valid json");
    let paths: Vec<&str> = value["config_files"]
        .as_array()
        .expect("config_files")
        .iter()
        .map(|file| file["path"].as_str().expect("path"))
        .collect();
    assert_eq!(
        paths,
        [
            "floe.yml",
            "silver/sales/_domain.yml",
            "silver/sales/orders.yml"
        ]
    );

    write_project_file(
        dir,
        "silver/sales/orders.yml",
        &PROJECT_ENTITY.replace("type: string", "type: int64"),
    );
    let after: Value = serde_json::from_str(&project_manifest(&config_path)).expect("valid json");
    assert_ne!(after["manifest_id"], value["manifest_id"]);
    assert_ne!(after["config_checksum"], value["config_checksum"]);
}

#[test]
fn multi_file_manifest_omits_checksums_if_a_config_file_cannot_be_read() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let dir = temp_dir.path();
    let config_path = dir.join("floe.yml");
    write_project_file(
        dir,
        "floe.yml",
        "version: \"0.3\"\ninclude:\n  domains: [\"silver/*/_domain.yml\"]\n",
    );
    write_project_file(
        dir,
        "silver/sales/_domain.yml",
        "name: sales\nincoming_dir: /tmp/incoming/sales\n",
    );
    write_project_file(dir, "silver/sales/orders.yml", PROJECT_ENTITY);

    let location = resolve_config_location(config_path.to_str().expect("utf8")).expect("resolve");
    let config = load_config(&location.path).expect("load config");
    std::fs::remove_file(dir.join("silver/sales/orders.yml")).expect("remove included file");
    let payload =
        build_common_manifest_json(&location, &config, &[], None, &ManifestOptions::default())
            .expect("build manifest");
    let value: Value = serde_json::from_str(&payload).expect("manifest is valid json");

    assert!(value.get("config_checksum").is_none());
    assert!(value.get("config_files").is_none());
}

#[test]
fn single_file_manifest_keeps_file_checksum_and_omits_config_files() {
    let config_path = std::fs::canonicalize(repo_root().join("example/config.yml"))
        .expect("canonicalize config path");
    let value: Value = serde_json::from_str(&project_manifest(&config_path)).expect("valid json");
    let bytes = std::fs::read(&config_path).expect("read config");
    use sha2::Digest;
    assert_eq!(
        value["config_checksum"],
        format!("sha256:{:x}", sha2::Sha256::digest(&bytes))
    );
    assert!(value.get("config_files").is_none());
}
