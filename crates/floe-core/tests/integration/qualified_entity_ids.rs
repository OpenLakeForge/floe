use std::fs;
use std::path::{Path, PathBuf};

use floe_core::{
    build_common_manifest_json, config_from_manifest_json, load_config, resolve_config_location,
    run, ManifestOptions, RunOptions,
};

/// Two domains (`sales`, `finance`) that both own an entity named `accounts`.
fn write_project(root: &Path, version: &str) -> PathBuf {
    for domain in ["sales", "finance"] {
        let dir = root.join("in").join(domain);
        fs::create_dir_all(&dir).expect("input dir");
        fs::write(dir.join("accounts.csv"), "id\n1\n").expect("write csv");
    }
    let entity = |domain: &str| {
        format!(
            r#"  - name: "accounts"
    domain: "{domain}"
    source:
      format: "csv"
      path: "{{{{domain.incoming_dir}}}}"
    sink:
      accepted:
        format: "parquet"
        path: "{root}/out/{domain}/accounts"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
            root = root.display()
        )
    };
    let yaml = format!(
        r#"version: "{version}"
report:
  path: "{root}/report"
domains:
  - name: "sales"
    incoming_dir: "{root}/in/sales"
  - name: "finance"
    incoming_dir: "{root}/in/finance"
entities:
{sales}{finance}"#,
        root = root.display(),
        sales = entity("sales"),
        finance = entity("finance"),
    );
    let path = root.join("config.yml");
    fs::write(&path, yaml).expect("write config");
    path
}

fn options(entities: &[&str]) -> RunOptions {
    RunOptions {
        profile: None,
        run_id: Some("it".to_string()),
        entities: entities.iter().map(ToString::to_string).collect(),
        dry_run: false,
        full_refresh: false,
    }
}

fn summary_names(root: &Path) -> Vec<String> {
    let summary: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(root.join("report/run_it/run.summary.json")).expect("summary"),
    )
    .expect("summary json");
    summary["entities"]
        .as_array()
        .expect("entities")
        .iter()
        .map(|e| e["name"].as_str().expect("name").to_string())
        .collect()
}

#[test]
fn v03_same_name_in_two_domains_reports_into_distinct_paths() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let root = temp.path();
    let config_path = write_project(root, "0.3");

    run(&config_path, options(&[])).expect("run");

    for domain in ["sales", "finance"] {
        let report = root.join(format!("report/run_it/{domain}/accounts/run.json"));
        assert!(report.exists(), "missing {}", report.display());
    }
    assert_eq!(
        summary_names(root),
        vec!["sales.accounts".to_string(), "finance.accounts".to_string()]
    );
}

#[test]
fn v03_entities_selector_accepts_qualified_ids_and_rejects_ambiguous_bare_names() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let root = temp.path();
    let config_path = write_project(root, "0.3");

    let err = run(&config_path, options(&["accounts"]))
        .expect_err("ambiguous bare name")
        .to_string();
    assert!(
        err.contains("ambiguous")
            && err.contains("sales.accounts")
            && err.contains("finance.accounts"),
        "unexpected error: {err}"
    );

    run(&config_path, options(&["finance.accounts"])).expect("run qualified");
    assert_eq!(summary_names(root), vec!["finance.accounts".to_string()]);
    assert!(!root.join("report/run_it/sales").exists());
}

#[test]
fn v03_manifest_names_are_qualified_and_replay_splits_them_back() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let config_path = write_project(temp.path(), "0.3");
    let location = resolve_config_location(config_path.to_str().expect("utf8")).expect("location");
    let config = load_config(&location.path).expect("load config");

    let manifest =
        build_common_manifest_json(&location, &config, &[], None, &ManifestOptions::default())
            .expect("manifest");
    let value: serde_json::Value = serde_json::from_str(&manifest).expect("json");
    let names: Vec<&str> = value["entities"]
        .as_array()
        .expect("entities")
        .iter()
        .map(|e| e["name"].as_str().expect("name"))
        .collect();
    assert_eq!(names, vec!["finance.accounts", "sales.accounts"]);
    assert_eq!(
        value["entities"][0]["asset_key"],
        serde_json::json!(["finance", "accounts"])
    );

    let (replayed, _) = config_from_manifest_json(&manifest).expect("reconstruct");
    let ids: Vec<String> = replayed
        .entities
        .iter()
        .map(|e| {
            assert_eq!(e.name, "accounts");
            replayed.entity_id(e)
        })
        .collect();
    assert_eq!(ids, vec!["finance.accounts", "sales.accounts"]);
}

#[test]
fn v02_domain_keeps_bare_names_and_report_paths() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let root = temp.path();
    let config_path = write_project(root, "0.2");

    // 0.2 identity is the bare name, so two `accounts` entities are still duplicates.
    let err = run(&config_path, options(&[]))
        .expect_err("duplicate")
        .to_string();
    assert!(err.contains("entity.name=accounts is duplicated"), "{err}");

    // Single domain: report path and summary name are unchanged from before 0.3.
    let yaml = fs::read_to_string(&config_path).expect("read");
    let single = &yaml[..yaml.rfind("  - name: \"accounts\"").expect("second entity")];
    fs::write(&config_path, single).expect("write");
    run(&config_path, options(&["accounts"])).expect("run");
    assert!(root.join("report/run_it/accounts/run.json").exists());
    assert_eq!(summary_names(root), vec!["accounts".to_string()]);
}
