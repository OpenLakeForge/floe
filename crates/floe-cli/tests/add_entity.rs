use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use tempfile::tempdir;

#[test]
fn add_entity_writes_config_that_validates() {
    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("config.yml");
    let input_path = dir.path().join("orders.csv");

    fs::write(&input_path, "id,total\n1,10.0\n2,20.0\n").expect("write csv");

    let mut add_cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    add_cmd
        .args(["add-entity", "-c"])
        .arg(&config_path)
        .args(["--input"])
        .arg(&input_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Entity added: orders"))
        .stdout(predicate::str::contains("format=csv"));

    let mut validate_cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    validate_cmd
        .args(["validate", "-c"])
        .arg(&config_path)
        .args(["--entities", "orders"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Config valid:"));
}

#[test]
fn add_entity_dry_run_does_not_write_config_file() {
    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("new-config.yml");
    let input_path = dir.path().join("employees.csv");

    fs::write(&input_path, "id,name\n1,Alice\n").expect("write csv");
    assert!(!config_path.exists());

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    cmd.args(["add-entity", "-c"])
        .arg(&config_path)
        .args(["--input"])
        .arg(&input_path)
        .arg("--dry-run")
        .assert()
        .success()
        .stdout(predicate::str::contains("# add-entity dry run"))
        .stdout(predicate::str::contains("name: employees"))
        .stdout(predicate::str::contains("format: csv"));

    assert!(!config_path.exists());
}

#[test]
fn add_entity_can_reference_a_declared_source() {
    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("config.yml");
    let input_path = dir.path().join("orders.csv");
    fs::write(
        &config_path,
        "version: \"0.3\"\nsources:\n  - name: crm\n    format: csv\n    path: ./bronze/{{resource}}.csv\nentities: []\n",
    )
    .expect("write config");
    fs::write(&input_path, "id,total\n1,10.0\n").expect("write csv");

    Command::new(assert_cmd::cargo::cargo_bin!("floe"))
        .args(["add-entity", "-c"])
        .arg(&config_path)
        .args(["--input"])
        .arg(&input_path)
        .args(["--source", "crm", "--resource", "crm_orders"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Entity added: orders"));

    let config = fs::read_to_string(&config_path).expect("read config");
    let entity = config.split("entities:").nth(1).expect("entities section");
    let source = entity
        .split_once("source:")
        .and_then(|(_, rest)| rest.split_once("sink:"))
        .map(|(source, _)| source)
        .expect("entity source section");
    assert!(source.contains("ref: crm"));
    assert!(source.contains("resource: crm_orders"));
    assert!(!source.contains("format:"));
    assert!(!source.contains("path:"));

    Command::new(assert_cmd::cargo::cargo_bin!("floe"))
        .args(["validate", "-c"])
        .arg(&config_path)
        .assert()
        .success();
}

#[test]
fn add_entity_unknown_source_lists_declared_sources() {
    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("config.yml");
    let input_path = dir.path().join("orders.csv");
    fs::write(
        &config_path,
        "version: \"0.3\"\nsources:\n  - name: crm\n    format: csv\n    path: ./bronze/{{resource}}.csv\nentities: []\n",
    )
    .expect("write config");
    fs::write(&input_path, "id\n1\n").expect("write csv");

    Command::new(assert_cmd::cargo::cargo_bin!("floe"))
        .args(["add-entity", "-c"])
        .arg(&config_path)
        .args(["--input"])
        .arg(&input_path)
        .args(["--source", "missing", "--resource", "orders"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("source.ref=missing"))
        .stderr(predicate::str::contains("available: crm"));
}
