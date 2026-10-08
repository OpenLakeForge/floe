use std::fs;
use std::path::Path;

use floe_core::load_config;

use super::super::common::write_temp_config;

const SALES_DOMAIN: &str = r#"name: "sales"
incoming_dir: "/tmp/incoming/sales"
defaults:
  sink:
    accepted:
      format: "parquet"
      path: "/tmp/out/sales"
"#;

const FINANCE_DOMAIN: &str = r#"name: "finance"
incoming_dir: "/tmp/incoming/finance"
"#;

fn entity(name: &str, domain_line: &str, sink: &str) -> String {
    format!(
        r#"name: "{name}"
{domain_line}source:
  format: "csv"
  path: "/tmp/input/{name}"
{sink}policy:
  severity: "warn"
schema:
  columns:
    - name: "id"
      type: "string"
"#
    )
}

const FINANCE_SINK: &str =
    "sink:\n  accepted:\n    format: \"parquet\"\n    path: \"/tmp/out/finance\"\n";

fn indent(yaml: &str, prefix: &str) -> String {
    yaml.lines()
        .enumerate()
        .map(|(i, line)| {
            let lead = if i == 0 { prefix } else { "    " };
            format!("{lead}{line}\n")
        })
        .collect()
}

fn write(dir: &Path, rel: &str, contents: &str) {
    let path = dir.join(rel);
    fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
    fs::write(path, contents).expect("write");
}

fn write_project(dir: &Path, version: &str) {
    write(
        dir,
        "floe.yml",
        &format!("version: \"{version}\"\ninclude:\n  domains: [\"silver/*/_domain.yml\"]\n"),
    );
    write(dir, "silver/sales/_domain.yml", SALES_DOMAIN);
    write(dir, "silver/sales/orders.yml", &entity("orders", "", ""));
    write(
        dir,
        "silver/sales/products.yml",
        &entity("products", "domain: \"sales\"\n", ""),
    );
    write(dir, "silver/finance/_domain.yml", FINANCE_DOMAIN);
    write(
        dir,
        "silver/finance/ledger.yml",
        &entity("ledger", "", FINANCE_SINK),
    );
}

#[test]
fn include_domains_equals_single_file_twin() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_project(dir.path(), "0.3");
    let mut assembled = load_config(&dir.path().join("floe.yml")).expect("assembled");

    let twin = format!(
        "version: \"0.3\"\ndomains:\n{}{}entities:\n{}{}{}",
        indent(FINANCE_DOMAIN, "  - "),
        indent(SALES_DOMAIN, "  - "),
        indent(
            &entity("ledger", "domain: \"finance\"\n", FINANCE_SINK),
            "  - "
        ),
        indent(&entity("orders", "domain: \"sales\"\n", ""), "  - "),
        indent(&entity("products", "domain: \"sales\"\n", ""), "  - "),
    );
    let mut single = load_config(&write_temp_config(&twin)).expect("single file");

    let root = fs::canonicalize(dir.path()).expect("canonical");
    let files: Vec<_> = assembled
        .config_files
        .iter()
        .map(|file| fs::canonicalize(file).expect("canonical file"))
        .collect();
    assert_eq!(
        files,
        [
            "floe.yml",
            "silver/finance/_domain.yml",
            "silver/finance/ledger.yml",
            "silver/sales/_domain.yml",
            "silver/sales/orders.yml",
            "silver/sales/products.yml",
        ]
        .map(|rel| root.join(rel))
    );
    assert_eq!(single.config_files.len(), 1);
    assembled.config_files.clear();
    single.config_files.clear();
    assert_eq!(format!("{assembled:?}"), format!("{single:?}"));
}

#[test]
fn include_pattern_matching_nothing_names_the_pattern() {
    let dir = tempfile::tempdir().expect("tempdir");
    write(
        dir.path(),
        "floe.yml",
        "version: \"0.3\"\ninclude:\n  domains: [\"gold/*/_domain.yml\"]\nentities: []\n",
    );
    let err = load_config(&dir.path().join("floe.yml")).expect_err("no match");
    assert_eq!(
        err.to_string(),
        "include.domains pattern \"gold/*/_domain.yml\" matched no file \
         (includes resolve against local files only; a remote project cannot include)"
    );
}

#[test]
fn include_entity_with_mismatching_domain_fails() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_project(dir.path(), "0.3");
    write(
        dir.path(),
        "silver/sales/orders.yml",
        &entity("orders", "domain: \"finance\"\n", ""),
    );
    let err = load_config(&dir.path().join("floe.yml")).expect_err("mismatch");
    assert!(
        err.to_string()
            .ends_with("orders.yml has domain \"finance\" but its _domain.yml names \"sales\""),
        "{err}"
    );
}

#[test]
fn include_requires_version_0_3() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_project(dir.path(), "0.2");
    let err = load_config(&dir.path().join("floe.yml")).expect_err("0.2");
    assert_eq!(
        err.to_string(),
        "root.include requires root.version >= \"0.3\""
    );
}
