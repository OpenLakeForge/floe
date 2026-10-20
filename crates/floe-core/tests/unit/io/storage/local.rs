use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use floe_core::config;
use floe_core::io::storage::extensions::glob_patterns_for_format;
use floe_core::io::storage::local::{resolve_local_inputs, LocalClient};
use floe_core::io::storage::{ConditionalWrite, StorageClient};

fn temp_dir(prefix: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    path.push(format!("{prefix}-{nanos}"));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

fn write_file(path: &Path, contents: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create parent");
    }
    fs::write(path, contents).expect("write file");
}

fn default_globs(format: &str) -> Vec<String> {
    glob_patterns_for_format(format).expect("default globs")
}

fn source_config(
    format: &str,
    path: &Path,
    options: Option<config::SourceOptions>,
) -> config::SourceConfig {
    config::SourceConfig {
        format: format.to_string(),
        path: path.display().to_string(),
        storage: None,
        options,
        cast_mode: None,
        source_ref: None,
        resource: None,
    }
}

#[test]
fn default_glob_filters_by_format() {
    let root = temp_dir("floe-resolve-default-glob");
    write_file(&root.join("a.csv"), "id\n1\n");
    write_file(&root.join("B.CSV"), "id\n2\n");
    write_file(&root.join("c.txt"), "id\n3\n");
    let source = source_config("csv", &root, None);
    let resolved = resolve_local_inputs(
        Path::new("."),
        "customers",
        &source,
        "local",
        &default_globs("csv"),
    )
    .expect("resolve");
    assert_eq!(resolved.files.len(), 2);
}

#[test]
fn glob_override_is_used() {
    let root = temp_dir("floe-resolve-glob-override");
    write_file(&root.join("a.csv"), "id\n1\n");
    write_file(&root.join("b.data"), "id\n2\n");
    let options = config::SourceOptions {
        glob: Some("*.data".to_string()),
        ..Default::default()
    };
    let source = source_config("csv", &root, Some(options));
    let resolved = resolve_local_inputs(
        Path::new("."),
        "customers",
        &source,
        "local",
        &default_globs("csv"),
    )
    .expect("resolve");
    assert_eq!(resolved.files.len(), 1);
    assert!(resolved.files[0].to_string_lossy().ends_with("b.data"));
}

#[test]
fn recursive_lists_nested_files() {
    let root = temp_dir("floe-resolve-recursive");
    write_file(&root.join("nested/a.csv"), "id\n1\n");
    let options = config::SourceOptions {
        recursive: Some(true),
        ..Default::default()
    };
    let source = source_config("csv", &root, Some(options));
    let resolved = resolve_local_inputs(
        Path::new("."),
        "customers",
        &source,
        "local",
        &default_globs("csv"),
    )
    .expect("resolve");
    assert_eq!(resolved.files.len(), 1);
}

#[test]
fn glob_path_input_is_resolved() {
    let root = temp_dir("floe-resolve-glob-input");
    write_file(&root.join("a.csv"), "id\n1\n");
    let pattern = root.join("*.csv");
    let source = source_config("csv", &pattern, None);
    let resolved = resolve_local_inputs(
        Path::new("."),
        "customers",
        &source,
        "local",
        &default_globs("csv"),
    )
    .expect("resolve");
    assert_eq!(resolved.files.len(), 1);
}

#[test]
fn list_is_sorted() {
    let root = temp_dir("floe-resolve-sorted");
    write_file(&root.join("b.csv"), "id\n1\n");
    write_file(&root.join("a.csv"), "id\n2\n");
    let source = source_config("csv", &root, None);
    let resolved = resolve_local_inputs(
        Path::new("."),
        "customers",
        &source,
        "local",
        &default_globs("csv"),
    )
    .expect("resolve");
    assert!(resolved.files[0].to_string_lossy().ends_with("a.csv"));
    assert!(resolved.files[1].to_string_lossy().ends_with("b.csv"));
}

#[test]
fn missing_path_errors() {
    let root = temp_dir("floe-resolve-missing");
    let missing = root.join("missing");
    let source = source_config("csv", &missing, None);
    let err = resolve_local_inputs(
        Path::new("."),
        "customers",
        &source,
        "local",
        &default_globs("csv"),
    )
    .expect_err("error");
    assert!(err.to_string().contains("entity.name=customers"));
}

#[test]
fn local_client_upload_copies_file() {
    let root = temp_dir("floe-local-upload");
    let src = root.join("src.txt");
    let dest = root.join("dest.txt");
    write_file(&src, "hello");
    let client = LocalClient::new();
    client
        .upload_from_path(&src, dest.to_string_lossy().as_ref())
        .expect("upload");
    assert_eq!(fs::read_to_string(dest).expect("read"), "hello");
}

#[test]
fn local_client_download_copies_file() {
    let root = temp_dir("floe-local-download");
    let src = root.join("src.txt");
    let dest_dir = root.join("dest");
    write_file(&src, "hello");
    let client = LocalClient::new();
    let downloaded = client
        .download_to_temp(src.to_string_lossy().as_ref(), &dest_dir)
        .expect("download");
    assert_eq!(fs::read_to_string(downloaded).expect("read"), "hello");
}

#[test]
fn local_client_download_failure_is_structured_floe_error() {
    use floe_core::{FloeError, FloeErrorKind};

    let root = temp_dir("floe-local-download-fail");
    let missing = root.join("does-not-exist.txt");
    let dest_dir = root.join("dest");
    let client = LocalClient::new();

    let err = client
        .download_to_temp(missing.to_string_lossy().as_ref(), &dest_dir)
        .expect_err("download of a missing source should fail");

    let floe = err
        .downcast_ref::<FloeError>()
        .expect("storage failures should be a structured FloeError");
    assert_eq!(floe.kind(), FloeErrorKind::Storage);
    match floe {
        FloeError::Storage { path, message } => {
            assert_eq!(
                path.as_deref(),
                Some(missing.display().to_string().as_str())
            );
            assert!(
                message.starts_with("local download failed from"),
                "unexpected message: {message}"
            );
        }
        other => panic!("expected Storage variant, got {other:?}"),
    }
}

#[test]
fn local_client_conditional_state_create_update_and_delete() {
    let root = temp_dir("floe-local-state-conditional");
    let state_path = root.join("state.json");
    let uri = state_path.to_string_lossy();
    let client = LocalClient::new();

    let created = client
        .write_object_conditional(uri.as_ref(), None, br#"{"a":1}"#)
        .expect("create");
    let ConditionalWrite::Written { version } = created else {
        panic!("create should write");
    };
    assert_eq!(
        fs::read_to_string(&state_path).expect("read created"),
        r#"{"a":1}"#
    );

    let stale_create = client
        .write_object_conditional(uri.as_ref(), None, br#"{"a":2}"#)
        .expect("stale create");
    assert_eq!(stale_create, ConditionalWrite::Conflict);

    let updated = client
        .write_object_conditional(uri.as_ref(), Some(&version), br#"{"aa":22}"#)
        .expect("update");
    let ConditionalWrite::Written { version: updated } = updated else {
        panic!("update should write");
    };
    assert_ne!(updated, version);

    let stale_update = client
        .write_object_conditional(uri.as_ref(), Some(&version), br#"{"a":3}"#)
        .expect("stale update");
    assert_eq!(stale_update, ConditionalWrite::Conflict);

    assert!(client
        .read_object(uri.as_ref())
        .expect("read object")
        .is_some());
    let deleted = client
        .delete_object_conditional(uri.as_ref(), Some(&updated))
        .expect("delete");
    assert!(matches!(deleted, ConditionalWrite::Written { .. }));
    assert!(!state_path.exists());
}

#[test]
fn stale_lock_is_broken() {
    let root = temp_dir("floe-local-stale-lock");
    let state_path = root.join("state.json");
    let uri = state_path.to_string_lossy();
    let lock_path = PathBuf::from(format!("{}.lock", state_path.display()));
    // A lock left behind by a process that died long ago (epoch acquisition time).
    write_file(&lock_path, "0:12345");

    let client = LocalClient::new();
    let written = client
        .write_object_conditional(uri.as_ref(), None, br#"{"a":1}"#)
        .expect("write should break the stale lock and succeed");
    assert!(matches!(written, ConditionalWrite::Written { .. }));
    assert!(!lock_path.exists(), "lock released after write");
}

#[test]
fn fresh_lock_blocks_until_released() {
    use std::sync::mpsc;
    use std::time::Duration;

    let root = temp_dir("floe-local-fresh-lock");
    let state_path = root.join("state.json");
    let uri = state_path.to_string_lossy().into_owned();
    let lock_path = PathBuf::from(format!("{}.lock", state_path.display()));
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    // A lock held by a live process must not be broken.
    write_file(&lock_path, &format!("{now_ms}:99999"));

    let (tx, rx) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        let client = LocalClient::new();
        let result = client.write_object_conditional(uri.as_ref(), None, br#"{"a":1}"#);
        tx.send(()).ok();
        result
    });

    assert!(
        rx.recv_timeout(Duration::from_millis(200)).is_err(),
        "write must block while a fresh lock is held"
    );

    fs::remove_file(&lock_path).expect("release lock");
    rx.recv_timeout(Duration::from_secs(5))
        .expect("write completes once the lock is released");
    let written = writer.join().expect("join writer").expect("write");
    assert!(matches!(written, ConditionalWrite::Written { .. }));
}
