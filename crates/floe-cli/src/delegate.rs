//! Companion-binary delegation for DuckDB sinks.
//!
//! The published `floe` binary is built lean (no `duckdb` feature) because the
//! bundled native DuckDB build does not cross-compile in the release pipeline and
//! would bloat every artifact. DuckDB support ships as a separate `floe-duckdb`
//! companion (same `floe-cli` source built `--features duckdb`), following the
//! companion-on-PATH model used by git/git-lfs and cargo subcommands.
//!
//! When the lean build is asked to run a config that writes to a DuckDB sink, it
//! locates the `floe-duckdb` companion and re-execs it with the identical argv.
//! The full build (`cfg!(feature = "duckdb")`) handles DuckDB directly, so its
//! delegation hook is a no-op.

// `FloeError` and the path types are only used by the lean build's companion-lookup
// helpers, which are themselves `#[cfg(not(feature = "duckdb"))]`.
#[cfg(not(feature = "duckdb"))]
use floe_core::FloeError;
#[cfg(not(feature = "duckdb"))]
use std::path::{Path, PathBuf};

use floe_core::config::RootConfig;
// Used by `maybe_delegate_duckdb` in both feature configs (the `duckdb` build's no-op
// variant returns `FloeResult<()>` too), so this import must not be feature-gated.
use floe_core::FloeResult;

#[cfg(not(feature = "duckdb"))]
const COMPANION_STEM: &str = "floe-duckdb";

/// True when any entity writes to a DuckDB *accepted* sink. Only the accepted
/// sink can target DuckDB: rejected sinks accept `csv` only
/// (`format::rejected_sink_adapter`), so a `rejected.format: duckdb` is always
/// invalid. Keying delegation on the rejected format too would re-exec the
/// companion for such a config and surface a misleading "install the companion"
/// hint instead of the real unsupported-rejected-sink validation error.
#[cfg_attr(feature = "duckdb", allow(dead_code))]
pub fn config_targets_duckdb(config: &RootConfig) -> bool {
    config
        .entities
        .iter()
        .any(|entity| entity.sink.accepted.format == "duckdb")
}

/// In the full build DuckDB is compiled in, so there is nothing to delegate.
#[cfg(feature = "duckdb")]
pub fn maybe_delegate_duckdb(_config: &RootConfig) -> FloeResult<()> {
    Ok(())
}

/// In the lean build, re-exec the `floe-duckdb` companion when the config targets
/// a DuckDB sink. On success this never returns — it propagates the companion's
/// exit status. Returns an actionable error when no companion can be located.
#[cfg(not(feature = "duckdb"))]
pub fn maybe_delegate_duckdb(config: &RootConfig) -> FloeResult<()> {
    if !config_targets_duckdb(config) {
        return Ok(());
    }

    let companion = find_companion().ok_or_else(|| {
        FloeError::config(format!(
            "this config writes to a DuckDB sink, but this is the lean `floe` build \
             without DuckDB support and no `{COMPANION_STEM}` companion was found on \
             PATH or alongside this executable. Install the DuckDB build via the \
             `ghcr.io/openlakeforge/floe-duckdb` image, the `floe-duckdb` release binary, \
             or `cargo install floe-cli --features duckdb`."
        ))
    })?;

    let status = std::process::Command::new(&companion)
        .args(std::env::args_os().skip(1))
        .status()
        .map_err(|err| {
            FloeError::config(format!(
                "failed to launch DuckDB companion at {}: {err}",
                companion.display()
            ))
        })?;

    std::process::exit(status.code().unwrap_or(1));
}

/// Locate the `floe-duckdb` companion, resolving ONLY from the directory of the
/// running executable or from `PATH`. The current working directory is never
/// consulted (git-lfs CVE GHSA-6rw3-3whw-jvjj: a CWD-resolved companion lets an
/// attacker who can drop a file next to untrusted data hijack execution).
#[cfg(not(feature = "duckdb"))]
fn find_companion() -> Option<PathBuf> {
    let name = format!("{COMPANION_STEM}{}", std::env::consts::EXE_SUFFIX);

    // 1. Alongside the running executable (canonicalized to defeat symlink tricks).
    if let Some(dir) = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.canonicalize().ok())
        .and_then(|exe| exe.parent().map(Path::to_path_buf))
    {
        let candidate = dir.join(&name);
        if is_executable_file(&candidate) {
            return Some(candidate);
        }
    }

    // 2. PATH, considering only absolute entries. Any non-absolute entry
    //    (empty, ".", "bin", "./.venv/bin", "..") is resolved relative to the
    //    CWD, which must never be trusted for companion lookup (git-lfs CVE
    //    GHSA-6rw3-3whw-jvjj), so skip them all.
    if let Some(path) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&path) {
            if !dir.is_absolute() {
                continue;
            }
            let candidate = dir.join(&name);
            if is_executable_file(&candidate) {
                return Some(candidate);
            }
        }
    }

    None
}

/// True only for a regular file that is actually executable. A non-executable
/// file named like the companion (e.g. a stray data file earlier in PATH) must
/// be skipped so the search continues to a real executable, mirroring how a
/// shell resolves a command on PATH.
#[cfg(not(feature = "duckdb"))]
fn is_executable_file(path: &Path) -> bool {
    if !path.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path)
            .map(|meta| meta.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        true
    }
}
