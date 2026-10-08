use crate::{config, FloeResult};

use crate::io::storage::Target;

#[derive(Debug, Clone)]
pub(crate) struct ResolvedEntityTargets {
    pub(crate) source: Target,
    pub(crate) accepted: Target,
    pub(crate) rejected: Option<Target>,
}

pub(crate) fn resolve_entity_targets(
    resolver: &config::StorageResolver,
    entity: &config::EntityConfig,
) -> FloeResult<ResolvedEntityTargets> {
    let source = resolver.resolve_path(
        &entity.name,
        "source.storage",
        entity.source.storage.as_deref(),
        &entity.source.path,
    )?;
    let source = Target::from_resolved(&source)?;

    // A MotherDuck DuckDB sink is addressed by `duckdb.connection`, not a filesystem
    // path: its `sink.accepted.path` is empty by design. Resolving that empty path
    // would fail the remote-config "path must be absolute" guard (and pointlessly
    // touch the storage layer the DuckDB writer never uses), so synthesize a
    // placeholder target the writer ignores instead of resolving it.
    let accepted = match motherduck_accepted_target(&entity.sink.accepted) {
        Some(target) => target,
        None => {
            let accepted = resolver.resolve_path(
                &entity.name,
                "sink.accepted.storage",
                entity.sink.accepted.storage.as_deref(),
                &entity.sink.accepted.path,
            )?;
            Target::from_resolved(&accepted)?
        }
    };

    let rejected = entity
        .sink
        .rejected
        .as_ref()
        .map(|rejected| {
            resolver.resolve_path(
                &entity.name,
                "sink.rejected.storage",
                rejected.storage.as_deref(),
                &rejected.path,
            )
        })
        .transpose()?;
    let rejected = rejected.as_ref().map(Target::from_resolved).transpose()?;
    Ok(ResolvedEntityTargets {
        source,
        accepted,
        rejected,
    })
}

/// If the accepted sink is a MotherDuck DuckDB target, build a placeholder
/// `Target` for it instead of resolving its (empty) filesystem path.
///
/// The DuckDB writer reaches MotherDuck through `duckdb.connection` and ignores
/// the resolved `Target`, so the placeholder only needs to be a non-remote target
/// the storage layer never dereferences. Returns `None` for every other sink so
/// the normal path-resolution flow applies.
fn motherduck_accepted_target(accepted: &config::SinkTarget) -> Option<Target> {
    if accepted.format != "duckdb" {
        return None;
    }
    let connection = accepted.duckdb.as_ref()?.connection.as_deref()?;
    if !crate::config::is_motherduck_connection(connection) {
        return None;
    }
    Some(Target::Local {
        storage: "motherduck".to_string(),
        uri: connection.trim().to_string(),
        base_path: String::new(),
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::config;

    fn motherduck_entity() -> config::EntityConfig {
        config::EntityConfig {
            name: "customers".to_string(),
            id: "customers".to_string(),
            metadata: None,
            domain: None,
            incremental_mode: config::IncrementalMode::None,
            state: None,
            source: config::SourceConfig {
                format: "csv".to_string(),
                // Absolute so source resolution survives the remote-config guard;
                // the point of the test is the *accepted* MotherDuck sink.
                path: "/data/in.csv".to_string(),
                storage: None,
                options: None,
                cast_mode: None,
            },
            sink: config::SinkConfig {
                write_mode: config::WriteMode::Overwrite,
                accepted: config::SinkTarget {
                    format: "duckdb".to_string(),
                    // MotherDuck targets carry an empty path by design.
                    path: String::new(),
                    storage: None,
                    options: None,
                    merge: None,
                    iceberg: None,
                    delta: None,
                    duckdb: Some(config::DuckDbSinkTargetConfig {
                        table: "customers".to_string(),
                        schema: None,
                        connection: Some("md:analytics".to_string()),
                        token: None,
                    }),
                    partition_by: None,
                    partition_spec: None,
                    write_mode: config::WriteMode::Overwrite,
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

    /// Regression: a MotherDuck accepted sink (empty `path`) under a *remote*
    /// config base must resolve without tripping the "path must be absolute when
    /// config is remote" guard — the writer reaches MotherDuck via the connection
    /// string, not the filesystem.
    #[test]
    fn motherduck_accepted_sink_resolves_under_remote_config() {
        let entity = motherduck_entity();
        let root = config::RootConfig {
            version: "0.1".to_string(),
            metadata: None,
            storages: None,
            catalogs: None,
            env: None,
            domains: Vec::new(),
            report: None,
            lineage: None,
            entities: vec![motherduck_entity()],
        };
        let base = config::ConfigBase::remote_from_uri(
            PathBuf::from("/tmp"),
            "s3://my-bucket/configs/demo.yml",
        )
        .expect("remote config base");
        let resolver = config::StorageResolver::new(&root, base).expect("storage resolver");

        let resolved =
            resolve_entity_targets(&resolver, &entity).expect("motherduck sink should resolve");

        // The accepted target is a non-remote placeholder the writer ignores.
        assert!(!resolved.accepted.is_remote());
        match &resolved.accepted {
            Target::Local { base_path, uri, .. } => {
                assert!(base_path.is_empty());
                assert_eq!(uri, "md:analytics");
            }
            other => panic!("expected a local placeholder target, got {other:?}"),
        }
    }

    #[test]
    fn non_duckdb_sink_is_not_treated_as_motherduck() {
        let mut entity = motherduck_entity();
        entity.sink.accepted.format = "parquet".to_string();
        assert!(motherduck_accepted_target(&entity.sink.accepted).is_none());
    }
}
