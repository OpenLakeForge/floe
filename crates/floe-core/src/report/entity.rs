use crate::io::format::CatalogRegistration;
use crate::{check, config, report};

use crate::report::build::{entity_metadata_json, source_options_json};
use crate::run::entity::ResolvedEntityTargets;
use crate::run::RunContext;

pub(crate) struct RunReportContext<'a> {
    pub context: &'a RunContext,
    pub entity: &'a config::EntityConfig,
    pub input: &'a config::SourceConfig,
    pub resolved_targets: &'a ResolvedEntityTargets,
    pub resolved_mode: report::ResolvedInputMode,
    pub resolved_files: &'a [String],
    pub reported_files: Vec<String>,
    pub totals: report::ResultsTotals,
    pub file_reports: Vec<report::FileReport>,
    pub severity: report::Severity,
    pub accepted_write_mode: config::WriteMode,
    pub accepted_parts_written: u64,
    pub accepted_files_written: Option<u64>,
    pub accepted_part_files: Vec<String>,
    pub accepted_table_version: Option<i64>,
    pub accepted_snapshot_id: Option<i64>,
    pub accepted_table_root_uri: Option<String>,
    pub accepted_catalog: Option<CatalogRegistration>,
    pub accepted_total_bytes_written: Option<u64>,
    pub accepted_avg_file_size_mb: Option<f64>,
    pub accepted_small_files_count: Option<u64>,
    pub accepted_merge_key: Vec<String>,
    pub accepted_inserted_count: Option<u64>,
    pub accepted_updated_count: Option<u64>,
    pub accepted_closed_count: Option<u64>,
    pub accepted_unchanged_count: Option<u64>,
    pub accepted_target_rows_before: Option<u64>,
    pub accepted_target_rows_after: Option<u64>,
    pub accepted_merge_elapsed_ms: Option<u64>,
    pub accepted_schema_evolution: report::SchemaEvolutionSummary,
    pub unique_constraints: Vec<check::UniqueConstraintResult>,
}

pub(crate) fn build_run_report(ctx: RunReportContext<'_>) -> report::RunReport {
    report::RunReport {
        spec_version: ctx.context.config.version.clone(),
        entity: report::EntityEcho {
            name: ctx.entity.id.clone(),
            metadata: ctx.entity.metadata.as_ref().map(entity_metadata_json),
        },
        source: report::SourceEcho {
            format: ctx.input.format.clone(),
            path: ctx.resolved_targets.source.target_uri().to_string(),
            options: ctx.input.options.as_ref().map(source_options_json),
            cast_mode: ctx.input.cast_mode.clone(),
            read_plan: report::SourceReadPlan::RawAndTyped,
            resolved_inputs: report::ResolvedInputs {
                mode: ctx.resolved_mode,
                file_count: ctx.resolved_files.len() as u64,
                files: ctx.reported_files,
            },
        },
        sink: report::SinkEcho {
            accepted: report::SinkTargetEcho {
                format: ctx.entity.sink.accepted.format.clone(),
                path: ctx.resolved_targets.accepted.target_uri().to_string(),
            },
            rejected: ctx
                .entity
                .sink
                .rejected
                .as_ref()
                .map(|rejected| report::SinkTargetEcho {
                    format: rejected.format.clone(),
                    path: ctx
                        .resolved_targets
                        .rejected
                        .as_ref()
                        .map(|target| target.target_uri().to_string())
                        .unwrap_or_else(|| rejected.path.clone()),
                }),
            archive: report::SinkArchiveEcho {
                enabled: ctx.entity.archive_enabled(),
                path: ctx
                    .entity
                    .sink
                    .archive
                    .as_ref()
                    .map(|archive| archive.path.clone()),
            },
        },
        policy: report::PolicyEcho {
            severity: ctx.severity,
        },
        accepted_output: report::AcceptedOutputSummary {
            path: ctx.resolved_targets.accepted.target_uri().to_string(),
            table_root_uri: ctx
                .accepted_table_root_uri
                .or_else(|| Some(ctx.resolved_targets.accepted.target_uri().to_string())),
            write_mode: Some(ctx.accepted_write_mode.as_str().to_string()),
            accepted_rows: ctx.totals.accepted_total,
            files_written: ctx.accepted_files_written,
            parts_written: ctx.accepted_parts_written,
            part_files: ctx.accepted_part_files,
            table_version: ctx.accepted_table_version,
            snapshot_id: ctx.accepted_snapshot_id,
            iceberg_catalog_name: ctx.accepted_catalog.as_ref().and_then(|c| match c {
                CatalogRegistration::IcebergGlue { catalog_name, .. }
                | CatalogRegistration::IcebergRest { catalog_name, .. } => {
                    Some(catalog_name.clone())
                }
                _ => None,
            }),
            iceberg_database: ctx.accepted_catalog.as_ref().and_then(|c| match c {
                CatalogRegistration::IcebergGlue { database, .. } => database.clone(),
                _ => None,
            }),
            iceberg_namespace: ctx.accepted_catalog.as_ref().and_then(|c| match c {
                CatalogRegistration::IcebergGlue { namespace, .. }
                | CatalogRegistration::IcebergRest { namespace, .. } => Some(namespace.clone()),
                _ => None,
            }),
            iceberg_table: ctx.accepted_catalog.as_ref().and_then(|c| match c {
                CatalogRegistration::IcebergGlue { table, .. }
                | CatalogRegistration::IcebergRest { table, .. } => Some(table.clone()),
                _ => None,
            }),
            delta_catalog_name: ctx.accepted_catalog.as_ref().and_then(|c| match c {
                CatalogRegistration::UnityDelta { catalog_name, .. } => Some(catalog_name.clone()),
                _ => None,
            }),
            delta_catalog_schema: ctx.accepted_catalog.as_ref().and_then(|c| match c {
                CatalogRegistration::UnityDelta { schema, .. } => Some(schema.clone()),
                _ => None,
            }),
            delta_catalog_table: ctx.accepted_catalog.as_ref().and_then(|c| match c {
                CatalogRegistration::UnityDelta { table, .. } => Some(table.clone()),
                _ => None,
            }),
            total_bytes_written: ctx.accepted_total_bytes_written,
            avg_file_size_mb: ctx.accepted_avg_file_size_mb,
            small_files_count: ctx.accepted_small_files_count,
            merge_key: ctx.accepted_merge_key,
            inserted_count: ctx.accepted_inserted_count,
            updated_count: ctx.accepted_updated_count,
            closed_count: ctx.accepted_closed_count,
            unchanged_count: ctx.accepted_unchanged_count,
            target_rows_before: ctx.accepted_target_rows_before,
            target_rows_after: ctx.accepted_target_rows_after,
            merge_elapsed_ms: ctx.accepted_merge_elapsed_ms,
        },
        schema_evolution: ctx.accepted_schema_evolution,
        unique_constraints: build_unique_constraint_reports(ctx.severity, &ctx.unique_constraints),
        results: ctx.totals,
        files: ctx.file_reports,
    }
}

fn build_unique_constraint_reports(
    severity: report::Severity,
    results: &[check::UniqueConstraintResult],
) -> Vec<report::UniqueConstraintReport> {
    results
        .iter()
        .map(|result| {
            let (action, status_effect) =
                unique_constraint_effect(severity, result.duplicates_count);
            report::UniqueConstraintReport {
                columns: result.columns.clone(),
                duplicates_count: result.duplicates_count,
                batch_duplicates_count: result.batch_duplicates_count,
                target_duplicates_count: result.target_duplicates_count,
                affected_rows_count: result.affected_rows_count,
                action: action.to_string(),
                status_effect: status_effect.to_string(),
                samples: result
                    .samples
                    .iter()
                    .map(|sample| report::UniqueConstraintSampleReport {
                        values: sample.values.clone(),
                        count: sample.count,
                    })
                    .collect(),
            }
        })
        .collect()
}

fn unique_constraint_effect(
    severity: report::Severity,
    duplicates_count: u64,
) -> (&'static str, &'static str) {
    if duplicates_count == 0 {
        return ("none", "none");
    }
    match severity {
        report::Severity::Warn => ("warn", "warning"),
        report::Severity::Reject => ("reject_rows", "rows_rejected"),
        report::Severity::Abort => ("abort_run", "run_aborted"),
    }
}
