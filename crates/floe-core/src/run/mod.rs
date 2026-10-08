use crate::errors::FloeError;
use std::path::Path;
use std::sync::Once;
use std::time::Instant;

use crate::report::build::project_metadata_json;
use crate::report::output::write_summary_report;
use crate::runtime::{DefaultRuntime, Runtime};
use crate::{config, io, report, FloeResult, RunOptions, ValidateOptions};

mod context;
pub(crate) mod entity;
pub mod events;
mod file;
mod output;
mod perf;

pub(crate) use context::RunContext;
use entity::{run_entity, EntityRunResult, ResolvedEntityTargets};
use events::{default_observer, event_time_ms, RunEvent};
use serde_json::json;

pub(super) const MAX_RESOLVED_INPUTS: usize = 50;

pub(crate) struct EntityRunPlan<'a> {
    pub(crate) entity: &'a config::EntityConfig,
    pub(crate) resolved_targets: ResolvedEntityTargets,
    pub(crate) resolved_inputs: io::storage::inputs::ResolvedInputs,
    pub(crate) temp_dir: Option<tempfile::TempDir>,
}

#[derive(Debug, Clone)]
pub struct RunOutcome {
    pub run_id: String,
    pub report_base_path: Option<String>,
    pub entity_outcomes: Vec<EntityOutcome>,
    pub summary: report::RunSummaryReport,
    pub dry_run_previews: Option<Vec<DryRunEntityPreview>>,
}

#[derive(Debug, Clone)]
pub struct DryRunEntityPreview {
    pub name: String,
    pub input_path: String,
    pub input_format: String,
    pub accepted_path: String,
    pub accepted_format: String,
    pub rejected_path: Option<String>,
    pub rejected_format: Option<String>,
    pub archive_path: String,
    pub archive_storage: Option<String>,
    pub report_file: Option<String>,
    pub scanned_files: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct EntityOutcome {
    pub report: crate::report::RunReport,
    pub file_timings_ms: Vec<Option<u64>>,
}

pub fn run(config_path: &Path, options: RunOptions) -> FloeResult<RunOutcome> {
    let config_base = config::ConfigBase::local_from_path(config_path);
    run_with_base(config_path, config_base, options)
}

pub fn run_with_base(
    config_path: &Path,
    config_base: config::ConfigBase,
    options: RunOptions,
) -> FloeResult<RunOutcome> {
    let mut runtime = DefaultRuntime::new();
    run_with_runtime(config_path, config_base, options, &mut runtime)
}

pub fn run_with_manifest_path(manifest_path: &Path, options: RunOptions) -> FloeResult<RunOutcome> {
    let mut runtime = DefaultRuntime::new();
    run_with_manifest_runtime(manifest_path, options, &mut runtime)
}

pub(crate) fn run_with_manifest_runtime(
    manifest_path: &Path,
    options: RunOptions,
    runtime: &mut dyn Runtime,
) -> FloeResult<RunOutcome> {
    init_thread_pool();
    let manifest_str = manifest_path.to_string_lossy();
    let location = config::resolve_config_location(&manifest_str)?;
    let json = std::fs::read_to_string(&location.path)?;
    let (config, report_base_uri) = crate::manifest::config_from_manifest_json(&json)?;
    let config_base = manifest_replay_config_base(&location.base, &json);
    config.select_entities(&options.entities)?;
    let context = RunContext::from_config(
        config,
        config_base,
        manifest_path,
        &report_base_uri,
        &options,
    )?;
    run_from_context(context, options, runtime)
}

/// Choose the path-resolution base for manifest replay.
///
/// A manifest is self-contained: its source/sink URIs are already resolved and it embeds a
/// `storages` block. When it is loaded from a **remote** URI, the manifest's own bucket/prefix
/// must NOT become the resolution base — otherwise a relative path in the manifest (e.g. an
/// Iceberg sink that did not resolve to an absolute URI) is re-based onto the manifest bucket
/// and trips the `storage <name> bucket mismatch` guard (issue #443). Instead, resolve local
/// paths under the target runtime's work-root (manifest `work_root` → `FLOE_WORK_ROOT` →
/// `RuntimeEnv::detect()` default) and leave `remote_base` unset so remote paths resolve purely
/// from their storage definitions.
///
/// For a **local** manifest file, behavior is unchanged: relative paths resolve against the
/// manifest file's directory, exactly as before.
pub(crate) fn manifest_replay_config_base(
    manifest_base: &config::ConfigBase,
    json: &str,
) -> config::ConfigBase {
    if manifest_base.remote_base().is_none() {
        return manifest_base.clone();
    }
    let (runtime_env, manifest_work_root) = crate::manifest::manifest_runtime(json);
    let work_root = manifest_work_root
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var("FLOE_WORK_ROOT")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .map(std::path::PathBuf::from)
        })
        .unwrap_or_else(|| runtime_env.work_root());
    // `local_from_path` uses the parent as the local base dir, so join a sentinel file name.
    config::ConfigBase::local_from_path(&work_root.join("manifest.json"))
}

pub fn run_with_runtime(
    config_path: &Path,
    config_base: config::ConfigBase,
    options: RunOptions,
    runtime: &mut dyn Runtime,
) -> FloeResult<RunOutcome> {
    init_thread_pool();
    let raw_config_env_vars = config::extract_raw_env_vars(config_path).unwrap_or_default();
    let profile_vars = options
        .profile
        .as_ref()
        .map(|p| {
            crate::resolve_vars(crate::VarSources {
                profile: &p.variables,
                cli: &std::collections::HashMap::new(),
                config: &raw_config_env_vars,
            })
        })
        .transpose()?
        .unwrap_or_default();
    let validate_options = ValidateOptions {
        entities: options.entities.clone(),
        profile_vars: profile_vars.clone(),
        profile_catalogs: options
            .profile
            .as_ref()
            .and_then(|profile| profile.catalogs.clone()),
        profile_storages: options
            .profile
            .as_ref()
            .and_then(|profile| profile.storages.clone()),
        profile_lineage: options
            .profile
            .as_ref()
            .and_then(|profile| profile.lineage.clone()),
    };
    crate::validate_with_base(config_path, config_base.clone(), validate_options)?;
    let context = RunContext::new(config_path, config_base, &options, profile_vars)?;
    context.config.select_entities(&options.entities)?;

    run_from_context(context, options, runtime)
}

fn run_from_context(
    context: RunContext,
    options: RunOptions,
    runtime: &mut dyn Runtime,
) -> FloeResult<RunOutcome> {
    let observer = default_observer();
    let perf_enabled = perf::phase_timing_enabled();
    let selected_entities = context.config.select_entities(&options.entities)?;
    if options.full_refresh {
        for entity in &selected_entities {
            if matches!(
                entity.sink.write_mode,
                config::WriteMode::MergeScd1 | config::WriteMode::MergeScd2
            ) {
                return Err(FloeError::config(format!(
                    "entity '{}': --full-refresh is not supported with write_mode '{}'",
                    entity.name,
                    entity.sink.write_mode.as_str()
                ))
                .into());
            }
        }
    }
    let resolution_mode = if options.dry_run {
        io::storage::inputs::ResolveInputsMode::ListOnly
    } else {
        io::storage::inputs::ResolveInputsMode::Download
    };
    if !options.dry_run {
        observer.on_event(RunEvent::RunStarted {
            run_id: context.run_id.clone(),
            config: context.config_path.display().to_string(),
            report_base: context.report_base_path.clone(),
            ts_ms: event_time_ms(),
        });
        crate::run::events::mark_run_started();
    }
    let resolve_start = perf_enabled.then(Instant::now);
    let plans = resolve_entity_plans(&context, runtime, &selected_entities, resolution_mode)?;
    if let Some(start) = resolve_start {
        perf::emit_perf_log(
            observer,
            &context.run_id,
            None,
            "perf_run_phase_timings",
            json!({
                "phase": "resolve_inputs",
                "elapsed_ms": start.elapsed().as_millis() as u64,
                "entity_count": selected_entities.len(),
                "mode": match resolution_mode {
                    io::storage::inputs::ResolveInputsMode::ListOnly => "list_only",
                    io::storage::inputs::ResolveInputsMode::Download => "download",
                },
            }),
        );
    }
    if options.dry_run {
        return create_dry_run_outcome(&context, plans);
    }

    let mut entity_outcomes = Vec::new();
    let mut abort_run = false;

    for plan in plans {
        let entity_name = plan.entity.id.clone();
        observer.on_event(RunEvent::EntityStarted {
            run_id: context.run_id.clone(),
            name: entity_name.clone(),
            ts_ms: event_time_ms(),
        });
        let entity_result = run_entity(&context, runtime, observer, plan);
        if let Err(err) = entity_result {
            observer.on_event(RunEvent::EntityFinished {
                run_id: context.run_id.clone(),
                name: entity_name,
                status: "failed".to_string(),
                files: 0,
                files_skipped: 0,
                rows: 0,
                accepted: 0,
                rejected: 0,
                warnings: 0,
                errors: 0,
                ts_ms: event_time_ms(),
            });
            return Err(err);
        }
        let EntityRunResult {
            outcome,
            abort_run: aborted,
        } = entity_result.unwrap();
        let report = &outcome.report;
        let (mut status, _) = report::compute_run_outcome(
            &report
                .files
                .iter()
                .map(|file| file.status)
                .collect::<Vec<_>>(),
        );
        status = upgrade_status_for_warnings(status, report.results.warnings_total);
        observer.on_event(RunEvent::EntityFinished {
            run_id: context.run_id.clone(),
            name: report.entity.name.clone(),
            status: run_status_str(status).to_string(),
            files: report.results.files_total,
            files_skipped: report.results.files_skipped,
            rows: report.results.rows_total,
            accepted: report.results.accepted_total,
            rejected: report.results.rejected_total,
            warnings: report.results.warnings_total,
            errors: report.results.errors_total,
            ts_ms: event_time_ms(),
        });
        entity_outcomes.push(outcome);
        abort_run = abort_run || aborted;
        if abort_run {
            break;
        }
    }
    let summary = build_run_summary(&context, &entity_outcomes);
    let written_summary_uri = if let Some(report_target) = &context.report_target {
        let summary_write_start = perf_enabled.then(Instant::now);
        let uri = write_summary_report(
            report_target,
            &context.run_id,
            &summary,
            runtime.storage(),
            &context.storage_resolver,
        )?;
        if let Some(start) = summary_write_start {
            perf::emit_perf_log(
                observer,
                &context.run_id,
                None,
                "perf_run_phase_timings",
                json!({
                    "phase": "write_summary_report",
                    "elapsed_ms": start.elapsed().as_millis() as u64,
                    "entity_count": entity_outcomes.len(),
                }),
            );
        }
        Some(uri)
    } else {
        None
    };
    let entity_report_uris: std::collections::HashMap<String, String> = entity_outcomes
        .iter()
        .filter_map(|outcome| {
            let uri = context.report_target.as_ref().map(|t| {
                t.join_relative(&report::ReportWriter::report_relative_path(
                    &context.run_id,
                    &context
                        .config
                        .entity_report_dir(&outcome.report.entity.name),
                ))
            })?;
            Some((outcome.report.entity.name.clone(), uri))
        })
        .collect();
    observer.on_event(RunEvent::RunFinished {
        run_id: context.run_id.clone(),
        status: run_status_str(summary.run.status).to_string(),
        exit_code: summary.run.exit_code,
        files: summary.results.files_total,
        files_skipped: summary.results.files_skipped,
        rows: summary.results.rows_total,
        accepted: summary.results.accepted_total,
        rejected: summary.results.rejected_total,
        warnings: summary.results.warnings_total,
        errors: summary.results.errors_total,
        summary_uri: written_summary_uri,
        report_base: context.report_base_path.clone(),
        entity_report_uris,
        ts_ms: event_time_ms(),
    });

    Ok(RunOutcome {
        run_id: context.run_id.clone(),
        report_base_path: context.report_base_path.clone(),
        entity_outcomes,
        summary,
        dry_run_previews: None,
    })
}

fn init_thread_pool() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        if std::env::var("RAYON_NUM_THREADS").is_ok() {
            return;
        }
        let cap = std::env::var("FLOE_MAX_THREADS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(4);
        let available = std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(1);
        let threads = available.min(cap).max(1);
        let _ = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build_global();
    });
}

fn resolve_entity_plans<'a>(
    context: &'a RunContext,
    runtime: &mut dyn Runtime,
    entities: &[&'a config::EntityConfig],
    resolution_mode: io::storage::inputs::ResolveInputsMode,
) -> FloeResult<Vec<EntityRunPlan<'a>>> {
    let mut plans = Vec::with_capacity(entities.len());
    for entity in entities {
        let input_adapter = runtime.input_adapter(entity.source.format.as_str())?;
        let resolved_targets = entity::resolve_entity_targets(&context.storage_resolver, entity)?;
        let needs_temp = matches!(
            resolution_mode,
            io::storage::inputs::ResolveInputsMode::Download
        ) && (resolved_targets.source.is_remote()
            || resolved_targets.accepted.is_remote()
            || resolved_targets
                .rejected
                .as_ref()
                .is_some_and(io::storage::Target::is_remote));
        let temp_dir = if needs_temp {
            Some(
                tempfile::TempDir::new()
                    .map_err(|err| FloeError::io(format!("tempdir failed: {err}")))?,
            )
        } else {
            None
        };
        let storage_client = Some(runtime.storage().client_for(
            &context.storage_resolver,
            resolved_targets.source.storage(),
            entity,
        )? as &dyn io::storage::StorageClient);
        let resolved_inputs = io::storage::ops::resolve_inputs(
            &context.config_dir,
            entity,
            input_adapter,
            &resolved_targets.source,
            resolution_mode,
            temp_dir.as_ref().map(|dir| dir.path()),
            storage_client,
        )?;
        plans.push(EntityRunPlan {
            entity,
            resolved_targets,
            resolved_inputs,
            temp_dir,
        });
    }
    Ok(plans)
}

fn build_run_summary(
    context: &RunContext,
    entity_outcomes: &[EntityOutcome],
) -> report::RunSummaryReport {
    let mut totals = report::ResultsTotals {
        files_total: 0,
        files_skipped: 0,
        rows_total: 0,
        accepted_total: 0,
        rejected_total: 0,
        warnings_total: 0,
        errors_total: 0,
    };
    let mut statuses = Vec::new();
    let mut entities = Vec::with_capacity(entity_outcomes.len());

    for outcome in entity_outcomes {
        let report = &outcome.report;
        totals.files_total += report.results.files_total;
        totals.files_skipped += report.results.files_skipped;
        totals.rows_total += report.results.rows_total;
        totals.accepted_total += report.results.accepted_total;
        totals.rejected_total += report.results.rejected_total;
        totals.warnings_total += report.results.warnings_total;
        totals.errors_total += report.results.errors_total;

        let file_statuses = report
            .files
            .iter()
            .map(|file| file.status)
            .collect::<Vec<_>>();
        let (mut status, _) = report::compute_run_outcome(&file_statuses);
        status = upgrade_status_for_warnings(status, report.results.warnings_total);
        statuses.extend(file_statuses);

        let report_file = context
            .report_target
            .as_ref()
            .map(|target| {
                target.join_relative(&report::ReportWriter::report_relative_path(
                    &context.run_id,
                    &context.config.entity_report_dir(&report.entity.name),
                ))
            })
            .unwrap_or_else(|| "disabled".to_string());
        entities.push(report::EntitySummary {
            name: report.entity.name.clone(),
            status,
            results: report.results.clone(),
            report_file,
        });
    }

    let (mut status, exit_code) = report::compute_run_outcome(&statuses);
    status = upgrade_status_for_warnings(status, totals.warnings_total);

    let finished_at = report::now_rfc3339();
    let duration_ms = context.run_timer.elapsed().as_millis() as u64;
    let report_base_path = context
        .report_base_path
        .clone()
        .unwrap_or_else(|| "disabled".to_string());
    let report_file = context
        .report_target
        .as_ref()
        .map(|target| {
            target.join_relative(&report::ReportWriter::summary_relative_path(
                &context.run_id,
            ))
        })
        .unwrap_or_else(|| "disabled".to_string());

    report::RunSummaryReport {
        spec_version: context.config.version.clone(),
        tool: build_tool_info(),
        run: report::RunInfo {
            run_id: context.run_id.clone(),
            started_at: context.started_at.clone(),
            finished_at,
            duration_ms,
            status,
            exit_code,
        },
        config: build_config_echo(context),
        report: build_report_echo(report_base_path, report_file),
        results: totals,
        entities,
    }
}

fn create_dry_run_outcome(
    context: &RunContext,
    plans: Vec<EntityRunPlan<'_>>,
) -> FloeResult<RunOutcome> {
    let mut previews: Vec<DryRunEntityPreview> = Vec::new();

    for plan in plans {
        let entity = plan.entity;
        let rejected_path = entity.sink.rejected.as_ref().map(|r| r.path.clone());
        let rejected_format = entity.sink.rejected.as_ref().map(|r| r.format.clone());
        let (archive_path, archive_storage) = entity
            .sink
            .archive
            .as_ref()
            .map(|a| (a.path.clone(), a.storage.clone()))
            .unwrap_or_else(|| (String::new(), None));

        let report_file = context.report_target.as_ref().map(|target| {
            target.join_relative(&report::ReportWriter::report_relative_path(
                &context.run_id,
                &entity.id_path(),
            ))
        });

        previews.push(DryRunEntityPreview {
            name: entity.id.clone(),
            input_path: entity.source.path.clone(),
            input_format: entity.source.format.clone(),
            accepted_path: entity.sink.accepted.path.clone(),
            accepted_format: entity.sink.accepted.format.clone(),
            rejected_path,
            rejected_format,
            archive_path,
            archive_storage,
            report_file,
            scanned_files: plan.resolved_inputs.listed,
        });
    }

    Ok(RunOutcome {
        run_id: context.run_id.clone(),
        report_base_path: context.report_base_path.clone(),
        entity_outcomes: Vec::new(),
        summary: report::RunSummaryReport {
            spec_version: context.config.version.clone(),
            tool: build_tool_info(),
            run: report::RunInfo {
                run_id: context.run_id.clone(),
                started_at: context.started_at.clone(),
                finished_at: report::now_rfc3339(),
                duration_ms: 0,
                status: report::RunStatus::Success,
                exit_code: 0,
            },
            config: build_config_echo(context),
            report: build_report_echo(
                context
                    .report_base_path
                    .clone()
                    .unwrap_or_else(|| "disabled".to_string()),
                "disabled (dry-run)".to_string(),
            ),
            results: report::ResultsTotals {
                files_total: 0,
                files_skipped: 0,
                rows_total: 0,
                accepted_total: 0,
                rejected_total: 0,
                warnings_total: 0,
                errors_total: 0,
            },
            entities: Vec::new(),
        },
        dry_run_previews: Some(previews),
    })
}

fn upgrade_status_for_warnings(status: report::RunStatus, warnings: u64) -> report::RunStatus {
    if status == report::RunStatus::Success && warnings > 0 {
        report::RunStatus::SuccessWithWarnings
    } else {
        status
    }
}

fn build_tool_info() -> report::ToolInfo {
    report::ToolInfo {
        name: "floe".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        git: None,
    }
}

fn build_config_echo(context: &RunContext) -> report::ConfigEcho {
    report::ConfigEcho {
        path: context.config_path.display().to_string(),
        version: context.config.version.clone(),
        metadata: context.config.metadata.as_ref().map(project_metadata_json),
    }
}

fn build_report_echo(path: String, report_file: String) -> report::ReportEcho {
    report::ReportEcho { path, report_file }
}

fn run_status_str(status: report::RunStatus) -> &'static str {
    match status {
        report::RunStatus::Success => "success",
        report::RunStatus::SuccessWithWarnings => "success_with_warnings",
        report::RunStatus::Rejected => "rejected",
        report::RunStatus::Aborted => "aborted",
        report::RunStatus::Failed => "failed",
    }
}
