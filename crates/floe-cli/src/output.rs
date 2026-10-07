use std::path::Path;

use floe_core::{report, DryRunEntityPreview, EntityOutcome, RunOutcome};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    Default,
    Quiet,
    Verbose,
}

const MAX_PREVIEW_FILES: usize = 50;

pub fn format_run_output(outcome: &RunOutcome, mode: OutputMode, dry_run: bool) -> String {
    let mut lines = Vec::new();
    if dry_run {
        lines.push("DRY RUN MODE - No actual execution performed".to_string());
    }
    if mode != OutputMode::Quiet {
        lines.push(format!("run id: {}", outcome.run_id));
        lines.push(format!(
            "report base: {}",
            outcome.report_base_path.as_deref().unwrap_or("(disabled)")
        ));
        lines.push(String::new());
    }

    if dry_run {
        if let Some(previews) = &outcome.dry_run_previews {
            if mode != OutputMode::Quiet {
                for (idx, preview) in previews.iter().enumerate() {
                    if idx > 0 {
                        lines.push(String::new());
                    }
                    lines.extend(format_dry_run_preview(preview, mode));
                }
                lines.push(String::new());
            }
            lines.extend(format_run_summary(outcome, mode == OutputMode::Quiet));
            return lines.join("\n");
        }
    }

    if mode != OutputMode::Quiet {
        for (idx, entity) in outcome.entity_outcomes.iter().enumerate() {
            if idx > 0 {
                lines.push(String::new());
            }
            lines.extend(format_entity_output(entity, mode));
        }
        lines.push(String::new());
    }

    lines.extend(format_run_summary(outcome, mode == OutputMode::Quiet));

    lines.join("\n")
}

fn format_dry_run_preview(preview: &DryRunEntityPreview, mode: OutputMode) -> Vec<String> {
    let mut lines = Vec::new();
    lines.push(format!(
        "==> entity {} (format={})",
        preview.name, preview.input_format
    ));
    if mode != OutputMode::Quiet {
        lines.push(format!(
            "  Input: {} ({})",
            preview.input_path, preview.input_format
        ));
        lines.push(format!(
            "  Accepted Output: {} ({})",
            preview.accepted_path, preview.accepted_format
        ));
        if let (Some(format), Some(path)) = (&preview.rejected_format, &preview.rejected_path) {
            lines.push(format!("  Rejected Output: {} ({})", path, format));
        } else {
            lines.push("  Rejected Output: -".to_string());
        }
        if !preview.archive_path.is_empty() {
            lines.push(format!(
                "  Archive Path: {} ({})",
                preview.archive_path,
                preview.archive_storage.as_deref().unwrap_or("default")
            ));
        }
        let total = preview.scanned_files.len();
        lines.push(format!("  Resolved Inputs: {}", total));
        lines.push("  Resolved Files:".to_string());
        let max = if mode == OutputMode::Verbose {
            total
        } else {
            MAX_PREVIEW_FILES.min(total)
        };
        for file in preview.scanned_files.iter().take(max) {
            lines.push(format!("    {}", file));
        }
        if mode != OutputMode::Verbose && total > max {
            lines.push(format!("    ... {} more", total - max));
        }
    }

    // Verbose-only extras
    if mode == OutputMode::Verbose {
        if let Some(report_file) = &preview.report_file {
            lines.push(format!("  report: {}", report_file));
        }
    }
    lines
}

fn format_entity_output(entity: &EntityOutcome, mode: OutputMode) -> Vec<String> {
    let mut lines = Vec::new();
    let report = &entity.report;
    lines.push(format!(
        "==> entity {} (severity={}, format={})",
        report.entity.name,
        format_severity(report.policy.severity),
        report.source.format
    ));

    if mode == OutputMode::Verbose {
        lines.push(format!(
            "  source: {} ({})",
            report.source.format, report.source.path
        ));
        lines.push(format!(
            "  sink accepted: {} ({})",
            report.sink.accepted.format, report.sink.accepted.path
        ));
        if let Some(rejected) = &report.sink.rejected {
            lines.push(format!(
                "  sink rejected: {} ({})",
                rejected.format, rejected.path
            ));
        }
        if let Some(cast_mode) = &report.source.cast_mode {
            lines.push(format!("  cast_mode: {}", cast_mode));
        }
        if !report.source.resolved_inputs.files.is_empty() {
            lines.push("  inputs:".to_string());
            for file in &report.source.resolved_inputs.files {
                lines.push(format!("    {}", file));
            }
        }
    }

    for (file_report, elapsed_ms) in report.files.iter().zip(entity.file_timings_ms.iter()) {
        lines.push(format_file_line(file_report, *elapsed_ms));
    }

    if report.files.len() > entity.file_timings_ms.len() {
        for file_report in report.files.iter().skip(entity.file_timings_ms.len()) {
            lines.push(format_file_line(file_report, None));
        }
    }

    lines
}

fn format_file_line(file: &report::FileReport, elapsed_ms: Option<u64>) -> String {
    let mut line = format!(
        "  {} {}",
        format_file_status(file.status),
        short_path(&file.input_file),
    );
    if let Some(ref reason) = file.skip_reason {
        line.push_str(&format!(" reason={reason}"));
    }
    line.push_str(&format!(
        " rows={} accepted={} rejected={}",
        file.row_count, file.accepted_count, file.rejected_count
    ));
    if let Some(ms) = elapsed_ms {
        line.push_str(&format!(" elapsed_ms={ms}"));
    }
    line.push_str(&format!(
        " accepted_out={} rejected_out={}",
        short_optional_path(&file.output.accepted_path),
        short_optional_path(&file.output.rejected_path)
    ));
    line
}

fn format_run_summary(outcome: &RunOutcome, include_run_info: bool) -> Vec<String> {
    let mut lines = Vec::new();
    if include_run_info {
        lines.push(format!("run id: {}", outcome.run_id));
        lines.push(format!(
            "report base: {}",
            outcome.report_base_path.as_deref().unwrap_or("(disabled)")
        ));
    }
    {
        let skipped = outcome.summary.results.files_skipped;
        let skipped_part = if skipped > 0 {
            format!(" skipped={skipped}")
        } else {
            String::new()
        };
        lines.push(format!(
            "Totals: files={} rows={} accepted={} rejected={}{}",
            outcome.summary.results.files_total,
            outcome.summary.results.rows_total,
            outcome.summary.results.accepted_total,
            outcome.summary.results.rejected_total,
            skipped_part
        ));
    }
    lines.push(format!(
        "Overall: {} (exit_code={})",
        format_run_status(outcome.summary.run.status),
        outcome.summary.run.exit_code
    ));
    lines.push(format!(
        "Run summary: {}",
        run_summary_path(&outcome.run_id, outcome.report_base_path.as_deref())
    ));
    lines
}

fn run_summary_path(run_id: &str, report_base_path: Option<&str>) -> String {
    let Some(report_base_path) = report_base_path else {
        return "(disabled)".to_string();
    };
    let run_dir = report::ReportWriter::run_dir_name(run_id);
    Path::new(report_base_path)
        .join(run_dir)
        .join("run.summary.json")
        .display()
        .to_string()
}

fn format_file_status(status: report::FileStatus) -> &'static str {
    match status {
        report::FileStatus::Success => "SUCCESS",
        report::FileStatus::Rejected => "REJECTED",
        report::FileStatus::Aborted => "ABORTED",
        report::FileStatus::Failed => "FAILED",
        report::FileStatus::Skipped => "SKIPPED",
    }
}

fn format_run_status(status: report::RunStatus) -> &'static str {
    match status {
        report::RunStatus::Success => "success",
        report::RunStatus::SuccessWithWarnings => "success_with_warnings",
        report::RunStatus::Rejected => "rejected",
        report::RunStatus::Aborted => "aborted",
        report::RunStatus::Failed => "failed",
    }
}

fn format_severity(severity: report::Severity) -> &'static str {
    match severity {
        report::Severity::Warn => "warn",
        report::Severity::Reject => "reject",
        report::Severity::Abort => "abort",
    }
}

fn short_optional_path(path: &Option<String>) -> String {
    path.as_deref()
        .map(short_path)
        .unwrap_or_else(|| "-".to_string())
}

fn short_path(path: &str) -> String {
    let trimmed = path.trim_end_matches(std::path::MAIN_SEPARATOR);
    Path::new(trimmed)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(trimmed)
        .to_string()
}
