use clap::{Parser, Subcommand, ValueEnum};
use floe_core::{
    add_entity_to_config, build_common_manifest_json, inspect_entity_state,
    load_config_with_profile_overrides, parse_profile, reset_entity_state, resolve_config_location,
    run_with_base, run_with_manifest_path, set_observer, validate_profile, validate_with_base,
    AddEntityOptions, FloeResult, MultiObserver, RunEvent, RunOptions, ValidateOptions,
};
use std::io::Write;

use crate::logging::LogFormat;

const VERSION: &str = env!("FLOE_VERSION");
const ROOT_LONG_ABOUT: &str = concat!(
    "Floe is a single-node, config-driven ingestion runner. It loads one YAML config\n",
    "and executes each entity in order, producing per-entity and summary reports.\n",
    "\n",
    "Config (v",
    env!("CARGO_PKG_VERSION"),
    ") structure:\n",
    "  version\n",
    "  metadata\n",
    "  report.path\n",
    "  entities[]\n",
    "\n",
    "entity:\n",
    "  name\n",
    "  metadata\n",
    "  source { format, path, options, cast_mode }\n",
    "  sink { accepted, rejected }\n",
    "  policy { severity }\n",
    "  schema { normalize_columns, columns[] }\n",
);

const RUN_LONG_ABOUT: &str = r#"Run all configured entities sequentially (or restrict with --entities).

Example config snippet:
  report:
    path: ./reports
  entities:
    - name: customers
      source: { format: csv, path: ./data/customers.csv }
      sink:
        accepted: { format: parquet, path: ./out/customers.parquet }

Example:
  floe run -c example/config.yml

Reports are written to:
  <report.path>/run_<run_id>/run.summary.json
  <report.path>/run_<run_id>/<entity.name>/run.json
"#;

const VALIDATE_LONG_ABOUT: &str = concat!(
    "Validate a configuration file before running.\n",
    "\n",
    "Validation checks:\n",
    "  - YAML parsing\n",
    "  - schema validation against the v",
    env!("CARGO_PKG_VERSION"),
    " structure\n",
    "\n",
    "Example:\n",
    "  floe validate -c example/config.yml\n",
);

const MANIFEST_LONG_ABOUT: &str = concat!(
    "Generate orchestrator manifest JSON from a validated Floe config.\n",
    "\n",
    "Examples:\n",
    "  floe manifest generate -c example/config.yml --output manifest.common.json\n",
    "  floe manifest generate -c example/config.yml --output -\n",
);

const ADD_ENTITY_LONG_ABOUT: &str = concat!(
    "Infer schema from an input file and append a new entity to an existing Floe YAML config.\n",
    "\n",
    "v0.3 scope:\n",
    "  - Supports csv, json, parquet input inference\n",
    "  - JSON inference uses top-level keys only (nested values are inferred as string)\n",
    "  - Creates a minimal config when -c points to a missing file\n",
    "  - Infers --format from file extension when omitted (.csv, .json, .parquet)\n",
    "  - Infers --name from the input filename stem when omitted\n",
    "\n",
    "Example:\n",
    "  floe add-entity -c config.yml --input ./in/customers.csv\n",
);

const STATE_LONG_ABOUT: &str = concat!(
    "Inspect or intentionally reset per-entity incremental state.\n",
    "\n",
    "Examples:\n",
    "  floe state inspect -c example/config.yml --entity customers\n",
    "  floe state inspect -c example/config.yml -p profiles/local.yml --entity customers\n",
    "  floe state reset -c example/config.yml --entity customers --yes\n",
    "  floe state reset -c example/config.yml -p profiles/local.yml --entity customers --yes\n",
);

mod delegate;
mod logging;
mod manifest_output;
mod output;

#[derive(Parser, Debug)]
#[command(
    name = "floe",
    version = VERSION,
    about = "YAML-driven technical ingestion tool",
    long_about = ROOT_LONG_ABOUT
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    #[command(about = "Validate a config file", long_about = VALIDATE_LONG_ABOUT)]
    Validate {
        #[arg(short, long, help = "Path or URI to the Floe config file")]
        config: Option<String>,
        #[arg(
            long,
            value_delimiter = ',',
            help = "Comma-separated list of entity names"
        )]
        entities: Vec<String>,
        #[arg(
            short = 'p',
            long,
            help = "Optional path to a Floe environment profile YAML file"
        )]
        profile: Option<String>,
    },
    #[command(about = "Run the ingestion pipeline", long_about = RUN_LONG_ABOUT)]
    Run {
        #[arg(
            short,
            long,
            conflicts_with = "manifest",
            help = "Path or URI to the Floe config file"
        )]
        config: Option<String>,
        #[arg(
            long,
            conflicts_with = "config",
            help = "Path to a self-contained Floe manifest JSON file"
        )]
        manifest: Option<String>,
        #[arg(long, help = "Optional run id (defaults to a generated value)")]
        run_id: Option<String>,
        #[arg(
            long,
            value_delimiter = ',',
            help = "Comma-separated list of entity names"
        )]
        entities: Vec<String>,
        #[arg(
            long,
            conflicts_with = "verbose",
            help = "Suppress non-essential output"
        )]
        quiet: bool,
        #[arg(long, conflicts_with = "quiet", help = "Enable verbose output")]
        verbose: bool,
        #[arg(
            long,
            value_enum,
            default_value_t = LogFormat::Off,
            help = "Log format for run events (off|text|json)"
        )]
        log_format: LogFormat,
        #[arg(long, help = "Resolve and print inputs/outputs without executing")]
        dry_run: bool,
        #[arg(
            long,
            short = 'F',
            help = "Bypass incremental state and reprocess all input files. Forces overwrite on append-mode sinks. Incompatible with merge_scd1/merge_scd2."
        )]
        full_refresh: bool,
        #[arg(
            short = 'p',
            long,
            help = "Optional path to a Floe environment profile YAML file"
        )]
        profile: Option<String>,
    },
    #[command(
        about = "Generate orchestrator manifest JSON",
        long_about = MANIFEST_LONG_ABOUT
    )]
    Manifest {
        #[command(subcommand)]
        command: ManifestCommand,
    },
    #[command(
        about = "Infer an entity schema from an input file and append it to a config",
        long_about = ADD_ENTITY_LONG_ABOUT
    )]
    AddEntity {
        #[arg(short, long, help = "Path to the Floe config file to update")]
        config: String,
        #[arg(long, help = "Input file path or file:// URI to infer schema from")]
        input: String,
        #[arg(
            long,
            value_enum,
            help = "Input format (csv|json|parquet). If omitted, infer from file extension"
        )]
        format: Option<AddEntityFormat>,
        #[arg(long, help = "Entity name (defaults to input file stem)")]
        name: Option<String>,
        #[arg(long, help = "Optional entity domain to set")]
        domain: Option<String>,
        #[arg(
            long,
            help = "Write updated config to a new path instead of overwriting -c"
        )]
        output: Option<String>,
        #[arg(long, help = "Print the updated YAML without writing the file")]
        dry_run: bool,
    },
    #[command(about = "Inspect or reset entity incremental state", long_about = STATE_LONG_ABOUT)]
    State {
        #[command(subcommand)]
        command: StateCommand,
    },
}

#[derive(Subcommand, Debug)]
enum StateCommand {
    #[command(about = "Inspect entity state")]
    Inspect {
        #[arg(short, long, help = "Path or URI to the Floe config file")]
        config: String,
        #[arg(long, help = "Entity name")]
        entity: String,
        #[arg(
            short = 'p',
            long,
            help = "Optional path to a Floe environment profile YAML file"
        )]
        profile: Option<String>,
    },
    #[command(about = "Reset entity state")]
    Reset {
        #[arg(short, long, help = "Path or URI to the Floe config file")]
        config: String,
        #[arg(long, help = "Entity name")]
        entity: String,
        #[arg(long, help = "Required confirmation to delete the state object")]
        yes: bool,
        #[arg(
            short = 'p',
            long,
            help = "Optional path to a Floe environment profile YAML file"
        )]
        profile: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
enum ManifestCommand {
    #[command(about = "Generate a manifest JSON file")]
    Generate {
        #[arg(short, long, help = "Path or URI to the Floe config file")]
        config: String,
        #[arg(
            short,
            long,
            help = "Output path or URI for manifest JSON, or '-' for stdout. \
                    Accepts local paths or remote URIs (s3://, gs://, abfs://). \
                    When a remote URI is given it is also baked into execution.base_args \
                    as the manifest URI that orchestrators use at run time."
        )]
        output: String,
        #[arg(
            long,
            value_delimiter = ',',
            help = "Optional comma-separated list of entity names"
        )]
        entities: Vec<String>,
        #[arg(
            short = 'p',
            long,
            help = "Optional path to a Floe environment profile YAML file"
        )]
        profile: Option<String>,
        #[arg(
            long,
            help = "Produce a fully deterministic manifest (sets generated_at_ts_ms=0); \
                    same config + profile + Floe version always produce identical output"
        )]
        deterministic: bool,
        #[arg(
            long,
            help = "Stable logical name for this manifest, e.g. 'sales.prod'. \
                    Stored as manifest_name in the JSON output."
        )]
        manifest_name: Option<String>,
        #[arg(
            long,
            help = "Default domain applied to entities that have no explicit domain field. \
                    Sets domain, group_name, and asset_key prefix for those entities."
        )]
        default_domain: Option<String>,
        #[arg(
            long,
            default_value = "default",
            help = "Path field mode in source/sink entries. \
                    'default': keep the original config path. \
                    'resolved-uri': set path=uri when resolved=true, making the manifest \
                    self-contained for remote replay."
        )]
        manifest_path_mode: String,
        #[arg(
            long,
            value_enum,
            help = "Runtime this manifest targets. \
                    'image': record local config/profile URIs absolute under the container \
                    work-root (local:///work/...), portable for remote replay and reproducible \
                    across containers. \
                    'cli': keep them relative/as-typed for cross-checkout reproducible ids. \
                    Omitted: auto-detect (container -> image, else cli; override with FLOE_RUNTIME)."
        )]
        runtime: Option<RuntimeArg>,
    },
}

/// CLI surface for `floe_core::RuntimeEnv`; `None` means auto-detect.
#[derive(Clone, Copy, Debug, ValueEnum)]
enum RuntimeArg {
    Image,
    Cli,
}

impl RuntimeArg {
    fn into_runtime_env(self) -> floe_core::RuntimeEnv {
        match self {
            RuntimeArg::Image => floe_core::RuntimeEnv::Image,
            RuntimeArg::Cli => floe_core::RuntimeEnv::Cli,
        }
    }
}

#[derive(Clone, Debug, ValueEnum)]
enum AddEntityFormat {
    Csv,
    Json,
    Parquet,
}

impl AddEntityFormat {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Json => "json",
            Self::Parquet => "parquet",
        }
    }
}

fn main() -> FloeResult<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Validate {
            config,
            entities,
            profile,
        } => {
            let parsed_profile = if let Some(ref profile_input) = profile {
                let profile_location = match resolve_config_location(profile_input) {
                    Ok(loc) => loc,
                    Err(err) => exit_with_error(err),
                };
                let parsed = match parse_profile(&profile_location.path) {
                    Ok(p) => p,
                    Err(err) => exit_with_error(err),
                };
                if let Err(err) = validate_profile(&parsed) {
                    exit_with_error(err);
                }
                Some(parsed)
            } else {
                None
            };

            let Some(config) = config else {
                match parsed_profile {
                    Some(profile) => {
                        println!("Profile valid: {}", profile.metadata.name);
                        println!("Schema: {}/{}", profile.api_version, profile.kind);
                        println!(
                            "Catalogs: {}",
                            profile
                                .catalogs
                                .as_ref()
                                .map(|catalogs| catalogs.definitions.len())
                                .unwrap_or(0)
                        );
                        let _ = std::io::stdout().lock().flush();
                        return Ok(());
                    }
                    None => {
                        exit_with_error(Box::new(floe_core::FloeError::config(
                            "floe validate requires --config unless --profile is provided"
                                .to_string(),
                        )));
                    }
                }
            };

            let config_location = match resolve_config_location(&config) {
                Ok(location) => location,
                Err(err) => exit_with_error(err),
            };

            let profile_vars = if let Some(ref parsed) = parsed_profile {
                let config_env_vars =
                    floe_core::extract_config_env_vars(&config_location.path).unwrap_or_default();
                match floe_core::resolve_vars(floe_core::VarSources {
                    profile: &parsed.variables,
                    cli: &std::collections::HashMap::new(),
                    config: &config_env_vars,
                }) {
                    Ok(vars) => vars,
                    Err(err) => exit_with_error(err),
                }
            } else {
                std::collections::HashMap::new()
            };

            let options = ValidateOptions {
                entities: entities.clone(),
                profile_vars: profile_vars.clone(),
                profile_catalogs: parsed_profile
                    .as_ref()
                    .and_then(|profile| profile.catalogs.clone()),
                profile_storages: parsed_profile
                    .as_ref()
                    .and_then(|profile| profile.storages.clone()),
                profile_lineage: parsed_profile
                    .as_ref()
                    .and_then(|profile| profile.lineage.clone()),
            };

            let validation_result =
                validate_with_base(&config_location.path, config_location.base.clone(), options);

            match validation_result {
                Ok(()) => {
                    let config = load_config_with_profile_overrides(
                        &config_location.path,
                        &profile_vars,
                        parsed_profile
                            .as_ref()
                            .and_then(|profile| profile.catalogs.as_ref()),
                        parsed_profile
                            .as_ref()
                            .and_then(|profile| profile.storages.as_ref()),
                        parsed_profile
                            .as_ref()
                            .and_then(|profile| profile.lineage.as_ref()),
                    )?;
                    println!("Config valid: {}", config_location.display);
                    println!("Version: {}", config.version);
                    println!(
                        "Report: {}",
                        config
                            .report
                            .as_ref()
                            .map(|report| report.path.as_str())
                            .unwrap_or("(disabled)")
                    );
                    println!("Entities: {}", config.entities.len());
                    for entity in &config.entities {
                        println!("Entity: {}", entity.name);
                        println!(
                            "  Source: {} ({})",
                            entity.source.format, entity.source.path
                        );
                        println!(
                            "  Sink accepted: {} ({})",
                            entity.sink.accepted.format, entity.sink.accepted.path
                        );
                        if let Some(rejected) = &entity.sink.rejected {
                            println!("  Sink rejected: {} ({})", rejected.format, rejected.path);
                        }
                        println!("  Severity: {}", entity.policy.severity);
                    }
                    if let Some(ref p) = profile {
                        let profile_display = std::fs::canonicalize(p)
                            .map(|c| c.display().to_string())
                            .unwrap_or_else(|_| p.clone());
                        println!(
                            "Next: floe run -c {} -p {}",
                            config_location.display, profile_display
                        );
                    } else {
                        println!("Next: floe run -c {}", config_location.display);
                    }
                    let _ = std::io::stdout().lock().flush();
                    Ok(())
                }
                Err(err) => exit_with_error(err),
            }
        }
        Command::Run {
            config,
            manifest,
            run_id,
            entities,
            quiet,
            verbose,
            log_format,
            dry_run,
            full_refresh,
            profile,
        } => {
            let started_at = floe_core::report::now_rfc3339();
            let computed_run_id =
                run_id.unwrap_or_else(|| floe_core::report::run_id_from_timestamp(&started_at));

            // Build log observer early so failure paths can emit directly to it.
            let log_obs = logging::build_log_observer(log_format.clone());

            if let Some(manifest_path_str) = manifest {
                // --- Manifest mode: no config file, no profile ---
                let manifest_path = std::path::PathBuf::from(&manifest_path_str);

                // Re-exec the floe-duckdb companion before any work when this lean
                // build is asked to run a DuckDB sink (no-op in the full build).
                // Dry runs never reach the DuckDB writer (run resolves inputs and
                // returns previews before any sink write), so they run on the lean
                // build without the companion.
                if !dry_run {
                    if let Some((cfg, _)) = floe_core::read_manifest_text(&manifest_path_str)
                        .ok()
                        .and_then(|json| floe_core::config_from_manifest_json(&json).ok())
                    {
                        if let Err(err) = delegate::maybe_delegate_duckdb(&cfg) {
                            exit_with_error(err);
                        }
                    }
                }

                // Wire up lineage from manifest's embedded lineage block.
                // Use read_manifest_text so remote URIs (s3://, gs://, abfs://) are
                // downloaded before parsing; std::fs::read_to_string would fail silently.
                // Pass the manifest's reconstructed entities to the observer so
                // replay emits the same source/sink dataset lineage as a direct
                // config run; an empty entity slice yields empty inputs/outputs
                // and no lineage edge (issue #455).
                let lineage_observer = floe_core::read_manifest_text(&manifest_path_str)
                    .ok()
                    .and_then(|json| {
                        match floe_core::lineage::build_observer_from_manifest_json(&json, "") {
                            Ok(obs) => obs,
                            Err(err) => {
                                eprintln!("Warning: lineage observer disabled: {err}");
                                None
                            }
                        }
                    });
                let mut obs_vec = Vec::new();
                let has_log_obs = log_obs.is_some();
                if let Some(log) = log_obs {
                    obs_vec.push(log);
                }
                if let Some(lin) = lineage_observer {
                    obs_vec.push(lin);
                }
                if !has_log_obs && !obs_vec.is_empty() {
                    obs_vec.push(logging::build_warn_sink());
                }
                if !obs_vec.is_empty() {
                    let _ = set_observer(std::sync::Arc::new(MultiObserver::new(obs_vec)));
                }

                let options = RunOptions {
                    run_id: Some(computed_run_id.clone()),
                    entities,
                    dry_run,
                    full_refresh,
                    profile: None,
                };

                let outcome = match run_with_manifest_path(&manifest_path, options) {
                    Ok(outcome) => outcome,
                    Err(err) => {
                        if !floe_core::run::events::is_run_started() {
                            floe_core::run::events::default_observer().on_event(
                                RunEvent::RunStarted {
                                    run_id: computed_run_id.clone(),
                                    config: manifest_path_str.clone(),
                                    report_base: None,
                                    ts_ms: floe_core::run::events::event_time_ms(),
                                },
                            );
                        }
                        logging::emit_failed_run_events(&computed_run_id, err.as_ref());
                        let mut err_out = std::io::stderr().lock();
                        let _ = writeln!(err_out, "Error: {err}");
                        let _ = err_out.flush();
                        std::process::exit(1);
                    }
                };
                let mode = if quiet {
                    output::OutputMode::Quiet
                } else if verbose {
                    output::OutputMode::Verbose
                } else {
                    output::OutputMode::Default
                };
                let summary = output::format_run_output(&outcome, mode, dry_run);
                match log_format {
                    LogFormat::Json => {
                        let mut err = std::io::stderr().lock();
                        let _ = writeln!(err, "{summary}");
                        let _ = err.flush();
                    }
                    LogFormat::Text | LogFormat::Off => {
                        let mut out = std::io::stdout().lock();
                        let _ = writeln!(out, "{summary}");
                        let _ = out.flush();
                    }
                }
                std::process::exit(outcome.summary.run.exit_code);
            }

            // --- Config mode (default) ---
            let config = match config {
                Some(c) => c,
                None => {
                    let mut err_out = std::io::stderr().lock();
                    let _ = writeln!(err_out, "Error: one of --config or --manifest is required");
                    let _ = err_out.flush();
                    std::process::exit(1);
                }
            };

            let profile_config = if let Some(ref profile_input) = profile {
                let profile_location = match resolve_config_location(profile_input) {
                    Ok(loc) => loc,
                    Err(err) => {
                        logging::emit_failed_run_events(&computed_run_id, err.as_ref());
                        let mut err_out = std::io::stderr().lock();
                        let _ = writeln!(err_out, "Error: {err}");
                        let _ = err_out.flush();
                        std::process::exit(1);
                    }
                };
                let parsed = match parse_profile(&profile_location.path) {
                    Ok(p) => p,
                    Err(err) => {
                        logging::emit_failed_run_events(&computed_run_id, err.as_ref());
                        let mut err_out = std::io::stderr().lock();
                        let _ = writeln!(err_out, "Error: {err}");
                        let _ = err_out.flush();
                        std::process::exit(1);
                    }
                };
                if let Err(err) = validate_profile(&parsed) {
                    logging::emit_failed_run_events(&computed_run_id, err.as_ref());
                    let mut err_out = std::io::stderr().lock();
                    let _ = writeln!(err_out, "Error: {err}");
                    let _ = err_out.flush();
                    std::process::exit(1);
                }
                Some(parsed)
            } else {
                None
            };

            // Extract profile vars before moving profile_config into RunOptions.
            let profile_vars_for_lineage = profile_config
                .as_ref()
                .map(|p| p.variables.clone())
                .unwrap_or_default();

            let options = RunOptions {
                run_id: Some(computed_run_id.clone()),
                entities,
                dry_run,
                full_refresh,
                profile: profile_config,
            };

            let config_location = match resolve_config_location(&config) {
                Ok(location) => location,
                Err(err) => {
                    // set_observer not called yet; emit directly to log_obs.
                    if let Some(ref obs) = log_obs {
                        logging::emit_failed_run_events_to(
                            obs.as_ref(),
                            &computed_run_id,
                            err.as_ref(),
                        );
                    }
                    let mut err_out = std::io::stderr().lock();
                    let _ = writeln!(err_out, "Error: {err}");
                    let _ = err_out.flush();
                    std::process::exit(1);
                }
            };

            // Resolve inter-variable references (e.g. "${HOST}/api") using the
            // same resolve_vars path the runner uses, now that we have config_location.
            let profile_vars_for_lineage = {
                let config_env_vars =
                    floe_core::extract_config_env_vars(&config_location.path).unwrap_or_default();
                floe_core::resolve_vars(floe_core::VarSources {
                    profile: &profile_vars_for_lineage,
                    cli: &std::collections::HashMap::new(),
                    config: &config_env_vars,
                })
                .unwrap_or(profile_vars_for_lineage)
            };

            // Load config early to check for lineage block so we can install
            // a composed observer before run_with_base. Apply profile vars so
            // that {{VAR}} placeholders in lineage.url / lineage.api_key are expanded,
            // and apply profile overrides so that a lineage block defined only in the
            // profile is visible here.
            let early_config = load_config_with_profile_overrides(
                &config_location.path,
                &profile_vars_for_lineage,
                options.profile.as_ref().and_then(|p| p.catalogs.as_ref()),
                options.profile.as_ref().and_then(|p| p.storages.as_ref()),
                options.profile.as_ref().and_then(|p| p.lineage.as_ref()),
            );
            // Re-exec the floe-duckdb companion before any work when this lean
            // build is asked to run a DuckDB sink (no-op in the full build).
            // Dry runs never reach the DuckDB writer (run resolves inputs and
            // returns previews before any sink write), so they run on the lean
            // build without the companion.
            if !dry_run {
                if let Ok(ref cfg) = early_config {
                    if let Err(err) = delegate::maybe_delegate_duckdb(cfg) {
                        exit_with_error(err);
                    }
                }
            }

            let lineage_observer = early_config
                .as_ref()
                .ok()
                .and_then(|c| c.lineage.as_ref().map(|l| (l, c.entities.as_slice())))
                .and_then(|(lineage_cfg, entities)| {
                    match floe_core::lineage::build_observer(
                        lineage_cfg,
                        entities,
                        config_location.path.to_str().unwrap_or(""),
                    ) {
                        Ok(obs) => Some(obs),
                        Err(err) => {
                            eprintln!("Warning: lineage observer disabled: {err}");
                            None
                        }
                    }
                });

            // Install the combined observer exactly once.
            let mut obs_vec = Vec::new();
            let has_log_obs = log_obs.is_some();
            if let Some(log) = log_obs {
                obs_vec.push(log);
            }
            if let Some(lin) = lineage_observer {
                obs_vec.push(lin);
            }
            // When lineage is active but no log observer (e.g. --log-format off),
            // the global observer is set to the lineage observer, which ignores
            // RunEvent::Log. Add a stderr warn sink so that lineage HTTP errors
            // (401, timeouts) emitted via warnings::emit are not silently dropped.
            if !has_log_obs && !obs_vec.is_empty() {
                obs_vec.push(logging::build_warn_sink());
            }
            if !obs_vec.is_empty() {
                let _ = set_observer(std::sync::Arc::new(MultiObserver::new(obs_vec)));
            }

            let outcome =
                match run_with_base(&config_location.path, config_location.base.clone(), options) {
                    Ok(outcome) => outcome,
                    Err(err) => {
                        // Only emit RunStarted if run_with_runtime did not already emit it
                        // (e.g. failures during validate_with_base, RunContext construction,
                        // or validate_entities happen before the core runner reaches that point).
                        // Errors after RunStarted — such as resolve_entity_plans — must not
                        // produce a duplicate START for the same run id.
                        if !floe_core::run::events::is_run_started() {
                            floe_core::run::events::default_observer().on_event(
                                RunEvent::RunStarted {
                                    run_id: computed_run_id.clone(),
                                    config: config_location.path.display().to_string(),
                                    report_base: None,
                                    ts_ms: floe_core::run::events::event_time_ms(),
                                },
                            );
                        }
                        logging::emit_failed_run_events(&computed_run_id, err.as_ref());
                        let mut err_out = std::io::stderr().lock();
                        let _ = writeln!(err_out, "Error: {err}");
                        let _ = err_out.flush();
                        std::process::exit(1);
                    }
                };
            let mode = if quiet {
                output::OutputMode::Quiet
            } else if verbose {
                output::OutputMode::Verbose
            } else {
                output::OutputMode::Default
            };
            let summary = output::format_run_output(&outcome, mode, dry_run);

            match log_format {
                LogFormat::Json => {
                    let mut err = std::io::stderr().lock();
                    let _ = writeln!(err, "{summary}");
                    let _ = err.flush();
                }
                LogFormat::Text | LogFormat::Off => {
                    let mut out = std::io::stdout().lock();
                    let _ = writeln!(out, "{summary}");
                    let _ = out.flush();
                }
            }

            std::process::exit(outcome.summary.run.exit_code);
        }
        Command::Manifest { command } => match command {
            ManifestCommand::Generate {
                config,
                output,
                entities,
                profile,
                deterministic,
                manifest_name,
                default_domain,
                manifest_path_mode,
                runtime,
            } => {
                // Explicit --runtime wins; otherwise auto-detect from the environment.
                let runtime_env = runtime
                    .map(RuntimeArg::into_runtime_env)
                    .unwrap_or_else(floe_core::RuntimeEnv::detect);
                let config_location = match resolve_config_location(&config) {
                    Ok(location) => location,
                    Err(err) => {
                        let mut err_out = std::io::stderr().lock();
                        let _ = writeln!(err_out, "Error: {err}");
                        let _ = err_out.flush();
                        std::process::exit(1);
                    }
                };

                // Load profile via resolve_config_location so remote URIs (s3://, gs://, abfs://)
                // are supported in addition to local paths.
                let profile_location =
                    profile
                        .as_deref()
                        .map(|p| match resolve_config_location(p) {
                            Ok(loc) => loc,
                            Err(err) => exit_with_error(err),
                        });

                let profile_config = if let Some(ref loc) = profile_location {
                    let parsed = match parse_profile(&loc.path) {
                        Ok(p) => p,
                        Err(err) => exit_with_error(err),
                    };
                    if let Err(err) = validate_profile(&parsed) {
                        exit_with_error(err);
                    }
                    Some(parsed)
                } else {
                    None
                };

                let profile_vars = if let Some(ref parsed) = profile_config {
                    let config_env_vars = floe_core::extract_config_env_vars(&config_location.path)
                        .unwrap_or_default();
                    match floe_core::resolve_vars(floe_core::VarSources {
                        profile: &parsed.variables,
                        cli: &std::collections::HashMap::new(),
                        config: &config_env_vars,
                    }) {
                        Ok(vars) => vars,
                        Err(err) => exit_with_error(err),
                    }
                } else {
                    std::collections::HashMap::new()
                };

                let options = ValidateOptions {
                    entities: entities.clone(),
                    profile_vars: profile_vars.clone(),
                    profile_catalogs: profile_config
                        .as_ref()
                        .and_then(|profile| profile.catalogs.clone()),
                    profile_storages: profile_config
                        .as_ref()
                        .and_then(|profile| profile.storages.clone()),
                    profile_lineage: profile_config
                        .as_ref()
                        .and_then(|profile| profile.lineage.clone()),
                };
                if let Err(err) =
                    validate_with_base(&config_location.path, config_location.base.clone(), options)
                {
                    exit_with_error(err);
                }

                let config = load_config_with_profile_overrides(
                    &config_location.path,
                    &profile_vars,
                    profile_config
                        .as_ref()
                        .and_then(|profile| profile.catalogs.as_ref()),
                    profile_config
                        .as_ref()
                        .and_then(|profile| profile.storages.as_ref()),
                    profile_config
                        .as_ref()
                        .and_then(|profile| profile.lineage.as_ref()),
                )?;

                // Match config_uri: absolutize the local profile path against the target
                // runtime's work-root (`image` → `/work`, `cli` → host-canonical); remote
                // URIs are recorded as-is.
                let profile_uri = profile_location
                    .as_ref()
                    .map(|loc| floe_core::local_uri_for_env(&loc.uri, &loc.path, runtime_env));
                let profile_path = profile_location.as_ref().map(|loc| loc.path.clone());

                // When --output is a remote URI it is also the manifest's deployed location,
                // so bake it into execution.base_args automatically.
                let manifest_uri = if output.contains("://") {
                    Some(output.clone())
                } else {
                    None
                };

                let path_mode = match manifest_path_mode.as_str() {
                    "default" => floe_core::PathMode::Default,
                    "resolved-uri" => floe_core::PathMode::ResolvedUri,
                    other => exit_with_error(format!(
                        "unknown --manifest-path-mode value '{}'; expected 'default' or 'resolved-uri'",
                        other
                    ).into()),
                };

                let manifest_options = floe_core::ManifestOptions {
                    deterministic,
                    manifest_name,
                    profile_uri,
                    profile_path,
                    manifest_uri,
                    default_domain,
                    path_mode,
                    runtime_env,
                };
                let manifest_json = build_common_manifest_json(
                    &config_location,
                    &config,
                    &entities,
                    profile_config.as_ref(),
                    &manifest_options,
                )?;
                manifest_output::write_manifest(&output, &manifest_json)?;
                if output != "-" {
                    let mut out = std::io::stdout().lock();
                    let _ = writeln!(out, "Manifest written: {output}");
                    let _ = out.flush();
                }
                Ok(())
            }
        },
        Command::AddEntity {
            config,
            input,
            format,
            name,
            domain,
            output,
            dry_run,
        } => {
            let config_path = std::path::PathBuf::from(&config);
            let output_path = output.as_ref().map(std::path::PathBuf::from);
            let outcome = match add_entity_to_config(AddEntityOptions {
                config_path,
                output_path,
                input,
                format: format.as_ref().map(|value| value.as_str().to_string()),
                name,
                domain,
                dry_run,
            }) {
                Ok(outcome) => outcome,
                Err(err) => {
                    let mut err_out = std::io::stderr().lock();
                    let _ = writeln!(err_out, "Error: {err}");
                    let _ = err_out.flush();
                    std::process::exit(1);
                }
            };

            let mut out = std::io::stdout().lock();
            if let Some(yaml) = outcome.rendered_yaml.as_ref() {
                let _ = writeln!(
                    out,
                    "# add-entity dry run (no file written)\n# entity: {}",
                    outcome.entity_name
                );
                for note in &outcome.notes {
                    let _ = writeln!(out, "# note: {note}");
                }
                let _ = writeln!(out, "{yaml}");
            } else {
                let _ = writeln!(
                    out,
                    "Entity added: {} (format={}, columns={})",
                    outcome.entity_name, outcome.format, outcome.column_count
                );
                let _ = writeln!(out, "Config written: {}", outcome.output_path.display());
                for note in &outcome.notes {
                    let _ = writeln!(out, "Note: {note}");
                }
            }
            let _ = out.flush();
            Ok(())
        }
        Command::State { command } => match command {
            StateCommand::Inspect {
                config,
                entity,
                profile,
            } => {
                let parsed_profile = load_state_profile(profile);
                let config_location = resolve_or_exit(&config);
                let effective_config =
                    load_effective_config_for_state(&config_location, parsed_profile);
                let inspection = match inspect_entity_state(
                    &effective_config,
                    config_location.base.clone(),
                    &entity,
                ) {
                    Ok(inspection) => inspection,
                    Err(err) => exit_with_error(err),
                };

                let mut out = std::io::stdout().lock();
                let _ = writeln!(out, "Entity: {}", inspection.entity_name);
                let _ = writeln!(
                    out,
                    "Incremental mode: {}",
                    inspection.incremental_mode.as_str()
                );
                let _ = writeln!(out, "State path: {}", inspection.path.uri);
                match inspection.state {
                    Some(state) => {
                        let _ = writeln!(out, "State exists: yes");
                        let _ = writeln!(out, "Tracked files: {}", state.files.len());
                        let _ = writeln!(out, "Active claims: {}", state.claims.len());
                        let _ = writeln!(
                            out,
                            "Updated at: {}",
                            state.updated_at.as_deref().unwrap_or("(unknown)")
                        );
                        let _ = writeln!(out);
                        let _ = serde_json::to_writer_pretty(&mut out, &state);
                        let _ = writeln!(out);
                    }
                    None => {
                        let _ = writeln!(out, "State exists: no");
                        let _ = writeln!(out, "Tracked files: 0");
                        let _ = writeln!(out, "Active claims: 0");
                    }
                }
                let _ = out.flush();
                Ok(())
            }
            StateCommand::Reset {
                config,
                entity,
                yes,
                profile,
            } => {
                if !yes {
                    exit_with_error(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "state reset is destructive, rerun with --yes to confirm",
                    )));
                }

                let parsed_profile = load_state_profile(profile);
                let config_location = resolve_or_exit(&config);
                let effective_config =
                    load_effective_config_for_state(&config_location, parsed_profile);
                let removed = match reset_entity_state(
                    &effective_config,
                    config_location.base.clone(),
                    &entity,
                ) {
                    Ok(removed) => removed,
                    Err(err) => exit_with_error(err),
                };
                let inspection = match inspect_entity_state(
                    &effective_config,
                    config_location.base.clone(),
                    &entity,
                ) {
                    Ok(inspection) => inspection,
                    Err(err) => exit_with_error(err),
                };

                let mut out = std::io::stdout().lock();
                let _ = writeln!(out, "Entity: {}", inspection.entity_name);
                let _ = writeln!(out, "State path: {}", inspection.path.uri);
                if removed {
                    let _ = writeln!(out, "State reset: removed state object");
                } else {
                    let _ = writeln!(out, "State reset: no state object found");
                }
                let _ = out.flush();
                Ok(())
            }
        },
    }
}

fn resolve_or_exit(config: &str) -> floe_core::ConfigLocation {
    match resolve_config_location(config) {
        Ok(location) => location,
        Err(err) => exit_with_error(err),
    }
}

fn exit_with_error(err: Box<dyn std::error::Error + Send + Sync>) -> ! {
    let mut err_out = std::io::stderr().lock();
    let _ = writeln!(err_out, "Error: {err}");
    let _ = err_out.flush();
    std::process::exit(1);
}

fn load_state_profile(profile: Option<String>) -> Option<floe_core::ProfileConfig> {
    let profile_input = profile?;
    let profile_location = match resolve_config_location(&profile_input) {
        Ok(loc) => loc,
        Err(err) => exit_with_error(err),
    };
    let parsed = match parse_profile(&profile_location.path) {
        Ok(p) => p,
        Err(err) => exit_with_error(err),
    };
    if let Err(err) = validate_profile(&parsed) {
        exit_with_error(err);
    }
    Some(parsed)
}

fn load_effective_config_for_state(
    config_location: &floe_core::ConfigLocation,
    parsed_profile: Option<floe_core::ProfileConfig>,
) -> floe_core::config::RootConfig {
    let profile_vars = if let Some(ref parsed) = parsed_profile {
        let config_env_vars =
            floe_core::extract_config_env_vars(&config_location.path).unwrap_or_default();
        match floe_core::resolve_vars(floe_core::VarSources {
            profile: &parsed.variables,
            cli: &std::collections::HashMap::new(),
            config: &config_env_vars,
        }) {
            Ok(vars) => vars,
            Err(err) => exit_with_error(err),
        }
    } else {
        std::collections::HashMap::new()
    };
    match load_config_with_profile_overrides(
        &config_location.path,
        &profile_vars,
        parsed_profile.as_ref().and_then(|p| p.catalogs.as_ref()),
        parsed_profile.as_ref().and_then(|p| p.storages.as_ref()),
        parsed_profile.as_ref().and_then(|p| p.lineage.as_ref()),
    ) {
        Ok(config) => config,
        Err(err) => exit_with_error(err),
    }
}
