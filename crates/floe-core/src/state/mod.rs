use crate::errors::FloeError;
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;

use crate::config::{
    ConfigBase, EntityConfig, IncrementalMode, ResolvedPath, RootConfig, StorageResolver,
};
use crate::io::storage::{
    extensions, local::LocalClient, CloudClient, ConditionalWrite, StorageClient, StoredObject,
};
use crate::FloeResult;

pub const ENTITY_STATE_SCHEMA_V1: &str = "floe.state.file-ingest.v1";
pub const ENTITY_STATE_SCHEMA_V2: &str = "floe.state.file-ingest.v2";
pub const ENTITY_STATE_FILENAME: &str = "state.json";
const STATE_CAS_RETRIES: usize = 8;
const STATE_CAS_BASE_BACKOFF_MS: u64 = 50;
const STATE_CAS_MAX_BACKOFF_MS: u64 = 2_000;
pub const CLAIM_TTL_SECONDS: i64 = 60 * 60;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntityState {
    pub schema: String,
    pub entity: String,
    pub updated_at: Option<String>,
    #[serde(default)]
    pub files: BTreeMap<String, EntityFileState>,
    #[serde(default)]
    pub claims: BTreeMap<String, EntityFileClaim>,
}

impl EntityState {
    pub fn new(entity: impl Into<String>) -> Self {
        Self {
            schema: ENTITY_STATE_SCHEMA_V2.to_string(),
            entity: entity.into(),
            updated_at: None,
            files: BTreeMap::new(),
            claims: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntityFileState {
    pub processed_at: String,
    pub size: Option<u64>,
    pub mtime: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntityFileClaim {
    pub run_id: String,
    pub acquired_at: String,
    pub expires_at: String,
    pub size: Option<u64>,
    pub mtime: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EntityStateInspection {
    pub entity_name: String,
    pub incremental_mode: IncrementalMode,
    pub path: ResolvedPath,
    pub state: Option<EntityState>,
}

pub fn resolve_entity_state_path(
    resolver: &StorageResolver,
    entity: &EntityConfig,
) -> FloeResult<ResolvedPath> {
    if let Some(state) = &entity.state {
        if let Some(path) = state.path.as_deref() {
            let resolved = if is_remote_uri(path) {
                resolver.resolve_path(
                    &entity.name,
                    "entity.state.path",
                    entity.source.storage.as_deref(),
                    path,
                )?
            } else {
                resolver.resolve_local_path(path)?
            };
            return Ok(resolved);
        }
    }

    let resolved_source = resolver.resolve_path(
        &entity.name,
        "entity.source.path",
        entity.source.storage.as_deref(),
        &entity.source.path,
    )?;
    let source_root = derive_source_root(
        &entity.source.path,
        &entity.source.format,
        resolved_source.local_path.as_deref(),
    );
    let default_path = join_state_path(&source_root, &entity.id_path());
    let resolved = resolver.resolve_path(
        &entity.name,
        "entity.state.path",
        entity.source.storage.as_deref(),
        &default_path,
    )?;
    Ok(resolved)
}

pub fn read_entity_state(path: &Path) -> FloeResult<Option<EntityState>> {
    if !path.exists() {
        return Ok(None);
    }
    let payload = fs::read_to_string(path)?;
    let state = parse_entity_state(payload.as_bytes())?;
    Ok(Some(state))
}

fn parse_entity_state(payload: &[u8]) -> FloeResult<EntityState> {
    let mut state: EntityState = serde_json::from_slice(payload)?;
    if state.schema == ENTITY_STATE_SCHEMA_V1 {
        state.schema = ENTITY_STATE_SCHEMA_V2.to_string();
        state.claims.clear();
    }
    Ok(state)
}

#[derive(Debug, Clone)]
pub enum EntityStateTarget {
    Local { path: PathBuf, uri: String },
    Remote { storage: String, uri: String },
}

#[derive(Debug, Clone)]
pub struct LoadedEntityState {
    pub target: EntityStateTarget,
    pub state: EntityState,
    pub version: Option<String>,
    pub existed: bool,
}

#[derive(Debug, Clone)]
pub struct ClaimedEntityState {
    pub target: EntityStateTarget,
    pub state: EntityState,
    pub version: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EntityStateClaimOutcome {
    pub pending_inputs: Vec<crate::io::format::InputFile>,
    pub claimed_state: Option<ClaimedEntityState>,
    pub active_claims: Vec<String>,
    pub already_processed: Vec<(crate::io::format::InputFile, EntityFileState)>,
}

pub fn claim_entity_inputs(
    resolver: &StorageResolver,
    cloud: &mut CloudClient,
    entity: &EntityConfig,
    run_id: &str,
    input_files: Vec<crate::io::format::InputFile>,
) -> FloeResult<EntityStateClaimOutcome> {
    if input_files.is_empty() {
        return Ok(EntityStateClaimOutcome {
            pending_inputs: Vec::new(),
            claimed_state: None,
            active_claims: Vec::new(),
            already_processed: Vec::new(),
        });
    }

    for attempt in 0..STATE_CAS_RETRIES {
        if attempt > 0 {
            cas_backoff(attempt - 1);
        }
        let mut loaded = load_entity_state(resolver, cloud, entity)?;
        remove_expired_claims(&mut loaded.state);
        let mut pending_inputs = Vec::new();
        let mut active_claims = Vec::new();
        let mut already_processed = Vec::new();
        let acquired_at = now_rfc3339();
        let expires_at = rfc3339_after_seconds(CLAIM_TTL_SECONDS);

        for input_file in &input_files {
            if let Some(recorded) = loaded.state.files.get(&input_file.source_uri) {
                already_processed.push((input_file.clone(), recorded.clone()));
                continue;
            }
            match loaded.state.claims.get(&input_file.source_uri) {
                Some(_) => {
                    active_claims.push(input_file.source_uri.clone());
                }
                None => {
                    loaded.state.claims.insert(
                        input_file.source_uri.clone(),
                        EntityFileClaim {
                            run_id: run_id.to_string(),
                            acquired_at: acquired_at.clone(),
                            expires_at: expires_at.clone(),
                            size: input_file.source_size,
                            mtime: input_file.source_mtime.clone(),
                        },
                    );
                    pending_inputs.push(input_file.clone());
                }
            }
        }

        if pending_inputs.is_empty() {
            if active_claims.is_empty() {
                let _ = persist_loaded_state(cloud, resolver, &loaded)?;
            }
            return Ok(EntityStateClaimOutcome {
                pending_inputs,
                claimed_state: None,
                active_claims,
                already_processed,
            });
        }

        loaded.state.schema = ENTITY_STATE_SCHEMA_V2.to_string();
        loaded.state.updated_at = Some(acquired_at);
        match persist_loaded_state(cloud, resolver, &loaded)? {
            Some(version) => {
                return Ok(EntityStateClaimOutcome {
                    pending_inputs,
                    claimed_state: Some(ClaimedEntityState {
                        target: loaded.target,
                        state: loaded.state,
                        version,
                    }),
                    active_claims,
                    already_processed,
                });
            }
            None => continue,
        }
    }

    Err(FloeError::config(format!(
        "entity.name={} incremental state update conflicted after {STATE_CAS_RETRIES} retries",
        entity.name
    ))
    .into())
}

/// Full-refresh variant of `claim_entity_inputs`.
///
/// Loads existing state to get the current CAS version, then writes a blank
/// `EntityState` containing fresh claims for ALL `input_files` — discarding
/// the historical `files` and `claims` maps. On successful commit the state
/// file will contain only the files processed in this run.
///
/// Returns `None` when `input_files` is empty (nothing to claim).
pub fn claim_all_entity_inputs(
    resolver: &StorageResolver,
    cloud: &mut CloudClient,
    entity: &EntityConfig,
    run_id: &str,
    input_files: Vec<crate::io::format::InputFile>,
) -> FloeResult<Option<ClaimedEntityState>> {
    let acquired_at = now_rfc3339();
    let expires_at = rfc3339_after_seconds(CLAIM_TTL_SECONDS);

    for attempt in 0..STATE_CAS_RETRIES {
        if attempt > 0 {
            cas_backoff(attempt - 1);
        }
        // Read only to obtain the current CAS version; content is discarded.
        let loaded = load_entity_state(resolver, cloud, entity)?;

        let mut fresh_state = EntityState::new(&entity.name);
        fresh_state.files = loaded.state.files.clone();
        fresh_state.updated_at = Some(acquired_at.clone());
        for input_file in &input_files {
            fresh_state.claims.insert(
                input_file.source_uri.clone(),
                EntityFileClaim {
                    run_id: run_id.to_string(),
                    acquired_at: acquired_at.clone(),
                    expires_at: expires_at.clone(),
                    size: input_file.source_size,
                    mtime: input_file.source_mtime.clone(),
                },
            );
        }

        let fresh_loaded = LoadedEntityState {
            target: loaded.target,
            state: fresh_state,
            version: loaded.version,
            existed: loaded.existed,
        };
        match persist_loaded_state(cloud, resolver, &fresh_loaded)? {
            Some(version) => {
                return Ok(Some(ClaimedEntityState {
                    target: fresh_loaded.target,
                    state: fresh_loaded.state,
                    version,
                }));
            }
            None => continue,
        }
    }

    Err(FloeError::config(format!(
        "entity.name={} full-refresh state write conflicted after {STATE_CAS_RETRIES} retries",
        entity.name
    ))
    .into())
}

pub fn promote_claimed_entity_state(
    resolver: &StorageResolver,
    cloud: &mut CloudClient,
    entity_name: &str,
    run_id: &str,
    claimed: &ClaimedEntityState,
) -> FloeResult<()> {
    mutate_claimed_state(resolver, cloud, entity_name, claimed, |state, our_uris| {
        let processed_at = now_rfc3339();
        let claimed_files: Vec<String> = state
            .claims
            .iter()
            .filter(|(uri, claim)| claim.run_id == run_id && our_uris.contains(*uri))
            .map(|(source_uri, _)| source_uri.clone())
            .collect();
        for source_uri in claimed_files {
            if let Some(claim) = state.claims.remove(&source_uri) {
                state.files.insert(
                    source_uri,
                    EntityFileState {
                        processed_at: processed_at.clone(),
                        size: claim.size,
                        mtime: claim.mtime,
                    },
                );
            }
        }
        state.updated_at = Some(processed_at);
    })
}

pub fn promote_full_refresh_claimed_entity_state(
    resolver: &StorageResolver,
    cloud: &mut CloudClient,
    entity_name: &str,
    run_id: &str,
    claimed: &ClaimedEntityState,
) -> FloeResult<()> {
    mutate_claimed_state(resolver, cloud, entity_name, claimed, |state, our_uris| {
        let processed_at = now_rfc3339();
        let claimed_files: Vec<String> = state
            .claims
            .iter()
            .filter(|(uri, claim)| claim.run_id == run_id && our_uris.contains(*uri))
            .map(|(uri, _)| uri.clone())
            .collect();
        state.files.clear();
        for source_uri in claimed_files {
            if let Some(claim) = state.claims.remove(&source_uri) {
                state.files.insert(
                    source_uri,
                    EntityFileState {
                        processed_at: processed_at.clone(),
                        size: claim.size,
                        mtime: claim.mtime,
                    },
                );
            }
        }
        state.updated_at = Some(processed_at);
    })
}

pub fn release_claimed_entity_state(
    resolver: &StorageResolver,
    cloud: &mut CloudClient,
    entity_name: &str,
    run_id: &str,
    claimed: &ClaimedEntityState,
) -> FloeResult<()> {
    mutate_claimed_state(resolver, cloud, entity_name, claimed, |state, our_uris| {
        state
            .claims
            .retain(|uri, claim| !(claim.run_id == run_id && our_uris.contains(uri)));
        state.updated_at = Some(now_rfc3339());
    })
}

pub fn renew_claimed_entity_state(
    resolver: &StorageResolver,
    cloud: &mut CloudClient,
    entity_name: &str,
    run_id: &str,
    claimed: &ClaimedEntityState,
) -> FloeResult<()> {
    mutate_claimed_state(resolver, cloud, entity_name, claimed, |state, our_uris| {
        let now = now_rfc3339();
        let expires_at = rfc3339_after_seconds(CLAIM_TTL_SECONDS);
        for (uri, claim) in state.claims.iter_mut() {
            if claim.run_id == run_id && our_uris.contains(uri) {
                claim.expires_at = expires_at.clone();
            }
        }
        state.updated_at = Some(now);
    })
}

fn mutate_claimed_state(
    resolver: &StorageResolver,
    cloud: &mut CloudClient,
    entity_name: &str,
    claimed: &ClaimedEntityState,
    mutate: impl Fn(&mut EntityState, &HashSet<String>),
) -> FloeResult<()> {
    let our_uris: HashSet<String> = claimed.state.claims.keys().cloned().collect();
    for attempt in 0..STATE_CAS_RETRIES {
        if attempt > 0 {
            cas_backoff(attempt - 1);
        }
        let mut loaded = if attempt == 0 {
            LoadedEntityState {
                target: claimed.target.clone(),
                state: claimed.state.clone(),
                version: claimed.version.clone(),
                existed: claimed.version.is_some(),
            }
        } else {
            load_target_state_with_entity_name(
                cloud,
                resolver,
                entity_name,
                claimed.target.clone(),
            )?
        };
        mutate(&mut loaded.state, &our_uris);
        loaded.state.schema = ENTITY_STATE_SCHEMA_V2.to_string();
        let persisted = if loaded.state.files.is_empty() && loaded.state.claims.is_empty() {
            delete_loaded_state(cloud, resolver, &loaded)?
        } else {
            persist_loaded_state(cloud, resolver, &loaded)?
        };
        if persisted.is_some() {
            return Ok(());
        }
    }
    Err(FloeError::config(format!(
        "entity.name={} incremental state update conflicted after {STATE_CAS_RETRIES} retries",
        entity_name
    ))
    .into())
}

pub fn inspect_entity_state_with_base(
    config_path: &Path,
    config_base: ConfigBase,
    entity_name: &str,
) -> FloeResult<EntityStateInspection> {
    let config = crate::load_config(config_path)?;
    inspect_entity_state(&config, config_base, entity_name)
}

pub fn inspect_entity_state(
    config: &RootConfig,
    config_base: ConfigBase,
    entity_name: &str,
) -> FloeResult<EntityStateInspection> {
    let (entity, path) = resolve_entity_state_target(config, config_base.clone(), entity_name)?;
    let resolver = StorageResolver::new(config, config_base)?;
    let target = state_target_from_resolved(&path)?;
    let mut cloud = CloudClient::new();
    let loaded = load_target_state_with_resolver(&mut cloud, &resolver, entity, target)?;
    let state = loaded.existed.then_some(loaded.state);

    Ok(EntityStateInspection {
        entity_name: entity.name.clone(),
        incremental_mode: entity.incremental_mode,
        path,
        state,
    })
}

pub fn reset_entity_state(
    config: &RootConfig,
    config_base: ConfigBase,
    entity_name: &str,
) -> FloeResult<bool> {
    let (entity, path) = resolve_entity_state_target(config, config_base.clone(), entity_name)?;
    let target = state_target_from_resolved(&path)?;
    match target {
        EntityStateTarget::Local { path, .. } => {
            if path.exists() {
                fs::remove_file(&path)?;
                return Ok(true);
            }
            Ok(false)
        }
        EntityStateTarget::Remote { storage, uri } => {
            let mut cloud = CloudClient::new();
            let resolver = StorageResolver::new(config, config_base)?;
            let client = cloud.client_for_context(
                &resolver,
                &storage,
                &format!("entity.name={}", entity.name),
            )?;
            let Some(object) = client.read_object(&uri)? else {
                return Ok(false);
            };
            match client.delete_object_conditional(&uri, Some(&object.version))? {
                ConditionalWrite::Written { .. } => Ok(true),
                ConditionalWrite::Conflict => Err(FloeError::config(format!(
                    "entity.name={} remote state changed while resetting: {}",
                    entity.name, uri
                ))
                .into()),
            }
        }
    }
}

pub fn reset_entity_state_with_base(
    config_path: &Path,
    config_base: ConfigBase,
    entity_name: &str,
) -> FloeResult<bool> {
    let config = crate::load_config(config_path)?;
    reset_entity_state(&config, config_base, entity_name)
}

fn resolve_entity_state_target<'a>(
    config: &'a RootConfig,
    config_base: ConfigBase,
    entity_name: &str,
) -> FloeResult<(&'a EntityConfig, ResolvedPath)> {
    let entity = config.select_entities(&[entity_name.to_string()])?[0];
    let resolver = StorageResolver::new(config, config_base)?;
    let path = resolve_entity_state_path(&resolver, entity)?;
    Ok((entity, path))
}

pub fn write_entity_state_atomic(path: &Path, state: &EntityState) -> FloeResult<()> {
    let parent = path.parent().ok_or_else(|| {
        FloeError::config(format!(
            "state path has no parent directory: {}",
            path.display()
        ))
    })?;
    fs::create_dir_all(parent)?;

    let mut temp = NamedTempFile::new_in(parent)?;
    serde_json::to_writer_pretty(temp.as_file_mut(), state)?;
    temp.as_file_mut().sync_all()?;
    temp.persist(path).map_err(|err| err.error)?;
    Ok(())
}

fn load_entity_state(
    resolver: &StorageResolver,
    cloud: &mut CloudClient,
    entity: &EntityConfig,
) -> FloeResult<LoadedEntityState> {
    let resolved = resolve_entity_state_path(resolver, entity)?;
    let target = state_target_from_resolved(&resolved)?;
    load_target_state_with_resolver(cloud, resolver, entity, target)
}

fn load_target_state_with_resolver(
    cloud: &mut CloudClient,
    resolver: &StorageResolver,
    entity: &EntityConfig,
    target: EntityStateTarget,
) -> FloeResult<LoadedEntityState> {
    load_target_state_with_entity_name(cloud, resolver, &entity.name, target)
}

fn load_target_state_with_entity_name(
    cloud: &mut CloudClient,
    resolver: &StorageResolver,
    entity_name: &str,
    target: EntityStateTarget,
) -> FloeResult<LoadedEntityState> {
    match target {
        EntityStateTarget::Local { path, uri } => {
            let object = LocalClient::new().read_object(&uri)?;
            let (state, version, existed) = resolve_loaded_state(entity_name, object)?;
            Ok(LoadedEntityState {
                target: EntityStateTarget::Local { path, uri },
                state,
                version,
                existed,
            })
        }
        EntityStateTarget::Remote { storage, uri } => {
            let client = cloud.client_for_context(
                resolver,
                &storage,
                &format!("entity.name={entity_name}"),
            )?;
            let object = client.read_object(&uri)?;
            let (state, version, existed) = resolve_loaded_state(entity_name, object)?;
            Ok(LoadedEntityState {
                target: EntityStateTarget::Remote { storage, uri },
                state,
                version,
                existed,
            })
        }
    }
}

fn resolve_loaded_state(
    entity_name: &str,
    object: Option<StoredObject>,
) -> FloeResult<(EntityState, Option<String>, bool)> {
    match object {
        Some(object) => Ok((
            validate_entity_state_name(entity_name, parse_entity_state(&object.body)?)?,
            Some(object.version),
            true,
        )),
        None => Ok((EntityState::new(entity_name), None, false)),
    }
}

fn with_state_client<R, F>(
    cloud: &mut CloudClient,
    resolver: &StorageResolver,
    target: &EntityStateTarget,
    f: F,
) -> FloeResult<R>
where
    F: FnOnce(&str, &dyn StorageClient) -> FloeResult<R>,
{
    match target {
        EntityStateTarget::Local { uri, .. } => f(uri, &LocalClient::new()),
        EntityStateTarget::Remote { uri, storage } => {
            let client = cloud.client_for_context(resolver, storage, "entity state")?;
            f(uri, client)
        }
    }
}

fn conditional_write_to_version(cw: ConditionalWrite) -> Option<Option<String>> {
    match cw {
        ConditionalWrite::Written { version } => Some(Some(version)),
        ConditionalWrite::Conflict => None,
    }
}

fn persist_loaded_state(
    cloud: &mut CloudClient,
    resolver: &StorageResolver,
    loaded: &LoadedEntityState,
) -> FloeResult<Option<Option<String>>> {
    let mut state = loaded.state.clone();
    state.schema = ENTITY_STATE_SCHEMA_V2.to_string();
    let body = serde_json::to_vec_pretty(&state)?;
    let version = loaded.version.as_deref();
    let cw = with_state_client(cloud, resolver, &loaded.target, |uri, client| {
        client.write_object_conditional(uri, version, &body)
    })?;
    Ok(conditional_write_to_version(cw))
}

fn delete_loaded_state(
    cloud: &mut CloudClient,
    resolver: &StorageResolver,
    loaded: &LoadedEntityState,
) -> FloeResult<Option<Option<String>>> {
    let version = loaded.version.as_deref();
    let cw = with_state_client(cloud, resolver, &loaded.target, |uri, client| {
        client.delete_object_conditional(uri, version)
    })?;
    Ok(conditional_write_to_version(cw))
}

fn state_target_from_resolved(resolved: &ResolvedPath) -> FloeResult<EntityStateTarget> {
    if let Some(path) = &resolved.local_path {
        return Ok(EntityStateTarget::Local {
            path: path.clone(),
            uri: resolved.uri.clone(),
        });
    }
    if is_remote_uri(&resolved.uri) {
        return Ok(EntityStateTarget::Remote {
            storage: resolved.storage.clone(),
            uri: resolved.uri.clone(),
        });
    }
    Err(FloeError::config(format!(
        "state path is neither local nor supported remote: {}",
        resolved.uri
    ))
    .into())
}

fn remove_expired_claims(state: &mut EntityState) {
    let now = time::OffsetDateTime::now_utc();
    state.claims.retain(|_, claim| {
        time::OffsetDateTime::parse(
            &claim.expires_at,
            &time::format_description::well_known::Rfc3339,
        )
        .map(|expires_at| expires_at > now)
        .unwrap_or(false)
    });
}

fn rfc3339_offset(seconds: i64) -> String {
    (time::OffsetDateTime::now_utc() + time::Duration::seconds(seconds))
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| crate::report::now_rfc3339())
}

fn now_rfc3339() -> String {
    rfc3339_offset(0)
}

/// Capped exponential backoff window (in ms) for a zero-based retry attempt.
fn cas_backoff_window_ms(attempt: usize) -> u64 {
    let shift = attempt.min(20) as u32;
    STATE_CAS_BASE_BACKOFF_MS
        .saturating_mul(1u64 << shift)
        .min(STATE_CAS_MAX_BACKOFF_MS)
}

/// Full-jitter delay drawn from `[0, window]`. Jitter is seeded from the clock's
/// sub-millisecond nanoseconds — adequate for spreading out colliding writers
/// without pulling in a PRNG dependency.
fn cas_backoff_jitter_ms(window_ms: u64) -> u64 {
    if window_ms == 0 {
        return 0;
    }
    let entropy = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    entropy % (window_ms + 1)
}

/// Sleep before retrying a conflicted CAS write so concurrent writers (e.g. an
/// orchestrator backfill fanning out parallel entity runs) stop colliding in
/// lockstep. `attempt` is the zero-based index of the attempt that just failed.
fn cas_backoff(attempt: usize) {
    let window = cas_backoff_window_ms(attempt);
    std::thread::sleep(std::time::Duration::from_millis(cas_backoff_jitter_ms(
        window,
    )));
}

fn rfc3339_after_seconds(seconds: i64) -> String {
    rfc3339_offset(seconds)
}

fn join_state_path(source_root: &str, entity_name: &str) -> String {
    if source_root.is_empty() || source_root == "." {
        format!(".floe/state/{entity_name}/{ENTITY_STATE_FILENAME}")
    } else {
        format!(
            "{}/.floe/state/{entity_name}/{ENTITY_STATE_FILENAME}",
            source_root.trim_end_matches(is_path_separator)
        )
    }
}

fn derive_source_root(
    raw_path: &str,
    source_format: &str,
    resolved_local_path: Option<&Path>,
) -> String {
    let trimmed = raw_path.trim_end_matches(is_path_separator);
    if trimmed.is_empty() {
        return String::new();
    }

    if let Some(prefix) = prefix_before_first_glob(trimmed) {
        if prefix.is_empty() {
            return String::new();
        }
        if prefix.ends_with(is_path_separator) {
            return prefix.trim_end_matches(is_path_separator).to_string();
        }
        return parent_like(prefix)
            .unwrap_or(prefix)
            .trim_end_matches(is_path_separator)
            .to_string();
    }

    if let Some(path) = resolved_local_path.filter(|path| path.exists()) {
        if path.is_dir() {
            return trimmed.to_string();
        }
        if path.is_file() {
            return parent_like(trimmed)
                .unwrap_or(trimmed)
                .trim_end_matches(is_path_separator)
                .to_string();
        }
    }

    if matches_source_file_suffix(trimmed, source_format) {
        return parent_like(trimmed)
            .unwrap_or(trimmed)
            .trim_end_matches(is_path_separator)
            .to_string();
    }

    trimmed.to_string()
}

fn prefix_before_first_glob(value: &str) -> Option<&str> {
    let index = value.find(['*', '?', '['])?;
    Some(&value[..index])
}

fn matches_source_file_suffix(value: &str, source_format: &str) -> bool {
    let Some(segment) = value.rsplit(is_path_separator).next() else {
        return false;
    };
    let segment = segment.to_ascii_lowercase();

    extensions::suffixes_for_format(source_format)
        .map(|suffixes| {
            suffixes
                .iter()
                .any(|suffix| segment.ends_with(&suffix.to_ascii_lowercase()))
        })
        .unwrap_or(false)
}

fn parent_like(value: &str) -> Option<&str> {
    value.rfind(is_path_separator).map(|index| {
        if index == 0 {
            &value[..1]
        } else {
            &value[..index]
        }
    })
}

fn is_path_separator(ch: char) -> bool {
    ch == '/' || ch == '\\'
}

fn is_remote_uri(value: &str) -> bool {
    crate::io::storage::uri::is_remote_uri(value)
}

pub fn validate_entity_state(entity: &EntityConfig, state: EntityState) -> FloeResult<EntityState> {
    validate_entity_state_name(&entity.name, state)
}

fn validate_entity_state_name(entity_name: &str, state: EntityState) -> FloeResult<EntityState> {
    if state.schema != ENTITY_STATE_SCHEMA_V1 && state.schema != ENTITY_STATE_SCHEMA_V2 {
        return Err(FloeError::config(format!(
            "entity.name={} state schema mismatch: expected {} or {}, got {}",
            entity_name, ENTITY_STATE_SCHEMA_V1, ENTITY_STATE_SCHEMA_V2, state.schema
        ))
        .into());
    }

    if state.entity != entity_name {
        return Err(FloeError::config(format!(
            "entity.name={} state entity mismatch: expected {}, got {}",
            entity_name, entity_name, state.entity
        ))
        .into());
    }

    Ok(state)
}

#[cfg(test)]
mod backoff_tests {
    use super::*;

    #[test]
    fn window_grows_exponentially_and_caps() {
        assert_eq!(cas_backoff_window_ms(0), STATE_CAS_BASE_BACKOFF_MS);
        assert_eq!(cas_backoff_window_ms(1), STATE_CAS_BASE_BACKOFF_MS * 2);
        assert_eq!(cas_backoff_window_ms(2), STATE_CAS_BASE_BACKOFF_MS * 4);
        assert_eq!(cas_backoff_window_ms(100), STATE_CAS_MAX_BACKOFF_MS);
        assert!(cas_backoff_window_ms(usize::MAX) <= STATE_CAS_MAX_BACKOFF_MS);
    }

    #[test]
    fn jitter_stays_within_window() {
        for _ in 0..1_000 {
            assert_eq!(cas_backoff_jitter_ms(0), 0);
            assert!(cas_backoff_jitter_ms(50) <= 50);
            assert!(cas_backoff_jitter_ms(STATE_CAS_MAX_BACKOFF_MS) <= STATE_CAS_MAX_BACKOFF_MS);
        }
    }
}
