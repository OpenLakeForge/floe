# Changelog

All notable changes to Floe are documented in this file.

## v0.6.11

- **Manifest replay now emits dataset lineage (#455).** `floe run --manifest`
  built the OpenLineage observer with an empty entity set, so entity `COMPLETE`
  events were posted with empty `inputs`/`outputs` and no upstream lineage edge
  was created (OpenMetadata accepted the events with HTTP 200 but produced no
  Bronze-to-Silver edge). Replay now passes the manifest's reconstructed
  entities to the observer, so source and sink datasets are emitted exactly as
  in a direct config run. A resolved `s3://` source is reported with an
  `s3://<bucket>` namespace and its bucket-relative key as the dataset name.
  Direct config execution is unchanged. See `docs/lineage.md`.

## v0.6.10

- **Runtime `OPENLINEAGE_API_KEY` resolution for lineage auth (#452).**
  - When `lineage.api_key` is unset, Floe now resolves the Bearer token from the
    `OPENLINEAGE_API_KEY` environment variable when the lineage observer is
    constructed, matching the OpenLineage Python client and `dbt-ol`. This lets
    orchestrated runners and manifest replays authenticate from a mounted
    Kubernetes Secret without persisting the credential in the generated
    manifest, Git, or object storage.
  - Precedence: an explicit `lineage.api_key` still wins (backward compatible);
    otherwise the environment variable is used; if neither is present, events are
    sent unauthenticated as before.
  - An unresolved `{{VAR}}` placeholder that survives into a manifest (e.g. a
    profile-only `api_key` merged after config templating) is treated as absent,
    so replay falls back to `OPENLINEAGE_API_KEY` instead of sending the literal
    placeholder as the token. See `docs/lineage.md`.

## v0.6.9

- **Configurable OpenLineage endpoint and Iceberg dataset namespace (#450).**
  - `lineage.endpoint` configures the request path, defaulting to
    `api/v1/lineage`; this supports OpenMetadata's native
    `api/v1/openlineage/lineage` route without a proxy.
  - `lineage.dataset_namespace` separates accepted Iceberg output dataset
    identity from the stable OpenLineage job namespace, defaulting to
    `lineage.namespace` for backward compatibility. See `docs/lineage.md`.

## v0.6.8

- **Fix image runner using custom S3 URL**
  The published Floe image can hit S3-compatible endpoints through a hostname such as host.lima.internal, while the host CLI repro uses an IP literal such as 127.0.0.1. Floe already lets aws-config pick up AWS_ENDPOINT_URL / AWS_ENDPOINT_URL_S3, but the plain Rust S3Client only enabled path-style addressing from the YAML path_style_access field.

  That left image runs using virtual-hosted-style requests against SeaweedFS/MinIO-style endpoints even when users set AWS_S3_FORCE_PATH_STYLE=true, which matches the failing direct-config and remote-manifest cases in the issue.

## v0.6.7

- **Manifest local paths are always absolute, targeted at the replay runner (#443).**
  v0.6.6 (#438) made `manifest generate` record local `config_uri` / `profile_uri` relative
  (`local://domains/...`) for cross-checkout reproducible ids. That broke manifests generated
  inside the container image, which downstream orchestrators deploy to object storage and
  replay remotely: the portable `local:///work/...` form was lost, and a manifest loaded from a
  remote URI resolved its relative paths against the *manifest's own bucket*, failing an
  Iceberg write with `storage <name> bucket mismatch`. The definitive rule is now: because the
  runner that generates a manifest is not the one that replays it, every local path is recorded
  **absolute**, resolved against the target runner's work-root.
  - New `manifest generate --runtime image|cli` (manifest field `runtime_env`). `image` records
    local URIs absolute under the container work-root (`/work`) — portable for remote replay
    *and* reproducible across containers; `cli` records them host-absolute (canonicalized) for
    same-host execution. Omitted → auto-detect (container ⇒ image, else cli; override with
    `FLOE_RUNTIME`; work-root override `FLOE_WORK_ROOT`). Cross-environment reproducible
    `manifest_id` now comes from `--runtime image`, not from relative paths.
  - `floe run --manifest` loaded from a **remote** URI no longer treats the manifest's own
    location as the path-resolution base; local paths resolve under the recorded `work_root`
    and remote paths resolve purely from their storage definitions. Local manifest files are
    unchanged. See `docs/manifest.md`.
  - Behaviour change: local-config `config_uri` / `manifest_id` change again (now absolute) —
    regenerate committed manifests, preferring `--runtime image` for CI-stable ids.

## dagster-floe v0.2.5

- **Fix: vendored manifest schema rejected the new `runtime_env` / `work_root` fields.**
  `dagster-floe` validates every manifest against its own bundled copy of
  `floe.manifest.v1.json` before parsing it. That copy predated #443/#444 and still set
  `additionalProperties: false` without the two new fields, so `load_manifest()` failed with
  `Additional properties are not allowed ('runtime_env', 'work_root' were unexpected)` for
  every manifest generated by `floe` v0.6.7+. The vendored copy is now resynced with the
  canonical schema (also picking up a `duckdb` sink-target field it had missed independently
  of #443/#444) and a drift guard (`scripts/check_orchestrator_drift.py`) now checks the two
  files stay byte-identical. `airflow-floe` was unaffected — it has no `jsonschema` dependency
  and hand-parses only the fields it needs.


## v0.6.6

- **Reproducible manifest IDs across environments (#438).** `manifest generate` baked the
  host-absolute, canonicalized config/profile path into `config_uri` / `profile_uri`, so
  the same config referenced by the same relative path produced different `manifest_id`
  and `manifest_revision` under Docker (`/work/...`) versus a native checkout
  (`/Users/...`). These URIs now record the path exactly as passed — a relative `-c`
  stays relative — restoring the documented `--deterministic` byte-identical guarantee
  across machines and containers. Remote config URIs (`s3://`, `gs://`, `abfs://`) stay
  absolute and scheme-normalized. See `docs/manifest.md`.
  - Behaviour change: `manifest_id` / `manifest_revision` values change for manifests
    generated from local-file configs — regenerate any committed manifests.
- **Fix broken arm64 container image (#437).** The `aarch64-unknown-linux-gnu` release
  binary was built on `ubuntu-24.04-arm` (glibc 2.39) and packaged into
  `debian:bookworm-slim` (glibc 2.36), so `ghcr.io/malon64/floe:0.6.5` failed on ARM64
  with `GLIBC_2.38/2.39 not found`. The lean `floe` and companion `floe-duckdb` aarch64
  binaries now build on `ubuntu-22.04-arm` (glibc 2.35), which runs on bookworm and older
  distros.
- Internal hardening:
  - **CD: drop the redundant `docker-duckdb` dry-run job.** It was the last consumer of a
    workflow_dispatch dry-run path that no longer adds coverage.

## v0.6.5

- **Glue Iceberg tables now expose their schema to Iceberg REST clients (#433).** Floe previously
  registered Glue tables with `StorageDescriptor.Columns` empty, leaving only a `metadata_location`
  pointer. The Glue Iceberg REST endpoint reconstructs table metadata from its own catalog state,
  so `loadTable` returned no `schemas` array — causing DuckDB's `ENDPOINT_TYPE GLUE` integration
  to report the table as non-existent even though the data was intact on S3. Floe now mirrors the
  full Iceberg schema into Glue's native column list (with correct Hive type mappings) on every
  create and overwrite commit.
- Internal hardening:
  - **CI: parallelize DuckDB job and centralize change detection (#435).** The DuckDB CI job
    previously ran clippy → tests sequentially on one runner. It is now split into two parallel
    jobs (`duckdb-clippy` / `duckdb-test`) sharing a cached bundled native build, and a single
    upfront `changes` job gates all downstream jobs at the job level so runners do not boot when
    relevant paths are untouched.

## v0.6.4

- **Iceberg sink on Azure Data Lake Storage Gen2 (#411).** Iceberg tables can now be
  written to ADLS Gen2 using filesystem catalog semantics, with account-key, SAS-token, or
  service-principal credentials resolved from the environment. See `docs/sinks/iceberg.md`.
- **Resolved credentials are wrapped in a redacting `Secret` newtype (#392).** Unity / REST
  catalog tokens and the MotherDuck token now live in a `Secret` type whose `Debug`/`Display`
  render `[REDACTED]`, so an accidental `{:?}` in a log line or error can no longer leak a
  PAT. The raw value is reachable only through an explicit accessor at the trust boundaries
  that need it (HTTP bearer header, DuckDB connection config). Serde stays transparent, so
  YAML config parsing and manifest round-trips are unchanged, and existing manifest token
  redaction is unaffected. Docs recommend `${ENV_VAR}` references over literal secrets in YAML.
- Internal hardening:
  - Bumped `apache-avro` from 0.16 to 0.21 (#401).
  - Repaired the release CD: the `publish` job now polls the crates.io sparse index (not the
    web API) before publishing `floe-cli` and retries the resolve-sensitive publish, and the
    lean `aarch64-unknown-linux-gnu` binary builds on a native runner instead of `cross`
    (#431). Bumped `actions/checkout` from 6 to 7 (#414).

## v0.6.3

- **Fixes the `floe-duckdb` companion build (CLI binaries, Docker images).** Building
  `floe-cli` with `--features duckdb` failed to compile (`error[E0425]: cannot find type
  FloeResult`) because the `FloeResult` import in `crates/floe-cli/src/delegate.rs` was gated
  to the lean build (`#[cfg(not(feature = "duckdb"))]`) yet used by the duckdb build's no-op
  `maybe_delegate_duckdb`. The import is now unconditional. The lean `floe` binary and PyPI
  wheels were unaffected; only the DuckDB companion artifacts failed to build for v0.6.2.
- **CI now compiles `floe-cli --features duckdb`.** The path-gated `duckdb` job previously
  only linted `floe-core`; it now also builds the CLI binary the release ships as
  `floe-duckdb` and triggers on `floe-cli` source changes, so feature-gated CLI breakage is
  caught before a tagged release instead of in the release pipeline.

## v0.6.2

- **`manifest generate` resolves profile variables in storage definitions (#424).**
  `{{VAR}}` placeholders in config-level `storages.definitions` fields
  (`bucket`/`region`/`account`/`container`/`prefix`/`endpoint`) supplied by a profile
  `variables:` block are now substituted during manifest generation, not just `prefix`.
- **Generated manifests are self-contained for `floe run --manifest` (#424, #425).** The
  manifest now embeds the effective `storages`/`catalogs`/`lineage` (config-level
  definitions merged with the profile, with profile variables already resolved) instead of
  only the profile sections, so manifest replay can resolve named storages such as
  `source.storage` / `sink.*.storage`. A present-but-malformed embedded block now produces a
  clear `manifest <section> block is malformed` error instead of being silently dropped
  (which previously resurfaced as the confusing `no storages block` failure). See
  `docs/manifest.md`.
- **EKS Pod Identity / container credentials for Glue/S3 Iceberg writes (#426).** The Iceberg
  S3 write/seed path resolves AWS credentials through the AWS SDK default chain (which honors
  `AWS_CONTAINER_CREDENTIALS_FULL_URI` + token file, the same chain the Glue client and S3
  reads already use) and hands them to the opendal-backed writer as static credentials, so
  writes no longer fall back to EC2 IMDS in EKS Pod Identity pods. Existing static-env / IMDS
  setups are unaffected. See `docs/sinks/iceberg.md`.
- Internal hardening: removed a duplicated `#[cfg(feature = "delta")]` attribute flagged by
  newer clippy.

## v0.6.1

- **Structured `FloeError` type (#395).** `floe-core` previously returned
  `Box<dyn Error>` over four stringly-typed wrappers
  (`ConfigError`/`RunError`/`StorageError`/`IoError`); failures could only be
  classified by inspecting message strings. Every failure is now a structured
  `FloeError` enum variant (`Config`/`Validation`/`Storage`/`Sink`/`State`/`Run`/`Io`)
  carrying typed context fields (entity, path, rule). Library consumers — including
  the Python bindings and the CLI log emitter — recover the kind via
  `err.downcast_ref::<FloeError>()` and `FloeError::kind()`.
  - No user-visible behaviour change: `Display` output is byte-identical to the
    old wrappers, the typed Python exceptions (`FloeConfigError`, `FloeRunError`,
    `FloeStorageError`, `FloeIoError`) and CLI error codes are unchanged.
  - `FloeResult<T>` remains the boxed alias so foreign errors keep flowing through
    `?`. See `context/decisions/structured-floe-error.md`.
- Internal-only release: no config, CLI, or sink behaviour changes.

## v0.6.0

- **HMAC-SHA256 keyed hashing for `strategy: hash` (`docs/pii.md`, #388).**
  The optional `key:` field on a `pii.columns[]` entry selects HMAC-SHA256 instead of
  plain SHA-256, making hash-masked values computationally infeasible to reverse by
  dictionary attack while preserving referential integrity (same input + same key → same
  output across runs). The key accepts a plain literal or a `${ENV_VAR}` reference
  resolved at masking time.
  - Plain SHA-256 remains the default for backward compatibility.
  - A configuration warning is emitted when `strategy: hash` is used without `key:`.
  - Literal keys are redacted from manifest serialization so they never appear in the
    run manifest JSON.
- **`abfss://` URIs now accepted everywhere (#393).**
  The secure `abfss://` spelling (the one Azure surfaces in the portal and Databricks
  shows in external-location URIs) was previously not recognized and silently fell
  through to local-path handling. Floe now normalizes `abfss://` to the canonical
  `abfs://` at config-parse time, so both spellings work end-to-end in source paths,
  sink paths, state paths, and storage config.
  - `is_remote_uri` was deduplicated from four copy-pasted sites into a single
    canonical helper (`io/storage/core/uri.rs`); adding a scheme is now a one-line change.
- **Run reliability hardening (#389, #390).**
  - *Unity Catalog HTTP timeout*: the `reqwest` client used for Unity Catalog table
    registration had no timeout. A hung or unreachable Databricks workspace now times out
    after 30 seconds instead of blocking forever.
  - *Stale lock recovery*: a local `.lock` file left behind by a killed or OOM-killed
    process (previously permanent) is now broken automatically when older than 5 minutes.
    The lock records its acquisition timestamp + PID; the fallback is file mtime for
    legacy locks.
  - *CAS jittered backoff*: conditional-write retries no longer fire in lockstep with no
    delay. Full-jitter exponential backoff (50 ms base, 2 s cap) is applied between
    failed attempts, and the retry count is raised from 5 to 8. No external PRNG
    dependency — entropy is derived from the system clock.

### Internal hardening

- **pyo3 0.22 → 0.29 (#391):** clears two supply-chain advisories
  (RUSTSEC-2025-0020, RUSTSEC-2026-0177) that were parked under `[advisories].ignore`
  in `deny.toml`. The `cargo-deny` supply-chain gate is re-armed with both ignore
  entries removed. No API or behavior change visible to Python callers.
- **`cargo update`:** ~250 transitive lockfile bumps to latest semver-compatible
  versions (tokio 1.49→1.52.3, aws-sdk-s3 1.120→1.137, openssl 0.10.75→0.10.81,
  rustls 0.23.36→0.23.40, arrow 57.2→57.3.1, deltalake 0.30.1→0.30.2, and more).

## v0.5.6

- **Fixes the `build-duckdb-cli` Windows build failure introduced in v0.5.5.**
  The bundled C++ DuckDB library (`libduckdb-sys`) fails to compile with MSVC
  `cl.exe` (exit code 2 on `ub_src_storage_compression_chimp.cpp`). Windows was
  mistakenly included in the `build-duckdb-cli` matrix even though the
  `build-duckdb-wheel` job already excluded it for the same reason. The Windows
  entry is now dropped from `build-duckdb-cli`; four Unix/macOS platforms remain
  (Linux x86\_64/arm64, macOS x86\_64/arm64). The lean `floe.exe` Windows binary is
  **unaffected** — it is built by a separate job that does not enable the `duckdb`
  feature. Windows users who need DuckDB support can use
  `docker run ghcr.io/malon64/floe-duckdb` or build from source:
  `cargo build -p floe-cli --release --features duckdb`.
- **Supply-chain hardening and dependency migrations (PR #396):**
  - `cargo-deny` gate added (`deny.toml`): denies known advisories, yanked crates,
    non-whitelisted licenses, and non-whitelisted crate sources; bans
    re-introduction of `serde_yaml` and deprecated `azure_storage*` 0.x crates.
    A new `.github/workflows/supply-chain.yml` workflow runs the gate on every PR
    touching dependency files, weekly on a schedule, and on demand.
  - Dependabot configured (`.github/dependabot.yml`): weekly Cargo updates
    (minor + patch, grouped by ecosystem), GitHub Actions pin updates, and pip
    updates. `duckdb`/`libduckdb-sys` are ignored until the Arrow 58 migration
    lands (tracked in issue #387).
  - **Five security advisories resolved:**
    - `aws-lc-sys` 0.36 → 0.41: fixes RUSTSEC-2026-0044/0045/0046/0047/0048
      (X.509 name-constraint bypass, AES-CCM timing side-channel, PKCS7
      validation bypasses, CRL logic error).
    - `rustls-webpki` 0.103.9 → 0.103.13: fixes RUSTSEC-2026-0098/0099/0104/0049.
    - `lz4_flex` 0.11.5 → 0.11.6 and 0.12.0 → 0.12.1: fixes RUSTSEC-2026-0041
      (uninitialized-memory leak on invalid input).
  - **`serde_yaml` removed** (archived / unmaintained): `add_entity.rs` now uses
    the `yaml-rust2` stack already present in the config parser — one YAML library
    in the dependency tree instead of two.
  - **Azure SDK migrated to GA 1.0:** `azure_core` / `azure_identity` /
    `azure_storage` / `azure_storage_blobs` 0.20 (deprecated unofficial track) →
    `azure_core` / `azure_identity` / `azure_storage_blob` 1.0. Conditional writes
    (`If-Match` / `If-None-Match: *` on put; `If-Match` on delete) are preserved;
    conflict detection is now typed (`ErrorKind::HttpResponse` 412/409) instead of
    fragile string matching; 409 is now correctly mapped to `Conflict`.
    `DefaultAzureCredential` is replaced by a `ChainedCredential` that tries
    service-principal env vars → workload identity → managed identity → Azure CLI.
- Distribution-only and dependency patch — no engine, API, or config changes.

## v0.5.5

- **Distributes `floe-duckdb` as a prebuilt standalone CLI binary for all platforms.**
  Previous releases shipped the DuckDB-enabled build only as a Docker image
  (`ghcr.io/malon64/floe-duckdb`) and a Python companion wheel. The CLI binary had to
  be compiled from source. From this release, `floe-duckdb` binaries are attached to
  every GitHub Release for all five supported platforms (Linux x86\_64/arm64,
  macOS x86\_64/arm64, Windows x86\_64) and are available via the existing
  Homebrew tap and Scoop bucket:
  ```sh
  # macOS / Linux — Homebrew
  brew tap malon64/floe   # skip if already tapped
  brew install malon64/floe/floe-duckdb

  # Windows — Scoop
  scoop bucket add floe https://github.com/malon64/scoop-floe
  scoop install floe-duckdb
  ```
  `floe-duckdb` is a complete, standalone CLI (same commands, same config format as
  `floe`) with the DuckDB sink compiled in. It can be used directly (`floe-duckdb run
  config.yml`) or placed alongside the lean `floe` binary — `floe` auto-delegates a
  DuckDB-sink run to `floe-duckdb` without user intervention.
- Distribution-only patch — no engine, API, or config changes. DuckDB sink behavior
  and supported targets (Local + MotherDuck) are unchanged.

## v0.5.4

- **Fixes the Python DuckDB companion so `floe.run()` actually delegates to it.**
  The off-PyPI `floe-duckdb` wheel installs its native extension as a **top-level
  `_floe_duckdb`** package (maturin's pure-Rust layout), but the lean `floe`
  wrapper only looked for the nested `floe._floe_duckdb` submodule via
  `importlib.util.find_spec`. The two never matched, so a DuckDB-sink run on an
  install that *had* the companion still raised *"the `floe-duckdb` companion
  wheel is not installed"* — the Python DuckDB channel was non-functional as
  shipped. The wrapper now accepts **both** layouts (`floe._floe_duckdb` and
  top-level `_floe_duckdb`), preferring the nested name when present. Verified
  end-to-end against the published `floe-duckdb` wheel: a `merge_scd1` DuckDB sink
  inserts on first run and upserts idempotently on re-run.
- Operational note (no code change): the off-PyPI PEP 503 index at
  `https://malon64.github.io/floe/simple/` requires **GitHub Pages** to be enabled
  on the repository (source `gh-pages`); the release pipeline pushes the index to
  that branch but cannot enable Pages itself.
- **Fixes four OpenLineage emission bugs that made Floe's OL events unusable by
  OpenMetadata 1.12.x and other strict consumers** (fixes #382, PR #386):
  - S3 input dataset names no longer carry a leading slash (`split_storage_uri` now
    drops the `/` separator, so `s3://bucket/path` splits into `("s3://bucket", "path")`).
  - `runId` is now a spec-valid UUID v4 (a single `Uuid::new_v4()` is generated at
    `RunStarted` and reused at `RunFinished`; each entity gets its own UUID) instead
    of the raw timestamp-based run ID that OpenMetadata silently dropped.
  - `inputs`/`outputs` now use resolvable storage coordinates (Iceberg → catalog
    namespace + `{ns}.{table}`; all other sinks → `split_storage_uri` on the storage
    path) instead of unmappable logical names, so lineage edges are derivable. The
    redundant symlinks facets are removed.
  - Entity job names no longer prepend the OL namespace (it already lives in the job
    object's `namespace` field).
- The DuckDB distribution changes above are distribution-only — DuckDB sink behavior
  and supported targets (Local + MotherDuck) are unchanged.

## v0.5.3

- **Fixes the release pipeline so the GitHub Release assets, Homebrew/Scoop
  formulae, and off-PyPI `floe-duckdb` wheel actually publish.** In v0.5.2 the
  `release` job's `Download artifacts` step had no `pattern`, so it pulled *every*
  run artifact (~1.1 GB — including the ~80 MB DuckDB wheels and ~70 MB Python
  wheels) and intermittently failed the download "after 5 retries", which left the
  GitHub Release with no binary assets and skipped the dependent off-PyPI wheel
  publish. The step is now scoped to `pattern: dist-*` (only the platform binary
  archives it actually needs for the release assets and Homebrew/Scoop checksums);
  the DuckDB wheels remain handled by the separately-scoped `publish-duckdb-wheel`
  job. This was invisible to dry-runs because the `release` job is gated off on
  dry-runs.
- No engine or API changes: distribution-only patch. The DuckDB sink behavior,
  config surface, and supported targets (Local + MotherDuck) are identical to
  v0.5.0–v0.5.2.

## v0.5.2

- **Fixes the v0.5.1 release pipeline so the DuckDB companion actually ships.** v0.5.1
  published the lean `floe` (PyPI wheel + `ghcr.io/malon64/floe` image) correctly, but
  the `floe-duckdb` companion image and off-PyPI wheel never published because the
  companion image build failed. `Dockerfile.duckdb`'s dependency-cache layer copied
  `crates/floe-python/Cargo.toml` (which declares an explicit `[lib] name = "_floe"`)
  without its source, so `cargo fetch` failed parsing the workspace manifest
  (*"can't find library `_floe`"*). The fetch layer now stubs
  `crates/floe-python/src/lib.rs` before fetching; the real source replaces it in the
  subsequent `COPY`. This path was invisible to release dry-runs because the Docker
  jobs are gated off on dry-runs.
- No engine or API changes: distribution-only patch. The DuckDB sink behavior, config
  surface, and supported targets (Local + MotherDuck) are identical to v0.5.0/v0.5.1.

## v0.5.1

- **DuckDB is now available to non-Rust users via a lean-primary + heavy-companion
  distribution.** v0.5.0 made `duckdb` an opt-in Cargo feature, which left it reachable
  only by building from source. v0.5.1 ships the existing DuckDB sink (local `.duckdb`
  files and MotherDuck `md:` databases) as a separate companion that the lean,
  default-distributed `floe` transparently delegates to — modeled on the
  companion-on-PATH pattern used by `git`/`git-lfs` and cargo subcommands, and the
  ONNX-Runtime / PyTorch split-package model for Python.
  - **CLI:** the lean `floe` binary auto-detects a DuckDB sink in the resolved config
    and re-execs a `floe-duckdb` companion (same `floe-cli` source built
    `--features duckdb`) with identical arguments, propagating its exit status. The
    companion is resolved **only** from `PATH` or the canonicalized directory of the
    running executable — never the current working directory (avoids the git-lfs
    CVE GHSA-6rw3-3whw-jvjj class of attack). If no companion is found, the run fails
    with an actionable install hint instead of a cryptic missing-feature error.
  - **Docker:** new `ghcr.io/malon64/floe-duckdb` image (built from `Dockerfile.duckdb`)
    bundles the native DuckDB build with `ENTRYPOINT ["floe-duckdb"]`. The lean
    `ghcr.io/malon64/floe` image is unchanged.
  - **Python:** new off-PyPI `floe-duckdb` wheel installs a `floe._floe_duckdb`
    companion extension into the same `floe` package; the lean `floe.run()`
    transparently delegates a DuckDB-sink run to it. Because the bundled native build
    exceeds PyPI's 100 MB per-file limit and does not build in the manylinux/musllinux
    containers, the companion wheel is published off PyPI (GitHub Release assets + a
    PEP 503 simple index), à la `download.pytorch.org`:
    `pip install floe-python` then
    `pip install floe-duckdb --index-url https://malon64.github.io/floe/simple/`
    (`--index-url`, not `--extra-index-url`, so a squatted `floe-duckdb` on PyPI can't
    shadow the off-PyPI companion).
  - No engine changes: this release only changes *distribution*. The DuckDB sink
    behavior, config surface, and supported targets (Local + MotherDuck) are identical
    to v0.5.0. DuckLake and remote-DuckDB (Quack) sinks remain deferred (they require a
    DuckDB ≥1.5.3 / Arrow 58 bump that is blocked upstream until the Iceberg crate
    publishes an Arrow 58 release).
  - See [DuckDB sink](docs/sinks/duckdb.md) and [installation](docs/installation.md).

- **`floe` 0.5.1**: version bump for this release.

## dagster-floe v0.2.3

- **Floe assets now carry a `compute_kind="floe"` branding badge** (PR #384):
  - Every asset materialized by `dagster-floe` displays a "floe" compute-kind badge
    in the Dagster UI, consistent with how `dagster-dbt` and `dagster-duckdb` brand
    their assets. Cosmetic/UX only — no change to asset keys, execution, or run config.

## dagster-floe v0.2.2

- **Expose public job run-config helpers for mixed asset jobs** (closes #376, PR #378):
  - `resolve_execution_config(execution)` — derives the Dagster multiprocess run-config dict from a parsed `ManifestExecution` object; returns `None` when no concurrency constraint is set.
  - `build_job_run_config_from_manifest(manifest_path)` — convenience wrapper that loads the manifest and delegates to `resolve_execution_config`.
  - Both are exported from the top-level `floe_dagster` package.
  - Users composing mixed Dagster asset jobs (Floe + dlt + dbt + other assets) can now pass the manifest-derived run config directly to `define_asset_job(config=...)` without duplicating internal logic.

## v0.5.0

- **Accepted sinks are now opt-in Cargo features** (PR #375):
  - `delta`, `iceberg`, and `duckdb` are each gated behind their own Cargo feature in `floe-core`.
    Parquet (via Polars) is always compiled; the heavy sink dependencies are pulled in only when
    requested. `floe-core`'s default features are `["delta", "iceberg"]`, and `floe-cli` /
    `floe-python` forward the same defaults — so the shipped CLI and published wheels keep
    Delta + Iceberg support and stay lean.
  - **`duckdb` is opt-in everywhere.** The bundled native DuckDB build inflated compile times and
    broke the manylinux/musllinux wheel cross-compile (and pushed wheels past PyPI's per-file size
    limit), so it is excluded from default builds. Enable it from source with `--features duckdb`.
  - **Breaking for library consumers:** code depending on `floe-core` with `default-features = false`
    no longer compiles any sink; add the features you need (`delta`, `iceberg`, `duckdb`).
  - Config validation and manifest generation accept any *known* sink format — including one whose
    feature is not compiled into the current build — so a lean build still validates and round-trips
    a config it cannot execute. Only the runtime write path enforces the feature, failing with a
    clear `rebuild with --features <sink>` hint.
  - CI now compiles and tests each sink selectively (path-filtered jobs with isolated caches), so the
    expensive DuckDB bundle is built only when DuckDB code changes.

- **`floe` 0.5.0**: version bump for this release.

## v0.4.7

- **DuckDB accepted sink** (closes #248, PR #372):
  - New `sink.accepted.format: duckdb` writes accepted output into a DuckDB database — a local
    `.duckdb` file or a remote [MotherDuck](https://motherduck.com) (`md:<database>`) database.
  - Full write-mode parity with Delta: `overwrite`, `append`, `merge_scd1`, and `merge_scd2` via
    DuckDB's native `MERGE INTO` (bundled DuckDB 1.5.0). Merge metrics are reported from
    `RETURNING merge_action`.
  - Arrow-native ingestion (no intermediate file); the bundled `duckdb` crate pins Arrow 57 to
    match floe-core.
  - MotherDuck token auth via `${ENV}` substitution, passed through the connection configuration
    (never as SQL) and never logged.
  - Hardened connection handling: a process-wide single-writer connection cache keyed by canonical
    database id serializes writes and reuses one handle across flushes and entities. The MotherDuck
    cache key includes a non-reversible token fingerprint so distinct credentials never share a
    cached connection.
  - The `duckdb` sink block is carried through generated manifests, so manifest replay can address
    the target table / MotherDuck database.
  - `overwrite` replaces only the target table, leaving other tables in the same database intact.
  - Object-store `.duckdb` file paths (S3/GCS/ADLS) are rejected at validation with a pointer to
    MotherDuck (DuckDB cannot read-write database files over object storage).
  - See [DuckDB sink](docs/sinks/duckdb.md).

- **Fix: normalize raw headers before projection in Parquet and ORC readers** (PR #373):
  - `projected_columns` was comparing raw file headers (e.g. `"Customer ID"`) against
    already-normalised declared column names (e.g. `"customer_id"`), so any column renamed by
    `schema.normalize_columns` was silently projected out before reaching checks or the sink.
  - Fix: `normalize_name` is now applied to each raw header before the set-membership test,
    matching the pattern already used in the Avro reader.

- **`floe` 0.4.7**: version bump for this release.

## v0.4.6

- **`dagster-floe`: load remote report URIs via fsspec** (fixes #361, PR #362):
  - `summary_uri` and `entity_report_uri` values pointing at `s3://`, `gs://`, or `abfs://` were raising a `ValueError` that was silently swallowed, causing asset checks to fall back to summary-only mode. They are now fetched correctly via `fsspec`.
  - New `remote` optional extra (`dagster-floe[remote]`) installs `s3fs`, `gcsfs`, and `adlfs` so all three cloud providers are covered out of the box.

- **`dagster-floe`: tolerate mixed stdout and surface floe events in Dagster logs** (PR #363):
  - The log parser now handles non-JSON lines interleaved with NDJSON floe events, so plain-text output from wrappers or logging middleware no longer breaks event parsing.
  - Floe run events (`run_started`, `entity_finished`, `run_finished`) are surfaced as structured Dagster log entries with appropriate log levels.

- **Soft-buffered accepted writes — cap per-entity memory at `max_size_per_file`** (PR #366):
  - Previously all accepted rows for an entity were accumulated in memory across every input file and written in a single pass at the end. Peak RAM scaled with input fanout.
  - Accepted rows are now flushed to the sink incrementally whenever the in-memory accumulation reaches `sink.accepted.options.max_size_per_file` (default 256 MB). Peak memory is bounded regardless of how many input files the entity processes.
  - `merge_scd1` / `merge_scd2` modes are unaffected — they still accumulate the full dataset before writing.
  - **Durability note**: flushes commit while the batch is still running — if a later input file fails, rows from earlier files are already committed and will be re-written on the next run. In `append` mode this produces duplicate output rows; use `schema.unique_keys`, `merge_scd1`, or `merge_scd2` to guard against this. See [Parquet sink limitations](docs/sinks/parquet.md#limitations) for details (the same partial-commit exposure applies to Delta and Iceberg accepted sinks, though their individual flushes are transactional).

- **Stream Parquet writes via the Polars `new_streaming` engine** (PR #369):
  - Accepted Parquet writes now emit one row group at a time rather than buffering the full chunk plus Arrow encoding state. This reduces per-write peak memory, most noticeably on wide schemas or large `row_group_size` values.
  - No config changes required. Output file naming, part counts, and the run report shape are unchanged.

- **Profile `execution.orchestration` → manifest → Dagster job concurrency** (PR #367):
  - New `orchestration` block in the profile's `execution` section (`strategy`, `max_concurrent_entities`). The manifest builder emits it as `execution.orchestration`; `dagster-floe` wires `max_concurrent` into the Dagster `multiprocess` executor config, driving entity-level parallelism end-to-end without per-job boilerplate.
  - `max_concurrent_entities: 0` is rejected at parse time with an immediate config error.
  - Profile JSON Schema updated to declare the new block (previously rejected by `additionalProperties: false`).

- **REST Iceberg: resolve environment-variable credentials** (PR #370):
  - REST catalog configs that use `${ENV_VAR}` placeholders for credentials are now resolved against the process environment before the HTTP client is constructed, fixing authentication failures when credentials are injected at runtime rather than baked into the config file.
  - Malformed placeholder errors are redacted in log output to avoid leaking partial secrets.

- **`floe` 0.4.6, `dagster-floe` 0.2.1, `airflow-floe` 0.1.5**: version bumps for this release.

## v0.4.5

- **Iceberg: propagate S3 endpoint and path-style-access into FileIO** (fixes #352, PR #353):
  - For S3-compatible storage (SeaweedFS, MinIO), Iceberg/OpenDAL requires an explicit `endpoint` and `path_style_access` in addition to region. Without these, OpenDAL picks up the wrong `AWS_ENDPOINT_URL` and fails at Parquet file close.
  - Added optional `endpoint` and `path_style_access` fields to `StorageDefinition`; `iceberg_store_config()` now forwards them as `s3.endpoint` and `s3.path-style-access` when present.
  - The metadata `S3Client` is also wired: `build_client()` passes `endpoint_url` and `force_path_style` from `StorageDefinition`, so listing and uniqueness-seed calls to MinIO/SeaweedFS use the correct addressing style.

- **Manifest: write remote summary and enrich `run_finished` event** (fixes #354, PR #356):
  - In manifest mode with a remote `report_base_uri`, `RunContext::from_config()` was skipping `Target` construction and the run summary was never uploaded; `run_finished.summary_uri` was always `null`.
  - `StorageResolver` gains `find_definition_name_for_uri` / `register_definition` so a synthetic storage definition is created for remote report URIs even when the bucket is not listed in the config `storages` block.
  - `RunFinished` is extended with `report_base` and `entity_report_uris` (both `skip_serializing_if`-guarded, so existing parsers see identical JSON when no report is configured).
  - `dagster-floe`: `FloeRunFinished` + `parse_run_finished` + `assets.py` consume `entity_report_uris` directly when available.
  - `airflow-floe`: `operators.py` and `kubernetes_runner.py` pass the new fields through to the XCom payload.

- **Manifest: deterministic generation, source checksums, and content revision** (fixes #355, PR #357):
  - New `--deterministic` flag: sets `generated_at_ts_ms=0` so the same inputs produce byte-identical output — clean `diff` in CI without stripping the timestamp.
  - New `--manifest-name` flag: stores a stable logical name (e.g. `sales.prod`) in the manifest JSON.
  - `config_checksum` (SHA-256) is computed on every generation; `profile_checksum` is added when a profile is supplied.
  - `manifest_revision`: SHA-256 of canonical manifest content (excluding `generated_at_ts_ms`) provides a stable content fingerprint.
  - `manifest_id` now incorporates `config_checksum` so the ID changes when source config content changes.
  - All `HashMap` fields in manifest structs (`exit_codes`, `env`, `definitions`, `tags`, `env_parameters`) replaced with `BTreeMap` for stable alphabetical key ordering in serialised JSON.
  - `dagster-floe` and `airflow-floe` manifest dataclasses parse and surface the four new optional fields (`manifest_name`, `manifest_revision`, `profile_uri`, `profile_checksum`).
  - JSON Schema updated to declare the new fields, preventing `additionalProperties: false` rejections in the Dagster manifest validator.

- **`floe manifest generate` — deployment URI overrides** (fixes #358, PR #359):
  - `-c` and `-p` now accept remote URIs (`s3://`, `gs://`, `abfs://`), downloading transparently and recording the URI as `config_uri` / `profile_uri` in the manifest.
  - `--output` accepts remote URIs; when a remote URI is given the manifest is uploaded and that URI is automatically baked into `execution.base_args` (replacing the `{manifest_uri}` placeholder), eliminating post-generation JSON patching.
  - `--default-domain <domain>` sets `domain`, `group_name`, and `asset_key` prefix for entities that do not specify a domain.
  - `--manifest-path-mode resolved-uri` sets each entity's `source.path` and sink `path` fields to the fully resolved URI (stripping `local://` for local paths), making the manifest a standalone replay contract without a local config directory.
  - Unknown `--manifest-path-mode` values now produce an immediate CLI error instead of silently defaulting.

- **`floe` 0.4.5, `dagster-floe` 0.1.8, `airflow-floe` 0.1.5**: version bumps for this release.

## v0.4.4

- **REST catalog: correct storage factory for S3 and GCS** (fixes #348 #349):
  - `build_rest_catalog()` previously always used `LocalFsStorageFactory` regardless of warehouse scheme, causing Parquet data file reads/writes to fail with OS-level errors on any cloud-backed REST catalog (Polaris, Snowflake Open Catalog, Nessie with S3).
  - The factory is now selected based on the concrete table location URI (`s3://`, `s3a://` → `OpenDalStorageFactory::S3`; `gs://` → `OpenDalStorageFactory::Gcs`; everything else → `LocalFsStorageFactory`).
  - When the warehouse field is a catalog name rather than a storage URI (e.g. Polaris `"lakehouse"`), the table location from `warehouse_location` / `table_root_uri` is used as the primary dispatch key, with `warehouse` as fallback.
  - Both write and uniqueness-seed (Append dedup) paths are fixed.

- **`floe` 0.4.4**: version bump for this release.

## v0.4.3

- **Remote manifest support for `floe run --manifest`** (`docs/cli.md`, fixes #342):
  - `floe run --manifest` now accepts `s3://`, `gs://`, and `abfs://` URIs, downloading the manifest to a temporary directory using the same `resolve_config_location` infrastructure used by `floe run --config`.

- **OpenLineage: stable job name, versioned `_producer`, column lineage, and Dagster job injection** (`docs/lineage.md`, fixes #335 #336 #337 #338):
  - Top-level `RunStarted`/`RunFinished` events now use a **stable job name** derived from the config file stem (e.g. `orders.yml` → job name `orders`) instead of the ephemeral run UUID, so run history accumulates on a single job node in Marquez.
    - An optional `lineage.job_name` field in the config overrides the derived name.
  - The `_producer` URI is now **versioned** via `env!("CARGO_PKG_VERSION")` at compile time (e.g. `https://github.com/malon64/floe/releases/tag/v0.4.3`) instead of a static string, enabling consumers to correlate lineage events with the exact release.
  - **`ColumnLineageDatasetFacet`** is emitted on accepted output datasets, mapping each output column to its source field (or itself when no `source:` rename is configured). The `inputFields` dataset identity uses the same `(namespace, name)` tuple as the input dataset in the same event, so column-level edges are correctly joined in OpenLineage consumers.
  - `dagster-floe` now injects **`DAGSTER_JOB_NAME`** into the floe subprocess environment, so the `ParentRunFacet` carries the correct Dagster job name rather than an empty string.

- **OpenLineage: Marquez-compatible dataset naming and facet placement** (`docs/lineage.md`, fixes #340):
  - Source input datasets use a `.source` sub-namespace (e.g. `myns.source`) to avoid colliding with the logical entity name used for accepted outputs.
  - `SchemaDatasetFacet`, `DataQualityMetricsOutputDatasetFacet`, and `FloeQualityRunFacet` are placed on the accepted output dataset (the write side), matching OpenLineage's semantic: output facets describe what was written.
  - Rejected output datasets use a `.rejected` sub-namespace and carry their own `DataQualityMetricsOutputDatasetFacet` with the rejected row counts.

- **`dagster-floe`: `SourceAsset` registration and enriched rejected metadata** (fixes #339):
  - Each entity now registers a companion `SourceAsset` (key `<entity_key>_source`) so Dagster's asset graph shows the upstream data source as a node. `register_source_assets=False` opts out for pipelines that manage their own upstream declarations.
  - Rejected output metadata now includes `dominant_rejection_reason` and `files_with_rejections` computed from the entity report, giving Dagster run pages actionable context without opening the floe report file.

- **`dagster-floe` 0.1.7**, **`floe` 0.4.3**: version bumps for this release.

## v0.4.2

- **OpenLineage: source and sink datasets emitted separately** (`docs/lineage.md`, fixes #317):
  - Entity `COMPLETE`/`FAIL` events now set `inputs` to the **source dataset** (named after `source.path`) and `outputs` to the **accepted sink dataset** (named after `sink.accepted.path`).
  - When a rejected sink is configured, a second output dataset (named after `sink.rejected.path`) is appended to `outputs`.
  - Schema (`SchemaDataset`), `DataQualityMetrics`, and `FloeQualityRun` facets are attached to the source input dataset, which is the correct OpenLineage placement for input facets.
  - Previously, `inputs` contained a single dataset named after the entity (not the path) and `outputs` was always empty, causing Marquez to show a dead-end lineage graph.

- **OpenLineage observer now installed from profile `lineage` block** (fixes #316):
  - `floe run --profile` now uses `load_config_with_profile_overrides` for the early config load that wires up the OpenLineage observer. Previously it used `load_config_with_profile_vars`, which substituted `{{VAR}}` placeholders but did not merge the `lineage` section from the profile. Users who declared `lineage:` only in their profile file saw no lineage events emitted.

- **Profile support for `floe state inspect` and `floe state reset`** (fixes #320):
  - Both state subcommands now accept `-p / --profile <path>`. Without a profile, configs that reference `{{VAR}}` placeholders would fail with "unknown variable" errors. The same profile-loading path used by `validate` and `run` (variable resolution + storages/catalogs/lineage merging) is now applied before reading or resetting entity state.

- **Incremental state committed for rejected terminal outcomes** (fixes #327):
  - In `incremental_mode: file`, files whose rows are fully or partially rejected under `severity: reject` are now committed to state after the run. Previously, any `RunStatus::Rejected` caused state to be released, making Floe reprocess the same file on every subsequent run and duplicate accepted/rejected sink outputs. `Aborted` and `Failed` outcomes still release state, as those represent incomplete processing where reprocessing is safe.

- **`dagster-floe` 0.1.6**: version bump accompanying the floe 0.4.2 release.

## v0.4.1

- **Profile `storages` and `lineage` overrides** (`docs/profiles.md`):
  - Profile files can now declare a `storages` section and a `lineage` section alongside the existing `catalogs` override. Both are applied as wholesale config replacements before validation and run, following the same pattern as `catalogs`.
  - `floe validate --profile`, `floe run --profile`, and `floe manifest generate --profile` all honour the new sections.
  - Python bindings (`floe.validate`) pass storages and lineage through to the merged config.
  - Profile validation now enforces the same semantic constraints as config validation:
    - `storages.default` is required whenever `storages` is declared.
    - Storage definitions must use a supported type (`local`, `s3`, `adls`, `gcs`) and supply type-required fields (`s3`: bucket + region; `adls`: account + container; `gcs`: bucket).
    - `lineage.namespace` must not be empty; `lineage.max_failures` must be ≥ 1 when set.

## v0.4.0

- **OpenLineage circuit breaker and retry** (`docs/lineage.md`):
  - Transient failures (connection errors, HTTP 5xx, 429) are retried up to 3 times with 0 / 100 / 500 ms backoff before counting as a failure.
  - Non-retryable 4xx responses (e.g. 401 bad API key) are counted immediately without retrying and emit a `lineage_http_error` warning.
  - After `max_failures` consecutive failures (default 3, configurable via `lineage.max_failures`), the circuit opens and all subsequent events in the run are skipped with no HTTP calls.
  - A single `lineage_circuit_open` warning is emitted when the circuit trips. The circuit resets at `RunStarted` so a recovered endpoint is retried in the next pipeline execution without restarting the process.
  - A `lineage_http_error` warning is emitted whenever any event is dropped so silent event loss is always visible in operator logs.

- **Hardened remote incremental state** (`docs/how-it-works.md`):
  - State schema bumped to v2 — a `claims` map sits alongside the existing `files` map, enabling CAS-based concurrent-writer safety on S3, GCS, and ADLS via conditional writes (`If-Match` / generation numbers).
  - Local storage uses `FileLock` (O_CREAT|O_EXCL) to serialize version-check + write, preventing two concurrent local processes from both returning `Written`.
  - Claim scope is restricted to the owning runner: promotion, release, and renewal only touch URIs held by the current `ClaimedEntityState`, preventing cross-runner interference when multiple processes share a `run_id`.
  - Unexpired claims from the same `run_id` now block re-claiming — two processes sharing a `run_id` can no longer process the same files concurrently.
  - Deletes remain conditional even when `expected_version` is `None`, preserving state written by a concurrent runner.
  - V1 state files (no `claims` field) are silently upgraded to V2 on read.

- **Kubernetes manifest runner fields** (`context/orchestrators.md`):
  - `floe manifest generate` with a Kubernetes profile now emits full runner fields in the manifest: `image`, `namespace`, `service_account`, `resources` (cpu / memory_mb), `env`, and structured `secrets`.
  - `secrets` changed from `Vec<String>` to `Vec<{name, secret_name, key}>` to match the shape expected by `kubernetes_runner.py`.
  - `dagster-floe[kubernetes]` optional extra added to express the Kubernetes client dependency explicitly.

- **Internal hardening** (no config or API changes):
  - `PolicySeverity` is now a typed enum parsed at config load time — string comparison in mismatch, precheck, and run/entity paths replaced with enum match.
  - `AcceptedWriteReportState` removed — `run_accepted_write_phase` returns `AcceptedWriteOutput` directly, eliminating a 24-field manual field-by-field copy.
  - Error types (`ConfigError`, `RunError`, `StorageError`, `IoError`) adopt `thiserror` — removes ~40 lines of boilerplate `Display + Error` impls.
  - `SinkFormat` trait centralises write, seed, and data-driven config validation — adding a new format no longer requires changes to validate.rs or dispatch code.
  - State module eliminates duplicated load / persist / claim patterns (76 fewer lines, same behaviour).

- **macOS wheels**: universal2 wheel replaces separate aarch64 + x86_64 artifacts — a single `.whl` runs natively on both Apple Silicon and Intel Macs.

- **`dagster-floe` 0.1.5**: adds `dagster-floe[kubernetes]` optional extra so consumers can declare the `kubernetes>=28` dependency explicitly when using the Kubernetes runner.

## v0.3.9

- **Column-level PII masking** (`docs/pii.md`):
  - New `pii:` entity block with per-column transform strategies: `hash` (SHA-256), `drop`, `nullify`, `redact`, and `mask`.
  - `mask` strategy supports a pattern string with `{firstN}` / `{lastN}` reveal tokens (e.g. `{first4}****{last4}` for card numbers).
  - `redact` strategy replaces every non-null value with a configurable string (default `[REDACTED]`).
  - Masking runs after schema validation and data-quality checks — rejected rows are never masked.
  - Compatible with `normalize_columns`: `pii.columns[].name` matches schema names, not post-normalization runtime names.
  - Full config-validation coverage: duplicate columns, unknown strategies, missing `mask_pattern`, and `strategy: drop` on primary-key columns are all rejected at load time.

- **OpenLineage integration** (`docs/lineage.md`):
  - New optional `lineage:` top-level config block that posts run and entity lifecycle events to any OpenLineage-compatible HTTP endpoint (Marquez, Atlan, OpenMetadata, Astronomer, etc.).
  - Emits four facets per entity `COMPLETE` event: `DataQualityMetrics`, `FloeQualityRun`, `SchemaDataset`, and `ParentRun`.
  - `ParentRun` facet auto-detected from Airflow (`AIRFLOW_CTX_*`) and Dagster (`DAGSTER_*`) environment variables.
  - Bearer auth via `api_key` field; supports `{{VAR}}` placeholder expansion and nested `${HOST}/path` variable resolution via `--profile`.
  - Configurable `timeout_secs` (default 5); fail-silent — HTTP errors emit warnings and do not affect run outcome.
  - Lineage emission is independent of `--log-format`; events are always posted when configured.
  - Lineage HTTP error warnings (401, timeouts) are always surfaced to stderr, even with `--log-format off`.
  - Lifecycle correctness: every run produces a well-formed `START → COMPLETE/FAIL` sequence; pre-execution failures (config validation, context construction) emit a paired `START` before `FAIL`; post-start errors do not re-emit `START`.

- **Profile validation and catalog hardening** (v0.3.8 patch):
  - Profile variable resolution errors now produce explicit config errors instead of silently falling back to unresolved placeholders.
  - Catalog override validation tightened for Unity and REST catalog blocks.

## v0.3.8

- Delta Lake + Unity Catalog registration:
  - new `unity` catalog type — after writing a Delta table to cloud storage, Floe
    optionally registers (or confirms) it as an EXTERNAL DELTA table in Databricks
    Unity Catalog via the REST API (`POST /api/2.1/unity-catalog/tables`).
  - existing-table safety: location mismatch and missing `storage_location`
    (managed table / view collision) are returned as explicit errors.
  - ADLS: `abfs://` URIs are normalised to `abfss://` before registration.
  - new `sink.accepted.delta` block: `catalog`, `schema`, `table` overrides.
  - `create_schema_if_missing` flag on the catalog definition.
  - Validation: `sink.accepted.delta` rejected on non-delta formats; Unity type
    rejected for Iceberg sinks; local storage rejected for Unity targets.
  - Three new report fields: `delta_catalog_name`, `delta_catalog_schema`,
    `delta_catalog_table`.
  - `token` accepts a literal PAT or a single `${ENV_VAR}` reference resolved from the OS environment at run time.

## v0.3.7

- Windows binary now distributed via [Scoop](https://github.com/malon64/scoop-floe): `scoop bucket add floe https://github.com/malon64/scoop-floe` then `scoop install floe`.
- Release pipeline extended with a `x86_64-pc-windows-msvc` build target; artifact packaged as `.zip`.
- GitHub Actions upgraded to Node.js 24 runtime across all workflows.

## v0.3.6

- Added `--profile <path>` flag to `floe run` for environment-specific variable injection into `{{VAR}}` config placeholders.
- Profile variables support `${KEY}` cross-references; precedence chain: `env.vars` > `env.file` > profile variables.
- Updated `profile.schema.yaml` with `kubernetes_job`/`databricks_job` runner types and accurate variable semantics.
- Internal cleanup: shared test helper, consolidated `emit_log`, removed 8 redundant unit tests.
- README rewrite: architecture diagram, pipeline stage table, `--profile` example.

## v0.3.5

- Incremental ingestion release finalization:
  - documented `incremental_mode: file`, entity `state.path`, and the `floe state inspect/reset` CLI flow
  - refreshed the repository example config to show file-based incremental ingestion state explicitly
  - clarified incremental planning and state-commit behavior in the docs summary/reference set
- Packaging/version updates:
  - bumped `floe-core` and `floe-cli` to `0.3.5`

## v0.3.4

- Databricks runner foundation for orchestrator connectors:
  - added `databricks_job` runner contract support in Floe profile/manifest flow
  - added connector-side Databricks job client flow for Airflow and Dagster
  - standardized Databricks run status/result mapping with backend metadata and failure reason handling
  - added mocked integration coverage for success, failure, timeout, canceled, and infra failure paths
- Packaging/version updates:
  - bumped `floe-core` and `floe-cli` to `0.3.4`
  - bumped `airflow-floe` and `dagster-floe` Python packages to `0.1.4`

## v0.3.3

- Runner contract and architecture cleanup:
  - standardized Kubernetes runner naming to `kubernetes_job` across core/CLI/orchestrator-facing manifest surfaces
  - removed legacy runner alias handling for `kubernetes`
  - removed legacy Airflow manifest bridge from `floe.plan.v1` to `floe.manifest.v1` (manifest-v1 only)
- Connector reliability/refactor:
  - improved Kubernetes status normalization and failure-reason surfacing for orchestrator runner execution paths
  - reduced Airflow/Dagster drift in k8 runner handling and metadata surfaces
- Packaging/version updates:
  - bumped `floe-core` and `floe-cli` to `0.3.3`
  - bumped `airflow-floe` and `dagster-floe` Python packages to `0.1.3`

## v0.3.2

- Delta schema evolution rollout:
  - added additive schema evolution support for Delta accepted writes across `append`, `overwrite`, `merge_scd1`, and `merge_scd2`
  - enforced rollout guardrails: additive-only changes, partitioned-table limitations, and merge-key protection against schema-evolved columns
  - added explicit observability via the entity report `schema_evolution` block and the `schema_evolution_applied` lifecycle event

## v0.3.1

- Delta merge write modes:
  - added `merge_scd1` support for keyed upserts into accepted Delta outputs
  - added `merge_scd2` support for history-preserving merges with managed current/validity columns
- Schema/config validation:
  - added `schema.primary_key` as the explicit merge key surface, including validation for required, known, unique, non-nullable key columns
- Merge/reporting refinements:
  - added configurable Delta merge options and SCD2 closed/unchanged reporting metrics

## v0.3.0

- Core performance and observability:
  - added internal phase timing instrumentation (gated by `FLOE_PERF_PHASE_TIMINGS`) for run- and entity-level phases
  - reduced CSV read overhead by reusing prechecked input columns in the read path
  - optimized JSON `read_parse` path by reducing intermediate row allocations during selector extraction.
- Core refactoring (no intended behavior change):
  - split `run/entity` validation + split/reject flow into dedicated modules
  - split accepted write phase/report-state plumbing out of `run/entity/mod.rs`
  - further modularized writer internals (Delta/Iceberg) and report plumbing for maintainability.
- Documentation:
  - refreshed v0.3 release communication draft and docs updates for the new core organization/perf instrumentation.

## v0.2.8

- Iceberg accepted sink:
  - runtime execution of `sink.accepted.partition_spec` for supported transforms (`identity`, `year`, `month`, `day`, `hour`)
  - write-time file sizing metrics in entity reports (`files_written`, `total_bytes_written`, `avg_file_size_mb`, `small_files_count`)
- Delta accepted sink:
  - exact remote commit-log write metrics (S3/GCS/ADLS) collected via object store commit entry parsing (best-effort fallback keeps metrics nullable)
- `dagster-floe` connector package version bumped to `0.1.2` in the same release prep.

## v0.2.7

- Added Iceberg S3 accepted sink support (filesystem catalog, no data catalog / Glue yet).
- Improved `floe add-entity` bootstrap UX:
  - create a new config when target `-c` file does not exist
  - infer default entity name from input filename stem when `--name` is omitted
  - infer default format from input file extension when `--format` is omitted.
- Hardened archive mode safety:
  - archive only resolved/processed inputs
  - collision-safe archive behavior (no silent overwrite on repeated filenames).

## v0.2.6

- Added local Iceberg accepted sink MVP (`sink.accepted.format: iceberg`) with filesystem table layout and append/overwrite snapshot behavior.
- Added cloud source glob pattern support (e.g. `s3://.../sales_*.csv`) with prefix-derived listing + glob filtering.
- Added `floe add-entity` CLI command to infer a schema from an input file and append an entity definition to a Floe config.

## v0.2.5

- Added `floe manifest generate` as the common manifest surface for orchestrators (`floe.manifest.v1`).
- Airflow connector updates:
  - manifest-first operator flow (`FloeRunOperator` / `FloeManifestHook`)
  - multi-manifest DAG registration (`FLOE_MANIFEST_DIR`, one DAG per manifest)
  - streamed Floe stdout/stderr in task logs
  - enriched asset event metadata with per-entity report references.
- Dagster connector updates:
  - migration to common manifest contract
  - strict manifest schema validation
  - multi-manifest loading with one Dagster job per manifest and collision guards.
- CLI cleanup:
  - removed `floe validate --output json` (orchestrator automation now relies on generated manifests).
- Documentation updates for local orchestrator workflows and manifest-driven integration specs.

## v0.2.4

- Added new source input formats: `tsv`, `xlsx`, `fixed`, `orc`, `avro`, and `xml`.
- Added nested JSON selector extraction from `schema.columns[].source` with top-level-only mismatch checks.
- Added source-column mapping behavior (`source` as input selector, `name` as output column) in validation and run surfaces.
- Added XML selector support (`.` or `/` paths and terminal `@attribute`) with required `source.options.row_tag`.
- Dry-run now resolves inputs before execution, shows resolved file lists (with capped preview), and uses the same resolved plan as real runs.
- Refactored runtime/storage boundaries to reduce format- and storage-specific coupling in run orchestration.

## v0.2.3

- Added append/overwrite write modes for accepted parquet and delta outputs.
- Rejected outputs now use dataset-style part files with write modes.
- Append mode now seeds unique checks from existing accepted data (parquet + delta).
- Part naming improvements:
  - UUID parts for append
  - sequential parts for overwrite
  - unified part listing/cleanup helpers.
- Delta append fixes:
  - enforce schema nullability correctly
  - respect `normalize_columns` schema names.
- Dagster Docker runner now writes outputs correctly.

## v0.2.2

- Added Dagster orchestrator connector (`orchestrators/dagster-floe`) with:
  - validate plan ingestion
  - NDJSON run event parsing
  - local and Docker runner integration examples/tests.
- Introduced accepted write-mode scaffolding and design groundwork:
  - `WriteMode` in config model (default overwrite behavior retained)
  - accepted writer abstraction/module split for future append support
  - design note: `docs/design/write_modes.md`.
- Release workflow hardening:
  - build Linux AMD64 artifacts with `cross` to improve glibc compatibility.

## v0.2.1

- Added official Docker image packaging and GHCR publishing on version tags.
- Documentation: Docker usage added to installation guide and summary.
- Added machine-readable run event logs (json/text) behind `--log-format`.
- Fixes: `metadata.project` optional, CSV empty fields treated as null by default, default `cast_mode=strict`.

## v0.2.0

- Cloud storage registry with local/S3/ADLS/GCS support (canonical URIs).
- Cloud IO for CSV/JSON/Parquet inputs via temp download + local read.
- Accepted outputs: parquet (multipart with `max_size_per_file`) and delta (transactional object_store).
- Reports and rejected outputs can be written to cloud via temp upload.
- Schema mismatch policy (missing/extra columns) with file-level rejection handling.
- Deterministic run pipeline (file precheck → row validation → entity-level unique).
- JSON input modes: array (default) and NDJSON.
- Config templating (`env` + `domains`) and improved validation errors.

## v0.1.7

- Added ADLS and GCS storage definitions with URI resolution and validation.
- Implemented ADLS and GCS storage clients (list/download/upload) for file IO.
- Added Delta transactional writes via object_store for S3, ADLS, and GCS.
- Parquet accepted output now supports multipart chunking via `max_size_per_file`.
- Added config templating (`env` + `domains`) and duplicate domain validation.
- Refactored storage interface and reorganized test binaries/modules for faster CI.
- CI: path-based filters, shared rust-cache across branches, and controlled test threading.

## v0.1.6

- Added JSON array input mode alongside NDJSON, with config validation updates.
- Added entity-level accepted output with part files and cleared stale S3 outputs.
- Enforced validation order (file → row → entity) and fixed unique checks by rechunking.
- Expanded and reorganized core test suites (grouped modules) and improved CI test caching.

## v0.1.5

- Refactored storage configuration and IO to use a shared storage client abstraction.
- Split the entity runner into focused modules and centralized warning handling.
- Added configurable row error formatting (json/csv/text) in reports.
- Unified input resolution logic and reorganized run/config tests.
- Improved warn-mode performance and added lazy scan paths for CSV/Parquet.
- Added benchmark harness docs/scripts and example reports for v0.1.5.

## v0.1.4

- Added delta accepted sink support with overwrite semantics (local storage only).
- Added iceberg accepted sink configuration stub with a clear not-implemented error.
- Added `sink.accepted.options` for parquet compression and row group sizing.

## v0.1.3

- Added a format registry with adapter traits for inputs and sinks.
- Centralized file extension handling for local/S3 resolution.
- Expanded input format scaffolding (Parquet/JSON readers) and IO dispatch.
- Run pipeline refactor to use format adapters and shared IO helpers.

## v0.1.2

- Config validation now rejects unknown fields and adds broader negative test coverage.
- CLI and report examples use build-time version strings (no hardcoded 0.1.0).
- Release pipeline hardening: portable version bumping, allow-dirty publish, and tap checkout fixes.

## v0.1.1

- Global report layout with run-scoped directories and per-entity reports.
- CI/CD release pipeline (crate publish, binaries, checksums, GitHub Release, Homebrew tap).
- CLI output improvements and documentation refreshes for reports/checks.

## v0.1.0

- Initial CLI (`validate`, `run`) and YAML configuration schema.
- CSV ingestion with Parquet accepted output and CSV rejected output.
- Core checks: casting, not-null, unique; row-level rejection and run reports.
- Multi-entity support with run-scoped reporting.
