# Decision — Delta Lake sink for 1.0

## Status

Proposed (design for #1263, Unity Catalog #1266). No code yet. The maintainer approved the `deltalake` dependency
behind a `delta` extra on 2026-11-20 (comment on #1263). This ADR answers the design questions of #1263 and splits
the work into Task sub-issues; the planner creates them. Reverses D1 for Delta (and, with #1266, for Unity) once
accepted.

## Context

Floe 0.8 dropped Delta with the Rust port (D1, `python-core-for-olf.md`). The target is Floe running on Databricks
(`databricks_job` runner) writing Delta tables Databricks users query as normal tables. Everything below was checked
against `deltalake` 1.6.6 in a throwaway venv (what was run is listed under "Verified by running code"); external
claims cite a source, and what could not be confirmed is marked **unconfirmed**.

## Decision in one paragraph

Call `deltalake.write_deltalake` directly (not Polars `write_delta`), pinned `deltalake>=1.6.6,<1.7` behind a
`delta` extra. Floe-created tables keep delta-rs's minimal protocol; tables that carry a feature delta-rs cannot write
are refused with a named error, never altered. Commits are filesystem commits (S3 conditional put), so 1.0 supports
**path-based tables and Unity Catalog external tables**; Unity *managed* tables need catalog commits that delta-rs
cannot do and are out of scope. The retry guard is a Delta `txn` action (`appId = floe:<input_hash>`) found with
`DeltaTable.transaction_version`, with `floe.run_id` / `floe.input_hash` also in `commitInfo` for audit. This
deviates from #1263's "commit metadata" wording, because `commitInfo` does not survive checkpoints and does not
conflict (evidence in Q4). Unique keys are seeded with `DeltaTable.scan` (bounded memory). Merges, later, use
delta-rs native `merge`.

## Q1. `deltalake` version, and Polars `write_delta`

- **Latest:** 1.6.6, uploaded 2026-09-24, `requires_python >=3.10` ([PyPI JSON](https://pypi.org/pypi/deltalake/json)).
  Required dependencies are `arro3-core` and `deprecated`; `pyarrow` is an optional extra and Floe already requires
  `pyarrow>=26` (read from `importlib.metadata.requires("deltalake")` and `pyproject.toml`).
- **Pin `deltalake>=1.6.6,<1.7`.** 1.6.6 is the only version this ADR ran. Minor releases of 1.x change the API:
  1.6.1 removed `S3DynamoDbLogStore`, 1.6.4 removed partition-filter `delete`, 1.6.6 removed `DeltaTableConfig`
  ([release list](https://github.com/delta-io/delta-rs/releases)). 1.6.4 added column-mapping create support and
  `DeltaTable.scan`, both used below. Bumps get their own PR (AGENTS.md §3).
- **Call `write_deltalake` directly.** Polars 2.1.0 `DataFrame.write_delta` is a thin wrapper (read from
  `inspect.getsource`): it takes a whole in-memory `DataFrame` (`data=self`), forwards `delta_write_options` to
  `write_deltalake` and adds only a Polars `credential_provider` and a type check. Floe already feeds the Iceberg
  sink a streaming `pa.RecordBatchReader` (`iceberg/sink.py:_reader`), and `write_deltalake` accepts one (verified). The
  wrapper adds nothing Floe needs and costs an in-memory frame. `pl.scan_delta` is not used either (Q5).
- **Memory ceiling, measured:** `write_deltalake` fed a streaming reader is not memory-bounded on local storage. Peak
  RSS was 657 / 875 / 1358 MB for 10M / 20M / 40M rows (3 columns, 121 / 240 / 480 MB on disk); a 16 MB
  `target_file_size` did not help (1182 MB at 40M). Writing the data files in chunks with pyarrow and committing once
  through `DeltaTable.create_write_transaction` (the shape of `iceberg/write.py:write_table`) stayed flat at 377 MB for
  both 10M and 40M rows. Recommendation: phase 1 uses `write_deltalake` (delta-rs owns statistics, partition values and
  column mapping); mark it `ponytail:` with the ceiling (peak memory follows the entity, about twice its compressed
  size) and the upgrade path (chunked files plus `create_write_transaction`; it also lets a commit conflict be retried
  without re-reading the data). The chunked path was verified only on a plain table, not on a column-mapped one
  (hand-written Parquet would need physical column names): the follow-up must test that. Heavy loads remain an Iceberg
  strength (`python-core-for-olf.md`).

## Q2. Databricks table features

**What Databricks enables.** Delta tables have no features by default; features switch on by syntax or property, and
the protocol upgrade is mostly irreversible ([feature compatibility](https://docs.databricks.com/aws/en/delta/feature-compatibility)):

| Feature | Default on Databricks | Source |
|---|---|---|
| Deletion vectors | "Depends on workspace admin settings and Databricks Runtime version" (new UC managed and SQL tables; region-dependent default; Databricks advises disabling it for clients without DV support) | [table properties](https://docs.databricks.com/aws/en/tables/table-properties), [DV page](https://docs.databricks.com/aws/en/delta/deletion-vectors); admin setting from a search summary of [the workspace setting page](https://docs.databricks.com/aws/en/admin/workspace-settings/deletion-vectors), not opened |
| Row tracking | `delta.enableRowTracking` is `false`, but automatic upgrades turn it on for UC managed tables | [row tracking](https://docs.databricks.com/aws/en/delta/row-tracking), [automatic upgrades](https://docs.databricks.com/gcp/en/tables/automatic-upgrades) |
| Column mapping | `delta.columnMapping.mode` is `none`; automatic upgrade is Public Preview | [table properties](https://docs.databricks.com/aws/en/tables/table-properties), automatic upgrades |
| Liquid clustering | opt in with `CLUSTER BY`; "automatic liquid clustering" for new UC managed tables | feature compatibility, automatic upgrades |
| Checkpoint V2, catalog commits, in-commit timestamps | automatic upgrades, new and existing UC managed tables | automatic upgrades |
| Vacuum protocol check, type widening, identity columns | not stated on the pages read: **unconfirmed** | |

Automatic upgrades exclude **external tables** and cannot see path-based access; external clients seen by name in a
schema stop the upgrade for that schema (same page). So a table Floe creates and registers as an external table is not
upgraded by Databricks on its own, while a UC managed table increasingly is.

**What delta-rs 1.6.6 can write.** I created a table, hand-edited its protocol to each feature set, and ran an append
and an overwrite (see "Verified by running code"):

| Result | Features |
|---|---|
| Writes (append and overwrite) | appendOnly, invariants, checkConstraints, generatedColumns, changeDataFeed, timestampNtz, variantType, v2Checkpoint, deletionVectors, columnMapping (on a table delta-rs created; also creates one, 1.6.4+) |
| Refused with `Unsupported table features required: [...]` | identityColumns, allowColumnDefaults, domainMetadata, rowTracking, icebergCompatV2, clustering (liquid), typeWidening, inCommitTimestamp, vacuumProtocolCheck |
| Refused differently | catalogManaged: `Max catalog version is required when loading a catalog-managed table`; an unknown feature: `Feature 'fooBar' is not supported` |
| `create` refused | `delta.enableRowTracking`, `delta.enableIcebergCompatV2`, `delta.enableTypeWidening`, clustering, domainMetadata, inCommitTimestamps (`Error parsing property`) |

Caveats: a DV table was written without real deletion vectors (delta-rs `delete` rewrote the file instead of writing
a DV), so reading a table that holds real DVs is **unconfirmed**; 1.6.4's notes say DV scans are supported
([release notes](https://github.com/delta-io/delta-rs/releases)). `appendOnly` makes `overwrite` fail
(`transaction includes Remove action with data change but Delta table is append-only`). A default create gives protocol
reader 1 / writer 2 and no features.

**Recommendation: refuse, and create minimal.**
1. A table Floe creates uses delta-rs's defaults (protocol 1/2, no features) and sets no properties. That is the
   "downgrade on create": there is nothing to downgrade, and a protocol 1/2 table asks no feature support of any reader or writer.
2. A table that already carries a feature delta-rs cannot write is refused with a `FloeRunError` that names the
   features from delta-rs's message and says to disable them or write another table. Floe never alters an existing
   table's protocol: upgrades are irreversible and disabling a feature is the table owner's call.
3. The managed-table features Databricks auto-enables (row tracking, in-commit timestamps, catalog commits) are all in
   the refused set, which is the same fact as Q3: Floe cannot write UC managed tables by path.
4. The docs list the supported set above and recommend UC external tables.

Also from running code (writer sub-issue must handle both): unsigned integers are silently narrowed (`uint8` becomes
`Int8`, `uint32` `Int32`, `uint64` `Int64`) and a value above the signed range makes the table unreadable (`Failed to cast
x from Int8 to UInt8`), so widen them as `iceberg/sink.py:_WIDER` does and refuse `UInt64`. `ns` timestamps become `us`. An
append whose column type differs from the table's is cast silently (`"3"` into an `int64` column worked, `"x"`
failed), so check schema compatibility before writing, as `_check_pyarrow_schema_compatible` does for Iceberg.

## Q3. Concurrent writers

- **delta-rs on S3 (verified in docs, run against a stand-in).** Delta-rs 1.0 dropped `S3DynamoDbLogStore`; "S3-based
  conditional put semantics are now the default supported mode for writes on S3", `AWS_S3_LOCKING_PROVIDER` and
  `DELTA_DYNAMO_*` are ignored, and `AWS_S3_ALLOW_UNSAFE_RENAME=true` "disables commit safety entirely"
  ([discussion 4482](https://github.com/delta-io/delta-rs/discussions/4482)). The older docs pages still describe
  DynamoDB locking ([locking page](https://delta-io.github.io/delta-rs/usage/writing/writing-to-s3-with-locking-provider));
  they are stale against the discussion. I ran 1.6.6 against an S3 endpoint that rejects a second `If-None-Match` PUT
  (moto 5.2.5, checked with boto3): with no locking option, create worked and 12 racing appends all committed (13 log
  files, versions 0 to 12, 39 rows, no lost commit). That is consistent with conditional commits but does not prove the
  PUT carried the header. Not run on real AWS S3 or SeaweedFS.
- **Reuse an invariant Floe already has.** The incremental state needs conditional put on every store
  (`incremental-state-cas.md`), so any store Floe supports has the primitive Delta commits need. Floe builds the
  delta-rs storage options from its `StorageDefinition` and sets no locking option, ever.
- **Floe racing a Databricks job on one path-based table.** Both are optimistic filesystem commits on the same
  `_delta_log/N.json`. Databricks says the S3 commit service applies to classic compute on Runtime 13.3 LTS and below
  only, newer runtimes and serverless use S3 conditional writes
  ([S3 commit service](https://docs.databricks.com/aws/en/security/network/classic/s3-commit-service)); the old service
  does not coordinate an external writer. Delta-rs's discussion says its coordination with Delta/Spark writers via
  DynamoDB "will no longer work" and that Delta/Spark "still does not support this functionality properly"
  (discussion 4482). That DBR 14+ conditional writes and delta-rs's agree on the exact commit protocol is therefore
  **unconfirmed**; the manual test in #1266 must run a Databricks write against a Floe write on one table. Logical
  conflicts are caught on both sides: delta-rs raises `a concurrent transaction deleted data this operation read`
  (verified: overwrite racing appends gave 1 winner), Databricks raises its own conflict exceptions
  ([concurrency control](https://docs.databricks.com/aws/en/delta/concurrency-control)). Append against append never
  conflicts in Delta, so two appends both land.
- **Unity Catalog managed tables.** External Delta clients can write a managed table only with catalog commits
  enabled, through a Unity Catalog client: the REST page lists Spark and DuckDB, not delta-rs
  ([unity-rest](https://docs.databricks.com/aws/en/external-access/unity-rest),
  [credential vending](https://docs.databricks.com/aws/en/external-access/credential-vending)); table property changes
  and feature changes from external clients are not supported on managed tables (search summary, not opened). The
  protocol says writers of a catalog-managed table "must not perform ordinary filesystem-based commits"
  ([PROTOCOL.md](https://raw.githubusercontent.com/delta-io/delta/master/PROTOCOL.md)); delta-rs 1.6.6 refuses such a
  table (Q2). Catalog commits are a Databricks default via automatic upgrades (Q2), so managed tables are
  increasingly unwritable by path. DuckDB's Unity extension can append to managed tables but only `INSERT`, and the
  docs describe DuckDB 2.0.1 while Floe pins `duckdb==1.5.6`
  ([DuckDB Unity extension](https://duckdb.org/docs/2.0/core_extensions/unity_catalog.html)): a possible later spike,
  not 1.0.
- **Recommendation, consistent with AGENTS.md §7.** 1.0 commits go to storage with conditional put and nothing else:
  no lock table, no lease, no catalog coordinator. Supported: path-based tables and UC **external** tables. UC managed
  tables are out of scope with a clear error when delta-rs refuses them. Floe runs on one table are separated by the
  existing claims and the exclusive accepted-path key in `run/parallel.py` (`exclusive_keys` already keys every format
  by its accepted path). Document that S3 interop with Databricks needs DBR 14+ or serverless.

## Q4. Commit guarantees (the Delta rows of `entity-commit-guarantees.md`)

**One commit per entity.** `write_deltalake` (and later `create_write_transaction`) produces one commit holding all the
entity's Add (and, for overwrite, Remove) actions. An append of no rows commits nothing, as for Iceberg. Overwrite
without a predicate is one commit that removes every live file and adds the new ones (verified: the version advanced
by one, rows were replaced, operation `WRITE` mode `Overwrite`).

**Retry outcome per mode and crash point** (P1 before the commit, P2 after the commit before promote, P3 after promote
before the report; same meaning as the Iceberg table):

| Mode | P1 | P2 | P3 |
|---|---|---|---|
| overwrite | Retry rewrites; same result. Orphan data files until `vacuum`. | Retry overwrites with the same inputs: same content, one extra version. OK. | Report lost (D18, accepted). |
| append | Retry writes. OK. | Duplicates without the guard. With it: skip the write, promote. | OK |
| merge_scd1 / merge_scd2 (later) | Retry merges. | No-op through the change guard (`IS DISTINCT FROM`), verified: a rerun changed 0 rows and added 0 files. Same S7 caveat. | OK |

**The retry guard.** Same placement as D35: right after the claim, before reading any file (a seed taken afterwards
rejects the dead run's own rows).
- **Marker:** the append commit carries a `txn` action `{appId: "floe:<input_hash>", version: 1, lastUpdated: <ms>}`
  (`CommitProperties(app_transactions=[Transaction(...)])`) and, for audit parity with Iceberg, `floe.run_id` and
  `floe.input_hash` in `commitInfo` (`CommitProperties(custom_metadata=...)`). `input_hash(entity, files)` is the one
  Iceberg uses, scoped by `entity.id`.
- **Lookup:** `DeltaTable.transaction_version("floe:<hash>")` is not `None`. It reads the table state in memory, not
  history: 17 ms for 300 commits. `file` mode recomputes a dead run's hash from its expired claims exactly as for
  Iceberg (D35); `archive`/`none` hash the pending set.
- **Why `txn` and not only `commitInfo`, measured:**

  | | `commitInfo` only | `txn` action |
  |---|---|---|
  | Survives a checkpoint plus log cleanup | No: 301 commits, `delta.logRetentionDuration` 0 s, checkpoint, `cleanup_metadata`: `history()` kept 2, the hash was gone. The spec: "Checkpoints and log compaction files do not preserve it" ([PROTOCOL.md](https://raw.githubusercontent.com/delta-io/delta/master/PROTOCOL.md)); the default retention is 30 days ([Delta table properties](https://docs.delta.io/latest/table-properties.html)) | Yes: `transaction_version` still returned 1 after the same cleanup |
  | Two racing runs of the same batch (S8) | 6 of 6 threads committed: 21 rows (seed 3 + 6 x 3), every duplicate landed | 1 commit, 5 `CommitFailedError: Concurrent transaction failed` (the same on the moto S3 endpoint) |
  | Cost | `history()` reads commit JSON files | O(1) from state |

  A late starter (one that loads the table after the winner committed) is not rejected by delta-rs, so the pre-write
  guard stays; the commit conflict is the backstop for the overlapping window, as the 409 is for Iceberg (S8).
- **Evidence lifetime.** A `txn` lives until `delta.setTransactionRetentionDuration` expires it (default: none). Verified:
  delta-rs honours the property, but only for a `txn` carrying `lastUpdated`, so Floe always sets it. The retention
  floor decision for Iceberg (7 days, at least 10 snapshots) has no Delta counterpart: if the property is set and a stale
  claim is older than it, treat the answer as unknown and fail closed with the `floe state reset` message
  (`run/incremental.py:skip_committed`, `found is None`). Floe does not set the property: one 100-byte action per commit
  is the price; document `delta.setTransactionRetentionDuration` for tables with very many commits. Whether Databricks
  surfaces custom `commitInfo` keys in its history view is **unconfirmed**.
- **Commit conflict (S8/S10).** `write_deltalake` consumes the reader once, so a conflict cannot re-commit the same
  files. Phase 1: on `CommitFailedError`, refresh, re-run the guard (hit: report `already_committed`), otherwise fail
  the entity with the delta-rs message and let the orchestrator rerun; the guard makes the rerun safe. Appends never
  conflict, so this fires only for overwrite against a concurrent writer and for a same-batch race. A bounded in-process
  retry (as `COMMIT_ATTEMPTS` in `iceberg/write.py`) needs the chunked writer of Q1.
- **S9 (two different batches, overlapping keys).** Not detected: Delta has no commit conflict between two appends.
  Verified mechanism for later: a table-level `txn` appId shared by all Floe writers (`floe:table`, any version) made 6
  racing appends yield 1 commit and 5 conflicts, while distinct appIds yielded 6 commits. That serialises Floe runs on
  one table, as the Iceberg main-branch CAS does, but not a Databricks append, which carries no such appId. Recommend
  deferring it; document that key uniqueness is enforced against the table as read at validation time, not against
  concurrent non-Floe writers.
- **Rejected output, archive, state:** unchanged (D18, S1, S5, S6 are format-independent).
- **Report:** `table_version` carries the Delta version (the existing field is "version identifier when the sink exposes
  one", `docs/reference/report.md`); no `snapshot_id`. Any new field is a report-contract change and needs the
  maintainer's call in the writer sub-issue.

## Q5. Unique keys on append

Mirror `iceberg/unique.py:cache_table_keys`: stream the key columns of the existing table to one local Parquet file per
entity, join each chunk against it. Use `DeltaTable.scan(columns=keys)` (delta-rs 1.6.4+, kernel-based), converted with
`pa.RecordBatchReader.from_stream` and written with `pq.ParquetWriter`.
- **Bounded memory, measured:** 40M rows (479 MB on disk): peak RSS 338 MB before and after the scan (the key file was
  226 MB), 4.8 s.
- **Why not the alternatives, measured:** `pl.scan_delta` fails on a column-mapped table (`requires reader version 2
  (column mapping), which polars does not support`) and `to_pyarrow_table` / `to_pyarrow_dataset` / `pl.scan_delta(use_pyarrow=True)`
  fail on a table with `deletionVectors` (`not yet supported by the deltalake reader`). `DeltaTable.scan` read both, and a
  partitioned table, correctly. Both kinds of table appear on Databricks.
- A key column the table lacks is skipped, as in the Iceberg seed. Real deletion vectors: **unconfirmed** (Q2).
- Revalidating keys after a conflict is not needed in phase 1 (Q4: no append conflicts).

## Q6. Merge engine (later sub-issue)

**delta-rs native `merge`.** DuckDB cannot be the engine as it is for Iceberg: its Delta extension supports blind
appends and states "UPDATE, DELETE, MERGE INTO, ALTER TABLE, and DROP TABLE are not supported"
([DuckDB Delta extension](https://duckdb.org/docs/2.0/core_extensions/delta.html)). Verified with delta-rs 1.6.6: `merge`
takes a `RecordBatchReader` source, accepts `commit_properties` (custom metadata and `txn` were written), and with
`when_matched_update_all(predicate="(t.v IS DISTINCT FROM s.v) OR (t.s IS DISTINCT FROM s.s)")` a rerun of the same batch
changed 0 rows and wrote 0 files. Parenthesise each `IS DISTINCT FROM`: without parentheses delta-rs failed in type
coercion. A 2M-row source into a 10M-row table (one file) took 4.5 s with a 1466 MB peak; it has `streamed_exec`,
`max_spill_size` and `max_temp_directory_size` knobs, not tuned here. SCD2 (closing and inserting versions) was not
tried: the sub-issue must prototype it first. This is the same decision `merge-delegated-to-sink.md` recorded for Rust
(Delta merges through the library).

## Q7. ADLS and GCS

Delta I/O does not go through Floe's storage client: delta-rs talks to `abfss://container@account.dfs.core.windows.net/...`
and `gs://bucket/...` with its own `storage_options`
([ADLS](https://raw.githubusercontent.com/delta-io/delta-rs/main/docs/integrations/object-storage/adls.md),
[GCS](https://raw.githubusercontent.com/delta-io/delta-rs/main/docs/integrations/object-storage/gcs.md)). What blocks
Floe on Azure and GCP Databricks is Floe's own client for state, rejected output, archive and reports, i.e. #1267 (ADLS)
and #1268 (GCS), both blocked by #1271 (obstore). Not redesigned here. Once the `adls` and `gcs` storage types exist, a
small follow-up maps the definition to delta-rs `storage_options` in the Delta sink. **Unconfirmed:** the delta-rs
pages say nothing on concurrent writes on these stores, and `object_store`'s `PutMode::Create` is documented as an
atomic create ([docs.rs](https://docs.rs/object_store/latest/object_store/enum.PutMode.html)) without saying which
stores implement it; the follow-up must test commit races against the emulators named in #1267 and #1268.

## Unity Catalog (#1266) — answers to its design questions

- **Managed or external:** external only in 1.0 (Q3). The Create Table API "for external clients only supports creating
  external delta tables" and asks for Spark-compatible `columns`, which it does not validate
  ([tables/create](https://docs.databricks.com/api/workspace/tables/create)); creating an external table needs
  `CREATE EXTERNAL TABLE` and `EXTERNAL USE LOCATION` on the location, and external data access enabled on the metastore
  ([external access](https://docs.databricks.com/aws/en/external-access/admin),
  [create external tables](https://docs.databricks.com/gcp/external-access/create-external-tables)).
  The archived 0.7 Unity target registered tables on S3/GCS/ADLS storage and rejected local storage
  ([floe-rust delta.md](https://raw.githubusercontent.com/OpenLakeForge/floe-rust/main/docs/sinks/delta.md)).
- **Flow:** Floe commits the Delta table to its storage path first (same writer as the path-based sink), then registers
  it with `POST /api/2.1/unity-catalog/tables` (`table_type: EXTERNAL`, `data_source_format: DELTA`, `storage_location`,
  `columns`, `properties`). Needs an Arrow-to-Spark type mapping for `columns`. What the API answers for a table
  that already exists is **unconfirmed**; part 2 checks it.
- **Credentials:** 1.0 uses the credentials of the Floe `storage` definition (the Databricks driver's own role or
  keys). Credential vending (`POST /api/2.0/unity-catalog/temporary-path-credentials`, operations `PATH_READ_WRITE` /
  `PATH_CREATE_TABLE`, requires `EXTERNAL USE LOCATION`; [API](https://docs.databricks.com/api/workspace/temporarypathcredentials/generatetemporarypathcredentials))
  is a follow-up: it needs expiry refresh in the middle of a write.
- **Auth to the workspace:** the profile's `service_principal_oauth_ref` / `FLOE_DATABRICKS_OAUTH_TOKEN` pattern
  (`docs/guides/profiles.md`); HTTP via `urllib` as `lineage/observer.py` does, no new dependency.
- **Commits:** path commits suffice for external tables (Q3).
- **Table features:** Q2: Floe sets none.
- **Iceberg REST comparison:** the credential-vending page lists managed Iceberg tables as readable, writable and
  creatable by external engines, so Floe's existing Iceberg sink might already work against Unity Catalog's Iceberg
  endpoint. Whether it does is **unconfirmed** and out of this ADR; a spike, not a build.
- **0.7 config shape:** `sink.accepted.delta: {catalog, schema, table}` and a `unity` catalog
  `{type, host, token, create_schema_if_missing}`, with report fields `delta_catalog_name`, `delta_catalog_schema`,
  `delta_catalog_table` (floe-rust delta.md). Reuse it so 0.7 Unity configs run again.

## Proposed Task sub-issues, in build order

Sizes are estimates of non-test lines under `src/floe/` (budget: at most 300 lines and 8 files each). Golden cases ride
with the PR of the behaviour they pin (`testing-strategy.md`: a sink change needs a golden case), so there is no
golden-only PR.

| # | Title | Scope and files | Est. | Test that must fail first |
|---|---|---|---|---|
| 1 | `chore(deps): add the delta extra` | `deltalake>=1.6.6,<1.7` in `delta` and `all` extras; `uv.lock`. Own PR (AGENTS.md §3). `pyproject.toml` | 3 lines, 2 files | `tests/unit`: `import deltalake` after `uv sync --extra delta` (fails on `main`) |
| 2 | `feat(config): accept sink.accepted.format=delta` | Lift the D1 refusal (`config/validate.py:385`, `:706`), add `delta` to `SINK_FORMATS`, make `partition_by` checks reachable (`validate.py:247`: columns exist, exclusive with `partition_spec`, `partition_spec` iceberg-only), refuse `merge_*` and `add_columns` with a "not supported with delta yet" message, keep the `sink.accepted.delta` block refused until #8, `require("delta", "deltalake", ...)`. Reuses the 0.7 shape (`format: delta`, `partition_by`, `options.max_size_per_file`) so 0.7 path-based configs validate. `config/validate.py`, `config/types.py`, `config/entity.py` | ~90 lines, 3 files | `conformance/config/d1_sink_delta` flips to `{"ok": true}`; unit tests assert exact messages for an unknown `partition_by` column and `delta` + `partition_spec` |
| 3 | `feat(delta): write accepted rows as a Delta table (overwrite, append)` | New `src/floe/delta/` (`sink.py`: resolve path and storage to uri and `storage_options`, reader reuse, widen unsigned, schema-compat check, `write_deltalake` once, named error for unsupported features; `write.py`: result and report fields), wire into `run/entity.py` (`:237`, `:428`) and the manifest/report mapping. Golden cases `delta_overwrite`, `delta_append`; `conformance/record.py` reads a Delta dir with `DeltaTable.scan` instead of globbing `part-*.parquet` (it would include removed files) | ~230 lines, 5 files | unit: two overwrites leave only the second batch; `uint8` 200 round-trips; an unsupported-feature table raises the named `FloeRunError`; type drift on append is refused |
| 4 | `feat(delta): retry guard for append` | `txn` + `commitInfo` stamp, `committed(table, hash)`, make `run/incremental.py:skip_committed` pick the table kind, `CommitFailedError` handling (refresh, guard, else fail), `file`-mode hash from expired claims, fail closed when `setTransactionRetentionDuration` outlived the claim. `delta/write.py`, `delta/guard.py`, `run/incremental.py`, `run/entity.py` | ~130 lines, 4 files | unit: crash after commit (P2) then retry reports `already_committed` and adds no rows; 6 threads appending one batch give 1 commit (fails with `commitInfo` only) |
| 5 | `feat(delta): unique keys on append` | `delta/unique.py` (`DeltaTable.scan` to Parquet cache), generalise `run/entity.py:_key_cache` (`plan.table` is Iceberg-typed today). 2-3 files | ~70 lines, 3 files | unit: appended key already in a column-mapped table is rejected (fails with `pl.scan_delta`); memory-bounded test on a multi-file table |
| 6 | `feat(delta): partition_by` | Pass identity `partition_by` on create; on an existing table map delta-rs's `Specified table partitioning does not match table partitioning` to a `FloeRunError`. `delta/sink.py` | ~40 lines, 1 file | unit: partition directories created and `partition_columns` set; mismatch message asserted exactly; golden `delta_partition_by` |
| 7 | `test(delta): S3-compatible integration` | `-m s3` tests on SeaweedFS (`python-integration.yml`): create, append, overwrite, concurrent commit, guard. Tests only; confirms the conditional-put assumption on the store CI uses | 0 src | the racing-writers test (1 commit wins), run on SeaweedFS |
| 8 | `feat(catalog): unity catalog type for Delta external tables` (#1266, part 1) | `unity` in `catalogs.definitions` (host, catalog, schema, token ref), `sink.accepted.delta: {catalog, schema, table}`, validation (S3 storage only until #1267/#1268; local refused), lift `config/storage.py:42`, `:138`. | ~90 lines, 3 files | `conformance/config/d1_catalog_unity` flips to ok; unit: exact messages |
| 9 | `feat(delta): register the table in Unity Catalog` (#1266, part 2) | `delta/unity.py`: `POST /api/2.1/unity-catalog/tables`, Arrow to Spark type mapping, OAuth token, report fields `delta_catalog_*`; manual test on a workspace (create, overwrite, append, read from a SQL warehouse; also a Databricks write racing a Floe write, Q3) | ~160 lines, 3 files | unit against a local HTTP stub: the request body for a created table; manual run as in `docs/reference/support-matrix.md` real-AWS row |
| 10 | `docs: Delta sink` | New `docs/sinks/delta.md` (supported features, Databricks interop, DBR 14+ for S3, external tables), `docs/reference/config.md` (`partition_by`, `table_version`), `docs/reference/support-matrix.md`, `docs/guides/profiles.md`, `context/project.md`, `context/decisions/python-core-for-olf.md` D1 note, #264 note, `CHANGELOG.md` | docs only | none (docs) |

Blocked or later (not in the 1.0 critical path):
- **ADLS and GCS for Delta**, ~30 lines each: map the `adls` / `gcs` storage definition to delta-rs `storage_options`;
  blocked by #1267 and #1268 (which are blocked by #1271); must test commit races on the emulators (Q7).
- **Bounded-memory writer:** chunked Parquet plus `create_write_transaction`, in-process commit retry (Q1, Q4).
- **Merge sink:** `merge_scd1` then `merge_scd2` through delta-rs `merge` (Q6).
- **`schema_evolution: add_columns`:** `schema_mode="merge"` (verified to add a column); 0.7 rejected partitioned-table
  additions and merge-key additions, keep that.
- **`floe maintain` for Delta:** `optimize`, `vacuum`, log cleanup.
- **Floe-writer serialisation:** table-level `txn` appId (Q4, S9).
- **Credential vending for Unity external tables** (#1266 follow-up).

## Trade-offs

- The `txn` guard departs from "stored in commit metadata" in #1263. It costs one `txn` action per append commit and
  makes the retry evidence outlive log cleanup and conflict-check at commit; the `commitInfo` fields stay for audit.
- Phase 1 has no in-process commit retry and a memory ceiling proportional to the entity; both are named upgrades
  (chunked writer), not 1.0 blockers for light and medium loads.
- UC managed tables are unsupported. That excludes the Databricks default going forward; the escape hatch is external
  tables, which Databricks's automatic upgrades also leave alone.
- Uniqueness is not enforced against concurrent non-Floe appends.

## Verified by running code

`deltalake` 1.6.6, Polars 2.1.0, pyarrow 26.0.1, Python 3.13, moto 5.2.5, on local storage unless stated; scripts were
throwaway (not committed). Numbers above are from these runs: streamed write and `create_write_transaction` memory;
custom metadata and `txn` readback; feature matrix by editing the protocol of a delta-rs table; concurrent appends
(6 threads) with and without a shared appId, locally and on a moto S3 endpoint; `history()` and `transaction_version`
after checkpoint and `cleanup_metadata`; `setTransactionRetentionDuration` with and without `lastUpdated`;
`DeltaTable.scan` vs `pl.scan_delta` vs `to_pyarrow_*` on DV and column-mapped tables; unsigned, `ns` timestamp and type
drift writes; `merge` with change guard, rerun and `RecordBatchReader` source. Not verified: real AWS S3, SeaweedFS,
Databricks, ADLS, GCS, real deletion vectors.
