# Support matrix

This is the canonical statement of what Floe 1.0 supports and what evidence backs it. It reflects
**current behavior** (not aspirational); the sink, storage and write-mode pages link here instead of
repeating it. Floe 1.0 supports local, S3, ADLS and GCS storage. Cloud inputs are downloaded to a temp
directory and read locally; Parquet parts, rejected CSV and reports are built in memory, then uploaded;
Iceberg tables are written through PyIceberg (and DuckDB for merges) and Delta tables through delta-rs,
directly to the object store. ADLS and GCS need the `adls` or `gcs` extra; see [ADLS](../storages/adls.md)
and [GCS](../storages/gcs.md).

## How to read the status

| Status | Meaning |
|---|---|
| `CI-tested` | A test in this repository passes in CI against the harness named in the cell: the local filesystem, an emulator or a stub. It is **not** a claim about the real cloud service. |
| `real-provider-tested` | Run by hand against a real service, with a dated, linked record. It holds for the version and date on the record only. |
| `experimental` | Implemented and documented as subject to change. |
| `unsupported` | Refused by `floe validate` or at run time with an error. |
| `untested` | The code accepts it, but no test or record covers the combination. No promise is made. |

A backend library being installed (PyIceberg, `deltalake`, `obstore`, DuckDB) does not make a
combination supported: only a row below with `CI-tested` or `real-provider-tested` is evidence.

### What each harness is

| Harness | Where it runs | Stands in for | It is not |
|---|---|---|---|
| Local filesystem | `ci.yml`: `pytest` and `conformance/check.py`, every PR | local storage; Iceberg unit tests use a local test catalog | any object store |
| Polaris 1.8.0 + SeaweedFS 4.48 | `python-integration.yml`, job `polaris`: PRs and merge-queue entries that touch the Iceberg, Delta, storage, run or checks code, the integration tests or the dependencies, and nightly | an Iceberg REST catalog on an S3-API store; Delta and S3 storage | AWS S3, a cloud-hosted Polaris |
| Azurite 3.35.0 | job `object-stores` | ADLS Gen2 through the blob endpoint | Azure |
| fake-gcs-server 1.56.1 behind a test-only XML upload shim (#1342) | job `object-stores` | GCS | GCS: the emulator lacks the XML upload route the shim supplies |
| moto 5.2.5 | job `glue`, nightly and on demand only | the AWS Glue catalog API | AWS Glue |
| Unity Catalog stub | `pytest` in `ci.yml` (`tests/unit/delta/test_delta_unity.py`) | the Unity REST calls Floe makes | Databricks (open: #1393) |
| Real S3-compatible buckets | by hand, never in CI | Scaleway `fr-par`, OVHcloud `gra`, 2026-11-23: [Scaleway record](https://github.com/OpenLakeForge/floe/issues/1269#issuecomment-7095830987), [OVHcloud record](https://github.com/OpenLakeForge/floe/issues/1270#issuecomment-7095831339) | AWS S3, Azure, GCS |

On pull requests and in the merge queue, the `integration-gate` job of `python-integration.yml` (#1456) runs the
jobs a change selects and fails if one fails or runs no test. It blocks merging only once a maintainer marks it a
required check in the branch ruleset.

## Tested combinations

A concise selection, not a Cartesian product: a combination that is not listed is `untested`
unless a row says `unsupported`. Source formats do not change a row: every source format is read from a
local copy (cloud inputs are downloaded first). All nine formats are covered by the local golden suite
(`conformance/`); on object stores CI runs CSV sources and the provider records add a large Parquet download.

| Accepted sink | Storage | Catalog | `write_mode` | Status | Evidence and qualification |
|---|---|---|---|---|---|
| Parquet | local | none | `overwrite`, `append` | `CI-tested` (local) | golden suite and unit tests |
| Parquet | S3 | none | `overwrite` (default) | `CI-tested` (SeaweedFS) | `tests/integration/test_run_seaweedfs.py`; `append` on S3 is `untested` |
| Parquet | ADLS | none | `overwrite` (default) | `CI-tested` (Azurite) | `test_adls_azurite.py`; `append` is `untested` |
| Parquet | GCS | none | `overwrite` (default) | `CI-tested` (fake-gcs-server + shim) | `test_gcs_fake_server.py`; `append` is `untested` |
| Parquet | Scaleway, OVHcloud (S3 API) | none | default | `real-provider-tested` 2026-11-23 | storage operations, the incremental state's conditional writes, `incremental_mode: file` and `archive` runs; see the records above |
| Iceberg | S3 | REST (Polaris) | `overwrite`, `append`, `merge_scd1`, `merge_scd2` | `CI-tested` (Polaris + SeaweedFS) | `tests/integration/test_iceberg_*.py`; the fault scenarios are below |
| Iceberg | Scaleway, OVHcloud (S3 API) | REST (a **local** Polaris, not a provider's) | `append`, `merge_scd1`, rejected Iceberg table | `real-provider-tested` 2026-11-23 | the bucket was real, the catalog was not; `overwrite` and `merge_scd2` are not in the records |
| Iceberg | S3 | AWS Glue | `overwrite`, `append` | `CI-tested` (moto, nightly) | real AWS Glue and S3: `untested`; merges are `unsupported` on Glue |
| Iceberg | ADLS | REST | `overwrite`, `append`, merges | `untested` | only the FileIO settings, an `abfss://` read and write, and DuckDB's Azure secret are tested on Azurite (`test_iceberg_adls.py`); no catalog-backed run |
| Iceberg | GCS | REST | `overwrite`, `append` | `untested` | no emulator run; merges and compaction are `unsupported` (the run fails with `config_not_supported`) |
| Delta | local | none (path) | `overwrite`, `append` | `CI-tested` (local) | golden cases `delta_*` and `tests/unit/delta/` |
| Delta | S3 | none (path) | `overwrite`, `append` | `CI-tested` (SeaweedFS) | `tests/integration/test_delta_seaweedfs.py`: create, overwrite, append, the retry guard, racing appends |
| Delta | Scaleway, OVHcloud (S3 API) | none (path) | `append` | `real-provider-tested` 2026-11-23 | two `incremental_mode: file` runs and 4 concurrent appends; `overwrite` is not in the records |
| Delta + Unity Catalog registration | S3 | Unity (external table) | `overwrite`, `append` | `CI-tested` against a **stub only** | not run against Databricks: open #1393 |
| Delta | ADLS, GCS | | any | `unsupported` | `config_not_supported` |
| Parquet, Delta | any | | `merge_scd1`, `merge_scd2` | `unsupported` | `floe validate` rejects them; merges are Iceberg-only |

Rejected outputs are written to the storage of `sink.rejected.storage`, which is chosen independently of the accepted
sink's and falls back to `storages.default`, not to the accepted storage. Set it explicitly so quarantine data is not
left on local disk:

| Rejected sink | Storage | Status | Evidence and qualification |
|---|---|---|---|
| CSV | local | `CI-tested` (local) | golden suite |
| CSV | S3 | `CI-tested` (SeaweedFS) | archive and unique-key test in `test_run_seaweedfs.py` |
| CSV | ADLS, GCS | `CI-tested` with an in-memory object store only | `tests/unit/run/test_run_adls.py`, `test_run_gcs.py`; no emulator run |
| Parquet | local | `CI-tested` (local) | golden cases `rejected_parquet*` |
| Parquet | S3, ADLS, GCS | `untested` | the same writer as local, no object-store run |
| Iceberg quarantine table | S3 (REST or Glue catalog) | `CI-tested` (Polaris; Glue not covered) | `test_run_polaris_rejected.py`; real buckets 2026-11-23 as above |

Not covered by any row: Iceberg compaction (`floe maintain --compact`, `experimental`), and real AWS, Azure or
Google Cloud services (no record exists).

## Fault scenarios (Iceberg and S3)

Tested against local services (Polaris + SeaweedFS in `python-integration.yml`, moto for Glue), nightly
and on demand. Anything else is untested against a real cloud.

| Scenario | Status |
|---|---|
| Commit conflict on a REST catalog (stale writer retries on the fresh table) | Automated (Polaris) |
| Rival copy of the same batch is not appended twice | Automated (Polaris) |
| Writer failure mid-stream commits nothing; the retry lands once | Automated (Polaris) |
| Read-after-commit: a fresh reader sees the new snapshot | Automated (Polaris) |
| Glue catalog: overwrite, append, per-domain databases | Automated (moto, not real AWS) |
| Real AWS S3 + Glue, cloud-backed Polaris | Manual: needs credentials and costs money |
| Credential expiry mid-run | Manual: the local harness cannot expire a credential |
| Missing permissions (denied catalog or bucket access) | Manual |
| Object-store partial failure (a part uploaded, then the connection drops) | Manual |

## Transactional guarantees

Floe guarantees that **one table's write is atomic where the format has a commit, and that a retry
converges**; it does not make an entity's outputs commit together. There is no multi-entity or
multi-table atomicity, and Floe will not add it (single process, no locks: see
[the decision](https://github.com/OpenLakeForge/floe/blob/main/context/decisions/entity-commit-guarantees.md)).

| Output | Atomic? | Retry after a crash |
|---|---|---|
| Iceberg accepted table | Yes: one catalog commit per entity run, for every write mode. A failure before the commit leaves the table as it was; orphan data files can remain (`floe maintain` removes them). | `append`: the snapshot carries the input fingerprint, so a retry finds the batch already committed and skips it (`already_committed`). In `incremental_mode: none` or `archive` the fingerprint covers the whole pending set, so it matches only an identical set: if another input arrived before the retry, the committed batch is appended again. Retry before new files land, or use `file` mode. `overwrite` and `merge_scd1` converge on the same result. `merge_scd2` is a no-op if nothing changed since. A merge retried after a newer batch landed first can write older values (S7 in the decision). |
| Delta accepted table | Yes: one Delta commit per entity run. | `append`: a `txn` action is the guard, so a retry of the same batch never commits twice. As for Iceberg, in `none` or `archive` mode it matches only an identical input set. `overwrite` converges. In `file` mode, if the claim is older than the table's `delta.setTransactionRetentionDuration` the `txn` may have expired, and the run fails with `run_state_conflict` rather than guess. In `none` or `archive` mode there is no claim time to compare, so an expired `txn` is not detected and the retry appends the batch again. |
| Parquet accepted dataset | **No.** Each part is staged as `_staging-<id>-part-*`, then copied to its `part-*` name and the old parts are deleted. Readers never see a part of a run that failed while writing, but a crash or storage error during the final copy and delete steps can leave a mix of new and old parts. The dataset is never committed as one unit. | `overwrite` converges (the rerun rebuilds the directory). `append` **without** a primary or unique key can duplicate the rows if the run died after publishing and before the state was promoted (at-least-once; pinned by `test_crash_retry.py`). `append` with a key in `file` mode keys the part names by input, so a retry rewrites them. See [Parquet sink](../sinks/parquet.md#parquet-is-not-transactional). |
| Rejected CSV or Parquet | **No** (as accepted Parquet). Always appends, whatever `write_mode`. | A part is named from the entity and its input file (name, size and modification time), so a retry of the same input rewrites its own part instead of duplicating it. |
| Rejected Iceberg quarantine | One append commit per entity run, **independent** of the accepted table's commit. | At-least-once: a retry can append the same rejected row again; dedupe on `(__floe_source_uri, __floe_row_index)`, see [Rejected output](rejected.md#appending-and-duplicates). |
| Input archive, incremental state, run report | Independent steps. | The archive moves a file only after the accepted sink committed. The state promotes the files the run wrote. A crash after the promote and before `run.json` is written loses the report, not the data. |

An entity runs these steps in this order:

1. claim the input files in the incremental state (`file` mode);
2. validate each file, writing its rejected rows (CSV, Parquet) or its raw copy as it goes; the Iceberg quarantine is written after the last file;
3. write the accepted rows (one commit for Iceberg and Delta, staged parts for Parquet);
4. register the Delta table in Unity Catalog, when configured;
5. archive the inputs (`sink.archive`);
6. build the report and promote the state;
7. write `run.json`, then the run summary.

Consequences you can observe:

- The accepted and rejected sinks are written by separate steps and never in one transaction. A run that
  fails at step 3 has already written its rejected rows (step 2); the retry rewrites or re-appends them as in the table.
- A failed entity releases its claims, unless the accepted write already committed: then the committed files are
  promoted, so a rerun does not write them twice.
- Concurrent runs coordinate only through the incremental state's compare-and-swap and the table's commit
  conflict ([the decision](https://github.com/OpenLakeForge/floe/blob/main/context/decisions/incremental-state-cas.md)).
  Once a claim expires (TTL 1 h) it no longer keeps a second run off the files; the commit conflict then re-checks the batch.

## When a policy decides, and what it leaves behind

| Setting | Decided | Effect on outputs | Run result |
|---|---|---|---|
| `policy.severity: abort` (or a `rule_severity` of `abort`) | during the file's checks | The aborted file is copied whole to `sink.rejected`; its rows and every later file's rows are not accepted. **The files read before it are written** (earlier files commit). | entity `aborted`, exit `2`; the run continues with the next entity |
| `policy.min_rows_per_file` | per file, during its checks | At `reject` the file is copied whole to `sink.rejected` and none of its rows is accepted; at `abort` it aborts as above; at `warn` the rows are kept. | per level |
| `policy.max_rejection_rate` (`reject` only) | **after** the accepted commit and the archive, from the totals | Nothing is rolled back: the accepted and rejected rows are written, the inputs are archived (when `sink.archive` is set) and the state is promoted. | entity `aborted`, exit `2`, when `rejected_total / rows_total` is **above** the limit |
| `policy.min_rows` | **after** the accepted commit, against `results.rows_total` (rejected rows included) | Nothing is rolled back, as above. Not checked when every file of the run was skipped. | `aborted` at `abort` (or `reject`), `success_with_warnings` at `warn` |

Side effects to plan for:

- An `aborted` exit code does not mean the sink is unchanged. After a file `abort`, the earlier files' rows are
  committed. After `max_rejection_rate` or `min_rows`, **every** row of the run is committed.
- In `incremental_mode: file`, a `max_rejection_rate` or `min_rows` abort promotes the run's files (and `archive`
  mode moves them), so rerunning the same command skips them and exits `0`. Fix the rows, then use `floe state forget`
  (see [reprocess a changed file](https://github.com/OpenLakeForge/floe/blob/main/context/decisions/reprocess-changed-file.md)).
  After a **file** `abort`, `append` and the merge modes promote the files written before it and release the aborted
  file and those after it; `overwrite` releases every claim of the run ([Incremental](../concepts/incremental.md)).
- With `overwrite`, a file `abort` that happens on the **first** input file replaces the existing accepted output
  with an empty dataset. This looks like a bug and is tracked in #1496; until it is settled, do not rely on either behavior.
- A Delta `append` refuses `abort` at run time (`run_not_supported`): an aborted file would leave a smaller batch
  committed than the fingerprint the retry guard stamps.
- The decision is in the report: `policy.max_rejection_rate_exceeded`, `policy.min_rows_violated` and the file
  statuses ([Run report](report.md)).

## Airflow

airflow-floe 0.5.0 (Beta) declares `apache-airflow>=3.1,<4` and runs `floe run` from `FloeRunOperator`,
as a local subprocess or a Kubernetes or Databricks Job, with one asset event per entity. Its CI runs
the connector tests against Airflow 3.1.8 and 3.4.0. A manifest generated by current Floe runs (the
`{manifest_uri}` token is rendered, #1289), and a failed Kubernetes or Databricks Job fails the task
(#1290). Known gap: a local run that exits non-zero writes no asset event for the entities that
succeeded. The plan to close it is in
[context/airflow-1.0-plan.md](https://github.com/OpenLakeForge/floe/blob/main/context/airflow-1.0-plan.md).
dagster-floe 0.7.0 and airflow-floe 0.5.0 accept Floe 1.0 manifests (`spec_version` `1.0`).

## Dropped in 0.8

These 0.7 features are rejected by `floe validate` with a "not supported in 0.8" error (D1, D26):

| Dropped | Instead |
|---|---|
| Fixed-width input (`source.format: fixed`) | Convert to CSV upstream |
| Delta Lake `merge_scd1` and `merge_scd2` (#1266) | Iceberg. Delta `overwrite`, `append` and Unity Catalog registration are supported: see [Delta sink](../sinks/delta.md) |
| DuckDB / MotherDuck accepted sink | Iceberg (merges run through DuckDB) |
| Catalog-less Iceberg tables (filesystem catalog) | Iceberg REST catalog |

The 0.7 implementation is archived at
[OpenLakeForge/floe-rust](https://github.com/OpenLakeForge/floe-rust).

## Inputs

| Format | Local | S3, ADLS, GCS | Notes |
|---|---|---|---|
| CSV | ✅ | ✅ (temp) | Suffix filter `.csv` |
| TSV | ✅ | ✅ (temp) | Suffix filter `.tsv` |
| JSON (array) | ✅ | ✅ (temp) | `source.options.json_mode=array` (default) |
| JSON (ndjson) | ✅ | ✅ (temp) | `source.options.json_mode=ndjson` |
| Parquet | ✅ | ✅ (temp) | Suffix filter `.parquet` |
| ORC | ✅ | ✅ (temp) | Suffix filter `.orc` |
| Avro | ✅ | ✅ (temp) | Suffix filter `.avro` |
| XLSX | ✅ | ✅ (temp) | `source.options.sheet` (name), `header_row`/`data_row` (1-based) |
| XML | ✅ | ✅ (temp) | `source.options.row_tag` required; selectors in `schema.columns[].source`; DOCTYPEs are refused |

Notes:
- File extensions are matched case-insensitively: `.csv`, `.tsv`, `.parquet`, `.orc`, `.avro`,
  `.xlsx`, `.xml`, and `.json`, `.jsonl`, `.ndjson`, `.djson` for JSON.
- Object-store inputs are resolved by **prefix listing** plus suffix filtering, then downloaded to temp
  files. A glob in an object-store path needs a non-empty literal prefix before the first wildcard.
- `source.options.glob` and `recursive` apply to local directory inputs.
- `.floe/` paths and the entity's archive location are never listed as inputs (D23, D31).
- Nested JSON values (objects/arrays) are rejected.

## Outputs

A ✅ here means Floe accepts and implements the combination, not that it is tested: the status of each
is in [Tested combinations](#tested-combinations).

| Output | Local | S3 | ADLS, GCS | Notes |
|---|---|---|---|---|
| Accepted: Parquet | ✅ | ✅ (upload) | ✅ (upload) | Writes `part-*.parquet`; `overwrite` and `append` |
| Accepted: Iceberg | ❌ | ✅ (REST or Glue catalog) | ✅ (REST catalog; merges: ADLS `untested`, GCS `unsupported`) | `overwrite`, `append`, `merge_scd1` and `merge_scd2` in one catalog commit per entity; merges need a REST catalog; `floe validate` rejects local table storage |
| Accepted: Delta | ✅ | ✅ | ❌ | Path-based table, `overwrite` and `append`; Unity registration on S3 only; see [Delta sink](../sinks/delta.md) |
| Rejected: CSV | ✅ | ✅ (upload) | ✅ (upload) | Dataset parts `part-*.csv`; the default. Always appends |
| Rejected: Parquet | ✅ | ✅ (upload) | ✅ (upload) | Dataset parts `rows/part-*.parquet`, bounded by size; raw-value columns plus `__floe_*` metadata, see [Config](config.md). `output.rejected_path` is the `rows` directory. Always appends |
| Rejected: Iceberg | ❌ | ✅ (REST or Glue catalog) | ✅ (REST catalog) | Opt-in quarantine table through the accepted sink's catalog binding: all-string declared columns plus `__floe_*` metadata, one append commit per entity run; at-least-once (a retry appends again) and not atomic with the accepted commit. See [Config](config.md) |
| Reports: JSON | ✅ | ✅ (upload) | ✅ (upload) | |

Notes:
- `sink.write_mode` applies to the **accepted** sink only (`overwrite`, `append`, `merge_scd1`,
  `merge_scd2`; merges need an Iceberg accepted sink). Rejected CSV, Parquet and Iceberg output always
  appends, whatever `write_mode` is (#1254). A run for a different input never deletes an earlier run's
  rejected rows; reprocessing the same input (a retry, or a full refresh) rewrites that input's rejected CSV or
  Parquet part, so its earlier rows can be replaced. The Iceberg quarantine appends instead.
  See [Write modes](../concepts/write-modes.md#rejected-output-behavior).
- An Iceberg accepted sink requires a REST or AWS Glue catalog in `catalogs.definitions` (D26)
  and S3, GCS or ADLS storage for the table data.
- Iceberg `partition_spec` supports `identity`, `year`, `month`, `day`, `hour` and
  `bucket` (with `num_buckets`, D12).
- `schema.schema_evolution.mode: add_columns` is Iceberg-only and additive-only (D13): no type widening, so `int32` to `int64` or `float32` to `float64` fails the run.
- Iceberg `append` commits carry an input fingerprint, so a retry after a crash does not write
  the same batch twice (D5).
- Iceberg merges are merge-on-read and leave position delete files. `floe maintain` expires old
  snapshots and removes orphan files; see [CLI](cli.md).
- PII `strategy: drop` is rejected for Iceberg sinks (the table enforces the full declared schema).

## Accepted-output report metadata

- `accepted_output` in each entity report can include:
  - `files_written`, `parts_written`, `part_files`
  - `table_version` (Iceberg metadata version, Delta table version) and `snapshot_id` (Iceberg)
  - `total_bytes_written`, `avg_file_size_mb`, `small_files_count`
  - merge metrics for `merge_scd1` (see [report.md](report.md))

## Cloud storage behavior

- Storage paths resolve to canonical URIs: `s3://<bucket>/<prefix>/<path>`,
  `abfss://<container>@<account>.dfs.core.windows.net/<prefix>/<path>` and
  `gs://<bucket>/<prefix>/<path>`.
- Prefix listing is stable and lexicographically sorted, and a literal prefix matches by string
  prefix on all three.
- S3-compatible stores (MinIO, SeaweedFS, …) work through `AWS_ENDPOINT_URL_S3` /
  `AWS_ENDPOINT_URL`; see [storages/s3.md](../storages/s3.md).
- ADLS and GCS (the `adls` and `gcs` extras) cover sources, accepted Parquet, rejected output,
  reports, archive and incremental state; credentials come from the environment, and `endpoint`
  points at an emulator such as Azurite (CI runs the GCS tests on fake-gcs-server behind a test-only XML upload shim, #1342). See [ADLS](../storages/adls.md) and [GCS](../storages/gcs.md).

A cell names its status and harness (see [How to read the status](#how-to-read-the-status)).

| Feature | S3 | ADLS | GCS |
|---|---|---|---|
| Sources, accepted Parquet (`overwrite`), reports, incremental state | `CI-tested` (SeaweedFS); `real-provider-tested` 2026-11-23 (S3-compatible) | `CI-tested` (Azurite) | `CI-tested` (fake-gcs-server + shim) |
| `sink.archive` | `CI-tested` (SeaweedFS); `real-provider-tested` 2026-11-23 | `CI-tested` (Azurite) | `CI-tested` with an in-memory object store only |
| Rejected CSV | `CI-tested` (SeaweedFS) | `CI-tested` with an in-memory object store only | `CI-tested` with an in-memory object store only |
| Iceberg `overwrite` and `append` | `CI-tested` (Polaris + SeaweedFS) | `untested` end to end | `untested` |
| Iceberg merges | `CI-tested` (Polaris + SeaweedFS); `real-provider-tested` 2026-11-23 (`merge_scd1`) | `untested` | `unsupported`: fails `config_not_supported` (DuckDB's GCS secret takes HMAC keys only; `floe validate` does not catch it) |
| Delta `overwrite` and `append` (Unity Catalog registration optional) | `CI-tested` (SeaweedFS); Unity: stub only | `unsupported` | `unsupported` |
| `floe maintain` (orphan listing) | `real-provider-tested` 2026-11-23 (S3-compatible, Iceberg dry run) | `CI-tested` (Azurite) | `CI-tested` with an in-memory object store only |
| `floe manifest generate` and `replay`; manifest and profile read from a URI | `CI-tested` (SeaweedFS); `real-provider-tested` 2026-11-23 (manifest) | `CI-tested` (Azurite) | `CI-tested` with an in-memory object store only |

## S3-compatible providers

Per-provider results of `uv run pytest -m s3compat tests/integration/test_s3_compatible.py`, plus manual runs
against a local Polaris catalog backed by the bucket, see
[S3-compatible providers](../storages/s3.md#s3-compatible-providers). A cell is claimed only once a run against a
real bucket is recorded on the issue (Scaleway #1269, OVHcloud #1270), with its date. Results hold for the
version and date recorded on each issue; earlier versions need the notes in
[Provider notes](../storages/s3.md#provider-notes).

| Provider | List, read, write, copy, delete, large Parquet download | `If-None-Match: *` refused | Stale `If-Match` put refused | Stale `If-Match` delete refused | Archive copy and source mtime index | `floe run` with `incremental_mode: file` | Iceberg FileIO and DuckDB merges | Path-style | Virtual-hosted | Delta append, manifest on `s3://`, `floe maintain` |
|---|---|---|---|---|---|---|---|---|---|---|
| Scaleway Object Storage (`fr-par`) | ✅ 2026-11-23 | ✅ 2026-11-23 | ✅ 2026-11-23 | ✅ 2026-11-23 | ✅ 2026-11-23 | ✅ 2026-11-23 | ✅ 2026-11-23 | ✅ 2026-11-23 | ✅ 2026-11-23 | ✅ 2026-11-23 |
| OVHcloud Object Storage (`gra`) | ✅ 2026-11-23 | ✅ 2026-11-23 | ✅ 2026-11-23 | ✅ 2026-11-23 | ✅ 2026-11-23 | ✅ 2026-11-23 | ✅ 2026-11-23 | ✅ 2026-11-23 | ✅ 2026-11-23 | ✅ 2026-11-23 |

## Scale (200 entities)

`tests/scale/` generates a split project (`example/project/` layout) of 200 small entities in 5 domains:
3 named sources (CSV, JSON, Parquet), domain defaults for the sinks and `incremental_mode: file`, and 2 input
files per entity with 3 rejected rows out of 20. `pytest -m scale tests/scale -rP` runs it nightly and on demand
(`python-integration.yml`) and prints the table below. Measured on 4 vCPUs, local disk, Parquet sinks:

| Step | Runtime | Peak RSS |
|---|---|---|
| `floe --version` (cold start) | 0.4 s | 101 MiB |
| `floe validate` (200 entities) | 0.6 s | 105 MiB |
| `floe manifest generate` (200 entities) | 0.6 s | 107 MiB |
| `floe run`, 400 files, 4000 rows, 3400 accepted, 600 rejected, 4 entities at a time | 3.0 s | 189 MiB |
| the same, one entity at a time | 5.6 s | not measured |
| restart: all 400 files already ingested, nothing read | 1.8 s | 154 MiB |
| retry after 3 new files: only those 3 are read | 1.8 s | 181 MiB |
| `floe run --entities <one>` of the same project | 0.9 s | 178 MiB |

A run costs about 0.4 s of start-up plus about 12 ms per entity here. The 10-entity Iceberg subset
(`-m "polaris and scale"`, nightly in the `polaris` job) asserts one snapshot per table after the first run, none
added by a restart, one added by a retry with a new file, and the rejected counts of the run report; its timings
are in the nightly log, not measured locally (no Docker). Times are one machine's, and noisy: the tests assert
counts and a 600 MiB RSS ceiling, not runtimes.

## Config keys (storage)

```yaml
storages:
  default: local
  definitions:
    - name: local
      type: local
    - name: s3_raw
      type: s3
      bucket: my-bucket
      region: eu-west-1
      prefix: data
```

Storage references:
- `entities[].source.storage`
- `entities[].sink.accepted.storage`
- `entities[].sink.rejected.storage`
