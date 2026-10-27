# Rust 0.7 comparison archive

Until #970 the conformance suite compared the Python core with the Rust v0.7.0 release binary: `expected/` held
Rust's recordings, and each intentional delta (the D-register, #619) added a `delta.yml` with its ids and an
`expected_python/` with the files that differ. #970 folded `expected_python/` into `expected/`, so `expected/` now
holds the Python outputs. This file keeps the delta history; it moves to OpenLakeForge/floe-rust with the Rust
code. The Rust recordings themselves are in this repository's history at 4304ec64.

## Delta ids per case (`delta.yml`)

| case | deltas |
|---|---|
| `archive_mode` | D37 |
| `avro_read` | D37 |
| `cast_error_with_source` | D37 |
| `casts` | D9, D37 |
| `casts_coerce` | D9, D37 |
| `casts_date` | D9, D37 |
| `casts_datetime` | D9, D37 |
| `casts_typed` | D17, D37 |
| `config/d1_catalog_glue` | D1 |
| `config/d1_catalog_unity` | D1 |
| `config/d1_sink_delta` | D1 |
| `config/d1_sink_duckdb` | D1 |
| `config/d1_source_fixed` | D1 |
| `config/d1_storage_adls` | D1 |
| `config/d1_storage_gcs` | D1 |
| `config/div_S1` | D7 |
| `config/div_S10` | D1 |
| `config/div_S10_ctrl` | D1 |
| `config/div_S11` | D1 |
| `config/div_S11_ctrl` | D1 |
| `config/div_S12` | D1 |
| `config/div_S12_ctrl` | D1 |
| `config/div_S13_ctrl` | D33 |
| `config/div_S17` | D1 |
| `config/div_S17_ctrl` | D1 |
| `config/div_S2` | D7 |
| `config/div_S3` | D7 |
| `config/div_S4` | D7 |
| `config/div_S5` | D7 |
| `config/div_S6` | D7 |
| `config/div_S7a` | D7 |
| `config/div_S7b` | D7 |
| `config/div_S7c` | D7 |
| `config/example_config_yml` | D1 |
| `config/parse_catalogs_type_unsupported` | D1 |
| `config/parse_d28_date_column_missing` | D28 |
| `config/parse_d28_named_source_ok` | D28 |
| `config/parse_d28_unknown_key` | D28 |
| `config/parse_d30_lookback_days_negative` | D28, D30 |
| `config/parse_d30_lookback_days_ok` | D28, D30 |
| `config/parse_domains_defaults_drops_schema_columns_and_primary_key` | D7 |
| `config/parse_yaml_tab_indent` | D11 |
| `config/prof_catalogs_definition_type_unsupported` | D1 |
| `config/val1_d28_batch_column_same_as_date` | D28 |
| `config/val1_d28_batch_column_without_token` | D28 |
| `config/val1_d28_date_column_clash` | D28 |
| `config/val1_d28_date_column_clash_normalised` | D28 |
| `config/val1_d28_date_column_empty` | D28 |
| `config/val1_d28_partitioning_hour_hive_ok` | D28 |
| `config/val1_d28_partitioning_ok` | D28 |
| `config/val1_d28_pattern_missing_day` | D28 |
| `config/val1_d28_pattern_repeated_token` | D28 |
| `config/val1_d28_pattern_unknown_token` | D28 |
| `config/val1_d28_pattern_unmatched_brace` | D28 |
| `config/val1_d2_iceberg_merge_scd1` | D2 |
| `config/val1_d2_iceberg_merge_scd2` | D2 |
| `config/val1_d2_iceberg_merge_with_empty_primary_key` | D2 |
| `config/val1_d2_iceberg_merge_without_primary_key` | D2 |
| `config/val1_d2_pii_on_primary_key_with_merge` | D2 |
| `config/val1_version_d21_newer_minor` | D21 |
| `config/val1_version_future_major_ok` | D21 |
| `config/val2_d26_iceberg_no_catalog` | D26 |
| `config/val2_d28_partition_spec_day_ok` | D28 |
| `config/val2_d2_compare_pk` | D2 |
| `config/val2_d2_ignore_pk` | D2 |
| `config/val2_d2_merge_block_overwrite` | D2 |
| `config/val2_d2_merge_list_duplicate` | D2 |
| `config/val2_d2_merge_list_entry_empty` | D2 |
| `config/val2_d2_merge_list_unknown_column` | D2 |
| `config/val2_d2_merge_scd1_ignore_columns_ok` | D2 |
| `config/val2_d2_merge_scd1_ok` | D2 |
| `config/val2_d2_merge_scd2_ok` | D2 |
| `config/val2_d2_scd2_column_collides` | D2 |
| `config/val2_d2_scd2_column_empty` | D2 |
| `config/val2_d2_scd2_columns_not_unique` | D2 |
| `config/val2_d2_scd2_with_scd1` | D2 |
| `config/val2_evolution_add_columns_iceberg_ok` | D13 |
| `config/val2_part_spec_bucket_not_integer` | D12 |
| `config/val2_part_spec_bucket_ok` | D12, D26 |
| `config/val2_part_spec_bucket_on_day` | D12 |
| `config/val2_part_spec_bucket_zero` | D12 |
| `config/val2_part_spec_ok` | D26 |
| `config/val2_part_spec_same_column_other_transform_ok` | D26 |
| `config/val2_part_spec_transform_unsupported` | D12 |
| `config/val2_type_aliases_ok` | D38 |
| `config/val2_type_time` | D10 |
| `config/val2_type_unsupported` | D10 |
| `example/standalone` | D37 |
| `incremental_file` | D37 |
| `incremental_file_full_refresh` | D37 |
| `manifest_run` | D22 |
| `mismatch` | D37 |
| `mismatch_abort` | D37 |
| `mismatch_reject_file` | D37 |
| `orc_read` | D37 |
| `partitioning_archive_full_refresh` | D29, D37 |
| `partitioning_file_watermark` | D28, D30, D37 |
| `partitioning_hive_hour` | D28, D37 |
| `partitioning_two_days` | D28, D37 |
| `partitioning_unmatched` | D28 |
| `severity_abort` | D37 |
| `severity_warn` | D37 |
| `unique_pii` | D37 |
| `xml` | D37 |

## The delta mechanism (`conformance/README.md`)

Where Python must differ from Rust 0.7 on purpose (the D-register in #619), a case carries:

- `delta.yml`: one line listing the delta ids, e.g. `[D9]`;
- `expected_python/`: the files that differ, at the same relative path as in `expected/`.

`check.py --impl rust` always compares against `expected/`. `--impl python` compares each file against
`expected_python/<file>` when it exists, else `expected/<file>`, and prints the delta ids when such a case fails.
`record.py` only writes `expected/` (from Rust); `expected_python/` is derived from it and committed. The case
README says how (for `config/` cases, the delta table in `config/README.md`), so a reviewer can redo it.

D37 (report run directory per domain, #960): Python writes a single-domain run under `report/<domain>/run_<id>/`
and adds `run.domain` to `run.summary.json`. Every run case whose entities all share one domain carries `[D37]` and
an `expected_python/reports/report/<domain>/run_ID/` derived from its `expected/` (or existing `expected_python/`)
reports: the files move under the domain, every `/report/run_<id>` path in them (and in `events.jsonl`) becomes
`/report/<domain>/run_<id>`, and `"domain"` is inserted after `run.run_id`. When `expected_python/reports/` exists,
`check.py` takes the case's reports only from it (the moved files replace Rust's set rather than add to it).

D1 (dropped features) cases are not recorded: they hold only `expected_python/` with the Python error, `record.py`
skips them and `check.py --impl rust` prints `skip` for them.

### Manifest normalization

  D22 (#798): `check.py` also drops `schema` and masks `spec_version` on both sides at compare time, so the Rust 0.7
  recordings (`floe.manifest.v1`, the config version) stay as recorded; `tests/unit/test_manifest.py` checks Python's value.
  D20 (#796): `check.py` also drops the Python-only `sources` index and project `metadata` at compare time;
  `tests/unit/test_manifest.py` checks their content.
  A manifest run reports the manifest's `spec_version` (the Floe version), so `manifest_run` carries a `[D22]`
  `expected_python/` for its report files.


## Config deltas (`conformance/config/README.md`)

18 D1, 18 D2, 10 D7, 2 D10, 1 D11, 5 D12, 1 D13, 2 D21, 4 D26, 17 D28, 2 D30, 1 D33, 1 D38.
**The Python messages below are hand-derived proposals**, not recorded: the maintainer owns the wording. No Python
implementation exists yet; each is derived as its row says.

- **D1** (dropped features): the case holds only `expected_python/validate.json`; there is no `expected/`, so
  `check.py --impl rust` skips it. The Python error follows one pattern: `<what> is not supported in 0.8`, with the
  prefix the Rust message uses for that field (`entity.name=orders source.format=xlsx`, `catalogs.definitions name=u type=unity`).
  Every D1 config is accepted by Rust 0.7 (checked, not recorded), except the S10-S17 repros, which hit the Rust rule
  quoted in `why`.
- **D2**: Rust 0.7 rejects Iceberg `merge_scd1`/`merge_scd2` at the write-mode check; Python accepts. The `py` message of the
  merge-rule cases is the message of the Rust rule that would fire for a Delta sink, so these are the only coverage of
  those rules (they are listed in the table below with their `(D2)` mark).
- **D38** (#559): bare `decimal` is rejected (Rust 0.7 accepts it as Float64); `decimal(p,s)` is exact. Only `val2_type_aliases_ok` uses it.
- **D7**: S1-S7 of #584, schema wins (Python validate rejects). S3/S4/S5 reuse the Rust run-time message.

| case | delta | Python outcome | how it was derived |
|---|---|---|---|
| `div_S1` | D7 | `entity.name=orders schema.columns[0].type=string  is unsupported for column id (allowed: string, number, boolean, datetime, date)` | D7 (#584 S1-S7, schema wins): Python validate rejects what Rust 0.7 only catches at run time or ignores. Rust's own validate message for an unsupported type, with the value untrimmed (`canonical_column_type` stops trimming); the allowed list is D10's. |
| `div_S2` | D7 | `entity.name=orders schema.normalize_columns.strategy= lower is unsupported (allowed: snake_case, lower, camel_case, none)` | D7 (#584 S1-S7, schema wins): Python validate rejects what Rust 0.7 only catches at run time or ignores. Rust's own validate message for an unsupported strategy, with the value untrimmed. |
| `div_S3` | D7 | `entity.name=orders separator must be a single byte, got "ab"` | D7 (#584 S1-S7, schema wins): Python validate rejects what Rust 0.7 only catches at run time or ignores. Rust's run-time message (`csv_read_error: separator must be a single byte, got "ab"`) with the entity prefix of validate messages. |
| `div_S4` | D7 | `entity.name=orders unsupported encoding: latin1` | D7 (#584 S1-S7, schema wins): Python validate rejects what Rust 0.7 only catches at run time or ignores. Rust's run-time message (`unsupported encoding: latin1`) with the entity prefix of validate messages. |
| `div_S5` | D7 | `unsupported report.formatter: xml` | D7 (#584 S1-S7, schema wins): Python validate rejects what Rust 0.7 only catches at run time or ignores. Rust's run-time message, verbatim (`Error: unsupported report.formatter: xml`). |
| `div_S6` | D7 | `domain.name=sales.eu must be non-empty without '.', '/' or '\' when root.version >= "0.3"` | D7 (#584 S1-S7, schema wins): Python validate rejects what Rust 0.7 only catches at run time or ignores. The message of `validate.rs:102` for an entity, with `domain.name=` as the prefix (an unused domain has no entity). |
| `div_S7a` | D7 | `unknown field domains.defaults.name` | D7 (#584 S1-S7, schema wins): Python validate rejects what Rust 0.7 only catches at run time or ignores. Rust's unknown-key message (`unknown field <path>`) for a key the schema forbids in domain defaults. |
| `div_S7b` | D7 | `unknown field domains.defaults.schema.columns` | D7 (#584 S1-S7, schema wins): Python validate rejects what Rust 0.7 only catches at run time or ignores. As S7a. |
| `div_S7c` | D7 | `unknown field domains.defaults.schema.primary_key` | D7 (#584 S1-S7, schema wins): Python validate rejects what Rust 0.7 only catches at run time or ignores. As S7a. |
| `parse_domains_defaults_drops_schema_columns_and_primary_key` | D7 | `unknown field domains.defaults.schema.columns` | D7 (#584 S7): Rust 0.7 silently drops domain defaults `schema.columns` and `schema.primary_key`; Python rejects them as S7b/S7c do. |
| `div_S10` | D1 | `entity.name=orders sink.accepted.format=delta is not supported in 0.8` | D1 (dropped feature): the repro needs a Delta sink, which Python rejects at validate. Not recorded from Rust. Rust 0.7: `sink.accepted.partition_by has duplicate column id` (validate.rs:1180). |
| `div_S10_ctrl` | D1 | `entity.name=orders sink.accepted.format=delta is not supported in 0.8` | D1 (dropped feature): the repro needs a Delta sink, which Python rejects at validate. Not recorded from Rust.  |
| `div_S11` | D1 | `entity.name=orders sink.accepted.format=delta is not supported in 0.8` | D1 (dropped feature): the repro needs a Delta sink, which Python rejects at validate. Not recorded from Rust. Rust 0.7: `merge.ignore_columns has duplicate column amt` (validate.rs:977); the Iceberg side of the rule is `val2_d2_*`. |
| `div_S11_ctrl` | D1 | `entity.name=orders sink.accepted.format=delta is not supported in 0.8` | D1 (dropped feature): the repro needs a Delta sink, which Python rejects at validate. Not recorded from Rust.  |
| `div_S12` | D1 | `entity.name=orders sink.accepted.format=delta is not supported in 0.8` | D1 (dropped feature): the repro needs a Delta sink, which Python rejects at validate. Not recorded from Rust. Rust 0.7: `merge.compare_columns has duplicate column amt` (validate.rs:977); the Iceberg side of the rule is `val2_d2_*`. |
| `div_S12_ctrl` | D1 | `entity.name=orders sink.accepted.format=delta is not supported in 0.8` | D1 (dropped feature): the repro needs a Delta sink, which Python rejects at validate. Not recorded from Rust.  |
| `div_S17` | D1 | `entity.name=orders sink.accepted.format=delta is not supported in 0.8` | D1 (dropped feature): the repro needs a Delta sink, which Python rejects at validate. Not recorded from Rust. Rust 0.7: `merge.scd2 column names must be unique` (validate.rs:942); the Iceberg side of the rule is `val2_d2_*`. |
| `div_S17_ctrl` | D1 | `entity.name=orders sink.accepted.format=delta is not supported in 0.8` | D1 (dropped feature): the repro needs a Delta sink, which Python rejects at validate. Not recorded from Rust.  |
| `d1_sink_delta` | D1 | `entity.name=orders sink.accepted.format=delta is not supported in 0.8` | D1 (dropped feature, OLF scope): Python rejects it at validate. Not recorded from Rust. |
| `d1_sink_duckdb` | D1 | `entity.name=orders sink.accepted.format=duckdb is not supported in 0.8` | D1 (dropped feature, OLF scope): Python rejects it at validate. Not recorded from Rust. |
| `d1_catalog_unity` | D1 | `catalogs.definitions name=u type=unity is not supported in 0.8` | D1 (dropped feature, OLF scope): Python rejects it at validate. Not recorded from Rust. |
| `d1_catalog_glue` | D1 | `catalogs.definitions name=g type=glue is not supported in 0.8` | D1 (dropped feature, OLF scope): Python rejects it at validate. Not recorded from Rust. |
| `parse_catalogs_type_unsupported` | D1 | `catalogs.definitions[0]: catalogs.definitions name=c has unsupported type=hive (supported: rest)` | Hand-derived, not recorded: Rust's message with the supported list reduced to `rest`, since Glue and Unity are dropped features (D1). |
| `prof_catalogs_definition_type_unsupported` | D1 | `profile.catalogs.definitions[0]: profile.catalogs.definitions name=r has unsupported type=hive (supported: rest)` | Hand-derived, not recorded: Rust's message with the supported list reduced to `rest`, since Glue and Unity are dropped features (D1). |
| `d1_storage_adls` | D1 | `storages.definitions name=a type=adls is not supported in 0.8` | D1 (dropped feature, OLF scope): Python rejects it at validate. Not recorded from Rust. |
| `d1_storage_gcs` | D1 | `storages.definitions name=g type=gcs is not supported in 0.8` | D1 (dropped feature, OLF scope): Python rejects it at validate. Not recorded from Rust. |
| `d1_source_fixed` | D1 | `entity.name=orders source.format=fixed is not supported in 0.8` | D1 (dropped feature, OLF scope): Python rejects it at validate. Not recorded from Rust. |
| `example_config_yml` | D1 | `entity.name=customer sink.accepted.format=delta is not supported in 0.8` | D1 (dropped feature): example/config.yml writes the `customer` entity to a Delta table and `customer_warehouse` to DuckDB. Not recorded from Rust. |
| `parse_yaml_tab_indent` | D11 | `while scanning for the next token, found character '\t' that cannot start any token at byte 25 line 3 column 1` | PyYAML scanner wording and byte offset; derived by running floe.config.yaml.load_yaml (#684) on this file |
| `val1_d2_pii_on_primary_key_with_merge` | D2 | `entity.name=orders pii.columns[name=id]: PII strategy cannot be applied to a primary_key column with write_mode=merge_scd1: masking the merge key corrupts the merge predicate` | D2: Iceberg accepts merge_scd1/merge_scd2 in Python. The Rust error is the write-mode capability check (validate.rs:657), which Python drops; the rule that Rust's PII-on-merge-key check (validate.rs:327) then reports is applied by Python in its place, with Rust's wording for that rule. |
| `val1_d2_iceberg_merge_scd1` | D2 | ok | D2: Iceberg accepts merge_scd1 in Python (Rust allows only overwrite/append, iceberg.rs:178-180); the config is otherwise valid. |
| `val1_d2_iceberg_merge_scd2` | D2 | ok | D2: Iceberg accepts merge_scd2 in Python; the config is otherwise valid. |
| `val1_d2_iceberg_merge_without_primary_key` | D2 | `entity.name=orders sink.write_mode=merge_scd1 requires schema.primary_key` | D2: Iceberg accepts merge_scd1/merge_scd2 in Python. The Rust error is the write-mode capability check (validate.rs:657), which Python drops; the rule that Rust's primary-key requirement (validate.rs:674) then reports is applied by Python in its place, with Rust's wording for that rule. |
| `val1_d2_iceberg_merge_with_empty_primary_key` | D2 | `entity.name=orders sink.write_mode=merge_scd1 requires non-empty schema.primary_key` | D2: Iceberg accepts merge_scd1/merge_scd2 in Python. The Rust error is the write-mode capability check (validate.rs:657), which Python drops; the rule that Rust's non-empty primary-key requirement (validate.rs:680) then reports is applied by Python in its place, with Rust's wording for that rule. |
| `val2_d2_merge_scd1_ok` | D2 | ok | Rust 0.7 rejects every Iceberg merge at the write-mode check; D2 makes Iceberg accept merge_scd1/merge_scd2. py is the message of the validate.rs:650 rule for the same config (a format-agnostic rule in Rust, recordable from Rust only for delta/duckdb, which are dropped). |
| `val2_d2_merge_scd2_ok` | D2 | ok | Rust 0.7 rejects every Iceberg merge at the write-mode check; D2 makes Iceberg accept merge_scd1/merge_scd2. py is the message of the validate.rs:650 rule for the same config (a format-agnostic rule in Rust, recordable from Rust only for delta/duckdb, which are dropped). |
| `val2_d2_merge_scd1_ignore_columns_ok` | D2 | ok | Rust 0.7 rejects every Iceberg merge at the write-mode check; D2 makes Iceberg accept merge_scd1/merge_scd2. py is the message of the validate.rs:650 rule for the same config (a format-agnostic rule in Rust, recordable from Rust only for delta/duckdb, which are dropped). |
| `val2_d2_merge_block_overwrite` | D2 | `entity.name=orders sink.accepted.merge is only supported with sink.write_mode=merge_scd1 or merge_scd2` | Rust 0.7 rejects every Iceberg merge at the write-mode check; D2 makes Iceberg accept merge_scd1/merge_scd2. py is the message of the validate.rs:811 rule for the same config (a format-agnostic rule in Rust, recordable from Rust only for delta/duckdb, which are dropped). |
| `val2_d2_ignore_pk` | D2 | `entity.name=orders sink.accepted.merge.ignore_columns[0]=id cannot reference schema.primary_key column` | Rust 0.7 rejects every Iceberg merge at the write-mode check; D2 makes Iceberg accept merge_scd1/merge_scd2. py is the message of the validate.rs:869 rule for the same config (a format-agnostic rule in Rust, recordable from Rust only for delta/duckdb, which are dropped). |
| `val2_d2_compare_pk` | D2 | `entity.name=orders sink.accepted.merge.compare_columns[0]=id cannot reference schema.primary_key column` | Rust 0.7 rejects every Iceberg merge at the write-mode check; D2 makes Iceberg accept merge_scd1/merge_scd2. py is the message of the validate.rs:887 rule for the same config (a format-agnostic rule in Rust, recordable from Rust only for delta/duckdb, which are dropped). |
| `val2_d2_scd2_with_scd1` | D2 | `entity.name=orders sink.accepted.merge.scd2 is only supported with sink.write_mode=merge_scd2` | Rust 0.7 rejects every Iceberg merge at the write-mode check; D2 makes Iceberg accept merge_scd1/merge_scd2. py is the message of the validate.rs:897 rule for the same config (a format-agnostic rule in Rust, recordable from Rust only for delta/duckdb, which are dropped). |
| `val2_d2_scd2_column_empty` | D2 | `entity.name=orders sink.accepted.merge.scd2.current_flag_column must not be empty` | Rust 0.7 rejects every Iceberg merge at the write-mode check; D2 makes Iceberg accept merge_scd1/merge_scd2. py is the message of the validate.rs:924 rule for the same config (a format-agnostic rule in Rust, recordable from Rust only for delta/duckdb, which are dropped). |
| `val2_d2_scd2_column_collides` | D2 | `entity.name=orders sink.accepted.merge.scd2.current_flag_column=amt collides with schema column name` | Rust 0.7 rejects every Iceberg merge at the write-mode check; D2 makes Iceberg accept merge_scd1/merge_scd2. py is the message of the validate.rs:931 rule for the same config (a format-agnostic rule in Rust, recordable from Rust only for delta/duckdb, which are dropped). |
| `val2_d2_scd2_columns_not_unique` | D2 | `entity.name=orders sink.accepted.merge.scd2 column names must be unique` | Rust 0.7 rejects every Iceberg merge at the write-mode check; D2 makes Iceberg accept merge_scd1/merge_scd2. py is the message of the validate.rs:942 rule for the same config (a format-agnostic rule in Rust, recordable from Rust only for delta/duckdb, which are dropped). |
| `val2_d2_merge_list_entry_empty` | D2 | `entity.name=orders sink.accepted.merge.ignore_columns[0] must not be empty` | Rust 0.7 rejects every Iceberg merge at the write-mode check; D2 makes Iceberg accept merge_scd1/merge_scd2. py is the message of the validate.rs:963 rule for the same config (a format-agnostic rule in Rust, recordable from Rust only for delta/duckdb, which are dropped). |
| `val2_d2_merge_list_unknown_column` | D2 | `entity.name=orders sink.accepted.merge.compare_columns[0]=zz references unknown schema column` | Rust 0.7 rejects every Iceberg merge at the write-mode check; D2 makes Iceberg accept merge_scd1/merge_scd2. py is the message of the validate.rs:970 rule for the same config (a format-agnostic rule in Rust, recordable from Rust only for delta/duckdb, which are dropped). |
| `val2_d2_merge_list_duplicate` | D2 | `entity.name=orders sink.accepted.merge.ignore_columns has duplicate column amt` | Rust 0.7 rejects every Iceberg merge at the write-mode check; D2 makes Iceberg accept merge_scd1/merge_scd2. py is the message of the validate.rs:977 rule for the same config (a format-agnostic rule in Rust, recordable from Rust only for delta/duckdb, which are dropped). |
| `val2_type_time` | D10 | `entity.name=orders schema.columns[0].type=time is unsupported for column id (allowed: string, number, boolean, datetime, date)` | D10: Rust 0.7 accepts `time` at validate (every value later fails to cast); Python rejects it as not supported in 0.8. py is the validate.rs:1313 message with `time` dropped from the allowed list. |
| `val2_type_unsupported` | D10 | `entity.name=orders schema.columns[0].type=varchar is unsupported for column id (allowed: string, number, boolean, datetime, date)` | Hand-derived, not recorded: Rust's message with `time` dropped from the allowed list (D10), as for `val2_type_time`. |
| `val2_part_spec_transform_unsupported` | D12 | `entity.name=orders sink.accepted.partition_spec[0].num_buckets is required for transform=bucket` | D12 (#703): Python accepts `transform: bucket` with a required positive integer `num_buckets` on Iceberg sinks; Rust 0.7 rejects `bucket` (validate.rs:1227) and, at parse, any `num_buckets` key. This config has no `num_buckets`; wording follows `profile/validate.rs` `{field} is required for {kind}`. |
| `val2_part_spec_bucket_ok` | D12, D26 | `entity.name=orders sink.accepted.format=iceberg requires sink.accepted.iceberg with a catalog in 0.8` | As above, with `num_buckets: 16`; D12 accepts the spec, then D26 rejects the catalog-less sink (the D12 accept is covered by `test_bucket_transform_ok`). |
| `val2_part_spec_bucket_zero` | D12 | `entity.name=orders sink.accepted.partition_spec[0].num_buckets must be greater than 0` | As above; wording of the validate.rs `sink.accepted.options.{field} must be greater than 0` rule. |
| `val2_part_spec_bucket_not_integer` | D12 | `entities[0] (entity.name=orders): expected integer at sink.accepted.partition_spec[0].num_buckets` | As above; `num_buckets: 1.5` hits parse.rs's integer reader (`expected integer at`). |
| `val2_part_spec_bucket_on_day` | D12 | `entity.name=orders sink.accepted.partition_spec[0].num_buckets is only supported with transform=bucket` | As above; `num_buckets` on `transform: day`. Wording follows the validate.rs `is only supported with sink.write_mode=...` rules. |
| `val2_evolution_add_columns_iceberg_ok` | D13 | ok | D13: Python accepts `schema_evolution.mode: add_columns` on Iceberg sinks. Rust 0.7 rejects it unless the sink is Delta (validate.rs:1366), which D1 drops; other formats keep Rust's rejection. |
| `val1_version_future_major_ok` | D21 | `root.version=1.0 is unsupported (newer than Floe 0.8)` | D21 (#797): root.version is the minimum Floe release the config targets; Python rejects one newer than its own major.minor. Rust 0.7 accepts any u64 version. Wording mirrors the validate.rs:140 minimum-version message. |
| `val1_version_d21_newer_minor` | D21 | `root.version=0.9 is unsupported (newer than Floe 0.8)` | As `val1_version_future_major_ok`, one minor ahead. Its control `val1_version_d21_floe_ok` (`0.8`) is ok on both sides. |
| `val2_d26_iceberg_no_catalog` | D26 | `entity.name=orders sink.accepted.format=iceberg requires sink.accepted.iceberg with a catalog in 0.8` | D26 (#844): an Iceberg sink requires a REST catalog in 0.8; Rust 0.7 accepts a catalog-less one and writes a filesystem table. Checked after every other rule, so a config Rust rejects keeps its message. Wording is the Python run-time error (`iceberg/sink.py` `resolve_target`). |
| `val2_part_spec_ok` | D26 | `entity.name=orders sink.accepted.format=iceberg requires sink.accepted.iceberg with a catalog in 0.8` | As `val2_d26_iceberg_no_catalog`; the sink has no `iceberg` block. |
| `val2_part_spec_same_column_other_transform_ok` | D26 | `entity.name=orders sink.accepted.format=iceberg requires sink.accepted.iceberg with a catalog in 0.8` | As above. |
| `val1_d28_partitioning_ok` | D28 | ok | D28 (#883): Python accepts `source.partitioning` (`pattern`, `date_column`, `batch_column`); Rust 0.7 rejects the key (`unknown field source.partitioning`). |
| `val1_d28_partitioning_hour_hive_ok` | D28 | ok | As above; Hive-style literals around the tokens and `{HH}`, no `batch_column`. |
| `val2_d28_partition_spec_day_ok` | D28 | ok | As above; the derived `ingest_date` is a schema column for `partition_spec` (`transform: day`) and the merge `primary_key`. |
| `parse_d28_named_source_ok` | D28 | ok | As above, on a named source (`unknown field sources.partitioning` in Rust 0.7). |
| `parse_d28_unknown_key` | D28 | `entities[0] (entity.name=orders): unknown field source.partitioning.timezone` | As above; parse.rs's `unknown field` wording. |
| `parse_d28_date_column_missing` | D28 | `entities[0] (entity.name=orders): missing required field source.partitioning.date_column` | As above; parse.rs's `missing required field` wording. |
| `val1_d28_pattern_unknown_token` | D28 | `entity.name=orders source.partitioning.pattern={yyyy}/{MM}/{dd}/{mm}/{batch} has unknown token {mm} (allowed: {yyyy}, {MM}, {dd}, {HH}, {batch})` | As above; tokens are case-sensitive. Wording follows validate.rs `is unsupported (allowed: ...)`. |
| `val1_d28_pattern_unmatched_brace` | D28 | `entity.name=orders source.partitioning.pattern={yyyy}/{MM}/{dd}/{batch has an unmatched brace` | As above. |
| `val1_d28_pattern_repeated_token` | D28 | `entity.name=orders source.partitioning.pattern={yyyy}/{MM}/{dd}/{batch}/{batch} repeats token {batch}` | As above. |
| `val1_d28_pattern_missing_day` | D28 | `entity.name=orders source.partitioning.pattern={yyyy}/{MM}/{batch} must contain {yyyy}, {MM} and {dd}` | As above. |
| `val1_d28_date_column_empty` | D28 | `entity.name=orders source.partitioning.date_column must not be empty` | As above; validate.rs `must not be empty` wording. |
| `val1_d28_batch_column_without_token` | D28 | `entity.name=orders source.partitioning.batch_column requires {batch} in source.partitioning.pattern` | As above. |
| `val1_d28_batch_column_same_as_date` | D28 | `entity.name=orders source.partitioning.batch_column=ingest_date must differ from source.partitioning.date_column` | As above. |
| `val1_d28_date_column_clash` | D28 | `entity.name=orders source.partitioning.date_column=amt is already a column in schema.columns` | As above; a derived column must not clash with `schema.columns`. |
| `val1_d28_date_column_clash_normalised` | D28 | `entity.name=orders source.partitioning.date_column=ingest_date is already a column in schema.columns` | As above, after `normalize_columns` (#913): `Ingest Date` normalises to `ingest_date`. |
| `parse_d30_lookback_days_ok` | D28, D30 | ok | D30 (#885): Python accepts `source.partitioning.lookback_days`, a non-negative integer (default 1); Rust 0.7 rejects `source.partitioning` itself. |
| `parse_d30_lookback_days_negative` | D28, D30 | `entities[0] (entity.name=orders): expected positive integer at source.partitioning.lookback_days` | As above; parse.rs's integer wording (`opt_u32`). |
| `div_S13_ctrl` | D33 | ok; `expected_python/manifest.json` | D33 (#911): the manifest carries the Iceberg sink's `partition_spec`, so `run --manifest` keeps it; Rust 0.7 drops it (`manifest/reconstruct.rs:388`). The validate outcome is unchanged; only the manifest differs, by the `partition_spec` block. |

## How each run case's `expected_python/` was derived

### casts

`expected_python/` was derived by running Python (`uv run floe run -c config.yml` in a copy of the case) and checking
that the diff against `expected/` is exactly those rows: the accepted table loses r12 and r21; the rejected CSV gains
their lines and changes r8's `ts`; `run.json` and `run.summary.json` differ only in `accepted_*` (-2), `rejected_*`
(+2), `errors` (+2) and the `ts` column's `cast_error` violations (+2).

### casts_coerce

`expected_python/accepted/casts_coerce.parquet` is the accepted table of a Python run (`uv run floe run -c config.yml`
in a copy of the case); the reports are unchanged, as no row is rejected in coerce mode.

### casts_date

`expected_python/` was derived from `expected/` of the same case (no Python run exists yet):
- `accepted/casts_date.parquet`: the seven rows removed, others unchanged;
- `rejected/casts_date/part-00000.csv`: for each of the seven rows, a copy of a Rust-rejected `date` row with that `id`
  and `__floe_row_index` (its 0-based line in `in/`), merged in row-index order;
- `reports/`: `accepted_total`/`accepted_rows`/`accepted_count` minus 7, `rejected_total`/`rejected_count`/`errors`/`violations`
  (global and the `date` column) plus 7. Writer byte metrics keep Rust's values (ignored on compare).

### casts_datetime

`expected_python/` was derived from `expected/` of the same case (no Python run exists yet):
- `accepted/casts_datetime.parquet`: rejected rows removed; converted rows get the wall-clock time minus the offset;
- `rejected/casts_datetime/part-00000.csv`: for each newly rejected row, a copy of a Rust-rejected row for the same column
  with that `id` and `__floe_row_index` (its 0-based line in `in/`), merged in row-index order;
- `reports/`: `accepted_total`/`accepted_rows`/`accepted_count` minus 15, `rejected_total`/`rejected_count`/`errors`/`violations`
  plus 15 (global, `datetime` column +14, `timestamp` column +1). Writer byte metrics keep Rust's values (ignored on compare).

### casts_typed

`expected_python/` was derived from `expected/` (no Python run exists yet), for `typed_int8`, `int32`, `int64`, `uint32`, `uint64`:
- `rejected/`: an `f64` entry added to `r1`'s `__floe_errors` (schema column order); `typed_uint64` had accepted `r1`, so it
  gains the row (cells as in the other integer cases: `u64` 9223372036854775808);
- `accepted/typed_uint64.parquet`: `r1` removed;
- `reports/`: `errors_total`/`errors`/`violations` and the `f64` column violations plus 1 per entity, and for `typed_uint64`
  accepted minus 1 and rejected plus 1 (entity and global). Writer byte metrics keep Rust's values (ignored on compare).
