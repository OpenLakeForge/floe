# Config conformance cases (`floe validate`)

636 cases. Each is a config (and optionally a profile) with the outcome of `floe validate` recorded from the Rust
v0.7.0 release binary. See [../README.md](../README.md) for recording, checking and the delta mechanism.

## Layout

```
conformance/config/<case>/
├── config.yml          run as `floe validate -c config.yml` from a temp copy of the case
├── profile.yml         optional: adds `--profile profile.yml`
├── <other files>       includes, sources and domain files of a project layout
├── expected/validate.json
├── delta.yml           optional: intentional deltas, e.g. `[D7]`
└── expected_python/validate.json   optional: the Python outcome when it differs
```

`expected/validate.json` is `{"ok": true}` or `{"ok": false, "error": "<message>"}`. The message is the text after
`Error: ` on stderr, with absolute paths masked as `<ROOT>`. The Rust CLI prints no error kind, only the message, so
the kind is not recorded: every config parse and validate site builds a `FloeError::config`, except the I/O error
on an included file (`config/parse.rs:321`) and the foreign YAML, glob and file-read errors. A successful validate
is not compared beyond `ok`: its summary output belongs to the CLI cases.

Cases under `conformance/config/` are `floe validate` cases; every other case is a `floe run` case.
`--impl python` runs `uv run floe validate -c config.yml [--profile profile.yml]` and expects the same
`Error: <message>` line on stderr.

## Case names

| prefix | content |
|---|---|
| `val1_` | `config/validate.rs` up to the sink write mode: version, entities, lineage, report, PII, state, source, sink |
| `val2_` | `config/validate.rs` from merge options on: merge, catalog binding, partitioning, schema, keys, registries; `storage.rs`, `catalog.rs`, JSON selectors |
| `parse_` | `config/parse.rs`, `template.rs`, `types.rs`, `yaml_decode.rs`: YAML loading, includes, domains, sources, entity fields, templates |
| `prof_` | `profile/parse.rs`, `profile/validate.rs`, variable expansion, and the profile's catalogs, storages and lineage applied to the config |
| `div_` | both sides of each #584 divergence (S1-S32); `_ctrl` is the counterpart that differs only by the fix |
| `d1_` | one case per dropped feature |
| `example_` | every config under `example/`, plus its profiles |

Controls (`*_ok`, `*_ctrl`) are valid configs that sit on the passing side of a rule.

## Intentional deltas

18 D1, 18 D2, 10 D7, 2 D10, 1 D11, 5 D12, 1 D13, 2 D21, 4 D26, 14 D28.
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

## #584 divergences

Each id has the divergent config and a control that differs only by the fix. S1-S7: Rust `ok`, Python rejects (D7).
S8-S9, S13, S14a, S14b, S16a, S16b and S27-S32 are recorded errors. S10-S12, S15 and S17 reproduce only with a dropped feature
(Delta, XLSX), so both sides are D1. S18-S26 are Rust-accepted profiles.

| id | cases |
|---|---|
| S1 | `div_S1` (D7), `div_S1_ctrl` |
| S2 | `div_S2` (D7), `div_S2_ctrl` |
| S3 | `div_S3` (D7), `div_S3_ctrl` |
| S4 | `div_S4` (D7), `div_S4_ctrl` |
| S5 | `div_S5` (D7), `div_S5_ctrl` |
| S6 | `div_S6` (D7), `div_S6_ctrl` |
| S7 | `div_S7_ctrl` |
| S7a | `div_S7a` (D7) |
| S7b | `div_S7b` (D7) |
| S7c | `div_S7c` (D7) |
| S8 | `div_S8`, `div_S8_ctrl` |
| S9 | `div_S9`, `div_S9_ctrl` |
| S10 | `div_S10` (D1), `div_S10_ctrl` (D1) |
| S11 | `div_S11` (D1), `div_S11_ctrl` (D1) |
| S12 | `div_S12` (D1), `div_S12_ctrl` (D1) |
| S13 | `div_S13`, `div_S13_ctrl` |
| S14a | `div_S14a`, `div_S14a_ctrl_csv`, `div_S14a_ctrl_json` |
| S14b | `div_S14b`, `div_S14b_ctrl` |
| S15 | `div_S15` (D1), `div_S15_ctrl` (D1) |
| S16a | `div_S16a`, `div_S16a_ctrl` |
| S16b | `div_S16b`, `div_S16b_ctrl` |
| S17 | `div_S17` (D1), `div_S17_ctrl` (D1) |
| S18 | `div_S18`, `div_S18_ctrl` |
| S19 | `div_S19`, `div_S19_ctrl` |
| S20 | `div_S20`, `div_S20_ctrl` |
| S21 | `div_S21`, `div_S21_ctrl` |
| S22a | `div_S22a`, `div_S22a_ctrl` |
| S22b | `div_S22b`, `div_S22b_ctrl` |
| S23 | `div_S23`, `div_S23_ctrl` |
| S24 | `div_S24`, `div_S24_ctrl` |
| S25 | `div_S25`, `div_S25_ctrl` |
| S26 | `div_S26`, `div_S26_ctrl` |
| S27 | `div_S27`, `div_S27_ctrl` |
| S28 | `div_S28`, `div_S28_ctrl` |
| S29 | `div_S29`, `div_S29_ctrl` |
| S30 | `div_S30`, `div_S30_ctrl` |
| S31a | `div_S31a`, `div_S31a_ctrl` |
| S31b | `div_S31b`, `div_S31b_ctrl` |
| S32 | `div_S32`, `div_S32_ctrl` |

## Coverage of error sites

Every error constructor in the config validate path, found by `grep` for `FloeError::`, `message:` and the format error
helpers in these files: `config/{parse,validate,storage,catalog,template,types,yaml_decode}.rs`,
`profile/{parse,validate}.rs`, `io/format.rs` and `io/read/json_selector.rs`. The line is the line of the constructor in
the repository at v0.7.0 (the Rust core is frozen); paths are relative to `crates/floe-core/src/`. Rows with a name
instead of a line are foreign errors (YAML, glob, file reads) or `vars/resolve.rs` sites that are not in the scanned
files. A site with no recorded case says why. A case in parentheses is a delta case: a `(D2)` case lists the Rust rule
for Python only, because Rust 0.7 rejects that config earlier, at the write-mode check.

### Not covered by this suite

- The error kind (the CLI prints only the message).
- Inputs the runner cannot express: a missing or unreadable `-c` file, a missing `--profile` file, a non-UTF-8
  file, `floe validate --profile` without `-c`, `--entities`, remote (`s3://`) config URIs.
- `floe validate`'s success summary and its `warn:` lines on stderr.


#### `config/catalog.rs`

| line | message | case |
|---|---|---|
| 76 | catalogs.definitions name={} is duplicated | not recorded: runtime-only: CatalogResolver (run path); `floe validate` never calls it |
| 115 | entity.name={} sink.accepted.iceberg.catalog is required when no catalogs.default is set | not recorded: runtime-only: CatalogResolver (run path); `floe validate` never calls it |
| 122 | entity.name={} sink.accepted.iceberg.catalog references unknown catalog {} | not recorded: runtime-only: CatalogResolver (run path); `floe validate` never calls it |
| 136 | entity.name={} sink.accepted.iceberg.catalog references a unity catalog, which only s... | not recorded: runtime-only: CatalogResolver (run path); `floe validate` never calls it |
| 222 | entity.name={} sink.accepted.delta.catalog is required when no catalogs.default is set | not recorded: runtime-only: CatalogResolver (run path); `floe validate` never calls it |
| 230 | entity.name={} sink.accepted.delta.catalog references unknown catalog {} | not recorded: runtime-only: CatalogResolver (run path); `floe validate` never calls it |

#### `config/parse.rs`

| line | message | case |
|---|---|---|
| 126 | generated entity YAML must contain exactly one document | not recorded: add-entity only (validate_with_entity_yaml) |
| 133 | expected map at generated entity | not recorded: add-entity only (validate_with_entity_yaml) |
| 140 | expected map at root | not recorded: add-entity only (validate_with_entity_yaml) |
| 146 | expected array at entities | not recorded: add-entity only (validate_with_entity_yaml) |
| 159 | YAML is empty | `parse_yaml_empty_file`, `parse_yaml_comment_only` |
| 162 | YAML contains multiple documents; expected one | `parse_yaml_multiple_documents` |
| 196 | root.include requires root.version >= "0.3" | `parse_include_ok`, `parse_include_requires_version_0_3`, `parse_include_missing_version` +1 |
| 204 | root.sources and include.sources are mutually exclusive | `parse_include_sources_and_root_sources`, `parse_include_sources_with_domains` |
| 231 | missing required field include.domains | `parse_include_missing_domains` |
| 238 | (see source) | `parse_include_domains_no_match` |
| 266 | expected map at entity | `parse_include_entity_file_not_a_map`, `parse_include_sibling_non_yml_ignored` |
| 278 | {} has domain {domain:?} but its _domain.yml names {name:?} | `parse_include_entity_file_domain_mismatch`, `parse_include_entity_file_domain_matches` |
| 310 | expected array at {key} | `parse_include_entities_array_conflict`, `parse_include_root_entities_not_array` |
| 321 | (see source) | `parse_include_sources_file_missing` |
| 323 | (see source) | `parse_include_domain_file_not_a_map`, `parse_include_domain_file_empty`, `parse_include_domain_file_missing_name` +6 |
| 373 | root.storages and root.filesystems are mutually exclusive | `parse_root_storages_and_filesystems`, `parse_filesystems_alias_ok` |
| 441 | (see source) | `parse_missing_entity_source`, `parse_entity_without_name`, `parse_include_entity_file_error_names_file` +2 |
| 474 | {prefix}: {error} | `parse_include_source_ref_error_names_files`, `parse_domains_defaults_inherit_error_prefix` |
| 650 | domains.defaults requires root.version >= "0.3" | `parse_domains_defaults_requires_0_3`, `parse_domains_defaults_merge_ok`, `parse_domains_defaults_entity_overrides` +1 |
| 661 | missing required field domains.incoming_dir | `parse_domains_missing_incoming_dir_before_0_3`, `parse_domains_incoming_dir_optional_from_0_3` |
| 673 | root.sources requires root.version >= "0.3" | `parse_sources_requires_0_3` |
| 688 | duplicate source name {name} | `parse_sources_duplicate_name` |
| 713 | source.ref={name} is not a declared source (available: {}) | `parse_sources_ref_ok`, `parse_sources_ref_resource_override`, `parse_sources_ref_undeclared` +1 |
| 793 | unsupported value at {ctx}: {value} (allowed: none, archive, file, row) | `parse_incremental_mode_unsupported`, `parse_incremental_mode_case_insensitive` |
| 813 | source.storage and source.storage are mutually exclusive | `parse_source_storage_and_filesystem` |
| 930 | {ctx}.storage and {ctx}.storage are mutually exclusive | `parse_sink_accepted_storage_and_filesystem`, `parse_sink_rejected_storage_and_filesystem` |
| 982 | {ctx}.duckdb is only valid when format is "duckdb" (found format={format:?}) | `parse_sink_duckdb_block_on_parquet` |
| 1039 | unsupported value at {ctx}: {value} (allowed: overwrite, append, merge_scd1, merge_scd2) | `parse_write_mode_unsupported`, `parse_write_mode_case_insensitive` |
| 1164 | missing required field storages.definitions | `parse_storages_missing_definitions`, `prof_storages_definitions_missing` |
| 1173 | storages.definitions[{index}]: {err} | `parse_storages_definition_wrapper`, `prof_storages_definition_not_map`, `prof_storages_definition_unknown_key` +3 |
| 1223 | missing required field {definitions_context} | `parse_catalogs_missing_definitions`, `prof_catalogs_definitions_missing` |
| 1230 | {definitions_context}[{index}]: {err} | `parse_catalogs_definition_wrapper`, `prof_catalogs_definition_not_map`, `prof_catalogs_definition_unknown_key` +4 |
| 1312 | {context} name={name} has unsupported type={other} (supported: glue, rest, unity) | `parse_catalogs_type_unsupported`, `prof_catalogs_definition_type_unsupported` |
| 1338 | policy.severity={other} is unsupported (allowed: warn, reject, abort) | `parse_policy_severity_unsupported`, `parse_policy_severity_case_sensitive` |
| 1380 | schema.columns[{index}]: {err} | `parse_schema_column_wrapper` |
| 1444 | unsupported value at {ctx}: {value} (allowed: strict, add_columns) | `parse_schema_evolution_mode_unsupported`, `parse_schema_evolution_strict_fail_ok` |
| 1457 | unsupported value at {ctx}: {value} (allowed: fail) | `parse_schema_evolution_on_incompatible_unsupported`, `parse_schema_evolution_strict_fail_ok` |
| 1488 | missing required field {ctx}.{key} | `parse_missing_entities`, `parse_missing_version`, `parse_missing_entity_source` +10 |
| 1547 | expected boolean at {ctx}.{key} | `parse_expected_boolean`, `prof_storages_path_style_not_bool` |
| 1558 | expected positive integer at {ctx}.{key} | `parse_expected_positive_integer`, `prof_lineage_timeout_negative`, `prof_lineage_max_failures_negative` |
| 1565 | expected integer at {ctx}.{key} | `parse_expected_integer`, `prof_lineage_timeout_not_integer` |
| 1575 | value at {ctx}.{key} exceeds maximum allowed value | `parse_integer_exceeds_u32`, `parse_integer_u32_max_ok`, `prof_lineage_max_failures_too_large` |
| 1592 | pii.columns[{index}]: {err} | `parse_pii_column_wrapper` |
| 1615 | pii.columns[name={name}].strategy={other} is unsupported (allowed: hash, drop, nullif... | `parse_pii_strategy_unsupported` |
| glob | (foreign error, no constructor line) | `parse_include_glob_pattern_invalid` |

#### `config/storage.rs`

| line | message | case |
|---|---|---|
| 90 | unsupported config uri: {} | not recorded: CLI input, not config content: remote `-c` URI with an unsupported scheme; the runner always passes a local config.yml |
| 146 | entity.state.path must be a local path (got {}) | not recorded: runtime-only: StorageResolver (run path: state, report and sink path resolution); `floe validate` never calls it |
| 153 | entity.state.path must be absolute when config is remote | not recorded: runtime-only: StorageResolver (run path: state, report and sink path resolution); `floe validate` never calls it |
| 174 | storages.definitions name={} is duplicated | not recorded: runtime-only: StorageResolver (run path: state, report and sink path resolution); `floe validate` never calls it |
| 184 | storages.default is required | not recorded: runtime-only: StorageResolver (run path: state, report and sink path resolution); `floe validate` never calls it |
| 186 | storages.default={} does not match any definition | not recorded: runtime-only: StorageResolver (run path: state, report and sink path resolution); `floe validate` never calls it |
| 221 | entity.name={} {field} references unknown storage {} (no storages block) | not recorded: runtime-only: StorageResolver (run path: state, report and sink path resolution); `floe validate` never calls it |
| 230 | entity.name={} {field} references unknown storage {} | not recorded: runtime-only: StorageResolver (run path: state, report and sink path resolution); `floe validate` never calls it |
| 257 | entity.name={} {field} must be a local path (got {}) | not recorded: runtime-only: StorageResolver (run path: state, report and sink path resolution); `floe validate` never calls it |
| 264 | entity.name={} {field} must be absolute when config is remote | not recorded: runtime-only: StorageResolver (run path: state, report and sink path resolution); `floe validate` never calls it |
| 301 | storage type {} is unsupported | not recorded: runtime-only: StorageResolver (run path: state, report and sink path resolution); `floe validate` never calls it |
| 316 | report.storage references unknown storage {} (no storages block) | not recorded: runtime-only: StorageResolver (run path: state, report and sink path resolution); `floe validate` never calls it |
| 325 | report.storage references unknown storage {} | not recorded: runtime-only: StorageResolver (run path: state, report and sink path resolution); `floe validate` never calls it |
| 352 | report.path must be a local path (got {}) | not recorded: runtime-only: StorageResolver (run path: state, report and sink path resolution); `floe validate` never calls it |
| 359 | report.path must be absolute when config is remote | not recorded: runtime-only: StorageResolver (run path: state, report and sink path resolution); `floe validate` never calls it |
| 395 | storage type {} is unsupported | not recorded: runtime-only: StorageResolver (run path: state, report and sink path resolution); `floe validate` never calls it |
| 506 | storage {} requires bucket for type s3 | not recorded: runtime-only: resolve_s3/adls/gcs_uri (run path); `floe validate` never calls it |
| 513 | storage {} bucket mismatch: {} | not recorded: runtime-only: resolve_s3/adls/gcs_uri (run path); `floe validate` never calls it |
| 528 | storage {} requires account for type adls | not recorded: runtime-only: resolve_s3/adls/gcs_uri (run path); `floe validate` never calls it |
| 534 | storage {} requires container for type adls | not recorded: runtime-only: resolve_s3/adls/gcs_uri (run path); `floe validate` never calls it |
| 541 | storage {} adls account/container mismatch | not recorded: runtime-only: resolve_s3/adls/gcs_uri (run path); `floe validate` never calls it |
| 608 | storage {} requires bucket for type gcs | not recorded: runtime-only: resolve_s3/adls/gcs_uri (run path); `floe validate` never calls it |
| 615 | storage {} bucket mismatch: {} | not recorded: runtime-only: resolve_s3/adls/gcs_uri (run path); `floe validate` never calls it |

#### `config/template.rs`

| line | message | case |
|---|---|---|
| 32 | duplicate domain name {} | `parse_domains_duplicate_name` |
| 66 | entity.name={} references unknown domain {} | `parse_entity_unknown_domain` |
| 162 | env file {} is empty | `parse_env_file_ok`, `parse_env_file_empty`, `parse_env_vars_override_file` |
| 190 | {}{} missing closing '}}' | `parse_template_missing_closing` |
| 198 | {}{} empty placeholder | `parse_template_empty_placeholder` |
| 206 | {}{} references unknown variable {} | `parse_env_vars_ok`, `parse_template_unknown_variable`, `parse_template_entity_name_ok` +16 |
| load_env_file | (foreign error, no constructor line) | `parse_env_file_missing` |

#### `config/types.rs`

| line | message | case |
|---|---|---|
| 146 | entity {selector} is ambiguous; use one of: {} | not recorded: `floe validate --entities` flag: the conformance runner passes only -c/--profile (CLI-flag cases) |
| 156 | entities not found: {} | not recorded: `floe validate --entities` flag: the conformance runner passes only -c/--profile (CLI-flag cases) |
| 309 | separator must be a single byte, got {value:?} | not recorded: run-time only (CSV reader); #584 S3 (lead, D7) -> `div_S3` |
| 324 | unsupported encoding: {} | not recorded: run-time only (CSV reader); #584 S4 (lead, D7) -> `div_S4` |
| 699 | unsupported column type: {value} | not recorded: run-time only (cast); #584 S1 (lead, D7) -> `div_S1` |

#### `config/validate.rs`

| line | message | case |
|---|---|---|
| 40 | root.version={raw} is invalid; expected numeric major.minor format like "0.1" | `val1_version_no_dot` |
| 46 | root.version={raw} is invalid; expected numeric major.minor format like "0.1" | `val1_version_extra_dot` |
| 53 | root.version={raw} is invalid; expected numeric major.minor format like "0.1" | `val1_version_major_not_numeric` |
| 58 | root.version={raw} is invalid; expected numeric major.minor format like "0.1" | `val1_version_minor_not_numeric` |
| 76 | entities list is empty (at least one entity is required) | `val1_entities_empty` |
| 102 | entity.name={} entity.{field} must be non-empty without '.', '/' or '\' when root.ver... | `val1_id_name_with_dot`, `val1_id_name_with_slash`, `val1_id_domain_with_backslash` +1 |
| 111 | entity.name={} is duplicated in config (entities[{first}] and entities[{index}]) | `val1_entity_duplicate_name` |
| 124 | lineage.url must not be empty | `val1_lineage_url_blank` |
| 127 | lineage.namespace must not be empty | `val1_lineage_namespace_blank` |
| 131 | lineage.max_failures must be at least 1 | `val1_lineage_max_failures_zero` |
| 140 | root.version={} is unsupported (minimum supported version: {}.{}) | `val1_version_below_minimum` |
| 156 | report.path must be a local path (got {}) | `val1_report_remote_path_on_local` |
| 186 | (see source) | `val1_pii_abort_severity` |
| 196 | (see source) | `val1_pii_with_archive` |
| 207 | (see source) | `val1_pii_mismatch_missing_reject_file` |
| 216 | (see source) | `val1_pii_mismatch_extra_reject_file` |
| 273 | entity.name={} pii.columns: column name {} is duplicated | `val1_pii_duplicate_column` |
| 280 | entity.name={} pii.columns: {} is not an unknown schema column | `val1_pii_unknown_column` |
| 287 | entity.name={} pii.columns[name={}].strategy=tokenize is not yet supported | `val1_pii_tokenize` |
| 302 | (see source) | `val1_pii_hash_on_number`, `val1_pii_mask_on_number` |
| 313 | entity.name={} pii.columns[name={}].strategy=nullify cannot be applied to a nullable=... | `val1_pii_nullify_on_not_nullable` |
| 321 | entity.name={} pii.columns[name={}].strategy=drop cannot be applied to a primary_key ... | `val1_pii_drop_on_primary_key` |
| 327 | (see source) | `val1_d2_pii_on_primary_key_with_merge` (D2) |
| 338 | (see source) | `val1_pii_redact_on_unique_key`, `val1_pii_redact_on_primary_key` |
| 352 | (see source) | `val1_pii_hash_on_unique_key`, `val1_pii_hash_on_legacy_unique_column` |
| 371 | (see source) | `val1_pii_drop_on_iceberg` |
| 380 | entity.name={} pii.columns[name={}].strategy=mask requires mask_pattern | `val1_pii_mask_without_pattern` |
| 388 | entity.name={} pii.columns[name={}].mask_pattern must contain at least one token ({{f... | `val1_pii_mask_pattern_without_token`, `val1_pii_mask_pattern_zero_width_token` |
| 420 | entity.name={} entity.state.path must not be empty | `val1_state_path_blank` |
| 429 | entity.name={} incremental_mode=archive requires sink.archive | `val1_incremental_archive_without_sink_archive` |
| 449 | entity.name={} sink.archive conflicts with incremental_mode={}; use incremental_mode=... | `val1_archive_conflicts_with_file_mode`, `val1_archive_conflicts_with_row_mode` |
| 467 | entity.name={} source.cast_mode={} is unsupported (allowed: {}) | `val1_source_cast_mode_unknown` |
| 488 | entity.name={} source.options.json_mode={} is unsupported (allowed: array, ndjson) | `val1_source_json_mode_unknown` |
| 500 | entity.name={} source.options.sheet must not be empty | not recorded: D1: xlsx source (source.options.sheet) -> `d1_source_xlsx` |
| 509 | entity.name={} source.options.header_row must be greater than 0 | not recorded: D1: xlsx source (header_row) -> `d1_source_xlsx` |
| 517 | entity.name={} source.options.data_row must be greater than source.options.header_row | not recorded: D1: xlsx source (data_row vs header_row) -> `d1_source_xlsx` |
| 530 | entity.name={} source.options.row_tag is required for xml input | not recorded: no case yet |
| 541 | entity.name={} source.options.namespace must not be empty | not recorded: no case yet |
| 553 | entity.name={} source.options.value_tag must not be empty | not recorded: no case yet |
| 580 | entity.name={} sink.rejected is required when policy.severity=reject | `val1_reject_severity_without_rejected_sink` |
| 611 | entity.name={} sink.accepted.format={} is not supported on {} storage (supported: {}) | not recorded: D1: duckdb sink (the only sink that restricts storage; parquet and iceberg accept local, s3, gcs and adls) -> `d1_sink_duckdb`, `d1_storage_adls`, `d1_storage_gcs` |
| 637 | entity.name={} sink.archive.storage must match source.storage ({}) | `val1_archive_storage_differs_from_source` |
| 657 | entity.name={} sink.write_mode={} is not supported by sink.accepted.format={} | `val1_d2_pii_on_primary_key_with_merge` (D2), `val1_write_mode_merge_on_parquet`, `val1_d2_iceberg_merge_scd1` (D2) +1 |
| 674 | entity.name={} sink.write_mode={} requires schema.primary_key | `val1_d2_iceberg_merge_without_primary_key` (D2) |
| 680 | entity.name={} sink.write_mode={} requires non-empty schema.primary_key | `val1_d2_iceberg_merge_with_empty_primary_key` (D2) |
| 719 | entity.name={} sink.accepted.format=duckdb requires a sink.accepted.duckdb block | not recorded: D1: duckdb sink -> `d1_sink_duckdb` |
| 725 | entity.name={} sink.accepted.duckdb.table must not be empty | not recorded: D1: duckdb sink -> `d1_sink_duckdb` |
| 738 | (see source) | not recorded: D1: duckdb sink -> `d1_sink_duckdb` |
| 747 | (see source) | not recorded: D1: duckdb sink -> `d1_sink_duckdb` |
| 755 | (see source) | not recorded: D1: duckdb sink -> `d1_sink_duckdb` |
| 767 | (see source) | not recorded: D1: duckdb sink -> `d1_sink_duckdb` |
| 801 | entity.name={} sink.accepted.merge is only supported when sink.accepted.format suppor... | `val2_merge_on_parquet` |
| 811 | entity.name={} sink.accepted.merge is only supported with sink.write_mode=merge_scd1 ... | `val2_d2_merge_block_overwrite` (D2) |
| 869 | entity.name={} sink.accepted.merge.ignore_columns[{}]={} cannot reference schema.prim... | `val2_d2_ignore_pk` (D2) |
| 887 | entity.name={} sink.accepted.merge.compare_columns[{}]={} cannot reference schema.pri... | `val2_d2_compare_pk` (D2) |
| 897 | entity.name={} sink.accepted.merge.scd2 is only supported with sink.write_mode=merge_... | `val2_d2_scd2_with_scd1` (D2) |
| 924 | entity.name={} sink.accepted.merge.scd2.{} must not be empty | `val2_d2_scd2_column_empty` (D2) |
| 931 | entity.name={} sink.accepted.merge.scd2.{}={} collides with schema column name | `val2_d2_scd2_column_collides` (D2) |
| 942 | entity.name={} sink.accepted.merge.scd2 column names must be unique | `val2_d2_scd2_columns_not_unique` (D2) |
| 963 | entity.name={} {}[{}] must not be empty | `val2_d2_merge_list_entry_empty` (D2) |
| 970 | entity.name={} {}[{}]={} references unknown schema column | `val2_d2_merge_list_unknown_column` (D2) |
| 977 | entity.name={} {} has duplicate column {} | `val2_d2_merge_list_duplicate` (D2) |
| 996 | entity.name={} sink.accepted.iceberg is only supported for sink.accepted.format=iceberg | `val2_iceberg_block_on_parquet` |
| 1013 | entity.name={} sink.accepted.iceberg.catalog is required (or set catalogs.default) | `val2_iceberg_no_catalog` |
| 1021 | entity.name={} sink.accepted.iceberg.catalog references unknown catalog {} | `val2_iceberg_unknown_catalog` |
| 1028 | entity.name={} sink.accepted.iceberg.catalog={} has type=unity: unity catalogs only s... | not recorded: D1: unity catalog -> `d1_catalog_unity` |
| 1044 | entity.name={} sink.accepted.iceberg.catalog requires sink.accepted storage type s3 o... | `val2_iceberg_local_storage` |
| 1052 | catalogs.definitions name={} warehouse_storage references unknown storage {} | not recorded: unreachable: CatalogRegistry::new rejects the same unknown warehouse_storage first (validate.rs:1605) |
| 1059 | catalogs.definitions name={} warehouse_storage must reference s3 storage for glue cat... | not recorded: D1: glue catalog -> `d1_catalog_glue` |
| 1065 | catalogs.definitions name={} warehouse_storage must reference s3 or gcs storage for r... | not recorded: unreachable: CatalogRegistry::new rejects the same non-s3/gcs warehouse_storage first (validate.rs:1612) |
| 1086 | entity.name={} sink.accepted.delta is only supported for sink.accepted.format=delta | not recorded: D1: delta sink block / unity catalog binding -> `d1_sink_delta`, `d1_catalog_unity` |
| 1103 | entity.name={} sink.accepted.delta.catalog is required when no catalogs.default is set | not recorded: D1: delta sink block / unity catalog binding -> `d1_sink_delta`, `d1_catalog_unity` |
| 1112 | entity.name={} sink.accepted.delta.catalog references unknown catalog {} | not recorded: D1: delta sink block / unity catalog binding -> `d1_sink_delta`, `d1_catalog_unity` |
| 1119 | entity.name={} sink.accepted.delta.catalog={} has type={}: only unity catalogs suppor... | not recorded: D1: delta sink block / unity catalog binding -> `d1_sink_delta`, `d1_catalog_unity` |
| 1130 | entity.name={} sink.accepted.delta.catalog={} requires cloud storage (s3, gcs, or adl... | not recorded: D1: delta sink block / unity catalog binding -> `d1_sink_delta`, `d1_catalog_unity` |
| 1149 | entity.name={} sink.accepted.partition_by and sink.accepted.partition_spec are mutual... | `val2_part_both` |
| 1157 | entity.name={} sink.accepted.partition_by is only supported for sink.accepted.format=... | `val2_part_by_on_parquet` |
| 1163 | entity.name={} sink.accepted.partition_by must not be empty | not recorded: D1: delta partition_by (only valid for format=delta) -> `d1_sink_delta` |
| 1173 | entity.name={} sink.accepted.partition_by[{}] must not be empty | not recorded: D1: delta partition_by (only valid for format=delta) -> `d1_sink_delta` |
| 1180 | entity.name={} sink.accepted.partition_by has duplicate column {} | not recorded: D1: delta partition_by (only valid for format=delta) -> `d1_sink_delta` |
| 1187 | entity.name={} sink.accepted.partition_by[{}]={} references unknown schema column | not recorded: D1: delta partition_by (only valid for format=delta) -> `d1_sink_delta` |
| 1197 | entity.name={} sink.accepted.partition_spec is only supported for sink.accepted.forma... | `val2_part_spec_on_parquet` |
| 1203 | entity.name={} sink.accepted.partition_spec must not be empty | `val2_part_spec_empty` |
| 1213 | entity.name={} sink.accepted.partition_spec[{}].column must not be empty | `val2_part_spec_column_empty` |
| 1220 | entity.name={} sink.accepted.partition_spec[{}].column={} references unknown schema c... | `val2_part_spec_column_unknown` |
| 1227 | entity.name={} sink.accepted.partition_spec[{}].transform={} is unsupported (allowed:... | `val2_part_spec_transform_unsupported` |
| 1236 | entity.name={} sink.accepted.partition_spec has duplicate entry ({}, {}) | `div_S13`, `val2_part_spec_duplicate` |
| 1250 | entity.name={} schema.columns has {} entries which exceeds the JSON selector limit of {} | `val2_json_columns_limit` |
| 1260 | entity.name={} schema.columns[{}].width is required for source.format=fixed | not recorded: D1: fixed-width source -> `d1_source_fixed` |
| 1266 | entity.name={} schema.columns[{}].width must be greater than 0 | not recorded: D1: fixed-width source -> `d1_source_fixed` |
| 1277 | entity.name={} schema.normalize_columns.strategy={} is unsupported (allowed: {}) | `val2_normalize_strategy_unsupported` |
| 1290 | entity.name={} schema.mismatch.missing_columns={} is unsupported (allowed: {}) | `val2_mismatch_missing_unsupported` |
| 1300 | entity.name={} schema.mismatch.extra_columns={} is unsupported (allowed: {}) | `val2_mismatch_extra_unsupported` |
| 1313 | entity.name={} schema.columns[{}].type={} is unsupported for column {} (allowed: {}) | `div_S1` (D7), `val2_type_unsupported`, `val2_type_time` (D10) |
| 1325 | entity.name={} schema.columns[{}].source={} is invalid: {} | `div_S14a`, `div_S16a`, `val2_jsel_empty` +10 |
| 1335 | entity.name={} schema.columns[{}].source={} is invalid: {} | `div_S14b`, `div_S16b` |
| 1360 | entity.name={} schema.schema_evolution requires root.version >= "0.2" | `val2_evolution_old_version` |
| 1370 | entity.name={} schema.schema_evolution.mode={} requires sink.accepted.format=delta (D... | `val2_evolution_add_columns_not_delta` |
| 1385 | entity.name={} schema.primary_key must not be empty | `val2_pk_empty` |
| 1395 | entity.name={} schema.primary_key[{}] must not be empty | `val2_pk_entry_empty` |
| 1402 | entity.name={} schema.primary_key has duplicate column {} | `div_S8`, `val2_pk_duplicate`, `val2_pk_duplicate_after_trim` |
| 1414 | entity.name={} schema.primary_key[{}]={} references unknown schema column | `val2_pk_unknown_column` |
| 1420 | entity.name={} schema.primary_key column {} cannot set nullable=true | `val2_pk_nullable_true` |
| 1435 | entity.name={} schema.unique_keys must not be empty | `val2_uk_empty` |
| 1470 | entity.name={} schema.unique_keys[{}] must not be empty | `val2_uk_group_empty` |
| 1481 | entity.name={} schema.unique_keys[{}][{}] must not be empty | `val2_uk_entry_empty` |
| 1488 | entity.name={} schema.unique_keys[{}][{}]={} references unknown schema column | `val2_uk_unknown_column` |
| 1495 | entity.name={} schema.unique_keys[{}] has duplicate column {} | `div_S9`, `val2_uk_duplicate_column` |
| 1575 | catalogs.definitions must not be empty | `val2_catalogs_empty` |
| 1587 | catalogs.definitions name={} warehouse_storage references unknown storage {} | not recorded: D1: glue catalog -> `d1_catalog_glue` |
| 1594 | catalogs.definitions name={} warehouse_storage must reference s3 storage for glue cat... | not recorded: D1: glue catalog -> `d1_catalog_glue` |
| 1605 | catalogs.definitions name={} warehouse_storage references unknown storage {} | `val2_catalog_rest_wh_storage_unknown` |
| 1612 | catalogs.definitions name={} warehouse_storage must reference s3 or gcs storage for r... | `val2_catalog_rest_wh_storage_local` |
| 1626 | catalogs.definitions name={} is duplicated | `val2_catalog_duplicate` |
| 1636 | catalogs.default={} does not match any definition | `val2_catalog_default_unknown` |
| 1675 | storages.definitions must not be empty | `val2_storages_empty` |
| 1682 | storages.definitions name={} type={} is unsupported (allowed: {}) | `val2_storage_type_unsupported` |
| 1692 | storages.definitions name={} requires bucket for type s3 | `val2_storage_s3_no_bucket` |
| 1699 | storages.definitions name={} requires region for type s3 | `val2_storage_s3_no_region` |
| 1708 | storages.definitions name={} requires account for type adls | not recorded: D1: adls storage -> `d1_storage_adls` |
| 1715 | storages.definitions name={} requires container for type adls | not recorded: D1: adls storage -> `d1_storage_adls` |
| 1723 | storages.definitions name={} requires bucket for type gcs | not recorded: D1: gcs storage -> `d1_storage_gcs` |
| 1733 | storages.definitions name={} is duplicated | `val2_storage_duplicate` |
| 1743 | storages.default={} does not match any definition | `val2_storage_default_unknown` |
| 1750 | storages.default is required when storages is set | `val2_storage_default_missing` |
| 1774 | entity.name={} {field} requires storages.default | not recorded: unreachable: StorageRegistry::new rejects a missing storages.default first (validate.rs:1750) |
| 1787 | entity.name={} {field} references unknown storage {} (no storages block) | `val2_ref_source_unknown_no_block`, `val2_ref_accepted_unknown_no_block` |
| 1797 | entity.name={} {field} references unknown storage {} | `val2_ref_source_unknown`, `val2_ref_accepted_unknown`, `val2_ref_rejected_unknown` |
| 1824 | report.storage requires storages.default | not recorded: unreachable: StorageRegistry::new rejects a missing storages.default first (validate.rs:1750) |
| 1834 | {field} references unknown storage {} (no storages block) | `val2_ref_report_unknown_no_block` |
| 1844 | {field} references unknown storage {} | `val2_ref_report_unknown` |

#### `config/yaml_decode.rs`

| line | message | case |
|---|---|---|
| 18 | expected map at {ctx} | `parse_yaml_root_is_list`, `parse_yaml_root_is_scalar`, `parse_yaml_expected_map` +24 |
| 25 | expected array at {ctx} | `parse_yaml_expected_array`, `parse_include_domains_not_array`, `parse_domains_not_array` +6 |
| 32 | expected string at {ctx} | `parse_yaml_version_is_float`, `parse_yaml_name_is_int`, `parse_yaml_required_null` +29 |
| 70 | expected string key at {ctx} | `parse_yaml_non_string_key`, `prof_file_non_string_key` |
| 73 | unknown field {ctx}.{key} | `parse_yaml_unknown_root_field`, `parse_yaml_unknown_nested_field`, `parse_sink_rejected_rejects_accepted_only_keys` +18 |
| load_yaml | (foreign error, no constructor line) | `parse_yaml_syntax_error`, `parse_yaml_tab_indent` (D11), `parse_yaml_duplicate_key` +2 |

#### `io/format.rs`

| line | message | case |
|---|---|---|
| 35 | (see source) | not recorded: struct field, not an error site |
| 409 | (see source) | `val1_source_format_unknown` |
| 415 | entity.name={} {}={} is unsupported | `val1_source_format_unknown`, `val1_sink_accepted_format_unknown`, `val1_sink_rejected_format_parquet` |
| 422 | unsupported {}: {format} | not recorded: unreachable on the validate path: ensure_* discard the adapter error and re-raise the entity-named one (415) |
| 427 | (see source) | `val1_source_format_unknown` |
| 441 | (see source) | `val1_sink_accepted_format_unknown` |
| 452 | (see source) | `val1_sink_rejected_format_parquet` |
| 519 | entity.name={} sink.accepted.options.compression={} is unsupported (allowed: snappy, ... | `val1_sink_options_compression_unknown` |
| 528 | entity.name={} sink.accepted.options.row_group_size must be greater than 0 | `val1_sink_options_row_group_size_zero` |
| 537 | entity.name={} sink.accepted.options.max_size_per_file must be greater than 0 | `val1_sink_options_max_size_per_file_zero` |
| 558 | (see source) | not recorded: unreachable on the validate path: input_adapter's error is discarded by ensure_input_format |
| 569 | (see source) | not recorded: unreachable on the validate path: rejected_sink_adapter's error is discarded by ensure_rejected_sink_format |
| 655 | input column {} not found: {err} | not recorded: run-time (typed DataFrame building), not part of validate |
| 664 | failed to cast input column {}: {err} | not recorded: run-time (typed DataFrame building), not part of validate |
| 673 | failed to build typed dataframe: {err} | not recorded: run-time (typed DataFrame building), not part of validate |
| 678 | failed to read boolean column {} as string: {err} | not recorded: run-time (typed DataFrame building), not part of validate |
| 705 | input column {} not found: {err} | not recorded: run-time (typed DataFrame building), not part of validate |
| 709 | failed to cast input column {}: {err} | not recorded: run-time (typed DataFrame building), not part of validate |
| 712 | input column {} not found for update | not recorded: run-time (typed DataFrame building), not part of validate |
| 715 | failed to update input column {}: {err} | not recorded: run-time (typed DataFrame building), not part of validate |

#### `io/read/json_selector.rs`

| line | message | case |
|---|---|---|
| 19 | (see source) | not recorded: struct field declaration, not an error site |
| 34 | selector is empty | `val2_jsel_empty`, `val2_jsel_blank` |
| 50 | selector contains empty field | `val2_jsel_empty_field`, `val2_jsel_leading_dot`, `val2_jsel_name_used_when_no_source` |
| 71 | selector index is empty | `val2_jsel_index_empty`, `val2_jsel_index_unclosed` |
| 76 | selector index is not numeric: {} | `val2_jsel_index_not_numeric`, `val2_jsel_index_negative` |
| 80 | selector index is invalid: {} | `val2_jsel_index_overflow` |
| 87 | selector contains unexpected ']' | `val2_jsel_unexpected_bracket` |
| 108 | selector has no tokens | not recorded: runtime-only: evaluate_selector; `floe validate` never calls it |
| 158 | failed to serialize json value: {err} | not recorded: runtime-only: compact_json; `floe validate` never calls it |
| 163 | (see source) | not recorded: runtime-only: selector_tokens_or_error (reader); `floe validate` never calls it |

#### `profile/parse.rs`

| line | message | case |
|---|---|---|
| 22 | profile YAML is empty | `prof_file_empty`, `prof_file_only_comment` |
| 25 | profile YAML contains multiple documents; expected one | `prof_file_two_documents` |
| 37 | YAML parse error: {e} | not recorded: unreachable: parse_profile_from_str is a test-only API, the CLI uses parse_profile |
| 39 | profile YAML is empty | not recorded: unreachable: parse_profile_from_str is a test-only API, the CLI uses parse_profile |
| 42 | profile YAML contains multiple documents; expected one | not recorded: unreachable: parse_profile_from_str is a test-only API, the CLI uses parse_profile |
| 70 | profile.apiVersion: expected "{PROFILE_API_VERSION}", got "{api_version}" | `prof_api_version_wrong` |
| 78 | profile.kind: expected "{PROFILE_KIND}", got "{kind}" | `prof_kind_wrong` |
| 85 | profile.metadata is required | `prof_metadata_missing` |
| 171 | profile.execution.runner is required | `prof_execution_runner_missing` |
| 200 | profile.execution.orchestration.max_concurrent_entities: must be >= 1 | `prof_orch_max_zero` |
| 210 | profile.execution.orchestration.strategy: expected "sequential" or "parallel", got "{s}" | `prof_orch_strategy_invalid`, `prof_orch_strategy_case_sensitive` |
| 387 | profile.validation.strict must be a boolean | `prof_validation_strict_not_bool`, `prof_validation_strict_int` |
| 400 | {ctx}.{key} is required | `prof_api_version_missing`, `prof_kind_missing`, `prof_metadata_name_missing` +4 |
| 430 | {ctx}.{key} must be a non-negative integer | `div_S32`, `prof_orch_max_negative`, `prof_orch_max_float` +11 |

#### `profile/validate.rs`

| line | message | case |
|---|---|---|
| 19 | profile.metadata.name must not be empty | `div_S29`, `prof_metadata_name_empty` |
| 45 | profile.catalogs.definitions must not be empty | `prof_catalogs_definitions_empty` |
| 54 | profile.catalogs.definitions.name must not be empty | `div_S30`, `prof_catalogs_definition_name_empty` |
| 60 | profile.catalogs.definitions name={} is duplicated | `prof_catalogs_definition_duplicate` |
| 70 | profile.catalogs.default={} does not match any definition | `prof_catalogs_default_unknown` |
| 83 | profile.storages.definitions must not be empty | `prof_storages_definitions_empty` |
| 92 | profile.storages.definitions.name must not be empty | `prof_storages_definition_name_empty` |
| 98 | profile.storages.definitions name={} is duplicated | `prof_storages_definition_duplicate` |
| 105 | profile.storages.definitions name={} type={} is unsupported (allowed: {}) | `prof_storages_type_unsupported` |
| 115 | profile.storages.definitions name={} requires bucket for type s3 | `prof_storages_s3_bucket_missing` |
| 122 | profile.storages.definitions name={} requires region for type s3 | `prof_storages_s3_region_missing` |
| 131 | profile.storages.definitions name={} requires account for type adls | not recorded: D1: adls storage -> `d1_storage_adls` |
| 138 | profile.storages.definitions name={} requires container for type adls | not recorded: D1: adls storage -> `d1_storage_adls` |
| 146 | profile.storages.definitions name={} requires bucket for type gcs | not recorded: D1: gcs storage -> `d1_storage_gcs` |
| 154 | profile.storages.default is required when storages is set | `prof_storages_default_missing` |
| 160 | profile.storages.default={default_name} does not match any definition | `prof_storages_default_unknown` |
| 170 | profile.lineage.url must not be empty | `prof_lineage_url_empty`, `prof_lineage_url_blank` |
| 174 | profile.lineage.namespace must not be empty | `prof_lineage_namespace_empty` |
| 178 | profile.lineage.max_failures must be at least 1 | `prof_lineage_max_failures_zero` |
| 264 | profile variable "{key}" contains malformed placeholder: {placeholder} | `div_S31a`, `div_S31b`, `prof_vars_malformed_second_placeholder` +1 |
| 276 | profile variable "{key}" contains unresolved placeholder: {placeholder} | not recorded: unreachable: validate_merged_vars is a library API, the CLI never calls it |
| 288 | ,  | `prof_runner_type_unknown`, `prof_runner_type_case_sensitive` |
| 307 | profile.execution.runner.image is required for kubernetes_job | `div_S27`, `prof_k8s_image_missing`, `prof_k8s_image_blank` +1 |
| 319 | profile.execution.runner.namespace is required for kubernetes_job | `prof_k8s_namespace_missing`, `prof_k8s_namespace_blank` |
| 336 | profile.execution.runner.workspace_url is required for databricks_job | `div_S28`, `prof_dbx_workspace_url_missing`, `prof_dbx_workspace_url_blank` +1 |
| 348 | profile.execution.runner.existing_cluster_id is required for databricks_job | `prof_dbx_existing_cluster_id_missing`, `prof_dbx_existing_cluster_id_blank` |
| 361 | profile.execution.runner.config_uri is required for databricks_job | `prof_dbx_config_uri_missing`, `prof_dbx_config_uri_blank` |
| 373 | profile.execution.runner.python_file_uri is required for databricks_job | `prof_dbx_python_file_uri_missing`, `prof_dbx_python_file_uri_blank` |
| 386 | profile.execution.runner.auth.service_principal_oauth_ref is required for databricks_job | `prof_dbx_auth_missing`, `prof_dbx_auth_ref_blank`, `prof_dbx_auth_empty_map` |

#### `vars/resolve.rs`

| line | message | case |
|---|---|---|
| 98 | ${{{key}}}" | not recorded: nondeterministic: variable expansion depth error names the key it started from (HashMap order) |
| 108 | circular variable reference detected: {} -> {} | `prof_vars_self_cycle` |
| 117 | variable "${{{key}}}" is referenced but not defined | `prof_vars_unknown_reference`, `prof_vars_config_env_unknown_reference` |
| 148 | variable "{owner_key}": unclosed placeholder in value {value:?} | `prof_vars_config_env_unclosed` |
| 155 | variable "{owner_key}": empty placeholder ${{}} | `prof_vars_config_env_empty_placeholder` |
