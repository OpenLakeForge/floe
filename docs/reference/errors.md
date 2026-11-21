# Error codes

Floe errors carry a stable code, so callers can match on it instead of on message text.

From 1.0 the error codes and the exception classes (`FloeConfigError`, `FloeIoError`, `FloeRunError`, `FloeStorageError`, all deriving from `FloeError`) are the contract: they are not renamed or removed, and new codes are added in minor releases. Messages are for people and can be reworded in any release. A code is available as `err.code`; the check or reader rule that failed, when there is one, is `err.rule`. A code starts with the area of its exception class: `config_` for `FloeConfigError`, `io_` for `FloeIoError`, `run_` for `FloeRunError`, `storage_` for `FloeStorageError`.

The code shows up as `error_code` in JSON logs, in `run.failure.json`, and as `error_code=<code>` in text logs. The log's own `code` field is separate: it names the kind of log line.

| Code | Raised when |
|---|---|
| `config_invalid_yaml` | a config file is not valid YAML |
| `config_unknown_key` | a config has a key Floe does not know |
| `config_missing_key` | a required config key is missing |
| `config_invalid_value` | a config value has the wrong type or is outside the allowed values |
| `config_not_supported` | a config key or value Floe no longer accepts or does not support |
| `config_unknown_reference` | a config names a storage, catalog, runner, source or variable that is not defined |
| `config_duplicate` | a name or key is defined twice |
| `config_conflict` | two config options exclude each other |
| `config_version` | the config `version` is missing or not supported |
| `io_read_error` | an input file cannot be opened, parsed or read; the reader rule is in `rule` |
| `io_write_error` | a file Floe writes outside a run, such as a generated entity file, cannot be written |
| `io_not_found` | an input file or path does not exist |
| `io_schema_mismatch` | the file's columns do not match what the reader needs |
| `io_invalid_option` | a reader option or selector is invalid |
| `storage_not_found` | a storage object or bucket does not exist |
| `storage_access_denied` | the storage refuses the credentials |
| `storage_conflict` | a storage write failed its precondition, or a lock could not be acquired |
| `storage_error` | any other storage failure |
| `run_state_conflict` | the incremental state kept changing and the retries ran out, or it cannot be reconciled with what the sink committed |
| `run_commit_failed` | a sink commit failed, or kept conflicting with concurrent commits |
| `run_schema_evolution` | a schema change on an existing table is not allowed or failed |
| `run_catalog_error` | the Iceberg catalog request failed |
| `run_merge_key` | the merge or unique keys clash with data already committed |
| `run_aborted` | the run stopped on a policy abort |
| `run_not_supported` | a format, sink or table feature is not supported at run time |
| `run_invalid_input` | an input cannot be processed: for example a path that does not match the partitioning, or a value that cannot be cast |
| `run_invalid_setup` | something the run needs is missing: for example an environment variable, a storage client or a namespace |
| `run_archive_failed` | an input file was copied to the archive but deleting the original failed |
