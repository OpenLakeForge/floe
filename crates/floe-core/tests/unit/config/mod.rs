pub mod add_entity;
pub mod adls_storage;
pub mod adls_validation;
pub mod catalogs;
pub mod config_validation;
pub mod domain_defaults;
// DuckDB config validation is pure-string (the always-compiled `validate_duckdb_sink`
// plus format-agnostic checks) and does not need the bundled `duckdb` crate: a lean
// build still validates and round-trips duckdb configs, only refusing to *write* them.
// So these run in the default suite, not just under the `duckdb` feature.
pub mod duckdb_validation;
pub mod gcs_storage;
pub mod gcs_validation;
pub mod include_domains;
pub mod lineage_validation;
pub mod local_storage;
pub mod location;
pub mod parse;
pub mod pii_validation;
pub mod remote_base;
pub mod sources;
pub mod storage_resolver_uri;
pub mod templating;
