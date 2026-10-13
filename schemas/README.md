# Floe YAML schemas

`config.schema.yaml` describes a complete configuration document and holds the
shared definitions for entities, domain defaults and named sources. Its fields
follow the YAML parser in `crates/floe-core/src/config/parse.rs`; resolved fields
such as entity IDs and config file lists do not belong in YAML.

Complete entities require source, sink, policy and schema. An inline entity with
a domain can inherit values from that domain. Included entity documents receive
their domain during assembly and supply their own name and schema columns.
Domain defaults and named sources are partial blocks, merged before final
entity parsing. Domain source defaults cannot declare `ref` or `resource` because
reference resolution happens before defaults are merged.

JSON Schema checks document shape and locally available fields. It cannot load
Floe includes or resolve domain/source/storage names. Run `floe validate -c
config.yml` to validate the assembled configuration and its inherited values.

Run `python3 scripts/test_config_schema.py` to check the schemas and examples.
These checks also run in CI when schemas or example configs change.
