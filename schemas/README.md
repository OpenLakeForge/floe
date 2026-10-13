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

## Config 0.3 files

The split schemas reference the shared definitions in `config.schema.yaml`.
Associate each YAML file with its document schema in your editor:

| YAML document | Schema |
|---|---|
| Full inline config | `schemas/config.schema.yaml` |
| Project root | `schemas/0.3/project.schema.yaml` |
| `sources.yml` | `schemas/0.3/sources.schema.yaml` |
| `_domain.yml` | `schemas/0.3/domains.schema.yaml` |
| Sibling entity file | `schemas/0.3/entity.schema.yaml` |

For the VS Code YAML extension, a project's `.vscode/settings.json` can use:

```json
{
  "yaml.schemas": {
    "./schemas/0.3/project.schema.yaml": ["/example/project/config.yml"],
    "./schemas/0.3/sources.schema.yaml": ["/example/project/sources.yml"],
    "./schemas/0.3/domains.schema.yaml": ["/example/project/domains/*/_domain.yml"],
    "./schemas/0.3/entity.schema.yaml": ["/example/project/domains/*/accounts.yml"]
  }
}
```

Adapt the file patterns to your project. Fragment schemas describe local fields;
they do not turn a fragment into a standalone runnable config. Keep the schema
directory layout when copying these files so their relative references resolve.
