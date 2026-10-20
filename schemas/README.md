# Floe YAML schemas

Select the schema for the document you are editing:

| Document | Schema |
|---|---|
| Full inline config or project root | `project.schema.yaml` |
| `sources.yml` containing named sources | `sources.schema.yaml` |
| One domain document, such as `_domain.yml` | `domains.schema.yaml` |
| One included sibling entity | `entity.schema.yaml` |
| Environment profile | `profile.schema.yaml` |

Each schema is self-contained: its `$ref` values point to its own `$defs`.
There is no shared config schema or version directory to copy alongside it.

Config fields follow `crates/floe-core/src/config/types.rs`. The YAML parser in
`parse.rs` supplies the wire names and authoring-only fields: `ref`, named
`sources`, domain `defaults`, includes, and the legacy `filesystem` alias.
Resolved fields such as entity IDs, config file lists and resolved incoming
paths are not YAML inputs. The regression suite checks the model field coverage.

The project schema supports legacy full configs and config 0.3 project roots.
An inline entity can inherit from its declared domain. Included entity files
receive their domain from assembly and supply their own name and schema columns.
Domain defaults and named sources can be partial, because Floe merges them before
parsing complete entities. Domain source defaults cannot declare `ref` or
`resource`: reference resolution happens before domain defaults are merged.

Schemas check local structure and expressible constraints on complete values.
Run `floe validate -c config.yml` to resolve includes and validate inherited
values, names, column references and storage/catalog relationships.

JSON Schema defines `integer` by numeric value, so a generic JSON Schema validator
may accept a YAML real scalar such as `1.0`. Floe's YAML parser accepts integer
scalars such as `1` for integer fields and rejects real scalars; the regression
suite uses a YAML-aware integer checker for this distinction, and `floe validate`
is the authoring check for a project's actual YAML files.

For the VS Code YAML extension, example schema associations are:

```json
{
  "yaml.schemas": {
    "./schemas/project.schema.yaml": ["/example/project/config.yml", "/example/standalone/config.yml"],
    "./schemas/sources.schema.yaml": ["/example/project/sources.yml"],
    "./schemas/domains.schema.yaml": ["/example/project/domains/*/_domain.yml"],
    "./schemas/entity.schema.yaml": ["/example/project/domains/*/accounts.yml"]
  }
}
```

Adapt the document paths to your project. Run
`python3 scripts/test_config_schema.py` to check all four schemas, their internal
references, Rust model fields and checked-in project examples. CI runs these
checks when schemas or example configurations change.
