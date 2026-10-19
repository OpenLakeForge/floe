# Variable Resolution

Floe supports a two-source variable system that lets you inject
environment-specific values into config files without editing the config
itself.

---

## Sources and precedence

Variables are collected from two sources and merged before any
placeholder substitution occurs.  When the same key appears in more than
one source the **highest-precedence** value wins:

```
config variables  >  profile variables
    (highest)              (lowest)
```

| Source | Where defined | Precedence |
|--------|---------------|-----------|
| **Config variables** | `env.vars:` block in the main Floe YAML config | Highest |
| **Profile variables** | `variables:` block in the environment profile | Lowest |

---

## Placeholder syntax

### In variable *values* — `${VAR}`

Variable values may themselves reference other variables using the
`${VAR}` syntax:

```yaml
# profiles/prod.yml
variables:
  BASE_PATH: /data/prod
  INCOMING:  ${BASE_PATH}/incoming   # expands to /data/prod/incoming
  OUTPUT:    ${BASE_PATH}/output     # expands to /data/prod/output
```

References are resolved recursively — a value may reference a variable
whose value also contains `${…}` placeholders — up to the built-in depth
limit (32 levels).

### In config fields — `{{VAR}}`

Once all variables are fully resolved, Floe substitutes them into the
main config using the `{{VAR}}` (double-brace) syntax:

```yaml
# config.yml
entities:
  - name: customers
    source:
      format: csv
      path: "{{INCOMING}}/customers.csv"
    sink:
      accepted:
        format: parquet
        path: "{{OUTPUT}}/customers.parquet"
```

---

## Resolution rules

1. **Merge** – build a single map from both sources, higher-priority
   sources overwriting lower ones.
2. **Expand** – for every variable value, substitute `${KEY}` with the
   fully-resolved value of `KEY`, recursively.
3. **Fail-fast** – any placeholder that cannot be resolved or forms a cycle
   produces an immediate, actionable error before the run begins.

---

## Fail-fast errors

| Situation | Error (example) |
|-----------|-----------------|
| Reference to undefined variable | `variable "${MISSING}" is referenced but not defined` |
| Unclosed placeholder | `variable "PATH": unclosed placeholder in value "/data/${UNCLOSED"` |
| Empty placeholder | `variable "VAR": empty placeholder ${}` |
| Circular reference | `circular variable reference detected: A -> B -> A` |
| Depth limit exceeded | `variable expansion exceeded maximum depth (32)` |

---

## Examples

### Simple value override

```yaml
# profiles/dev.yml
variables:
  ENV:     dev
  CATALOG: dev_catalog

# profiles/prod.yml
variables:
  ENV:     prod
  CATALOG: prod_catalog
```

### Nested references

```yaml
variables:
  ROOT:     /mnt/data
  ENV:      prod
  BASE:     ${ROOT}/${ENV}       # → /mnt/data/prod
  INCOMING: ${BASE}/incoming     # → /mnt/data/prod/incoming
  OUTPUT:   ${BASE}/output       # → /mnt/data/prod/output
```

### Cross-source reference

A config variable can reference a value supplied by the profile:

```yaml
# profiles/prod.yml
variables:
  CATALOG: unity_prod

# config.yml
env:
  vars:
    TABLE: "{{CATALOG}}.bronze.customers"
    # CATALOG comes from the profile at runtime
```

---

## API reference (Rust)

```rust
use floe_core::{resolve_vars, VarSources};
use std::collections::HashMap;

let resolved = resolve_vars(VarSources {
    profile: &profile_vars,   // HashMap<String, String>
    cli:     &cli_vars,
    config:  &config_vars,
})?;
// resolved: HashMap<String, String> — fully expanded, no ${…} remaining
```

See also: [`docs/profiles.md`](profiles.md) for the full profile schema.
