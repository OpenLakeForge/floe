# Floe Projects

A Floe project is one config that describes the entities in a file ingestion
project. Start with one YAML file. As the project grows, move repeated source
settings into named sources, use domain defaults for shared output and policy
settings, and split domains and entities into separate files.

## Start with one config

For a small project, keep the domain, source, entity, and sinks together. This
is the runnable config from [`example/standalone/config.yml`](../example/standalone/config.yml):

```yaml
version: "0.8"

domains:
  - name: sales

entities:
  - name: orders
    domain: sales
    source:
      format: csv
      path: "./in/orders.csv"
      options:
        header: true
        separator: ","
    sink:
      accepted:
        format: parquet
        path: "./out/accepted/orders"
      rejected:
        format: csv
        path: "./out/rejected/orders"
    policy:
      severity: reject
    schema:
      columns:
        - name: id
          type: string
          nullable: false
        - name: item
          type: string

report:
  path: "./report"
```

Paths in a config resolve relative to that config file. Validate and run it
from the example directory:

```bash
cd example/standalone
floe validate -c config.yml
floe run -c config.yml
```

## Reuse sources and domain defaults

When multiple entities share an input pattern, declare a named source and refer
to it from each entity. A domain can provide shared sinks and policy settings.
The source and domain blocks below are from the runnable
[`example/project/`](../example/project/) project; they can live inline in the
root config before you split them into files.

```yaml
sources:
  - name: csv_drop
    format: csv
    path: "./bronze/csv_drop/{{resource}}.csv"
    options:
      header: true
      separator: ","

domains:
  - name: finance
    defaults:
      sink:
        accepted:
          format: parquet
          path: "./out/accepted/{{domain.name}}/{{entity.name}}"
        rejected:
          format: csv
          path: "./out/rejected/{{domain.name}}/{{entity.name}}"
      policy:
        severity: reject

entities:
  - name: accounts
    domain: finance
    source:
      ref: csv_drop
      resource: ledger_accounts
    schema:
      columns:
        - name: id
          type: string
          nullable: false
        - name: label
          type: string
```

The entity supplies its source reference and schema. Domain defaults fill in
the sink and policy; values on the entity override defaults. Source paths can
use `{{resource}}`, which resolves from the entity's `source.resource` (or its
name when omitted). See [Config Reference](config.md#merge-rules) for the merge rules and
supported fields.

## Split a larger project into files

When the config becomes harder to navigate, keep one project root and move
source definitions, domain defaults, and entity definitions into files. The
complete layout is in [`example/project/`](../example/project/):

```text
project/
├── config.yml
├── sources.yml
├── bronze/
│   └── csv_drop/
│       ├── customer_accounts.csv
│       └── ledger_accounts.csv
└── domains/
    ├── finance/
    │   ├── _domain.yml
    │   └── accounts.yml
    └── sales/
        ├── _domain.yml
        └── accounts.yml
```

The root config includes the shared source file and each domain definition:

```yaml
version: "0.8"

metadata:
  project: "floe-project-example"
  description: "A runnable multi-domain project"

report:
  path: "./report"

include:
  sources: sources.yml
  domains:
    - "domains/*/_domain.yml"
```

`sources.yml` contains a top-level `sources` list. The path template is
relative to the project root:

```yaml
sources:
  - name: csv_drop
    format: csv
    path: "./bronze/csv_drop/{{resource}}.csv"
    options:
      header: true
      separator: ","
```

Each `_domain.yml` defines one domain and its defaults. Each sibling `*.yml`
file defines one entity in that domain. The following files match the finance
domain in the example:

```yaml
# domains/finance/_domain.yml
name: finance
defaults:
  sink:
    accepted:
      format: parquet
      path: "./out/accepted/{{domain.name}}/{{entity.name}}"
    rejected:
      format: csv
      path: "./out/rejected/{{domain.name}}/{{entity.name}}"
  policy:
    severity: reject
```

```yaml
# domains/finance/accounts.yml
name: accounts
source:
  ref: csv_drop
  resource: ledger_accounts
schema:
  columns:
    - name: id
      type: string
      nullable: false
    - name: label
      type: string
```

Paths in included files are still relative to the root `config.yml`. Included
domains and entity files are loaded along with the root config, so one command
validates or runs the whole project:

```bash
cd example/project
floe validate -c config.yml
floe run -c config.yml
```

To add an entity to an included domain, `floe add-entity` can infer its schema
from a sample file and write the entity beside that domain's other entities.
Use a sample file that exists, and set the resource to the filename stem used
by the named source:

```bash
floe add-entity -c config.yml \
  --input ./bronze/csv_drop/customer_accounts.csv \
  --name new_accounts \
  --domain finance \
  --source csv_drop \
  --resource customer_accounts
```

Generate one manifest from the project root when an orchestrator needs a
resolved run plan. For a multi-file project, the manifest's `config_checksum`
covers the config files Floe read, including the root config and included
source, domain, and entity files. It does not checksum the input data files.
See [Manifest Generation](manifest.md) for deterministic generation and
orchestrator use.

```bash
floe manifest generate -c config.yml --deterministic --output project.manifest.json
```
