---
name: building-floe-contracts
description: >-
  Build or modify Floe YAML data contracts for file ingestion, data quality,
  rejected-row quarantine, Parquet or Iceberg sinks, incremental files and
  SCD merges. Use when asked to add a Floe entity, validate a Bronze-to-Silver
  gate, inspect ingestion reports, or troubleshoot a Floe run.
license: MIT
---

# Building Floe contracts

Floe is a **single-node file validation and ingestion gate** (Polars,
PyIceberg, DuckDB for Iceberg MERGE). It does not extract from APIs, write dbt
models, or orchestrate jobs. For an OpenLakeForge project, preserve its
generated `lakehouse_code/silver/` layout and stage bindings: the OLF
pipeline skill owns the surrounding integration.

## Rules

- Read the existing config and entity files first. Use real input samples to
  establish types, nullability, keys and bad-row policy; do not guess them.
- Keep environment-specific storage, catalog locations and credentials in
  environment profiles or runtime bindings, not committed entity contracts.
- Start with validation and dry-run. **Ask before executing a real run** that
  writes accepted/rejected data or changes existing Iceberg tables.
- Never disable checks, broaden nullability, or silently coerce invalid values
  simply to make a run pass. Report rejected records and unknowns.
- Use one entity per input dataset. Floe versions the YAML contract with
  `version: "0.8"` in current examples; this is not the Python package version.

## Author the contract

For a standalone project, start from this minimal file and replace the
paths, column names and rules with verified facts:

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
      options: {header: true}
      cast_mode: strict
    schema:
      columns:
        - {name: order_id, type: string, nullable: false, unique: true}
        - {name: amount, type: number, nullable: false}
    sink:
      write_mode: overwrite
      accepted: {format: parquet, path: "./out/accepted/orders"}
      rejected: {format: csv, path: "./out/rejected/orders"}
    policy: {severity: reject}
report:
  path: "./report"
```

For many datasets, use a root `config.yml` with `include.sources` pointing
to `sources.yml` and `include.domains` matching
`domains/*/_domain.yml`; put entity YAML beside each `_domain.yml`, or set
`entities: "contracts/*.yml"` in it to load them from a subfolder. Shared source paths may use `{{resource}}`; domain defaults can provide
sinks and policy, but entity `schema.columns` are always specific.

- `nullable: false` enforces not-null; `unique: true` checks distinct
  values. `cast_mode: strict` reports invalid casts.
- `policy.severity`: `warn` keeps rows and logs failures; `reject`
  quarantines invalid rows; `abort` rejects the affected file.
- Full snapshots: `sink.write_mode: overwrite`. New files: set
  `incremental_mode: file` and an appropriate append or Iceberg merge mode.
  `merge_scd1` / `merge_scd2` require Iceberg and
  `schema.primary_key`; full refresh is not supported for SCD2.
- Floe is not a transformation layer: joins, aggregations and business
  logic belong in dbt or the downstream modeling tool.

## Validate, preview, inspect

```bash
floe validate -c config.yml
floe run -c config.yml --dry-run
```

For a new sample, `floe add-entity -c config.yml --input ./in/orders.csv --name orders --dry-run` previews an inferred entity. Review its schema
rather than trusting inference blindly. To build a deterministic
orchestrator manifest:

```bash
floe manifest generate -c config.yml --deterministic --output run.manifest.json
```

Only **after authorization** run `floe run -c config.yml` (or select
`--entities orders`). Read its JSON reports under `report.path` for
accepted/rejected counts and errors; never expose raw sensitive values.
For file incremental mode, check `floe state inspect -c config.yml --entity orders`
before considering a reset. `floe state reset`, `floe maintain`, and full refresh can change
state or delete data; do not perform them implicitly.

For exact schema fields and write semantics, consult Floe's repository docs
(`docs/reference/config.md`, `docs/guides/projects.md`, `docs/concepts/checks.md`,
`docs/concepts/write-modes.md`) or the installed CLI's `--help`.
