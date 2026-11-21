# Checks and severity

This document describes the validation checks implemented in Floe, the supported
column types for casting, and how `policy.severity` applies to each check.
For the full execution order (file-level vs row-level vs entity-level), see
[How Floe works](how-it-works.md).

## Checks

### not_null (missing value)

- **Trigger**: `schema.columns[].nullable: false`
- **Logic**: a row fails when the typed value is null.
- **Notes**:
  - Nulls include explicit nulls and any values listed in `source.options.null_values`.
  - Applies to every column with `nullable: false`.

### unique (dataset-level)

- **Trigger**: `schema.unique_keys` (each entry is one single- or multi-column key) and/or `schema.primary_key` (enforced as one more composite key).
- **Logic**: the first occurrence is accepted; any later duplicate values are flagged.
- **Notes**:
  - Null values are ignored (they do not count as duplicates).
  - Applies independently per key, across all input files in the entity.
  - The legacy `schema.columns[].unique` flag was removed in 1.0: `floe validate` and `floe run` fail on it. Use `schema.unique_keys`, e.g. `unique_keys: [[order_id]]`.

### cast_error (type mismatch)

- **Trigger**: any column whose logical type is not `string`.
- **Logic**: a row fails when the raw value is present but the typed value is null.
- **Notes**:
  - In `cast_mode: strict`, cast errors are reported.
  - In `cast_mode: coerce`, cast errors are ignored (values become null and may
    still fail `not_null` if the column is required).

## Supported column types

Type names are case-insensitive and normalized by removing `-` and `_`.
Examples of accepted values are listed below.

- `string`: `string`, `str`, `text`
- `boolean`: `boolean`, `bool`
- `integer`:
  - `int8`, `int16`, `int32`, `int64`
  - `int`, `integer`, `long`
- `unsigned integer`:
  - `uint8`, `uint16`, `uint32`, `uint64`
- `number` (float64):
  - `number`, `float64`, `float`, `double`
- `decimal(p,s)` (exact; see [Supported column types](../reference/config.md#supported-column-types))
- `float32`: `float32`
- `date`: `date`
- `datetime`: `datetime`, `timestamp`

## Severity behavior

The configured `policy.severity` applies uniformly across all checks:

- `warn`
  - Rows are kept.
  - Errors are logged for each failed check.
- `reject`
  - Rows with any check failure are rejected.
  - For `unique`, the first occurrence is kept, duplicates are rejected.
- `abort`
  - Any check failure aborts the entire file (file-level rejection).

All modes avoid logging raw row values; only rule, column, row index, and
message are emitted in reports.

### Severity per check

`policy.severity` is the default. A rule can override it, on one column
(`schema.columns[].rule_severity`: `not_null`, `cast_error`) or for the whole
entity (`policy.rule_severity`: the column rules as defaults, plus `unique` and
`schema_mismatch`). The level of a check is the column's entry, else the
entity's, else `policy.severity`.

```yaml
policy:
  severity: reject              # default for every check
  rule_severity:
    schema_mismatch: warn       # a requested reject_file only warns and fills nulls
    unique: abort               # a duplicate key aborts the entity
schema:
  mismatch:
    missing_columns: reject_file
  primary_key: [order_id]
  columns:
    - name: order_id
      type: int64
    - name: amount
      type: decimal(18,2)       # cast_error -> reject (the default)
    - name: email
      type: string
      nullable: false
      rule_severity:
        not_null: warn          # a missing email keeps the row and counts a warning
```

A row is rejected if any failed check resolves to `reject` or `abort`; a row
that only fails `warn` checks is kept and counted as a warning. Any
`abort`-level failure aborts the file. `sink.rejected` is required as soon as
any rule can resolve to `reject` or `abort`, and `abort` is refused with `pii`.
`unique` is one rule for every key: severity per key is not supported.

## Design notes

- Row-level rejection is the default because it preserves good data while isolating bad rows.
- `unique` is dataset-level and enforced by `policy.severity`:
  - `warn`: log duplicates and keep all rows.
  - `reject`: keep the first occurrence, reject duplicates.
  - `abort`: abort the file if duplicates are found.
- In `append` mode, `unique` checks seed from existing accepted data for the
  supported accepted sinks, so duplicates across runs can be rejected.
- `cast_mode` defines parsing behavior:
  - `strict`: invalid values cause rejection (or abort per policy).
  - `coerce`: invalid values become null; nullable rules decide acceptance.
