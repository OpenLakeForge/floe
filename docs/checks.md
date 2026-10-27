# Technical Checks

This document describes the validation checks implemented in Floe, the supported
column types for casting, and how `policy.severity` applies to each check.
For the full execution order (file-level vs row-level vs entity-level), see
`docs/how-it-works.md`.

## Checks

### not_null (missing value)

- **Trigger**: `schema.columns[].nullable: false`
- **Logic**: a row fails when the typed value is null.
- **Notes**:
  - Nulls include explicit nulls and any values listed in `source.options.null_values`.
  - Applies to every column with `nullable: false`.

### unique (dataset-level)

- **Trigger**: `schema.columns[].unique: true`
- **Logic**: the first occurrence is accepted; any later duplicate values are flagged.
- **Notes**:
  - Null values are ignored (they do not count as duplicates).
  - Applies independently per column, across all input files in the entity.

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
- `decimal(p,s)` (exact; see `docs/config.md`)
- `float32`: `float32`
- `date`: `date`
- `datetime`: `datetime`, `timestamp`
- `time`: `time`

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
