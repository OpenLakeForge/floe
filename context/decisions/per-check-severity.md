# Decision — Severity per check

## Status

Accepted (design for #1250, approved by the maintainer on the decision comment of #1250, refs checked against `main`
at `6526c789`). Docs only: the code lands in the sub-issues below.

## Context

One `policy.severity` drives every check of an entity: `PolicySeverity` (`src/floe/config/types.py:10`),
`PolicyConfig` (`src/floe/config/types.py:32-34`), and data-flow step 3 of `context/architecture.md`. A user cannot
warn on a missing email and still reject a failed cast, or abort on a duplicate key.

## Decision

Severity lives on the object that declares the check.

- A column gets an optional `rule_severity` map (rule to level).
- The entity gets an optional `policy.rule_severity` map for entity-scoped rules and entity-wide per-rule defaults.
- A single-check object (a #1252 predicate) takes a scalar `severity`.
- Convention: `severity` is always one level, `rule_severity` is always a map.

`policy.severity` stays required and is the default, so existing configs keep working unchanged.

```yaml
policy:
  severity: reject              # default for every check (required, as today)
  rule_severity:                # optional, entity-wide, per rule
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

### YAML shape

- `schema.columns[].rule_severity`: a map from a column-scoped rule to `warn|reject|abort`. Allowed today: `not_null`,
  `cast_error`. #1251 adds `out_of_range` (shared by `min`/`max`), `not_allowed`, `pattern_mismatch` and `length`. The
  keys are the rule names used in `__floe_errors` and `validation.rules[]`, so config, errors and report share one
  vocabulary.
- `policy.rule_severity`: takes the column rules (as entity-wide defaults) plus the entity-scoped rules `unique` and
  `schema_mismatch`. #1253 adds `min_rows` (`warn|abort` only). Because it is a map, domain defaults merge into it key
  by key (`src/floe/config/yaml.py:248-259`), with no new merge rule.
- #1252 predicates: each `schema.checks[]` entry takes a scalar `severity`, which falls back to `policy.severity`.
  Predicate names are user-chosen, so they are not keys of `policy.rule_severity`.
- `policy.rule_severity.schema_mismatch` sets the level used when `reject_file` is requested. Entity-level only.

### Rejected alternatives

- The inline object form (`nullable: {value: false, severity: warn}`). It would make every check key a union type
  across config, models, JSON schemas and manifest, and `cast_error` has no key to hang it on.
- `policy.overrides` keyed by rule and column. It moves severity away from the check, needs column cross-reference
  checks, and as a list it does not merge with domain defaults.

### Defaulting rule

`level(rule, column) = column.rule_severity[rule] ?? policy.rule_severity[rule] ?? policy.severity`.

These stay on `policy.severity` with no override: an unreadable file (`src/floe/run/file.py:129`), `csv_read_error`
and other reader rules (`file.py:372`). They are not contract checks. A config with no `rule_severity` gives
byte-identical behaviour, report and manifest.

### Mixed outcomes

- **Per row:** rejected if any failed check resolves to reject (or abort). A row whose failures are all warn is
  accepted and counts as warnings. Most severe wins: abort > reject > warn.
- **Per file:** any abort-level failure aborts the file as today. Status is `aborted`, the raw file is copied to
  `sink.rejected` (`file.py:424-425`), the entity stops (`run/entity.py:429-430`) and the exit code is 2. The cast
  shortcut (`file.py:290`) fires only when an abort-level cast check fails.
- **`__floe_errors`:** lists warn failures too (`error_column` already builds every violated check,
  `src/floe/checks/rows.py:80-102`). Only the reject decision narrows. Entries keep their format: a severity field
  would add a position to the `csv` formatter, which is breaking. `<stem>_reject_errors.json` holds the failing rows
  that were accepted with warnings. Rejected rows carry their errors in `__floe_errors`.
- **Merge key:** duplicates on the merge key are still forced out when `unique` resolves to warn (`file.py:382-386`).
- **Newest-first merge** (`run/entity.py:400`) applies only when no check can resolve to abort, for today's reason: an
  aborted file must not let newer files commit ahead of an older one.
- A failed cast leaves a null, so on a required column it also fails `not_null`. With `cast_error: warn` and
  `not_null: reject` that row is still rejected. This is existing behaviour and is kept.

### `max_rejection_rate`

Unchanged: `rejected_total / rows_total` (`src/floe/report/entity.py:145-151`). Warn rows are not counted. Merge-key
duplicates forced out under warn and every row of an aborted file are counted. The parse rule "requires
`policy.severity=reject`" (`src/floe/config/entity.py:138-142`) stays with the same message. Relaxing it later is
additive.

### Report (`run.json`, additive only)

- `files[].validation.rules[]` keeps `{rule, severity, violations, columns}`. Entries are split per (rule, resolved
  severity), in rule order (`report/entity.py:16`), then abort, reject, warn. With no overrides there is one entry
  per rule and the goldens do not change.
- `validation.errors` = violations of reject- and abort-level checks. `validation.warnings` = violations of warn-level
  checks plus the mismatch counts, as today. `results.*_total` is unchanged, with no per-severity totals.
- `unique_constraints[].action/status_effect` use the resolved `unique` level.
- The `policy` block adds `"rule_severity": {...}` (key-sorted) only when it is set.
- File status: `aborted` if any abort, else `rejected` if any rows were rejected, else `success`. The run outcome is
  unchanged (`src/floe/report/summary.py:42-50`).

### Manifest

- `src/floe/manifest.py:227-240` adds `policy_rule_severity` and column `rule_severity` via `_put` (left out when
  `None`), so configs without overrides give byte-identical manifests.
- Canonical schema: an optional object with values `warn|reject|abort` next to `policy_severity`
  (`orchestrators/schemas/floe.manifest.schema.json:897-912`) and in the column item (`:932-970`). The dagster
  vendored copy is updated in the same PR (`test_vendored_schema.py` requires byte identity).
- `manifest_run` reads both back strictly (`src/floe/manifest_run.py:110-130, 189-194`), never through `_lenient`
  (`:67-77`), which would turn a malformed map into "no overrides" and silently change which rows are rejected.
- `spec_version` stays the generating major.minor (D22), with no separate bump. The feature ships in a minor release
  (0.11 or 1.0, whichever comes first), never a patch. An older patch of the same minor would accept the manifest and
  silently ignore the keys. An older minor refuses it (`manifest_run.py:326`), and an older dagster-floe fails
  schema validation, which is the loud failure we want.

### Schema mismatch

`policy.rule_severity.schema_mismatch` replaces `policy.severity` at `src/floe/checks/mismatch.py:64`. Everything
below that line is unchanged: warn downgrades to fill/ignore with a warning, reject gives `rejected_file`, abort gives
`aborted`. There is no separate level for missing and extra columns. "A missing optional column warns" already works
with `missing_columns: fill_nulls` (or `reject_file` plus `schema_mismatch: warn`). A missing required column fills
with nulls and then fails `not_null` at its own level.

### Error codes for invalid configs

Existing codes only: `config_unknown_key`, `config_invalid_value`, `config_conflict`, `config_missing_key`. No new
code, and no `version:` gate: an older Floe already rejects the unknown key with `config_unknown_key`.

| Input | Code | Message pattern |
|---|---|---|
| Unknown rule key | `config_unknown_key` | `unknown field schema.columns.rule_severity.<key>` |
| Level not `warn/reject/abort` | `config_invalid_value` | `<ctx>.<rule>=<v> is unsupported (allowed: warn, reject, abort)` |
| Map is not a mapping | `config_invalid_value` | from `yaml_hash` |
| `cast_error` on a `string` column, or `not_null` on a column that is not required | `config_conflict` | names column and rule |
| A rule resolves to reject/abort and `sink.rejected` is unset | `config_missing_key` | today's message when `policy.severity` causes it, else names the rule |
| PII with any abort-level rule | `config_conflict` | today's message, or a variant naming the rule |

### AGENTS.md §7

Nothing here is a non-goal. `warn` adds no new output or sink. #1252 predicates only accept or reject rows. #1253
`min_rows` is evaluated over one run's files in one process. Statistical and freshness checks stay out of scope.

## Open risks, decided

1. **Duplicate rule entries in `run.json`:** decided. `validation.rules[]` is split per (rule, severity). Each entry
   already has a `severity` field (`src/floe/report/entity.py:59`), so the shape is unchanged. A rule appears more than
   once only when its columns resolve to different levels. dagster-floe already sums entries per severity
   (`asset_checks.py:313-319`). The fallback (a `severity` per `columns[]` item) is not taken.
2. **Severity per unique key:** deferred. `unique` is one rule, so the primary key cannot abort while a secondary
   `unique_keys` entry warns. The upgrade path is an object form of `unique_keys` entries
   (`{columns: [...], severity: ...}`), which is additive later. The parser marks it with a `ponytail:` comment (#1359).
3. **Release coupling:** #1357, #1358 and #1359 ship in one minor release. YAML parsing lands last, so no release
   accepts a `rule_severity` that the run path or the manifest ignores.
4. **dagster-floe abort buckets:** abort-severity entries land in neither count. Pre-existing, filed separately as
   #1360 (not a sub-issue, not fixed here).

## Sub-issues (merge in this order)

1. #1355 this record. Docs only. Its PR closes #1250.
2. #1356 `refactor(checks): resolve severity per check through one helper`. Behaviour-neutral prep.
3. #1357 `feat(checks): mixed severities per row and per file`.
4. #1358 `feat(manifest): carry rule_severity`.
5. #1359 `feat(config): parse rule_severity`. Lands last.

3, 4 and 5 ship in the same minor release.
