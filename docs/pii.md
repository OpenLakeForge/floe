# PII Masking

Floe supports column-level PII masking transforms applied to output rows
before they are written. Transforms run after schema validation and data quality
checks, so checks see the raw values, and the written rows hold masked ones.

## What is masked

- **Accepted rows** are masked before the accepted sink write.
- **Rejected rows** (`policy.severity: reject`) are masked the same way before they are
  written to `sink.rejected`: a hashed `email` holds the same digest in the accepted and the
  rejected output. The `__floe_errors` column names the rule and column, not the value.
- **Raw files are never written.** A config that would copy an input file unmasked to
  `sink.rejected` or an archive fails `floe validate` when `pii` is set: `policy.severity: abort`,
  `sink.archive`, and `schema.mismatch.missing_columns` or `extra_columns` set to `reject_file`.
  A file Floe cannot read is reported as failed without the raw copy it would otherwise get.

## Configuration

Add a `pii` block to any entity:

```yaml
entities:
  - name: customer
    # ...
    pii:
      columns:
        - name: email
          strategy: hash
        - name: credit_card
          strategy: mask
          mask_pattern: "{first4}****{last4}"
        - name: ssn
          strategy: redact
          redact_value: "***"
        - name: ip_address
          strategy: nullify
        - name: notes
          strategy: drop
```

## Strategies

| Strategy  | Description                                                                                    | Extra fields           |
|-----------|------------------------------------------------------------------------------------------------|------------------------|
| `hash`    | Replaces each value with a lowercase hex digest. Null values remain null. See security note.   | `key` (opt.)           |
| `drop`    | Removes the column from the accepted output entirely.                                          | —                      |
| `nullify` | Replaces every value with null, preserving the column and its type.                            | —                      |
| `redact`  | Replaces every non-null value with a fixed string (default `[REDACTED]`).                      | `redact_value` (opt.)  |
| `mask`    | Applies a pattern string with optional `{firstN}` / `{lastN}` reveal tokens (see below).      | `mask_pattern` (req.)  |
| `tokenize`| Reserved for a future lookup-table tokenization strategy; rejected at config validation now.   | —                      |

> **Security notice — `strategy: hash`**
>
> Without a `key`, the digest is plain, **unsalted SHA-256**. For low-entropy PII — emails,
> phone numbers, national IDs, credit-card numbers — an attacker with access to the masked
> output can recover most original values via precomputed dictionary. **Always set `key:`**
> when hashing real PII.
>
> When `key:` is present, **HMAC-SHA256** is used instead. The keyed digest is computationally
> infeasible to reverse, while still preserving referential integrity (same input + same key →
> same output across runs). The key accepts a plain literal or a `${ENV_VAR}` reference
> resolved at masking time:
>
> ```yaml
> pii:
>   columns:
>     - name: email
>       strategy: hash
>       key: "${FLOE_PII_HMAC_KEY}"   # read from environment at run time
> ```
>
> Floe 0.8 emits no warning for a keyless `hash`.

### `mask` pattern syntax

The `mask_pattern` field is a literal string placed verbatim as the output,
with two optional tokens substituted:

- `{firstN}` — replaced by the first N Unicode characters of the original value
- `{lastN}` — replaced by the last N Unicode characters of the original value

```yaml
mask_pattern: "{first4}****{last4}"   # 4111222233334444 → 4111****4444
mask_pattern: "***-**-{last4}"         # 123-45-6789       → ***-**-6789
mask_pattern: "{first2}****{last2}"    # hello             → he****lo
```

Rules:
- If `firstN` and `lastN` would overlap (value shorter than N+M chars), the
  last-N characters take priority and first-N is clamped so they never overlap.
- The mask is always applied — a zero-character value still produces the pattern
  without any substitution tokens.
- Null values remain null.

## Interaction with `normalize_columns`

When `schema.normalize_columns` is enabled, the `pii.columns[].name` field
refers to the **schema column name** (as declared in `schema.columns`), not the
post-normalization runtime name. Floe resolves the mapping automatically.

## Interaction with `strategy: drop`

Dropped columns are removed from the accepted DataFrame before the sink write.
They are not present in the Parquet output schema. If the column is part of
`schema.primary_key` or a `schema.unique_keys` entry, validation will fail at
config-load time.

> **Note:** `strategy: drop` is **not supported** for Iceberg sinks.
> Config validation will reject it at load time. Use `nullify` or `redact` to
> suppress sensitive values while preserving the column in the output schema.

## Validation rules

- `name` must match a column declared in `schema.columns`.
- `strategy: mask` requires a `mask_pattern` that contains at least one `{firstN}` or `{lastN}` token.
- `strategy: drop` is rejected for Iceberg sinks.
- `strategy: hash`, `redact` and `mask` apply to string columns only; use `nullify` or `drop`
  for other types.
- `strategy: nullify` is rejected on a `nullable: false` column.
- `strategy: drop` is rejected on a `schema.primary_key` column, and any strategy is rejected on
  a primary-key column with `merge_scd1` / `merge_scd2` (it would corrupt the merge key).
- Any strategy is rejected on a unique-key column: `drop`, `nullify`, `redact` and `mask` collapse
  values, and `hash` would leak raw values through the uniqueness samples in the run report.
- `pii` is rejected with `policy.severity: abort`, `sink.archive`, or a `reject_file` mismatch
  policy (see [What is masked](#what-is-masked)).
- `strategy: tokenize` is rejected at config validation (not yet implemented).
- `redact_value` and `mask_pattern` are ignored for strategies that do not use them.
- `key` is only used by `strategy: hash`; it is ignored for other strategies.
- Keyless `strategy: hash` is valid; no warning is emitted.
- Duplicate column names within `pii.columns` are rejected.
