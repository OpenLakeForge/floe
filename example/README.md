# Floe — hands-on example

This directory is a self-contained tutorial. You can run the full pipeline in under a minute using only the files already here.

## What this example covers

| Entity | Input | Sink | Key features demonstrated |
|---|---|---|---|
| `customer` | CSV files | Delta Lake | `strict` casting · `reject` severity · `unique` constraint · incremental state |
| `orders` | JSON files (nested) | Parquet | Nested field selectors · `warn` severity · `fill_nulls` mismatch policy |

---

## Prerequisites

```bash
# Verify floe is installed
floe --version
```

→ [Install floe](../docs/installation.md) if needed.

---

## Run it

```bash
# From the repo root
cd example

# 1. Validate the config (no data moved)
floe validate -c config.yml

# 2. Run the pipeline
floe run -c config.yml

# 3. Inspect the report
cat report/*/run.summary.json | head -40
```

## Config shapes

[`standalone/`](standalone/config.yml) shows one entity with an inline source and a declared
domain that does not need an `incoming_dir`. [`project/`](project/config.yml) shows a
multi-file project with two domains, included named sources, source references, and domain
defaults. The project source reads named resources from `bronze/csv_drop/`, while domain
defaults route outputs under separate sales and finance paths. Both examples use local CSV
inputs and write Parquet accepted rows, rejected CSVs, and reports beside their config when
run. Each input includes one deliberately invalid row, so `floe run` reports a rejected run
and exits nonzero as expected.

For example, run either from its directory with `floe validate -c config.yml` and
`floe run -c config.yml`.

---

## What just happened

```
example/
├── in/
│   ├── customer/          ← source CSV files (3 files: valid, invalid, missing columns)
│   └── orders/            ← source JSON files (1 valid, 1 invalid)
│
├── out/                   ← created by floe run (gitignored)
│   ├── accepted/
│   │   ├── customer_delta/   ← Delta Lake table
│   │   └── orders/           ← Parquet parts
│   └── rejected/
│       ├── customer/         ← rows that failed schema checks
│       └── orders/           ← rows that failed (warn mode: written but run still succeeds)
│
├── report/                ← run reports (gitignored)
│   └── run_<timestamp>/
│       ├── customer/run.json
│       ├── orders/run.json
│       └── run.summary.json
│
└── state/                 ← incremental state (gitignored)
    └── customer.json      ← tracks which CSV files were already ingested
```

Rows that pass all checks go to `out/accepted/`. Rows that fail go to `out/rejected/` with two extra columns: `__floe_row_index` (original row number) and `__floe_errors` (JSON array of error messages).

---

## Config walkthrough

### Global settings

```yaml
version: "0.8"

report:
  path: "./report"          # run reports land here
```

### Entity: `customer`

```yaml
- name: "customer"
  incremental_mode: "file"  # skip files already processed on re-run
  state:
    path: "./state/customer.json"
```

**`incremental_mode: file`** — floe records which source files it has processed in `state/customer.json`. Re-running with the same inputs is a no-op. Drop a new CSV file and floe picks it up.

```yaml
  source:
    format: "csv"
    path: "./in/customer"
    options:
      header: true
      null_values: ["", "NULL", "null"]
      glob: "*.csv"           # pick up all CSV files in the directory
    cast_mode: "strict"       # reject the whole row if a type cast fails
```

```yaml
  sink:
    accepted:
      format: "delta"         # writes a Delta Lake table
      path: "./out/accepted/customer_delta/"
    rejected:
      format: "csv"
      path: "./out/rejected/customer/"
```

```yaml
  policy:
    severity: "reject"        # rows failing checks go to rejected sink
                              # warn  → flag but keep in accepted
                              # abort → stop the run entirely
```

```yaml
  schema:
    mismatch:
      missing_columns: "reject_file"  # reject the whole file if expected columns are absent
      extra_columns: "ignore"
    normalize_columns:
      enabled: true
      strategy: "snake_case"   # rename source columns to snake_case automatically

    columns:
      - name: "customer_id"
        type: "string"
        nullable: false
        unique: true            # duplicate customer_ids across all accepted rows → rejected
      - name: "email"
        type: "string"
        nullable: true
        unique: true
```

### Entity: `orders`

```yaml
- name: "orders"
  source:
    format: "json"
    path: "./in/orders"
    options:
      json_mode: "array"      # input files are JSON arrays (not NDJSON)
    cast_mode: "strict"
```

Nested field selectors — extract deep values from the JSON structure using dot notation and array indexes:

```yaml
    columns:
      - name: "order_id"
        source: "order.id"         # → {"order": {"id": "..."}}
      - name: "order_total"
        source: "totals.amount"    # → {"totals": {"amount": 99.9}}
      - name: "first_item_sku"
        source: "items[0].sku"     # → {"items": [{"sku": "..."}]}
```

```yaml
  policy:
    severity: "warn"          # invalid rows logged but kept in accepted output
                              # run still exits 0
  schema:
    mismatch:
      missing_columns: "fill_nulls"   # missing columns are added as nulls (no file rejection)
```

---

## Try breaking it

**See rejections in action:**
```bash
# Add a duplicate customer_id to customers_valid.csv and re-run
# (delete state/customer.json first to force reprocessing)
rm -f state/customer.json
floe run -c config.yml
cat out/rejected/customer/part-*.csv
```

**See incremental state skip files:**
```bash
floe run -c config.yml      # first run processes all files
floe run -c config.yml      # second run: "0 files pending" — all already processed
```

**See a missing-column file rejection:**

`in/customer/customers_missing.csv` already has a column that does not match the schema. After running, check `out/rejected/customer/` — the whole file's rows are routed there.

---

## Profiles (environment overrides)

The `profiles/` directory shows how to override config values per environment without duplicating the full config:

```bash
floe run -c config.yml --profile profiles/prod.yml
```

→ [Profiles reference](../docs/profiles.md)

---

## Next steps

| Topic | Doc |
|---|---|
| Full config reference | [docs/config.md](../docs/config.md) |
| All check types | [docs/checks.md](../docs/checks.md) |
| Delta + Unity Catalog | [docs/sinks/delta.md](../docs/sinks/delta.md) |
| Iceberg + Glue / REST | [docs/sinks/iceberg.md](../docs/sinks/iceberg.md) |
| S3 / GCS / ADLS storage | [docs/storages/](../docs/storages/) |
| Incremental ingestion | [docs/profiles.md](../docs/profiles.md) |
| Run reports | [docs/report.md](../docs/report.md) |
| CLI reference | [docs/cli.md](../docs/cli.md) |
