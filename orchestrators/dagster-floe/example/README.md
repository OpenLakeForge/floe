# dagster-floe local example

Runs two Dagster jobs (HR + Sales) against the CSV files in `in/` and writes
accepted Parquet output to `out/`. Several rows in each file are intentionally
invalid so you can verify rejected asset metadata in the Dagster UI.

## What this example verifies (issue #334)

| Thing to look at | Expected behaviour |
|---|---|
| Asset lineage graph | `employees_source`, `orders_source`, `invoices_source` appear as **external** (grey/cloud) nodes upstream of each job asset |
| `employees_source` metadata | Shows `format: csv` and `dagster/uri` pointing to `in/hr/employees.csv` |
| After materialising employees | `employees_rejected` asset page shows `rejection_rate`, `dagster/uri` (rejected sink path), `files_with_rejections`, `dominant_rejection_reason` |

## Prerequisites

```bash
# 1. Install the floe CLI (binary must be on PATH as "floe")
pip install floe-python

# 2. Install Python dependencies
cd orchestrators/dagster-floe
uv sync
```

## Run

```bash
# From orchestrators/dagster-floe/
uv run dagster dev
```

Dagster UI opens at **http://localhost:3000**.

## Walkthrough

### 1. Check source asset lineage

1. Click **Assets** in the top nav.
2. Click **View global asset lineage**.
3. You should see three source nodes (`employees_source`, `orders_source`,
   `invoices_source`) displayed as external/untracked upstream boxes connected
   to each multi-asset.
4. Click `employees_source` → the sidebar shows `format: csv` and a
   clickable `dagster/uri` link.

### 2. Materialise and inspect rejected metadata

1. Click **Jobs** → `floe_mfv1_dagster_hr_job` → **Materialize all**.
2. Wait for the run to finish (it will succeed even though some rows are
   rejected — `policy.severity: reject` routes bad rows to the sink without
   failing the job).
3. Click **Assets** → `hr/employees_rejected`.
4. In the **Metadata** panel you should see:

   | Key | Example value |
   |---|---|
   | `rejected` | `3` |
   | `rejection_rate` | `0.6` |
   | `dagster/uri` | `…/out/rejected/employees` |
   | `files_with_rejections` | `1` |
   | `dominant_rejection_reason` | `cast_error` or `not_null` |

5. Repeat for **Sales** job (`floe_mfv1_dagster_sales_job`) and inspect
   `sales/orders_rejected` and `sales/invoices_rejected`.

## Input data

The `in/` files include intentionally invalid rows:

| File | Bad rows |
|---|---|
| `in/hr/employees.csv` | null `employee_id`, invalid `start_date`, invalid `salary` |
| `in/sales/orders.csv` | invalid `created_at`, non-numeric `amount` |
| `in/sales/invoices.csv` | invalid `paid` value, null `order_id` |
