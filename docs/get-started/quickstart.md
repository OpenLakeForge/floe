# Quickstart

Write a contract for a small CSV file, run Floe on it, and read the three
outputs: accepted rows, rejected rows with their reasons, and the run report.
It takes about five minutes and needs only Python 3.11 or later.

## 1. Install Floe

```bash
python -m venv .venv
. .venv/bin/activate
pip install floe-python
floe --version
```

[Installation](installation.md) covers uv, Docker and the optional extras for
S3, Iceberg and Excel.

## 2. Add a raw file

Create `in/orders.csv`. Three of its five rows are wrong: an amount that is not
a number, an order id seen twice, and a missing order id.

```text
order_id,amount
1001,19.90
1002,not_a_number
1003,42.00
1003,7.50
,5.00
```

## 3. Write the contract

Create `floe.yml` next to the `in` folder:

```yaml
version: "0.10"
entities:
  - name: orders
    source:
      format: csv
      path: ./in
      options:
        separator: ","     # Floe reads ";" unless told otherwise
    sink:
      accepted:
        format: parquet
        path: ./out/accepted/orders
      rejected:
        format: csv
        path: ./out/rejected/orders
    policy:
      severity: reject     # reject the bad rows, keep the good ones
    schema:
      columns:
        - name: order_id
          type: string
          nullable: false   # required
          unique: true      # no duplicates
        - name: amount
          type: "decimal(10,2)"
```

The contract says: read every CSV file in `./in` as the `orders` entity; each
row must have an `order_id` that is present and unique, and an `amount` that is
a decimal with two digits after the point. Rows that pass go to Parquet, rows
that fail go to a CSV quarantine.

## 4. Validate the contract

```bash
floe validate -c floe.yml
```

```text
Config valid: floe.yml
Version: 0.10
Report: report
Entities: 1
Entity: orders
  Source: csv (./in)
  Sink accepted: parquet (./out/accepted/orders)
  Sink rejected: csv (./out/rejected/orders)
  Severity: reject
```

`floe validate` reads no data. Run it in CI on every change to a contract.

## 5. Run it

```bash
floe run -c floe.yml
```

```text
==> entity orders (severity=reject, format=csv)
  REJECTED orders.csv rows=5 accepted=2 rejected=3 …

Totals: files=1 rows=5 accepted=2 rejected=3
Overall: rejected (exit_code=0)
```

Two rows were accepted and three rejected. A run with rejected rows still
exits `0`: rejecting rows is the contract working, not a failure.

## 6. Read the results

**Accepted rows** are a Parquet dataset, typed as the contract declares:

```bash
python -c "import polars as pl; print(pl.read_parquet('out/accepted/orders'))"
```

```text
┌──────────┬───────────────┐
│ order_id ┆ amount        │
│ str      ┆ decimal[10,2] │
╞══════════╪═══════════════╡
│ 1001     ┆ 19.90         │
│ 1003     ┆ 42.00         │
└──────────┴───────────────┘
```

**Rejected rows** keep their raw values, plus the row's position in the file
and every rule it broke:

```bash
cat out/rejected/orders/part-00000.csv
```

```text
order_id,amount,__floe_row_index,__floe_errors
1002,not_a_number,1,"[{""rule"":""cast_error"",""column"":""amount"",""message"":""invalid value for target type""}]"
1003,7.50,3,"[{""rule"":""unique"",""column"":""order_id"",""message"":""duplicate value""}]"
,5.00,4,"[{""rule"":""not_null"",""column"":""order_id"",""message"":""required value missing""}]"
```

The first `1003` was accepted; the second is the duplicate.

**The run report** is JSON under `report/run_<run_id>/`: a
`run.summary.json` for the run and one `run.json` per entity, with counts,
statuses, timings and the files written.

```text
report/run_<run_id>/run.summary.json
report/run_<run_id>/orders/run.json
```

[Run report](../reference/report.md) documents every key.

## 7. Try another severity

Change `severity: reject` to `warn` and run again: all five rows are accepted
(the bad amount becomes null), and the run ends `success_with_warnings`. Change
it to `abort`: the whole file is rejected, nothing is accepted, and the run
exits `2`, which an orchestrator treats as a failure. The severity is how a
contract says how strict to be; [Checks and severity](../concepts/checks.md)
gives the full rules.

## Next

- [Data contracts](../concepts/data-contracts.md): every part of a contract,
  and how to design one.
- [Config reference](../reference/config.md): every field.
- [`example/`](../../example/): a larger project with several entities,
  profiles and S3.
