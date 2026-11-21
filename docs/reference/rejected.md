# Rejected row envelope

The columns Floe adds to every rejected row, per rejected format. This envelope is a versioned contract (version 1.0):
consumers, and a future `floe replay` (#264), may rely on what is listed here.

Rows rejected by a row check go to `sink.rejected` (see [Config](config.md#sink)) with this envelope. A whole-file
reject (`reject_file`, `abort`) copies the raw file instead and has no envelope.

## Compatibility rule

In 1.x a `__floe_*` column may be **added**. It is never **removed, renamed or retyped**. A new column goes after the
columns listed here. Read the columns by name, and ignore a `__floe_*` column you do not know. The types in the table
below are the types a reader gets.

Each format's columns are pinned by a test or a golden case (see [Pinned by](#pinned-by)).

## Columns per format

Every format starts with the entity's declared output columns (after PII masking), then the `__floe_*` columns in the
order below. `floe validate` refuses a declared column name that starts with `__floe_` when `sink.rejected` is set.

| Column | CSV | Parquet | Iceberg | Meaning |
|---|---|---|---|---|
| `__floe_row_index` | yes (text) | yes, `UInt64` | yes, `long` | 0-based index of the row in the source file's record stream, across all chunks. Not a line number: a quoted multi-line CSV field is one record. |
| `__floe_errors` | yes (text) | yes, `String` | yes, `string` | The failed checks of the row, as described [below](#the-__floe_errors-value). |
| `__floe_run_id` | yes (text) | yes, `String` | yes, `string` | `run_id` of the run that rejected the row (as in `run.json`). |
| `__floe_source_uri` | yes (text) | yes, `String` | yes, `string` | URI of the file the row came from, as `files[].input_file` in the run report (before any archive move). |

A CSV rejected header ends with `__floe_errors,__floe_run_id,__floe_source_uri`. A reader that indexes CSV rejected
columns by position past `__floe_errors` must account for the two added columns; read by header name instead.

### Nullability

- `__floe_errors` is never null in rejected output: a row is rejected because a check failed.
- CSV: every `__floe_*` column is written for every row. A null declared value is an empty field.
- Parquet: the `__floe_*` columns are never null. A declared column is null where the source value is null, or
  was nullified by PII masking.
- Iceberg: every column of the table, `__floe_*` included, is optional in the Iceberg schema. Floe writes a value in
  all four `__floe_*` columns of every row.

## The `__floe_errors` value

It lists every violated check of the row, in check order (`not_null`, `cast_error`, then `unique`). Warn-level failures
are listed too ([per-check severity](https://github.com/OpenLakeForge/floe/blob/main/context/decisions/per-check-severity.md)):
a reject-level failure rejects the row, and a warn failure of the same row appears beside it. The rule names are those
of [Checks](../concepts/checks.md) and `validation.rules[]` in the [report](report.md). `column` is the output
column name.

The format follows `report.formatter` (default `json`) and is the same in all three rejected formats.

| `report.formatter` | Value | One entry |
|---|---|---|
| `json` | JSON array of objects | `{"rule":"…","column":"…","message":"…"}`. With a column `source`, `"source":"…"` sits between `column` and `message`. |
| `csv` | one JSON string, entries joined by `\n` (backslash and `n`) | `rule,column,message`. When the entity has a column `source`, every entry has the source field: `rule,column,source,message` (empty when unmapped). Fields with `,`, `"` or line breaks are CSV-quoted. |
| `text` | one JSON string, entries joined by `; ` | `rule:column message`. With a source: `rule:column source=<source> message`. |

Example, one Parquet rejected row of the `rejected_parquet` golden (`json`):

```json
[{"rule":"cast_error","column":"day","message":"invalid value for target type"},{"rule":"cast_error","column":"amount","message":"invalid value for target type"}]
```

With a column `source` (golden `cast_error_with_source`, CSV rejected output):

```text
id,quantity,__floe_row_index,__floe_errors
2,abc,1,"[{""rule"":""cast_error"",""column"":""qty"",""source"":""qty"",""message"":""invalid value for target type""}]"
```

One row failing `not_null` on `email` and `cast_error` on `qty` under `csv` and `text` (goldens `formatter_csv`,
`formatter_text`):

```text
"not_null,email,required value missing\ncast_error,qty,invalid value for target type"
"not_null:email required value missing; cast_error:qty invalid value for target type"
```

To parse `__floe_errors`, set `report.formatter: json`, the only formatter that gives a list. In 1.x entries never gain
a positional field: new information would be a new JSON key (ignore keys you do not know), not a new `csv` or `text`
position.

## Raw source values

The declared columns hold what the file contained, not the accepted (Silver) type: a failed cast keeps its source
value (`abc` for a `decimal`).

| Format | Declared columns hold |
|---|---|
| CSV | The raw parsed values, as text for text inputs (csv, tsv, fixed, json, xml). For parquet, orc and avro inputs a numeric, string, boolean, date, datetime or time column keeps its source value (written as CSV text). Any other native type (binary, duration, nested) cannot be written as CSV text, so that column holds the value cast to its declared type. |
| Parquet | The native source value as parsed: strings for text inputs, native types for parquet, orc and avro inputs. Never cast to the accepted type. |
| Iceberg | Always `string`, so the table never drifts with the accepted types. A typed source value (including binary and nested) is cast to string. |

Parsing normalises nulls and quoting, so a rejected row is not byte-faithful. For the original bytes use the source file
(`output.archived_path`, otherwise `input_file`, in the run report).

## PII entities

For an entity with `pii`, the rejected output holds the **masked** values, produced by the same strategy as the
accepted rows (`hash`, `redact`, `mask`, `nullify`). `drop` removes the column from the CSV and Parquet output, and
is refused for Iceberg sinks. There is no unmasked option. The `__floe_*` columns are the same as for any entity.

The rejected output of a PII entity is diagnostic and cannot be replayed. The replay input is the original file, with
rows selected by `__floe_row_index`; the index is only valid for the same bytes read with the run's `source.options`
([decision](https://github.com/OpenLakeForge/floe/blob/main/context/decisions/pii-rejected-replay.md)).

## Appending and duplicates

Rejected output is append-only, whatever `sink.write_mode` is. A retried run can write the same rejected row again
(the Iceberg table is at-least-once), so dedupe on `(__floe_source_uri, __floe_row_index)` and keep the latest
`__floe_run_id`.

## Pinned by

| Format | Test or golden case |
|---|---|
| CSV | The header line of the goldens with `sink.rejected.format: csv`, e.g. `conformance/formatter_csv`, `conformance/formatter_text`, `conformance/cast_error_with_source` and `conformance/casts`. |
| Parquet | Golden `conformance/rejected_parquet` (schema, types and rows of `rows.parquet`) and `tests/unit/run/test_run_entity.py::test_1154_rejected_parquet_holds_raw_values_and_metadata` (column names, order and `__floe_row_index` type). |
| Iceberg | `tests/unit/run/test_run_iceberg.py::test_rejected_iceberg_keeps_the_raw_values_as_strings` (names, types and order of the quarantine table). |
