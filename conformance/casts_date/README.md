# casts_date

String -> `date` cast as done by Rust 0.7 (Polars 0.52, chrono `NaiveDate` parsing), `cast_mode: strict` (default).
Each row varies one column; the others hold a valid filler (`date` 2024-01-01, `datetime`/`timestamp` 2024-01-01T10:00:00).
Accepted values are shown as stored in the accepted Parquet (`date` as a calendar day, `datetime` as milliseconds, rendered ISO).
Rejected rows go to `expected/rejected` with `__floe_errors` = one `cast_error` entry for the column.

| id | input | Rust 0.7 | outcome |
|---|---|---|---|
| d00 | `2024-01-01` | `2024-01-01` | accepted |
| d01 | `2024-02-29` | `2024-02-29` | accepted |
| d02 | `2023-02-29` | rejected | `__floe_errors`: `cast_error` on `date` ("invalid value for target type") |
| d03 | `2024-1-5` | `2024-01-05` | accepted |
| d04 | `2024-01-5` | `2024-01-05` | accepted |
| d05 | `2024-1-05` | `2024-01-05` | accepted |
| d06 | ` 2024-01-01` | `2024-01-01` | accepted |
| d07 | `2024-01-01 ` | `2024-01-01` | accepted |
| d08 | `  2024-01-01  ` | `2024-01-01` | accepted |
| d09 | `+2024-01-01` | `2024-01-01` | accepted |
| d10 | `+0001-01-01` | `1-01-01` | accepted |
| d11 | `0001-01-01` | `1-01-01` | accepted |
| d12 | `9999-12-31` | `9999-12-31` | accepted |
| d13 | `10000-01-01` | rejected | `__floe_errors`: `cast_error` on `date` ("invalid value for target type") |
| d14 | `12024-01-01` | rejected | `__floe_errors`: `cast_error` on `date` ("invalid value for target type") |
| d15 | `99999-01-01` | rejected | `__floe_errors`: `cast_error` on `date` ("invalid value for target type") |
| d16 | `999-01-01` | `999-01-01` | accepted |
| d17 | `24-01-01` | `24-01-01` | accepted |
| d18 | `2024-02-30` | rejected | `__floe_errors`: `cast_error` on `date` ("invalid value for target type") |
| d19 | `2024-01-00` | rejected | `__floe_errors`: `cast_error` on `date` ("invalid value for target type") |
| d20 | `2024-00-10` | rejected | `__floe_errors`: `cast_error` on `date` ("invalid value for target type") |
| d21 | `2024-13-01` | rejected | `__floe_errors`: `cast_error` on `date` ("invalid value for target type") |
| d22 | `2024-04-31` | rejected | `__floe_errors`: `cast_error` on `date` ("invalid value for target type") |
| d23 | `2024-01-32` | rejected | `__floe_errors`: `cast_error` on `date` ("invalid value for target type") |
| d24 | `` | null (accepted) | accepted |
| d25 | `   ` | rejected | `__floe_errors`: `cast_error` on `date` ("invalid value for target type") |
| d26 | `2024-01-01x` | rejected | `__floe_errors`: `cast_error` on `date` ("invalid value for target type") |
| d27 | `x2024-01-01` | rejected | `__floe_errors`: `cast_error` on `date` ("invalid value for target type") |
| d28 | `20240101` | rejected | `__floe_errors`: `cast_error` on `date` ("invalid value for target type") |
| d29 | `05/01/2024` | rejected | `__floe_errors`: `cast_error` on `date` ("invalid value for target type") |
| d30 | `2024/01/01` | rejected | `__floe_errors`: `cast_error` on `date` ("invalid value for target type") |
| d31 | `2024-01-01T10:00:00` | rejected | `__floe_errors`: `cast_error` on `date` ("invalid value for target type") |
| d32 | `2024-01-01 10:00:00` | rejected | `__floe_errors`: `cast_error` on `date` ("invalid value for target type") |
| d33 | `abc` | rejected | `__floe_errors`: `cast_error` on `date` ("invalid value for target type") |
| d34 | `2024- 01-01` | `2024-01-01` | accepted |
| d35 | `2024-01- 01` | `2024-01-01` | accepted |
| d36 | `2024 -01-01` | `2024-01-01` | accepted |
| d37 | `-` | rejected | `__floe_errors`: `cast_error` on `date` ("invalid value for target type") |
| d38 | `2024-01` | rejected | `__floe_errors`: `cast_error` on `date` ("invalid value for target type") |
| d39 | `2024-01-01-01` | rejected | `__floe_errors`: `cast_error` on `date` ("invalid value for target type") |
| d40 | `2024-001-01` | rejected | `__floe_errors`: `cast_error` on `date` ("invalid value for target type") |
| d41 | `2024-01-001` | rejected | `__floe_errors`: `cast_error` on `date` ("invalid value for target type") |
| d42 | `1700000000` | rejected | `__floe_errors`: `cast_error` on `date` ("invalid value for target type") |

## Observed rules
- Month/day may be unpadded; spaces around the value and around the `-` separators are accepted.
- A leading `+` or `-` on the year is accepted; unsigned years take up to 4 digits (`10000-01-01` rejected, `+10000-01-01` accepted). Years below 4 digits are taken as-is (`24-01-01` is year 24).
- Empty string is null (accepted, null in the output). Whitespace-only is rejected.
- Impossible calendar days are rejected; no time part is allowed.

## Out-of-range years (d43-d46)
Rust accepts a signed year outside 1..9999: `0000-01-01` (d43), `-0001-01-01` (d44), `-2024-01-01` (d45), `+10000-01-01` (d46).
They are stored as the proleptic Gregorian day (`d43` = day -719528). `check.py` renders them through Arrow, not Python `date`.

## Looks like a Rust bug (recorded as is; candidates for an intentional delta in #619)
- Whitespace inside the value (`2024- 01-01`, `2024 -01-01`) is accepted.

## Intentional delta D9 (`delta.yml`, `expected_python/`)
Python rejects years outside 1..9999 and whitespace inside a value (#617 decision, D9), as a `cast_error` on `date`
with the same message as Rust. Rows changed: d34, d35, d36 (spaces inside), d43-d46 (year outside 1..9999).
Leading/trailing spaces (d06-d08) and the `+` sign (d09) are not touched by D9 and stay accepted.

`expected_python/` was derived from `expected/` of the same case (no Python run exists yet):
- `accepted/casts_date.parquet`: the seven rows removed, others unchanged;
- `rejected/casts_date/part-00000.csv`: for each of the seven rows, a copy of a Rust-rejected `date` row with that `id`
  and `__floe_row_index` (its 0-based line in `in/`), merged in row-index order;
- `reports/`: `accepted_total`/`accepted_rows`/`accepted_count` minus 7, `rejected_total`/`rejected_count`/`errors`/`violations`
  (global and the `date` column) plus 7. Writer byte metrics keep Rust's values (ignored on compare).
