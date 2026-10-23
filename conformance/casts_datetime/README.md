# casts_datetime

String -> `datetime` (and its alias `timestamp`) cast as done by Rust 0.7 (Polars 0.52, chrono partial parse of
`%Y-%m-%dT%H:%M:%S%.f%:z`), `cast_mode: strict` (default). Ids `t..` vary the `datetime` column, ids `s..` the
`timestamp` column (same rules, only error column differs).
Each row varies one column; the others hold a valid filler (`date` 2024-01-01, `datetime`/`timestamp` 2024-01-01T10:00:00).
Accepted values are shown as stored in the accepted Parquet (`date` as a calendar day, `datetime` as milliseconds, rendered ISO).
Rejected rows go to `expected/rejected` with `__floe_errors` = one `cast_error` entry for the column.

| id | input | Rust 0.7 | outcome |
|---|---|---|---|
| t00 | `2024-01-01T10:00:00` | `2024-01-01T10:00:00.000` | accepted |
| t01 | `2024-01-01 10:00:00` | rejected | `__floe_errors`: `cast_error` on `datetime` ("invalid value for target type") |
| t02 | `2024-01-01T10:00` | `2024-01-01T10:00:00.000` | accepted |
| t03 | `2024-01-01 10:00` | rejected | `__floe_errors`: `cast_error` on `datetime` ("invalid value for target type") |
| t04 | `2024-01-01T10` | rejected | `__floe_errors`: `cast_error` on `datetime` ("invalid value for target type") |
| t05 | `2024-01-01T10:00:00.1` | `2024-01-01T10:00:00.100` | accepted |
| t06 | `2024-01-01T10:00:00.12` | `2024-01-01T10:00:00.120` | accepted |
| t07 | `2024-01-01T10:00:00.123` | `2024-01-01T10:00:00.123` | accepted |
| t08 | `2024-01-01T10:00:00.1234` | `2024-01-01T10:00:00.123` | accepted |
| t09 | `2024-01-01T10:00:00.12345` | `2024-01-01T10:00:00.123` | accepted |
| t10 | `2024-01-01T10:00:00.123456` | `2024-01-01T10:00:00.123` | accepted |
| t11 | `2024-01-01T10:00:00.1234567` | `2024-01-01T10:00:00.123` | accepted |
| t12 | `2024-01-01T10:00:00.12345678` | `2024-01-01T10:00:00.123` | accepted |
| t13 | `2024-01-01T10:00:00.123456789` | `2024-01-01T10:00:00.123` | accepted |
| t14 | `2024-01-01T10:00:00.1234567891` | `2024-01-01T10:00:00.123` | accepted |
| t15 | `2024-01-01T10:00:00.` | `2024-01-01T10:00:00.000` | accepted |
| t16 | `2024-01-01 10:00:00.5` | rejected | `__floe_errors`: `cast_error` on `datetime` ("invalid value for target type") |
| t17 | `2024-01-01T10:00:00Z` | `2024-01-01T10:00:00.000` | accepted |
| t18 | `2024-01-01 10:00:00Z` | rejected | `__floe_errors`: `cast_error` on `datetime` ("invalid value for target type") |
| t19 | `2024-01-01T10:00:00.5Z` | `2024-01-01T10:00:00.500` | accepted |
| t20 | `2024-01-01T10:00Z` | `2024-01-01T10:00:00.000` | accepted |
| t21 | `2024-01-01T10:00:00+02:00` | `2024-01-01T10:00:00.000` | accepted |
| t22 | `2024-01-01T10:00:00-05:30` | `2024-01-01T10:00:00.000` | accepted |
| t23 | `2024-01-01T10:00:00+0200` | `2024-01-01T10:00:00.000` | accepted |
| t24 | `2024-01-01T10:00:00+02` | `2024-01-01T10:00:00.000` | accepted |
| t25 | `2024-01-01T10:00:00.5+02:00` | `2024-01-01T10:00:00.500` | accepted |
| t26 | `2024-01-01T10:00:00 +02:00` | `2024-01-01T10:00:00.000` | accepted |
| t27 | `2024-01-01T10:00:00 Z` | `2024-01-01T10:00:00.000` | accepted |
| t28 | `2024-01-01T10:00+02:00` | `2024-01-01T10:00:00.000` | accepted |
| t29 | `2024-01-01T10:00:60` | `2024-01-01T10:01:00.000` | accepted |
| t30 | `2024-01-01T10:00:61` | `2024-01-01T10:00:00.000` | accepted |
| t31 | `2024-01-01T23:59:60` | `2024-01-02T00:00:00.000` | accepted |
| t32 | `2024-01-01T10:00:60.5` | `2024-01-01T10:01:00.500` | accepted |
| t33 | `2024-12-31T23:59:60` | `2025-01-01T00:00:00.000` | accepted |
| t34 | `2024-01-01T24:00:00` | rejected | `__floe_errors`: `cast_error` on `datetime` ("invalid value for target type") |
| t35 | `2024-01-01T24:00` | rejected | `__floe_errors`: `cast_error` on `datetime` ("invalid value for target type") |
| t36 | `2024-01-01T23:60:00` | rejected | `__floe_errors`: `cast_error` on `datetime` ("invalid value for target type") |
| t37 | `2024-01-01T10:60` | rejected | `__floe_errors`: `cast_error` on `datetime` ("invalid value for target type") |
| t38 | `2024-01-01T25:00:00` | rejected | `__floe_errors`: `cast_error` on `datetime` ("invalid value for target type") |
| t39 | `2024-01-01` | rejected | `__floe_errors`: `cast_error` on `datetime` ("invalid value for target type") |
| t40 | `2024-01-01T` | rejected | `__floe_errors`: `cast_error` on `datetime` ("invalid value for target type") |
| t41 | `2024-01-01 ` | rejected | `__floe_errors`: `cast_error` on `datetime` ("invalid value for target type") |
| t42 | ` 2024-01-01T10:00:00` | `2024-01-01T10:00:00.000` | accepted |
| t43 | `2024-01-01T10:00:00 ` | `2024-01-01T10:00:00.000` | accepted |
| t44 | `2024-01-01T10:00:00garbage` | `2024-01-01T10:00:00.000` | accepted |
| t45 | `2024-01-01T10:00:00.5garbage` | `2024-01-01T10:00:00.500` | accepted |
| t46 | `2024-01-01t10:00:00` | rejected | `__floe_errors`: `cast_error` on `datetime` ("invalid value for target type") |
| t47 | `2024-01-01T1:2:3` | `2024-01-01T01:02:03.000` | accepted |
| t48 | `2024-01-01T10:00:5` | `2024-01-01T10:00:05.000` | accepted |
| t49 | `2024-1-5T1:02` | `2024-01-05T01:02:00.000` | accepted |
| t50 | `2024-01-01  10:00:00` | rejected | `__floe_errors`: `cast_error` on `datetime` ("invalid value for target type") |
| t51 | `2024-01-01T 10:00:00` | `2024-01-01T10:00:00.000` | accepted |
| t52 | `+2024-01-01T10:00:00` | `2024-01-01T10:00:00.000` | accepted |
| t53 | `0001-01-01T00:00:00` | `1-01-01T00:00:00.000` | accepted |
| t54 | `9999-12-31T23:59:59` | `9999-12-31T23:59:59.000` | accepted |
| t55 | `10000-01-01T00:00:00` | rejected | `__floe_errors`: `cast_error` on `datetime` ("invalid value for target type") |
| t56 | `12024-01-01T10:00:00` | rejected | `__floe_errors`: `cast_error` on `datetime` ("invalid value for target type") |
| t57 | `24-01-01T10:00:00` | `24-01-01T10:00:00.000` | accepted |
| t58 | `2024-02-30T10:00:00` | rejected | `__floe_errors`: `cast_error` on `datetime` ("invalid value for target type") |
| t59 | `2024-13-01T10:00:00` | rejected | `__floe_errors`: `cast_error` on `datetime` ("invalid value for target type") |
| t60 | `2024-01-00T10:00:00` | rejected | `__floe_errors`: `cast_error` on `datetime` ("invalid value for target type") |
| t61 | `1700000000` | rejected | `__floe_errors`: `cast_error` on `datetime` ("invalid value for target type") |
| t62 | `` | null (accepted) | accepted |
| t63 | `abc` | rejected | `__floe_errors`: `cast_error` on `datetime` ("invalid value for target type") |
| t64 | `2024-01-01T10:00:00,123` | `2024-01-01T10:00:00.000` | accepted |
| t65 | `2024-01-01T10:00:00:00` | `2024-01-01T10:00:00.000` | accepted |
| t66 | `2024-01-01T10-00-00` | rejected | `__floe_errors`: `cast_error` on `datetime` ("invalid value for target type") |
| t67 | `2024-01-01T10:00:00.123.456` | `2024-01-01T10:00:00.123` | accepted |
| t68 | `2024/01/01 10:00:00` | rejected | `__floe_errors`: `cast_error` on `datetime` ("invalid value for target type") |
| t69 | `20240101T100000` | rejected | `__floe_errors`: `cast_error` on `datetime` ("invalid value for target type") |
| s00 | `2024-01-01 10:00:00` | rejected | `__floe_errors`: `cast_error` on `timestamp` ("invalid value for target type") |
| s01 | `2024-01-01T10:00` | `2024-01-01T10:00:00.000` | accepted |
| s02 | `2024-01-01T10:00:00.123456789` | `2024-01-01T10:00:00.123` | accepted |
| s03 | `2024-01-01T10:00:00Z` | `2024-01-01T10:00:00.000` | accepted |
| s04 | `2024-01-01T10:00:60` | `2024-01-01T10:01:00.000` | accepted |
| s05 | `2024-01-01` | rejected | `__floe_errors`: `cast_error` on `timestamp` ("invalid value for target type") |
| s06 | `2024-13-01T10:00:00` | rejected | `__floe_errors`: `cast_error` on `timestamp` ("invalid value for target type") |
| s07 | `abc` | rejected | `__floe_errors`: `cast_error` on `timestamp` ("invalid value for target type") |
| s08 | `` | null (accepted) | accepted |

## Observed rules
- Separator must be an uppercase `T` (a space or `t` is rejected). Hour and minute are required; seconds are optional. Fields may be unpadded.
- Fraction: 0 to any number of digits, truncated to milliseconds (no rounding).
- Offsets (`Z`, `+02:00`, `+0200`, `+02`, with or without a leading space) are accepted and ignored: the wall-clock time is stored unchanged, never converted to UTC.
- Second `60` rolls over to the next minute (`23:59:60` -> next day 00:00:00). Hour 24 and minute 60 are rejected.
- Date-only input is rejected. Empty string is null (accepted).

## Accepted by Rust but not in this case
Same pyarrow limit as casts_date; run once against the Rust 0.7 binary: `-2024-01-01T10:00:00` -> `-2024-01-02T10:00:00.000`,
`0000-01-01T00:00:00` -> `0-01-02T00:00:00.000`, `+10000-01-01T00:00:00` -> `10000-01-01T00:00:00.000`,
`9999-12-31T23:59:60` -> `10000-01-01T00:00:00.000`. (`10000-01-01T00:00:00` without a sign is rejected and is in the case.)

## Looks like a Rust bug (recorded as is; candidates for an intentional delta in #619)
- Anything after a valid `HH:MM[:SS[.f]]` prefix is ignored: `10:00:00garbage`, `10:00:00:00`, `10:00:00,123`, `.123.456` are accepted.
- Second `61` (and above?) is not rejected: it is dropped, giving `10:00:00` instead of an error.
- `T 10:00:00` (space after the `T`) is accepted.
- Years <= 0 are one day late (see casts_date and below).
- `9999-12-31T23:59:60` rolls over to year 10000, which unsigned `10000-...` input itself cannot express.
- Offsets are silently ignored rather than applied.
