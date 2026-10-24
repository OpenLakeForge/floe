# casts_coerce

The `casts_date` and `casts_datetime` inputs again, with `source.cast_mode: coerce`. Rust 0.7 accepts every row:
a value that is a `cast_error` in strict mode (see those READMEs) becomes null in the accepted Parquet, no row is
rejected, and no `__floe_errors` entry is written. Values that parse in strict mode parse identically here.
Ids `d..` vary `date`, `t..` vary `datetime`, `s..` vary `timestamp`, as in the strict cases.

## Intentional delta D9 (`delta.yml`, `expected_python/`)
Python does not tolerate the whitespace and offset forms Rust accepts (see `casts_date` and `casts_datetime`), so in
coerce mode those values differ from Rust: `date` d34-d36 (spaces inside) is null, a `datetime` offset is applied to
UTC (t21 is `08:00:00`), and the other datetime forms Rust accepts are null (t26, t27, t30, t44, t45, t51, t64, t65, t67).
`expected_python/accepted/casts_coerce.parquet` is the accepted table of a Python run (`uv run floe run -c config.yml`
in a copy of the case); the reports are unchanged, as no row is rejected in coerce mode.
