# casts_coerce

The `casts_date` and `casts_datetime` inputs again, with `source.cast_mode: coerce`. Rust 0.7 accepts every row:
a value that is a `cast_error` in strict mode (see those READMEs) becomes null in the accepted Parquet, no row is
rejected, and no `__floe_errors` entry is written. Values that parse in strict mode parse identically here.
Ids `d..` vary `date`, `t..` vary `datetime`, `s..` vary `timestamp`, as in the strict cases.
