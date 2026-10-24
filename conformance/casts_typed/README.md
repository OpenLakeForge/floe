# casts_typed

Native (Parquet) columns cast to each declared type by Rust 0.7 (`cast_with_options(NonStrict)`), `cast_mode: strict`.
One entity per declared type (`typed_<type>`), each declaring the same eight columns of `in/casts_typed.parquet`
(`i64`, `u64`, `f64`, `b`, `d`, `ts` naive, `tstz` UTC, `tzp` Asia/Kolkata); row `r1` holds 300, 2^63, 2.5, false and the epoch.
`typed_boolean` omits the date and timestamp columns: Rust aborts the whole run on them ("casting from Date to Boolean not supported").
`uint8` is not a case: Rust 0.7 aborts on any `uint8` column ("cannot create series from UInt8").
Out-of-range values become `cast_error`, a float with a fraction is truncated (2.5 becomes 2) and accepted.

## Intentional delta D17 (`delta.yml`, `expected_python/`)
In strict mode Python rejects a float with a fractional part declared as any integer type (`f64` 2.5 in `r1`), as a
`cast_error` on `f64` with the same message. 2.0, NaN and inf are unchanged.

`expected_python/` was derived from `expected/` (no Python run exists yet), for `typed_int8`, `int32`, `int64`, `uint32`, `uint64`:
- `rejected/`: an `f64` entry added to `r1`'s `__floe_errors` (schema column order); `typed_uint64` had accepted `r1`, so it
  gains the row (cells as in the other integer cases: `u64` 9223372036854775808);
- `accepted/typed_uint64.parquet`: `r1` removed;
- `reports/`: `errors_total`/`errors`/`violations` and the `f64` column violations plus 1 per entity, and for `typed_uint64`
  accepted minus 1 and rejected plus 1 (entity and global). Writer byte metrics keep Rust's values (ignored on compare).
