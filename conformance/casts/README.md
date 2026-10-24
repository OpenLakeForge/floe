# casts

A mixed bag of column types and values; severity `reject`.

## Intentional delta D9 (`delta.yml`, `expected_python/`)
Python rejects `10:00:00garbage` (r12) and a space before the offset (r21) as a `cast_error` on `ts`, and applies the
`+02:00` offset of r8 (`ts` `08:00:00`), where Rust ignores all three. So r12 and r21 move from accepted to rejected
and r8's rejected line shows the shifted `ts`.

`expected_python/` was derived by running Python (`uv run floe run -c config.yml` in a copy of the case) and checking
that the diff against `expected/` is exactly those rows: the accepted table loses r12 and r21; the rejected CSV gains
their lines and changes r8's `ts`; `run.json` and `run.summary.json` differ only in `accepted_*` (-2), `rejected_*`
(+2), `errors` (+2) and the `ts` column's `cast_error` violations (+2).
