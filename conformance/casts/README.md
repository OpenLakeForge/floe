# casts

A mixed bag of column types and values; severity `reject`.

## Python differs from Rust 0.7: D9
Python rejects `10:00:00garbage` (r12) and a space before the offset (r21) as a `cast_error` on `ts`, and applies the
`+02:00` offset of r8 (`ts` `08:00:00`), where Rust ignores all three. So r12 and r21 move from accepted to rejected
and r8's rejected line shows the shifted `ts`.
