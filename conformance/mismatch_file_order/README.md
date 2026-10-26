# mismatch_file_order

`in/b.csv` misses the `name` column and is rejected by the mismatch precheck; `in/a.csv` is read normally.
Rust prechecks every input first, so the entity report lists `b.csv` before `a.csv` (#825).
