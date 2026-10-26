# precheck_header_order

An empty `a.csv` (unreadable header) and a `b.csv` missing the `name` column (mismatch `reject_file`): Rust settles the header failure in its precheck, so both stay in input order, `a.csv` first (#825, #896).
