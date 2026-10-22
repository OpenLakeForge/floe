"""Exact unique check of a staged Parquet batch against an Iceberg table's history, in DuckDB.

Same result as Floe's Rust `UniqueTracker` seeded from the target (see reference.py): a row with a
NULL key column is skipped; every row whose key is in the history is flagged `target`; of the
remaining rows, every occurrence of a key after its first one in the batch is flagged `batch`.

Only the incoming keys are held in memory (DuckDB spills them past `memory_limit`). The history is
streamed through a join against the distinct incoming keys, reading its key columns only.
"""


def _key_expr(col, dtype):
    # Rust compares floats by bit pattern (-0.0 != 0.0); DuckDB's `=` does not. The shortest
    # round-trip text of a double is distinct per value. ponytail: every NaN maps to 'nan', so NaNs
    # with different payload bits compare equal here but not in Rust; upgrade path is a bit-cast.
    if dtype in ("DOUBLE", "FLOAT"):
        return f'"{col}"::VARCHAR'
    return f'"{col}"'


def unique_check(con, history, incoming_path, columns, out_path):
    """Writes the flagged incoming rows to `out_path` as Parquet (row_idx, kind)."""
    types = dict(con.execute(f"SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet('{incoming_path}'))").fetchall())
    keys = [f"k{i}" for i in range(len(columns))]
    select_keys = ", ".join(f"{_key_expr(c, types[c])} AS {k}" for c, k in zip(columns, keys))
    not_null = " AND ".join(f'"{c}" IS NOT NULL' for c in columns)
    key_list = ", ".join(keys)
    con.execute(f"""
        COPY (
            WITH inc AS (
                SELECT file_row_number AS row_idx, {select_keys}
                FROM read_parquet('{incoming_path}', file_row_number = true)
                WHERE {not_null}
            ),
            batch_keys AS (SELECT DISTINCT {key_list} FROM inc),
            hist AS (SELECT {select_keys} FROM {history} WHERE {not_null}),
            hits AS (SELECT DISTINCT {", ".join(f"b.{k}" for k in keys)}, true AS in_target
                     FROM batch_keys b JOIN hist h ON {" AND ".join(f"b.{k} = h.{k}" for k in keys)}),
            ranked AS (
                SELECT row_idx, {key_list}, row_number() OVER (PARTITION BY {key_list} ORDER BY row_idx) AS rn
                FROM inc
            )
            SELECT r.row_idx, CASE WHEN x.in_target THEN 'target' ELSE 'batch' END AS kind
            FROM ranked r LEFT JOIN hits x USING ({key_list})
            WHERE x.in_target OR r.rn > 1
            ORDER BY r.row_idx
        ) TO '{out_path}' (FORMAT parquet)
    """)
