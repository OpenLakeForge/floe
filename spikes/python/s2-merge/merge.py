"""SCD1 / SCD2 as one DuckDB MERGE INTO an Iceberg table, source = staged Parquet.

Semantics follow crates/floe-core/src/io/write/strategy/merge/{scd1,scd2,shared}.rs:
- merge predicate: equality on every key column;
- change predicate: null-safe inequality on the compare columns (scd2_changed_predicate);
- SCD2 closes the current version (is_current=false, valid_to=ts) and inserts the new one
  (is_current=true, valid_from=ts, valid_to=NULL). Rust does that in two Delta merges (two
  versions); here it is one MERGE, so one Iceberg snapshot: each changed row appears twice in
  the source, once with its key (matches and closes the current version) and once with a NULL
  merge key (never matches, so it is inserted).
"""

SYS = ("__floe_is_current", "__floe_valid_from", "__floe_valid_to")


def q(col):
    return '"' + col.replace('"', '""') + '"'


def changed(compare, left="t", right="s"):
    if not compare:
        return "false"
    return " OR ".join(f"{left}.{q(c)} IS DISTINCT FROM {right}.{q(c)}" for c in compare)


def scd1(con, target, source, key, columns):
    """Update non-key columns on key match, insert missing keys.

    Rust updates every matched row; here the update is guarded by the change predicate so an
    identical rerun writes nothing. The resulting rows are the same either way.
    """
    update = [c for c in columns if c not in key]
    on = " AND ".join(f"t.{q(k)} = s.{q(k)}" for k in key)
    sql = f"""
        MERGE INTO {target} t
        USING read_parquet('{source}') s
        ON {on}
        WHEN MATCHED AND ({changed(update)}) THEN UPDATE SET
            {", ".join(f"{q(c)} = s.{q(c)}" for c in update)}
        WHEN NOT MATCHED THEN INSERT ({", ".join(map(q, columns))})
            VALUES ({", ".join(f"s.{q(c)}" for c in columns)})
    """
    return con.execute(sql).fetchall()


def scd2(con, target, source, key, columns, compare, ts, sys_cols=SYS):
    is_current, valid_from, valid_to = map(q, sys_cols)
    mk = [f"__mk_{i}" for i in range(len(key))]
    src_cols = ", ".join(f"s.{q(c)}" for c in columns)
    ts_lit = f"TIMESTAMP '{ts.isoformat(sep=' ')}'"
    # Changed rows of the batch: they get a second source row with a NULL merge key.
    changed_rows = f"""
        SELECT {", ".join(f"NULL AS {m}" for m in mk)}, {src_cols}
        FROM read_parquet('{source}') s
        JOIN {target} t
          ON {" AND ".join(f"t.{q(k)} = s.{q(k)}" for k in key)} AND t.{is_current}
        WHERE {changed(compare)}
    """
    sql = f"""
        MERGE INTO {target} t
        USING (
            SELECT {", ".join(f"s.{q(k)} AS {m}" for k, m in zip(key, mk))}, {src_cols}
            FROM read_parquet('{source}') s
            UNION ALL
            {changed_rows}
        ) s
        ON {" AND ".join(f"t.{q(k)} = s.{m}" for k, m in zip(key, mk))} AND t.{is_current}
        WHEN MATCHED AND ({changed(compare)}) THEN UPDATE SET
            {is_current} = false, {valid_to} = {ts_lit}
        WHEN NOT MATCHED THEN INSERT ({", ".join(map(q, columns))}, {is_current}, {valid_from}, {valid_to})
            VALUES ({src_cols}, true, {ts_lit}, NULL)
    """
    return con.execute(sql).fetchall()
