"""Row-level read parity: PyIceberg and DuckDB must return the same rows (count + order-independent hash)."""
import duckdb
from common import duck, catalog

H = "hash(id, cat, d, amount, payload)::HUGEINT"


def duckdb_fp(con, name, snapshot_id=None):
    at = f" AT (VERSION => {snapshot_id})" if snapshot_id else ""
    return tuple(con.execute(f"SELECT count(*), sum({H}), sum(id) FROM polaris.{name}{at}").fetchone())


def pyiceberg_fp(tbl, snapshot_id=None):
    """PyIceberg read (positional deletes applied by PyIceberg), one file task at a time: the whole-scan
    `to_arrow_batch_reader()` read ahead ~14 GB RSS at 500M and was OOM-killed."""
    from pyiceberg.io.pyarrow import ArrowScan

    scan = tbl.scan(snapshot_id=snapshot_id)
    c = duckdb.connect()
    c.execute("SET memory_limit='1GB'")
    n, h, s = 0, 0, 0
    arrow_scan = ArrowScan(tbl.metadata, tbl.io, scan.projection(), scan.row_filter, scan.case_sensitive)
    for task in scan.plan_files():
        for batch in arrow_scan.to_record_batches([task]):
            r = c.execute(f"SELECT count(*), sum({H}), sum(id) FROM batch").fetchone()
            n, h, s = n + r[0], h + (r[1] or 0), s + (r[2] or 0)
    return (n, h, s)


def parity(cat, name, con, snapshot_id=None):
    tbl = cat.load_table(name)
    d, p = duckdb_fp(con, name, snapshot_id), pyiceberg_fp(tbl, snapshot_id)
    return d, p, d == p
