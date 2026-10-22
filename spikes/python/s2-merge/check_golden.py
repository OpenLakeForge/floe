"""Golden SCD1 / SCD2 check against Polaris, one table per (mode, partition spec).

For each case: create the target with PyIceberg, apply additive schema evolution (`tier`) with
PyIceberg, stage the batch as Parquet, run one DuckDB MERGE, then verify
  - exactly one new snapshot,
  - DuckDB and PyIceberg read back the golden expected rows (delete files honoured),
  - every data file holds only rows of its partition value,
  - rerun safety (same SCD1 batch -> no change; SCD2 rerun has no changed rows -> no new versions).
Exits non-zero if any check fails. Writes results/golden.json.
"""

import datetime as dt
import json
import pathlib
import sys
import tempfile
import traceback
import uuid

import pyarrow.parquet as pq
from pyiceberg.transforms import BucketTransform, DayTransform, IdentityTransform
from pyiceberg.types import StringType

import golden as g
import merge
from common import NAMESPACE, catalog, duck, fq

HERE = pathlib.Path(__file__).parent
SPECS = {
    "unpartitioned": [],
    "identity(region)": [("region", IdentityTransform())],
    "day(event_date)": [("event_date", DayTransform())],
    "bucket(4,id)": [("id", BucketTransform(4))],
}


def rows_duckdb(con, name, cols):
    cur = con.execute(f"SELECT {', '.join(cols)} FROM {fq(name)}")
    return sorted(cur.fetchall(), key=sort_key)


def rows_pyiceberg(tbl, cols):
    data = tbl.scan(selected_fields=tuple(cols)).to_arrow().select(cols)
    return sorted([tuple(r.values()) for r in data.to_pylist()], key=sort_key)


def sort_key(r):
    return tuple((v is None, str(v)) for v in r)


def resolve(rows, ts):
    return sorted([tuple(ts if v == g.T else v for v in r) for r in rows], key=sort_key)


def partitions_consistent(tbl, fields):
    """Each data file's partition tuple equals the transform of every row it holds."""
    if not fields:
        return True, "n/a"
    schema = tbl.schema()
    files = tbl.inspect.files()
    data_files = [f for f in files.to_pylist() if f["content"] == 0]
    for f in data_files:
        part = f["partition"]
        rows = pq.read_table(tbl.io.new_input(f["file_path"]).open()).to_pylist()
        for i, (col, tr) in enumerate(fields):
            fn = tr.transform(schema.find_field(col).field_type)
            want = part[f"{col}_{i}"]
            if isinstance(want, dt.date):  # inspect.files() renders day partitions as dates
                want = (want - dt.date(1970, 1, 1)).days
            got = {fn(r[col]) if not isinstance(tr, IdentityTransform) else r[col] for r in rows}
            if got != {want}:
                return False, f"{f['file_path']}: partition {want!r} holds {got!r}"
    return True, f"{len(data_files)} data files consistent"


def file_counts(tbl):
    files = tbl.inspect.files().to_pylist()
    return {
        "data_files": sum(f["content"] == 0 for f in files),
        "position_delete_files": sum(f["content"] == 1 for f in files),
        "equality_delete_files": sum(f["content"] == 2 for f in files),
    }


def run_case(cat, mode, spec_name, fields, stage_dir):
    name = f"golden_{mode}_{uuid.uuid4().hex[:6]}"
    business = g.BUSINESS
    target_schema = g.scd2_schema(business) if mode == "scd2" else business
    target_rows = g.SCD2_TARGET_ROWS if mode == "scd2" else g.TARGET_ROWS
    tbl = cat.create_table(f"{NAMESPACE}.{name}", schema=target_schema)
    with tbl.update_spec() as upd:
        for col, tr in fields:
            upd.add_field(col, tr, f"{col}_{fields.index((col, tr))}")
    checks = {}
    try:
        con = duck()  # opened before the schema change on purpose: a long-lived connection
        # Seeded with DuckDB, not PyIceberg: PyIceberg 0.12 needs pyiceberg-core (not an
        # approved spike dependency) to append to day- or bucket-partitioned tables.
        seed = stage_dir / f"{name}_target.parquet"
        pq.write_table(g.table(target_schema, target_rows), seed)
        con.execute(f"INSERT INTO {fq(name)} SELECT * FROM read_parquet('{seed}')")
        # Additive schema evolution with PyIceberg, before the merge (no snapshot).
        with tbl.update_schema() as us:
            us.add_column("tier", StringType())
        source = stage_dir / f"{name}.parquet"
        pq.write_table(g.table(g.BATCH, g.BATCH_ROWS), source)
        business_cols = g.BATCH.names
        cols = business_cols + ([f.name for f in g.SYSTEM] if mode == "scd2" else [])
        compare = [c for c in business_cols if c not in g.KEY]

        tbl.refresh()
        snaps_before = len(tbl.snapshots())
        commits_before = len(tbl.metadata.metadata_log)
        ts = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
        if mode == "scd1":
            merge.scd1(con, fq(name), str(source), g.KEY, business_cols)
            expected = resolve(g.EXPECTED_SCD1, ts)
        else:
            merge.scd2(con, fq(name), str(source), g.KEY, business_cols, compare, ts)
            expected = resolve(g.EXPECTED_SCD2, ts)
        tbl.refresh()
        new_snaps = len(tbl.snapshots()) - snaps_before
        ops = [s.summary.operation.value for s in tbl.snapshots()[snaps_before:]]
        checks["one_snapshot"] = (new_snaps == 1, f"{new_snaps} new snapshot(s), ops={ops}")
        # One catalog commit = one new metadata.json, i.e. readers of the current table see
        # the merge atomically even when it adds more than one snapshot.
        commits = len(tbl.metadata.metadata_log) - commits_before
        checks["one_catalog_commit"] = (commits == 1, f"{commits} new metadata file(s)")
        got_duck = rows_duckdb(con, name, cols)
        got_py = rows_pyiceberg(tbl, cols)
        checks["duckdb_matches_golden"] = (got_duck == expected, diff(expected, got_duck))
        checks["pyiceberg_matches_golden"] = (got_py == expected, diff(expected, got_py))
        checks["partitions_consistent"] = partitions_consistent(tbl, fields)
        counts_after_first = file_counts(tbl)
        if mode == "scd2":
            closed = sum(1 for r in got_py if r[-1] == ts)
            inserted = sum(1 for r in got_py if r[-2] == ts)
            unchanged = len(g.BATCH_ROWS) - inserted
            m = {"closed_count": closed, "inserted_count": inserted, "unchanged_count": unchanged}
            checks["scd2_metrics"] = (m == g.EXPECTED_SCD2_METRICS, json.dumps(m))

        # Rerun: same batch again. SCD1 -> no change; SCD2 -> nothing changed, no new versions.
        snaps_before = len(tbl.snapshots())
        ts2 = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
        if mode == "scd1":
            merge.scd1(con, fq(name), str(source), g.KEY, business_cols)
        else:
            merge.scd2(con, fq(name), str(source), g.KEY, business_cols, compare, ts2)
        tbl.refresh()
        got_py2 = rows_pyiceberg(tbl, cols)
        got_duck2 = rows_duckdb(con, name, cols)
        new_snaps = len(tbl.snapshots()) - snaps_before
        checks["rerun_no_change"] = (
            got_py2 == expected and got_duck2 == expected,
            f"rows identical={got_py2 == expected}, new snapshots on rerun={new_snaps}",
        )
        checks["file_counts"] = (True, json.dumps({"after_merge": counts_after_first, "after_rerun": file_counts(tbl)}))
    except Exception as e:  # noqa: BLE001 - a spike records the failure, it doesn't hide it
        checks["error"] = (False, f"{type(e).__name__}: {e}".splitlines()[0][:400])
        traceback.print_exc()
    finally:
        cat.drop_table(f"{NAMESPACE}.{name}")
    return checks


def diff(expected, got):
    if expected == got:
        return f"{len(got)} rows identical"
    missing = [r for r in expected if r not in got]
    extra = [r for r in got if r not in expected]
    return f"missing={missing} extra={extra}"


def main():
    cat = catalog()
    results = {}
    ok = True
    with tempfile.TemporaryDirectory() as d:
        for mode in ("scd1", "scd2"):
            for spec_name, fields in SPECS.items():
                checks = run_case(cat, mode, spec_name, fields, pathlib.Path(d))
                results[f"{mode} / {spec_name}"] = {k: {"pass": v[0], "detail": v[1]} for k, v in checks.items()}
                for k, (passed, detail) in checks.items():
                    ok &= passed
                    print(f"{'PASS' if passed else 'FAIL'}  {mode:4} {spec_name:18} {k:26} {detail}")
    out = HERE / "results" / "golden.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(results, indent=2, default=str) + "\n")
    print("OK: all golden checks pass" if ok else "FAIL: see above")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
