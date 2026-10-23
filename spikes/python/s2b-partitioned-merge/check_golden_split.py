"""#598's SCD1/SCD2 golden fixture, run through the #609 approach: the batch is split by the
target's partition value and each partition gets its own MERGE, all inside one DuckDB transaction.

Reuses `../s2-merge/check_golden.py` unchanged (same tables, schema evolution, checks): only the
`merge` module it calls is swapped for the shim below. Per the #594 ruling the requirement is one
catalog commit per run, so `one_snapshot` is reported but not required.

Writes results/golden-split.json and results/golden-split.txt; exits non-zero on any failure.
"""

import json
import pathlib
import sys
import tempfile
import types

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "s2-merge"))

import pyarrow.compute as pc  # noqa: E402 - after the sys.path insert for ../s2-merge
import pyarrow.parquet as pq  # noqa: E402 - after the sys.path insert for ../s2-merge
from pyiceberg.transforms import BucketTransform, DayTransform, IdentityTransform  # noqa: E402 - after the sys.path insert for ../s2-merge

import check_golden  # noqa: E402 - after the sys.path insert for ../s2-merge
import merge  # noqa: E402 - after the sys.path insert for ../s2-merge
from common import NAMESPACE, catalog  # noqa: E402 - after the sys.path insert for ../s2-merge

INFORMATIONAL = {"one_snapshot", "file_counts"}
STATEMENTS = []  # MERGE statements per call, for the report


def split_by_partition(target, source):
    """One staged file per partition value of the target's spec (one file if unpartitioned)."""
    tbl = catalog().load_table(f"{NAMESPACE}.{target.rsplit('.', 1)[1]}")
    data = pq.read_table(source)
    fields = tbl.spec().fields
    if not fields:
        return [source]
    (f,) = fields
    col = tbl.schema().find_field(f.source_id).name
    if isinstance(f.transform, BucketTransform):
        from pyiceberg_core import transform

        part = transform.bucket(data.column(col).combine_chunks(), f.transform.num_buckets)
    elif isinstance(f.transform, DayTransform):
        part = pc.cast(pc.cast(data.column(col), "date32"), "int32")
    elif isinstance(f.transform, IdentityTransform):
        part = data.column(col)
    else:
        raise SystemExit(f"unsupported transform {f.transform}")
    out = []
    for i, v in enumerate(pc.unique(part).to_pylist()):
        mask = pc.is_null(part) if v is None else pc.fill_null(pc.equal(part, v), False)
        p = f"{source}.part{i}.parquet"
        pq.write_table(data.filter(mask), p)
        out.append(p)
    return out


def per_partition(fn):
    def run(con, target, source, *a, **kw):
        parts = split_by_partition(target, source)
        STATEMENTS.append(len(parts))
        con.execute("BEGIN")
        try:
            res = [fn(con, target, p, *a, **kw) for p in parts]
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        return res

    return run


def main():
    check_golden.merge = types.SimpleNamespace(scd1=per_partition(merge.scd1), scd2=per_partition(merge.scd2))
    cat = catalog()
    results, lines, ok = {}, [], True
    with tempfile.TemporaryDirectory() as d:
        for mode in ("scd1", "scd2"):
            for spec_name, fields in check_golden.SPECS.items():
                STATEMENTS.clear()
                checks = check_golden.run_case(cat, mode, spec_name, fields, pathlib.Path(d))
                checks["merge_statements_per_run"] = (True, str(STATEMENTS))
                results[f"{mode} / {spec_name}"] = {k: {"pass": v[0], "detail": v[1]} for k, v in checks.items()}
                for k, (passed, detail) in checks.items():
                    status = "INFO" if k in INFORMATIONAL else ("PASS" if passed else "FAIL")
                    ok &= passed or k in INFORMATIONAL
                    lines.append(f"{status}  {mode:4} {spec_name:18} {k:26} {detail}")
                    print(lines[-1], flush=True)
    lines.append("OK: all golden checks pass (one catalog commit per run)" if ok else "FAIL: see above")
    print(lines[-1])
    (HERE / "results").mkdir(exist_ok=True)
    (HERE / "results" / "golden-split.json").write_text(json.dumps(results, indent=2, default=str) + "\n")
    (HERE / "results" / "golden-split.txt").write_text("\n".join(lines) + "\n")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
