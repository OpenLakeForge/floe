# /// script
# requires-python = ">=3.10"
# dependencies = ["pyiceberg==0.12.0", "pyarrow==25.0.1", "polars==2.0.0", "boto3==1.43.117"]
# ///
"""Check a table after a run: exact row count, duplicate ids, snapshots, and orphaned files
(objects under the table location that no snapshot or metadata version references)."""

import argparse
import json
import sys

import polars as pl

from common import NAMESPACE, catalog, s3_client


def referenced(table) -> set[str]:
    io, meta = table.io, table.metadata
    refs = {e.metadata_file for e in meta.metadata_log} | {table.metadata_location}
    for snap in table.snapshots():
        refs.add(snap.manifest_list)
        for manifest in snap.manifests(io):
            refs.add(manifest.manifest_path)
            for entry in manifest.fetch_manifest_entry(io, discard_deleted=False):
                refs.add(entry.data_file.file_path)
    return refs


def listed(location: str) -> set[str]:
    bucket, _, prefix = location.removeprefix("s3://").partition("/")
    s3, keys = s3_client(), set()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix + "/"):
        keys |= {f"s3://{bucket}/{o['Key']}" for o in page.get("Contents", [])}
    return keys


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--table", required=True)
    p.add_argument("--expected-rows", type=int, required=True)
    a = p.parse_args()

    table = catalog().load_table(f"{NAMESPACE}.{a.table}")
    ids = pl.from_arrow(table.scan(selected_fields=("id",)).to_arrow())["id"]
    orphans = sorted(listed(table.location()) - referenced(table))
    out = {
        "table": a.table,
        "rows": len(ids),
        "distinct_ids": ids.n_unique(),
        "expected_rows": a.expected_rows,
        "snapshots": [
            {"op": s.summary.operation.value, "run_id": s.summary.additional_properties.get("floe.run_id")}
            for s in table.snapshots()
        ],
        "metadata_versions": len(table.metadata.metadata_log) + 1,
        "orphan_data_files": sum("/data/" in o for o in orphans),
        "orphan_metadata_files": sum("/metadata/" in o for o in orphans),
    }
    out["ok"] = out["rows"] == out["distinct_ids"] == a.expected_rows
    print(json.dumps(out))
    return 0 if out["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
