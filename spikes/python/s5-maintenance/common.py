"""Shared helpers for the S5 maintenance spike: catalog, DuckDB, S3, VmHWM. Throwaway spike code."""

import json
import os
import re
import time
from contextlib import contextmanager

import boto3
import duckdb
from botocore.config import Config
from pyiceberg.catalog import load_catalog

# Must match ../harness/docker-compose.yml and bootstrap.py.
CATALOG_URI = "http://localhost:8181/api/catalog"
WAREHOUSE = "spike"
NAMESPACE = "spike"
CLIENT_ID, CLIENT_SECRET = "spike-client", "spike-secret"
S3_ENDPOINT = "localhost:8333"
S3_KEY, S3_SECRET, S3_REGION = "spike", "spike-s3-secret", "us-east-1"
BUCKET = "warehouse"


def catalog():
    return load_catalog(
        "polaris",
        type="rest",
        uri=CATALOG_URI,
        warehouse=WAREHOUSE,
        credential=f"{CLIENT_ID}:{CLIENT_SECRET}",
        scope="PRINCIPAL_ROLE:ALL",
        **{
            "header.X-Iceberg-Access-Delegation": "",
            "s3.endpoint": f"http://{S3_ENDPOINT}",
            "s3.access-key-id": S3_KEY,
            "s3.secret-access-key": S3_SECRET,
            "s3.region": S3_REGION,
            "s3.force-virtual-addressing": "false",
        },
    )


def duck(memory_limit="4GB", threads=4, spill="/tmp/s5-duck-spill"):
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute(f"SET threads={threads}")
    os.makedirs(spill, exist_ok=True)
    con.execute(f"SET temp_directory='{spill}'")
    con.execute(f"""CREATE SECRET polaris (TYPE iceberg, CLIENT_ID '{CLIENT_ID}', CLIENT_SECRET '{CLIENT_SECRET}',
        OAUTH2_SERVER_URI '{CATALOG_URI}/v1/oauth/tokens', OAUTH2_SCOPE 'PRINCIPAL_ROLE:ALL')""")
    con.execute(f"""CREATE SECRET store (TYPE s3, KEY_ID '{S3_KEY}', SECRET '{S3_SECRET}', REGION '{S3_REGION}',
        ENDPOINT '{S3_ENDPOINT}', URL_STYLE 'path', USE_SSL false)""")
    con.execute(f"""ATTACH '{WAREHOUSE}' AS polaris (TYPE iceberg, SECRET polaris, ENDPOINT '{CATALOG_URI}',
        ACCESS_DELEGATION_MODE 'none')""")
    return con


def s3():
    return boto3.client(
        "s3", endpoint_url=f"http://{S3_ENDPOINT}", aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET, region_name=S3_REGION,
        config=Config(s3={"addressing_style": "path"}),
    )


def list_objects(prefix):
    """All objects under an s3://warehouse/<prefix> key prefix."""
    out = []
    for page in s3().get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        out.extend(page.get("Contents", []))
    return out


def key_of(location):
    m = re.match(r"s3a?://[^/]+/(.*)", location)
    return m.group(1) if m else location


def vmhwm_mb():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmHWM"):
                return int(line.split()[1]) / 1024
    return float("nan")


def vmrss_mb():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS"):
                return int(line.split()[1]) / 1024
    return float("nan")


@contextmanager
def timed(label, sink=None):
    t = time.time()
    yield
    d = time.time() - t
    if sink is not None:
        sink[label] = round(d, 2)
    print(f"[time] {label}: {d:.1f}s", flush=True)


def file_counts(tbl):
    """(data files, position-delete files, equality-delete files, total data bytes) of the current snapshot."""
    import pyarrow.compute as pc

    af = tbl.inspect.files()
    c = af.column("content").to_pylist()
    sizes = af.column("file_size_in_bytes").to_pylist()
    data = sum(1 for x in c if x == 0)
    pos = sum(1 for x in c if x == 1)
    eq = sum(1 for x in c if x == 2)
    return {"data_files": data, "pos_delete_files": pos, "eq_delete_files": eq,
            "data_bytes": sum(s for s, x in zip(sizes, c) if x == 0),
            "records_in_data": sum(r for r, x in zip(af.column("record_count").to_pylist(), c) if x == 0),
            "records_in_pos_deletes": sum(r for r, x in zip(af.column("record_count").to_pylist(), c) if x == 1)}


def emit(path, rec):
    with open(path, "a") as f:
        f.write(json.dumps(rec) + "\n")


def drop_and_purge(cat, name):
    """Polaris purge is disabled (#598): drop the table, then delete its objects."""
    try:
        loc = cat.load_table(name).location()
    except Exception:
        return
    cat.drop_table(name)
    c = s3()
    prefix = key_of(loc).rstrip("/") + "/"
    for page in c.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objs:
            c.delete_objects(Bucket=BUCKET, Delete={"Objects": objs})
