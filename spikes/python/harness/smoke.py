# /// script
# requires-python = ">=3.10"
# dependencies = ["pyiceberg==0.12.0", "pyarrow==25.0.1", "duckdb==1.5.6"]
# ///
"""Smoke test for the spike harness.

Writes 3 rows to a fresh Iceberg table through Polaris with PyIceberg, then reads them back with
PyIceberg and with DuckDB. Exits non-zero if either read fails or they disagree.
"""

import sys
import uuid

import duckdb
import pyarrow as pa
from pyiceberg.catalog import load_catalog

# Must match docker-compose.yml and bootstrap.py.
CATALOG_URI = "http://localhost:8181/api/catalog"
WAREHOUSE = "spike"
NAMESPACE = "spike"
CLIENT_ID = "spike-client"
CLIENT_SECRET = "spike-secret"
S3_ENDPOINT = "localhost:8333"
S3_KEY = "spike"
S3_SECRET = "spike-s3-secret"
S3_REGION = "us-east-1"


def main() -> int:
    catalog = load_catalog(
        "polaris",
        type="rest",
        uri=CATALOG_URI,
        warehouse=WAREHOUSE,
        credential=f"{CLIENT_ID}:{CLIENT_SECRET}",
        scope="PRINCIPAL_ROLE:ALL",
        # The catalog vends no credentials (stsUnavailable), so read and write S3 directly.
        **{
            "header.X-Iceberg-Access-Delegation": "",
            "s3.endpoint": f"http://{S3_ENDPOINT}",
            "s3.access-key-id": S3_KEY,
            "s3.secret-access-key": S3_SECRET,
            "s3.region": S3_REGION,
            "s3.force-virtual-addressing": "false",
        },
    )

    rows = pa.table({"id": pa.array([1, 2, 3], pa.int64()), "name": ["a", "b", "c"]})
    name = f"smoke_{uuid.uuid4().hex[:8]}"
    table = catalog.create_table(f"{NAMESPACE}.{name}", schema=rows.schema)
    try:
        table.append(rows)
        expected = sorted(rows.to_pylist(), key=lambda r: r["id"])

        from_pyiceberg = sorted(table.scan().to_arrow().to_pylist(), key=lambda r: r["id"])

        con = duckdb.connect()
        con.execute(f"""
            CREATE SECRET polaris (
                TYPE iceberg, CLIENT_ID '{CLIENT_ID}', CLIENT_SECRET '{CLIENT_SECRET}',
                OAUTH2_SERVER_URI '{CATALOG_URI}/v1/oauth/tokens', OAUTH2_SCOPE 'PRINCIPAL_ROLE:ALL')
        """)
        con.execute(f"""
            CREATE SECRET store (
                TYPE s3, KEY_ID '{S3_KEY}', SECRET '{S3_SECRET}', REGION '{S3_REGION}',
                ENDPOINT '{S3_ENDPOINT}', URL_STYLE 'path', USE_SSL false)
        """)
        con.execute(f"""
            ATTACH '{WAREHOUSE}' AS polaris (
                TYPE iceberg, SECRET polaris, ENDPOINT '{CATALOG_URI}',
                ACCESS_DELEGATION_MODE 'none')
        """)
        cur = con.execute(f"SELECT id, name FROM polaris.{NAMESPACE}.{name} ORDER BY id")
        cols = [d[0] for d in cur.description]
        from_duckdb = [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        catalog.drop_table(f"{NAMESPACE}.{name}")

    print(f"expected:  {expected}")
    print(f"pyiceberg: {from_pyiceberg}")
    print(f"duckdb:    {from_duckdb}")
    if not (from_pyiceberg == from_duckdb == expected):
        print("FAIL: readers disagree", file=sys.stderr)
        return 1
    print("OK: PyIceberg and DuckDB read the same 3 rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
