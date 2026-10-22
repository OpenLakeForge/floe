"""Connections to the #596 harness (Polaris + SeaweedFS). Settings copied from harness/smoke.py."""

import duckdb
from pyiceberg.catalog import load_catalog

CATALOG_URI = "http://localhost:8181/api/catalog"
WAREHOUSE = "spike"
NAMESPACE = "spike"
CLIENT_ID = "spike-client"
CLIENT_SECRET = "spike-secret"
S3_ENDPOINT = "localhost:8333"
S3_KEY = "spike"
S3_SECRET = "spike-s3-secret"
S3_REGION = "us-east-1"


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


def duck(memory_limit=None, temp_dir=None, threads=None):
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    if memory_limit:
        con.execute(f"SET memory_limit = '{memory_limit}'")
    if temp_dir:
        con.execute(f"SET temp_directory = '{temp_dir}'")
    if threads:
        con.execute(f"SET threads = {threads}")
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
    return con


def fq(name):
    """DuckDB name of an Iceberg table in the harness namespace."""
    return f"polaris.{NAMESPACE}.{name}"
