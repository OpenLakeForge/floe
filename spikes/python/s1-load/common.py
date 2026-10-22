"""Shared settings for the S1 spike (#597): harness connection and the fixed contract schema."""

import os

import polars as pl
import pyarrow as pa
from pyiceberg.catalog import load_catalog

# Must match spikes/python/harness (docker-compose.yml, bootstrap.py, smoke.py).
CATALOG_URI = "http://localhost:8181/api/catalog"
WAREHOUSE = "spike"
NAMESPACE = "spike"
CLIENT_ID = "spike-client"
CLIENT_SECRET = "spike-secret"
S3_ENDPOINT = "http://localhost:8333"
S3_KEY = "spike"
S3_SECRET = "spike-s3-secret"
S3_REGION = "us-east-1"

# The contract: every column arrives as a string and is cast to this type. No validation (#599).
CONTRACT = pa.schema(
    [
        ("id", pa.int64()),
        ("customer_id", pa.int64()),
        ("amount", pa.float64()),
        ("status", pa.large_string()),
        ("event_date", pa.date32()),
        ("event_ts", pa.timestamp("us")),
        ("note", pa.large_string()),
    ]
)
COLUMNS = CONTRACT.names
TS_FORMAT = "%Y-%m-%dT%H:%M:%S%.f"

# Polars 2.0 has no string -> date/datetime cast (see #594 port risks), so parse explicitly.
CASTS = [
    pl.col("id").cast(pl.Int64, strict=False),
    pl.col("customer_id").cast(pl.Int64, strict=False),
    pl.col("amount").cast(pl.Float64, strict=False),
    pl.col("status"),
    pl.col("event_date").str.to_date("%Y-%m-%d", strict=False),
    pl.col("event_ts").str.to_datetime(TS_FORMAT, time_unit="us", strict=False),
    pl.col("note"),
]


def catalog():
    return load_catalog(
        "polaris",
        type="rest",
        uri=os.environ.get("SPIKE_CATALOG_URI", CATALOG_URI),
        warehouse=WAREHOUSE,
        credential=f"{CLIENT_ID}:{CLIENT_SECRET}",
        scope="PRINCIPAL_ROLE:ALL",
        # The catalog vends no credentials (stsUnavailable), so read and write S3 directly.
        **{
            "header.X-Iceberg-Access-Delegation": "",
            "s3.endpoint": os.environ.get("SPIKE_S3_ENDPOINT", S3_ENDPOINT),
            "s3.access-key-id": S3_KEY,
            "s3.secret-access-key": S3_SECRET,
            "s3.region": S3_REGION,
            "s3.force-virtual-addressing": "false",
        },
    )


def s3_client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("SPIKE_S3_ENDPOINT", S3_ENDPOINT),
        aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET,
        region_name=S3_REGION,
    )
