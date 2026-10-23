"""Drop a spike table and delete its files (Polaris purge is disabled in the harness).

  drop.py TABLE
"""

import sys

import boto3

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent / "s2-merge"))
from common import NAMESPACE, S3_ENDPOINT, S3_KEY, S3_REGION, S3_SECRET, catalog  # noqa: E402 - after the sys.path insert for ../s2-merge

name = sys.argv[1]
cat = catalog()
loc = cat.load_table(f"{NAMESPACE}.{name}").metadata.location.removeprefix("s3://")
bucket, prefix = loc.split("/", 1)
cat.drop_table(f"{NAMESPACE}.{name}")
s3 = boto3.client("s3", endpoint_url=f"http://{S3_ENDPOINT}", aws_access_key_id=S3_KEY,
                  aws_secret_access_key=S3_SECRET, region_name=S3_REGION)
n = 0
for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
    keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
    if keys:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
        n += len(keys)
print(f"dropped {name}: deleted {n} objects under s3://{bucket}/{prefix}")
