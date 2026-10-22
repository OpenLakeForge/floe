# Spike harness: local Polaris + S3-compatible store

A local Iceberg REST catalog (Apache Polaris) on S3-compatible storage (SeaweedFS), for the
Python core spikes (#594: #597, #598, #600). Throwaway spike code, not for production: fixed
credentials, in-memory catalog state, ports bound to `127.0.0.1` only.

Needs Docker with Compose v2, `python3` and [`uv`](https://docs.astral.sh/uv/). Run every
command from this directory.

```bash
docker compose up -d --wait && python3 bootstrap.py   # up + bootstrap (safe to rerun)
uv run smoke.py                                       # smoke test
docker compose down -v                                # tear down, delete all data
```

`smoke.py` creates a table, appends 3 rows with PyIceberg, reads them back with PyIceberg and
with DuckDB (`ATTACH` to Polaris with OAuth2 client credentials, S3 secret in path style),
drops the table, and exits non-zero unless both readers return the same 3 rows. With the
stack down it fails with `Connection refused`.

DuckDB downloads its `iceberg`, `httpfs` and `avro` extensions from `extensions.duckdb.org` on
first use, so that host must be reachable once.

## What you get

| | Value |
|---|---|
| Iceberg REST catalog | `http://localhost:8181/api/catalog`, warehouse `spike` |
| Client credentials | `spike-client` / `spike-secret`, scope `PRINCIPAL_ROLE:ALL` |
| Namespace | `spike` |
| S3 endpoint | `http://localhost:8333`, path-style, region `us-east-1` |
| S3 key / secret | `spike` / `spike-s3-secret` |
| Bucket | `s3://warehouse` (catalog base location `s3://warehouse/spike`) |
| Polaris root | `root` / `s3cr3t` (bootstrap only) |

Polaris vends no storage credentials (`stsUnavailable`, since SeaweedFS has no STS here).
Clients use the S3 key directly: in PyIceberg, set `s3.*` properties and
`header.X-Iceberg-Access-Delegation` to `""`; in DuckDB, `ACCESS_DELEGATION_MODE 'none'`. See
`smoke.py` for both.

Polaris keeps its state in memory: after a restart, run `bootstrap.py` again. It creates the
catalog, the `spike` principal (with its credentials reset to the fixed pair above), the
`spike_role` principal role, the `catalog_admin` catalog role plus `CATALOG_MANAGE_CONTENT`, and
the namespace. Anything that already exists is left alone.

## Versions

| Component | Version |
|---|---|
| Apache Polaris | `apache/polaris:1.8.0` (`sha256:347615e736cab2c9be0d8c464552ac930be4755302bb0aeb745216bbae99ba5b`) |
| SeaweedFS | `chrislusf/seaweedfs:4.48` (`sha256:4e61d15fd35994cb1e43e1e553dff106794841fd9a99ade2fc8c8bfce4d7872d`) |
| PyIceberg | 0.12.0 |
| DuckDB | 1.5.6 |
| pyarrow | 25.0.1 |

Python dependencies are pinned in `smoke.py` (PEP 723 metadata, read by `uv run`).
`bootstrap.py` uses only the standard library.

## Sources

- `docker-compose.yml` adapts Apache Polaris's `getting-started/minio/docker-compose.yml`
  (tag `apache-polaris-1.3.0-incubating`) and `getting-started/quickstart/docker-compose.yml`
  (tag `apache-polaris-1.8.0`). SeaweedFS replaces MinIO: the `minio/minio` image is no longer
  published on Docker Hub.
- `bootstrap.py` follows the `polaris-setup` steps of that quickstart compose, and the
  management API in `spec/polaris-management-service.yml` (1.8.0) for `stsUnavailable` and the
  principal credential reset.
- SeaweedFS `weed mini` creates the bucket (`-bucket`) and the admin S3 identity from
  `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`.
