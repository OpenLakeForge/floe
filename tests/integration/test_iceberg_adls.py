"""Floe's ADLS settings for PyIceberg and DuckDB against Azurite (the blob service, no Polaris).

Skipped unless `FLOE_AZURITE_ENDPOINT` is set (the `object-stores` job), e.g. `http://127.0.0.1:10000/devstoreaccount1`;
Azurite's public dev account and key are used, the container is `FLOE_AZURITE_CONTAINER` (default `warehouse`). The catalog is a local sqlite `SqlCatalog`: DuckDB merges attach to REST catalogs only, so the DuckDB
test checks the azure secret Floe builds, not a merge.
"""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs

import base64
import hashlib
import hmac
import os
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog

from floe.config.storage import CatalogDefinition, StorageDefinition
from floe.iceberg.catalog import (
    _duckdb_setup,  # pyright: ignore[reportPrivateUsage] -- the secret statement is what is under test
    _open_catalog,  # pyright: ignore[reportPrivateUsage] -- the props it builds are what is under test
)

ENDPOINT = os.environ.get("FLOE_AZURITE_ENDPOINT")
ACCOUNT = "devstoreaccount1"
KEY = (
    os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")
    or "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)
CONTAINER = os.environ.get("FLOE_AZURITE_CONTAINER", "warehouse")
CATALOG = CatalogDefinition(name="rest", uri="http://localhost:1", credential=None, warehouse="w", oauth2_server_uri=None, scope=None, warehouse_storage=None, warehouse_prefix=None)  # fmt: skip

pytestmark = [
    pytest.mark.adls,
    pytest.mark.skipif(ENDPOINT is None, reason="FLOE_AZURITE_ENDPOINT is not set"),
]


def create_container(endpoint: str, name: str) -> None:
    """A SharedKey signed `Create Container`; an existing container is fine."""
    date = datetime.now(UTC).strftime("%a, %d %b %Y %H:%M:%S GMT")
    headers = f"x-ms-date:{date}\nx-ms-version:2021-08-06\n"
    resource = f"/{ACCOUNT}/{ACCOUNT}/{name}\nrestype:container"
    to_sign = f"PUT\n\n\n\n\n\n\n\n\n\n\n\n{headers}{resource}"
    signature = base64.b64encode(hmac.new(base64.b64decode(KEY), to_sign.encode(), hashlib.sha256).digest()).decode()  # fmt: skip
    request = urllib.request.Request(
        f"{endpoint.rstrip('/')}/{name}?restype=container",
        method="PUT",
        headers={
            "x-ms-date": date,
            "x-ms-version": "2021-08-06",
            "Authorization": f"SharedKey {ACCOUNT}:{signature}",
            "Content-Length": "0",
        },
    )
    try:
        urllib.request.urlopen(request).close()
    except urllib.error.HTTPError as e:
        if e.code != 409:
            raise


@pytest.fixture
def storage(monkeypatch: pytest.MonkeyPatch) -> StorageDefinition:
    assert ENDPOINT is not None
    create_container(ENDPOINT, CONTAINER)
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_KEY", KEY)
    for env in ("AZURE_STORAGE_SAS_TOKEN", "AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET"):  # fmt: skip
        monkeypatch.delenv(env, raising=False)
    return StorageDefinition(name="lake", type="adls", bucket=None, region=None, account=ACCOUNT, container=CONTAINER, prefix=None, endpoint=ENDPOINT, path_style_access=None)  # fmt: skip


def test_an_iceberg_table_round_trips_through_floes_catalog_props(
    storage: StorageDefinition, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`_open_catalog` builds the props; only the catalog itself is swapped for a local sqlite one."""
    opened: list[SqlCatalog] = []

    def local_catalog(name: str, **props: str) -> SqlCatalog:
        local = {k: v for k, v in props.items() if k != "type" and not k.startswith("header.")}  # fmt: skip
        # PyArrow's FileIO reads `abfs://<container>/...`, not `abfss://<container>@<account>...`
        local |= {"uri": f"sqlite:///{tmp_path}/catalog.db", "warehouse": f"abfs://{CONTAINER}/{tmp_path.name}"}  # fmt: skip
        opened.append(SqlCatalog(name, **local))
        return opened[-1]

    monkeypatch.setattr("floe.iceberg.catalog.load_catalog", local_catalog)
    first = _open_catalog(CATALOG, storage)
    first.create_namespace("ns")  # pyright: ignore[reportUnknownMemberType] -- PyIceberg's own typing
    data = pa.table({"id": pa.array([1, 2, 3], pa.int64()), "name": ["a", "b", "c"]})
    first.create_table("ns.t", schema=data.schema).append(data)

    again = _open_catalog(
        CATALOG, storage
    )  # a new FileIO, nothing cached from the write
    read = again.load_table("ns.t").scan().to_arrow().sort_by("id")
    assert read.to_pydict() == {"id": [1, 2, 3], "name": ["a", "b", "c"]}
    assert len(opened) == 2


def test_duckdb_reaches_azurite_through_the_connection_string_secret(
    storage: StorageDefinition,
) -> None:
    import duckdb

    secret = next(s for s in _duckdb_setup(CATALOG, storage) if "TYPE azure" in s)  # fmt: skip
    assert "CONNECTION_STRING" in secret
    con = duckdb.connect()
    con.execute(secret)
    uri = f"azure://{CONTAINER}/duckdb/rows.parquet"
    con.execute(f"COPY (SELECT 1 AS id, 'a' AS name) TO '{uri}' (FORMAT parquet)")
    assert con.execute(f"SELECT id, name FROM read_parquet('{uri}')").fetchall() == [(1, "a")]  # fmt: skip
