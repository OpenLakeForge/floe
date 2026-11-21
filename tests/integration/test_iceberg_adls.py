"""Floe's ADLS settings for PyIceberg's FileIO and for DuckDB, against Azurite (the blob service).

Skipped unless `FLOE_AZURITE_ENDPOINT` is set (the `object-stores` job), e.g. `http://127.0.0.1:10000/devstoreaccount1`;
the container is `FLOE_AZURITE_CONTAINER` (default `warehouse`) and the key `AZURE_STORAGE_ACCOUNT_KEY` (Azurite's
public dev key when unset). There is no catalog: the FileIO is built from the props `_open_catalog` hands PyIceberg.
"""

import base64
import hashlib
import hmac
import os
import urllib.error
import urllib.request
import uuid
from datetime import UTC, datetime

import pytest
from pyiceberg.io import FileIO, load_file_io

from floe.config.storage import CatalogDefinition, StorageDefinition
from floe.iceberg.catalog import (
    _duckdb_setup,  # pyright: ignore[reportPrivateUsage] -- the secret statement is what is under test
    _open_catalog,  # pyright: ignore[reportPrivateUsage] -- the props it builds are what is under test
)
from floe.storage.object_store import azure_store

ENDPOINT = os.environ.get("FLOE_AZURITE_ENDPOINT")
ACCOUNT = "devstoreaccount1"
KEY = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY") or "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="  # fmt: skip
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


def floe_file_io(storage: StorageDefinition, monkeypatch: pytest.MonkeyPatch, location: str) -> FileIO:  # fmt: skip
    """The FileIO PyIceberg builds from the props `_open_catalog` passes to `load_catalog`."""
    seen: dict[str, str] = {}

    def capture(_name: str, **props: str) -> object:
        seen.update(props)
        return object()

    monkeypatch.setattr("floe.iceberg.catalog.load_catalog", capture)
    _open_catalog(CATALOG, storage)
    return load_file_io(seen, location)


def round_trip(io: FileIO, location: str) -> bytes:
    with io.new_output(location).create(overwrite=True) as out:
        out.write(b"floe")
    with io.new_input(location).open() as src:
        return src.read()


def test_the_fileio_props_reach_azurite_at_a_container_location(
    storage: StorageDefinition, monkeypatch: pytest.MonkeyPatch
) -> None:
    location = f"abfs://{CONTAINER}/floe-it/{uuid.uuid4().hex}.bin"
    assert round_trip(floe_file_io(storage, monkeypatch, location), location) == b"floe"  # fmt: skip


def test_the_fileio_reads_and_writes_floes_own_abfss_location(
    storage: StorageDefinition, monkeypatch: pytest.MonkeyPatch
) -> None:
    location = azure_store(storage).resolve_uri(f"floe-it/{uuid.uuid4().hex}.bin")
    assert location.startswith(f"abfss://{CONTAINER}@{ACCOUNT}.dfs.core.windows.net/")
    assert round_trip(floe_file_io(storage, monkeypatch, location), location) == b"floe"  # fmt: skip


def test_duckdb_reaches_azurite_through_the_connection_string_secret(
    storage: StorageDefinition,
) -> None:
    import duckdb

    secret = next(s for s in _duckdb_setup(CATALOG, storage) if "TYPE azure" in s)
    assert "CONNECTION_STRING" in secret
    con = duckdb.connect()
    con.execute(secret)
    uri = f"azure://{CONTAINER}/floe-it/{uuid.uuid4().hex}.parquet"
    con.execute(f"COPY (SELECT 1 AS id, 'a' AS name) TO '{uri}' (FORMAT parquet)")
    assert con.execute(f"SELECT id, name FROM read_parquet('{uri}')").fetchall() == [(1, "a")]  # fmt: skip
