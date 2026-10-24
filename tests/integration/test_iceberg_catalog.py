"""Create, reopen and reject changes against a real Polaris.

Skipped unless `FLOE_POLARIS_URI` is set. Harness: `docker compose up -d --wait && python3 bootstrap.py`
(PR #602, `spikes/python/harness/`); then
`FLOE_POLARIS_URI=http://localhost:8181/api/catalog uv run pytest -m polaris`.
"""

import os
import uuid
from collections.abc import Iterator

import pytest
from pydantic import SecretStr
from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType, TimestampType

from floe.config.storage import CatalogDefinition, StorageDefinition
from floe.errors import FloeRunError
from floe.iceberg.catalog import ensure_table, open_catalog, transform_name

URI = os.environ.get("FLOE_POLARIS_URI")

pytestmark = [
    pytest.mark.polaris,
    pytest.mark.skipif(URI is None, reason="FLOE_POLARIS_URI is not set"),
]

SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "ts", TimestampType(), required=False),
)


def catalog_definition() -> CatalogDefinition:
    return CatalogDefinition(
        name="polaris",
        uri=URI or "",
        credential=SecretStr("spike-client:spike-secret"),
        warehouse="spike",
        oauth2_server_uri=None,
        scope="PRINCIPAL_ROLE:ALL",
        warehouse_storage=None,
        warehouse_prefix=None,
    )


STORAGE = StorageDefinition(
    name="s3",
    type="s3",
    bucket="warehouse",
    region="us-east-1",
    account=None,
    container=None,
    prefix=None,
    endpoint="http://localhost:8333",
    path_style_access=True,
)


@pytest.fixture
def catalog(monkeypatch: pytest.MonkeyPatch) -> Iterator[Catalog]:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "spike")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "spike-s3-secret")
    yield open_catalog(catalog_definition(), STORAGE)


def test_dotted_namespace_is_one_level(catalog: Catalog) -> None:
    namespace = f"a.b{uuid.uuid4().hex[:6]}"
    name = "t"
    try:
        ensure_table(catalog, namespace, name, SCHEMA, "e")
        assert catalog.table_exists((namespace, name))
    finally:
        if catalog.table_exists((namespace, name)):
            catalog.drop_table((namespace, name))
        if catalog.namespace_exists((namespace,)):
            catalog.drop_namespace((namespace,))


def test_create_reopen_and_reject_changes(catalog: Catalog) -> None:
    name = f"it_{uuid.uuid4().hex[:8]}"
    fields = [("ts", "day")]
    try:
        created = ensure_table(
            catalog, "spike", name, SCHEMA, "e", partition_fields=fields
        )
        assert [f.name for f in created.schema().fields] == ["id", "name", "ts"]
        assert [(f.name, transform_name(f)) for f in created.spec().fields] == [
            ("ts_day", "day")
        ]

        reopened = ensure_table(
            open_catalog(catalog_definition(), STORAGE),
            "spike",
            name,
            SCHEMA,
            "e",
            partition_fields=fields,
        )
        assert reopened.metadata_location == created.metadata_location

        wider = Schema(
            *SCHEMA.fields, NestedField(4, "extra", StringType(), required=False)
        )
        with pytest.raises(FloeRunError) as e:
            ensure_table(catalog, "spike", name, wider, "e", partition_fields=fields)
        assert str(e.value) == (
            "entity.name=e iceberg schema evolution is not supported "
            "(column count differs: existing=3, incoming=4)"
        )
        with pytest.raises(
            FloeRunError, match="partition spec evolution is not supported"
        ):
            ensure_table(
                catalog,
                "spike",
                name,
                SCHEMA,
                "e",
                partition_fields=[("id", "identity")],
            )
    finally:
        if catalog.table_exists(("spike", name)):
            catalog.drop_table(("spike", name))
