"""#1101: one opened catalog per `(CatalogDefinition, StorageDefinition)`; the REST catalog is the in-process one of `test_merge_commit_retry.py`."""

import threading

import pytest
from pyiceberg.catalog import Catalog, load_catalog
from test_merge_commit_retry import (  # pyright: ignore[reportMissingImports] -- a sibling test module, on pytest's path
    RestCatalog,
    server,  # noqa: F401  # pyright: ignore[reportUnusedImport] -- the fixture, used by name
)

import floe.iceberg.catalog as catalog_module
from floe.config.storage import CatalogDefinition, StorageDefinition
from floe.errors import FloeRunError
from floe.iceberg.catalog import open_catalog


@pytest.fixture
def opened(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """The names `load_catalog` was called with; the real call still happens."""
    calls: list[str] = []

    def counting(name: str, **props: str) -> Catalog:
        calls.append(name)
        return load_catalog(name, **props)

    monkeypatch.setattr(catalog_module, "load_catalog", counting)
    return calls


def storage(**values: object) -> StorageDefinition:
    fields: dict[str, object] = {"name": "s", "type": "s3", "bucket": None, "region": None, "account": None, "container": None, "prefix": None, "endpoint": None, "path_style_access": None}  # fmt: skip
    return StorageDefinition.model_validate(fields | values)


def test_the_same_definition_opens_the_catalog_once(server: tuple[RestCatalog, CatalogDefinition], opened: list[str]) -> None:  # noqa: F811 -- the imported fixture  # fmt: skip
    _, definition = server
    assert open_catalog(definition) is open_catalog(definition)
    assert open_catalog(definition, storage()) is open_catalog(definition, storage())
    assert len(opened) == 2  # the storage is part of the key


def test_concurrent_first_calls_open_it_once(server: tuple[RestCatalog, CatalogDefinition], opened: list[str]) -> None:  # noqa: F811 -- the imported fixture  # fmt: skip
    _, definition = server
    threads = [threading.Thread(target=open_catalog, args=(definition,)) for _ in range(8)]  # fmt: skip
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(opened) == 1


def test_a_failed_open_is_not_kept(opened: list[str]) -> None:
    broken = CatalogDefinition(name="fake", uri="http://127.0.0.1:1", warehouse_storage=None, warehouse_prefix=None)  # fmt: skip
    for _ in range(2):
        with pytest.raises(FloeRunError, match="rest catalog init failed"):
            open_catalog(broken)
    assert len(opened) == 2
