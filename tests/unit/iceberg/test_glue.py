"""AWS Glue catalogs, restored from Rust 0.7 (`config/catalog.rs`, `io/write/iceberg/glue.rs`); no AWS call is made."""

import json
from pathlib import Path
from typing import cast

import pytest
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.schema import Schema

import floe
from floe.config.parse import parse_config
from floe.config.validate import validate_config
from floe.errors import FloeConfigError, FloeRunError
from floe.iceberg.catalog import ensure_table, open_catalog, open_duckdb
from floe.iceberg.sink import resolve_target
from floe.manifest_run import load_manifest

CONFIG = """\
version: "0.2"
storages:
  default: silver
  definitions:
    - {{name: silver, type: s3, bucket: silver-bucket, region: eu-west-1}}
catalogs:
  default: glue
  definitions:
    - name: glue
      type: glue
      region: eu-west-1
      database: sales_silver
      warehouse_storage: silver
      warehouse_prefix: "s3://silver-bucket/warehouse/iceberg"
      create_database_if_missing: false
entities:
  - name: orders
    source: {{format: csv, path: in/orders.csv, storage: silver}}
    sink:
      write_mode: {mode}
      accepted:
        format: iceberg
        path: sales/orders
        iceberg: {{catalog: glue, namespace: sales, table: orders}}
    policy: {{severity: warn}}
    schema:
      primary_key: [id]
      columns:
        - {{name: id, type: string, nullable: false}}
"""


def config(tmp_path: Path, mode: str = "overwrite") -> Path:
    path = tmp_path / "floe.yml"
    path.write_text(CONFIG.format(mode=mode))
    return path


def test_glue_target_uses_the_database_and_the_warehouse_prefix(
    tmp_path: Path,
) -> None:
    root = parse_config(config(tmp_path))
    validate_config(root)
    target = resolve_target(root.storages, root.catalogs, tmp_path, root.entities[0])
    # the table lives in the Glue database; `iceberg.namespace` only names the location, as in 0.7
    assert (target.namespace, target.table) == ("sales_silver", "orders")
    assert target.location == "s3://silver-bucket/warehouse/iceberg/sales/orders"


ENTITY = """\
  - name: {name}
    {domain}
    source: {{format: csv, path: in/{name}.csv, storage: silver}}
    sink:
      accepted:
        format: iceberg
        path: {name}
        iceberg: {{catalog: glue{namespace}}}
    policy: {{severity: warn}}
    schema:
      primary_key: [id]
      columns:
        - {{name: id, type: string, nullable: false}}
"""


def per_domain_config(tmp_path: Path, database: str, *entities: tuple[str, str | None, str | None]) -> Path:  # fmt: skip
    """`entities` are (name, domain, namespace); the Glue `database` is the given template."""
    head = CONFIG.split("entities:")[0].format(mode="").replace("database: sales_silver", f"database: {database!r}")  # fmt: skip
    body = "".join(
        ENTITY.format(name=n, domain=f"domain: {d}" if d else "", namespace=f", namespace: {ns}" if ns else "")
        for n, d, ns in entities
    )  # fmt: skip
    path = tmp_path / "floe.yml"
    path.write_text(
        head
        + "domains:\n  - {name: sales, incoming_dir: in/sales}\n  - {name: finance, incoming_dir: in/finance}\nentities:\n"
        + body
    )
    return path


def targets(path: Path) -> list[tuple[str, str]]:
    root = parse_config(path)
    validate_config(root)
    found = [resolve_target(root.storages, root.catalogs, path.parent, e) for e in root.entities]  # fmt: skip
    return [(t.namespace, t.location) for t in found]


def test_glue_database_domain_token_expands_per_entity(tmp_path: Path) -> None:
    path = per_domain_config(tmp_path, "dev_{domain}", ("orders", "sales", None), ("ledger", "finance", None))  # fmt: skip
    assert targets(path) == [
        ("dev_sales", "s3://silver-bucket/warehouse/iceberg/sales/orders"),
        ("dev_finance", "s3://silver-bucket/warehouse/iceberg/finance/ledger"),
    ]  # the location keeps the plain namespace, never the template


def test_glue_database_domain_token_uses_the_namespace_over_the_domain(tmp_path: Path) -> None:  # fmt: skip
    path = per_domain_config(tmp_path, "dev_{domain}_silver", ("orders", "sales", "Sales EU"))  # fmt: skip
    assert targets(path) == [("dev_sales_eu_silver", "s3://silver-bucket/warehouse/iceberg/sales_eu/orders")]  # fmt: skip


def test_glue_database_without_the_token_is_unchanged(tmp_path: Path) -> None:
    path = per_domain_config(tmp_path, "fixed", ("orders", "sales", None), ("ledger", "finance", None))  # fmt: skip
    assert [db for db, _ in targets(path)] == ["fixed", "fixed"]


def test_glue_database_domain_token_needs_a_namespace_or_domain(tmp_path: Path) -> None:  # fmt: skip
    path = per_domain_config(tmp_path, "dev_{domain}", ("orders", None, None))
    with pytest.raises(FloeConfigError) as err:
        validate_config(parse_config(path))
    assert str(err.value) == (
        "entity.name=orders sink.accepted.iceberg.namespace or domain is required for catalog glue database containing {domain}"
    )


def test_glue_catalog_opens_through_pyiceberg(tmp_path: Path) -> None:
    root = parse_config(config(tmp_path))
    assert root.catalogs is not None
    catalog = open_catalog(root.catalogs.definitions[0])
    assert isinstance(catalog, GlueCatalog)
    assert catalog.properties["glue.region"] == "eu-west-1"


class _NoNamespace:
    def namespace_exists(self, namespace: tuple[str, ...]) -> bool:
        return False

    def create_namespace(self, namespace: tuple[str, ...]) -> None:
        raise AssertionError("must not create the Glue database")


def test_missing_glue_database_is_not_created_when_disabled() -> None:
    with pytest.raises(FloeRunError) as err:
        ensure_table(
            cast(Catalog, _NoNamespace()), "sales_silver", "orders", Schema(), "orders",
            create_namespace=False,
        )  # fmt: skip
    assert str(err.value) == (
        "entity.name=orders iceberg namespace sales_silver does not exist and create_database_if_missing is false"
    )


def test_glue_merges_are_rejected_at_validate(tmp_path: Path) -> None:
    with pytest.raises(FloeConfigError, match="sink.write_mode=merge_scd1 requires a rest catalog"):  # fmt: skip
        validate_config(parse_config(config(tmp_path, "merge_scd1")))
    root = parse_config(config(tmp_path))
    assert root.catalogs is not None
    with pytest.raises(FloeConfigError, match="require a rest catalog"):
        open_duckdb(root.catalogs.definitions[0])


def test_glue_catalog_survives_the_manifest(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(floe.build_manifest(str(config(tmp_path)))))
    root, _, _ = load_manifest(str(manifest))
    assert root.catalogs is not None
    [glue] = root.catalogs.definitions
    assert (glue.type, glue.region, glue.database, glue.create_database_if_missing) == (
        "glue", "eu-west-1", "sales_silver", False,
    )  # fmt: skip
