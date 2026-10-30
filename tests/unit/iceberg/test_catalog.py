"""Messages come from `iceberg/schema.rs`; no catalog involved."""

from pathlib import Path

import pytest
from pydantic import SecretStr
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType, TimestampType

from floe.config.storage import CatalogDefinition
from floe.errors import FloeConfigError, FloeRunError
from floe.iceberg.catalog import (
    _credential_props,  # pyright: ignore[reportPrivateUsage] -- Rust tests its private `expand_env_refs` the same way
    _duckdb_setup,  # pyright: ignore[reportPrivateUsage] -- the statements are checked without a catalog
    additive_columns,
    build_partition_spec,
    check_partition_spec_matches,
    check_schema_matches,
    ensure_table,
    open_duckdb,
    transform_name,
)


def schema(*fields: NestedField) -> Schema:
    return Schema(*fields)


ID = NestedField(1, "id", LongType(), required=True)
NAME = NestedField(2, "name", StringType(), required=False)
TS = NestedField(3, "ts", TimestampType(), required=False)


def test_schema_match_accepts_equal_schemas() -> None:
    check_schema_matches(schema(ID, NAME), schema(ID, NAME), "e")


@pytest.mark.parametrize(
    ("incoming", "message"),
    [
        (
            schema(ID, NAME, TS),
            "entity.name=e iceberg schema evolution is not supported (column count differs: existing=2, incoming=3)",
        ),
        (
            schema(ID, NestedField(2, "name", StringType(), required=True)),
            "entity.name=e iceberg schema evolution is not supported (column 1 differs: existing=name:string:optional incoming=name:string:required)",
        ),
        (
            schema(ID, NestedField(2, "label", StringType(), required=False)),
            "entity.name=e iceberg schema evolution is not supported (column 1 differs: existing=name:string:optional incoming=label:string:optional)",
        ),
        (
            schema(NestedField(1, "id", StringType(), required=True), NAME),
            "entity.name=e iceberg schema evolution is not supported (column 0 differs: existing=id:long:required incoming=id:string:required)",
        ),
    ],
)
def test_schema_match_rejects_any_difference(incoming: Schema, message: str) -> None:
    with pytest.raises(FloeRunError) as e:
        check_schema_matches(schema(ID, NAME), incoming, "e")
    assert str(e.value) == message


def test_partition_spec_names_and_transforms() -> None:
    spec = build_partition_spec(
        schema(ID, NAME, TS),
        [(" id ", "Identity"), ("ts", " DAY"), ("name", " Bucket[16]")],
        "e",
    )
    assert [(f.source_id, f.name, transform_name(f)) for f in spec.fields] == [
        (1, "id", "identity"),
        (3, "ts_day", "day"),
        (2, "name_bucket", "bucket[16]"),
    ]


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        (
            [("nope", "identity")],
            "entity.name=e iceberg partition_spec column nope was not found in runtime schema",
        ),
        (
            [("id", "bucket")],
            "entity.name=e iceberg partition_spec column id has unsupported runtime transform bucket",
        ),
        (
            [("id", "bucket[0]")],
            "entity.name=e iceberg partition_spec column id has unsupported runtime transform bucket[0]",
        ),
    ],
)
def test_partition_spec_rejects(fields: list[tuple[str, str]], message: str) -> None:
    with pytest.raises(FloeRunError) as e:
        build_partition_spec(schema(ID, NAME, TS), fields, "e")
    assert str(e.value) == message


def test_partition_spec_match() -> None:
    s = schema(ID, NAME, TS)
    day = build_partition_spec(s, [("ts", "day")], "e")
    check_partition_spec_matches(day, None, "e")
    check_partition_spec_matches(
        day, build_partition_spec(s, [("ts", "day")], "e"), "e"
    )
    with pytest.raises(FloeRunError) as e:
        check_partition_spec_matches(
            day, build_partition_spec(s, [("id", "identity")], "e"), "e"
        )
    assert str(e.value) == (
        "entity.name=e iceberg partition spec evolution is not supported "
        "(existing=3:ts_day:Day incoming=1:id:Identity)"
    )
    with pytest.raises(FloeRunError) as e:
        check_partition_spec_matches(build_partition_spec(s, [], "e"), day, "e")
    assert str(e.value).startswith(
        "entity.name=e iceberg partition spec evolution is not supported (existing=unpartitioned "
    )


def test_additive_columns_returns_new_columns() -> None:
    assert additive_columns(schema(ID, NAME), schema(ID, NAME, TS), "e") == [TS]


@pytest.mark.parametrize(
    ("incoming", "detail"),
    [
        (schema(ID), "missing existing column name"),
        (
            schema(NestedField(1, "id", StringType(), required=True), NAME),
            "column id type changed from long to string",
        ),
        (
            schema(NestedField(1, "id", LongType(), required=False), NAME),
            "column id nullability changed from non-nullable to nullable",
        ),
        (
            schema(NestedField(2, "name", LongType(), required=False)),
            "missing existing column id; column name type changed from string to long",
        ),
    ],
)
def test_additive_columns_rejects_incompatible_changes(
    incoming: Schema, detail: str
) -> None:
    with pytest.raises(FloeRunError) as e:
        additive_columns(schema(ID, NAME), incoming, "e")
    assert str(e.value) == (
        "entity.name=e iceberg schema evolution failed: add_columns supports additive changes only; "
        f"incompatible changes detected: {detail}"
    )


def test_credential_routing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLOE_TEST_ID", "client-id")
    monkeypatch.setenv("FLOE_TEST_SECRET", "client-secret")
    monkeypatch.setenv("FLOE_TEST_TOKEN", "pat-token")
    assert _credential_props(
        "polaris", "client_credentials:${FLOE_TEST_ID}:${FLOE_TEST_SECRET}"
    ) == {"credential": "client-id:client-secret"}
    assert _credential_props("polaris", "id:secret") == {"credential": "id:secret"}
    assert _credential_props("nessie", "token:${FLOE_TEST_TOKEN}") == {
        "token": "pat-token"
    }
    assert _credential_props("nessie", "token:abc${def}ghi") == {
        "token": "abc${def}ghi"
    }


@pytest.mark.parametrize(
    ("credential", "message"),
    [
        (
            "client_credentials:${FLOE_TEST_MISSING}:secret",
            "rest iceberg catalog polaris credential references env var FLOE_TEST_MISSING which is not set",
        ),
        (
            "client_credentials:${ID}:literal-secret:${UNCLOSED",
            "rest iceberg catalog polaris credential has unclosed env placeholder",
        ),
        (
            "client_credentials:${}:x",
            "rest iceberg catalog polaris credential has invalid env placeholder",
        ),
    ],
)
def test_credential_errors(
    monkeypatch: pytest.MonkeyPatch, credential: str, message: str
) -> None:
    monkeypatch.delenv("FLOE_TEST_MISSING", raising=False)
    monkeypatch.setenv("ID", "client-id")
    with pytest.raises(FloeRunError) as e:
        _credential_props("polaris", credential)
    assert str(e.value) == message
    assert "literal-secret" not in str(e.value)


def rest_catalog(credential: str) -> CatalogDefinition:
    return CatalogDefinition(
        name="polaris",
        uri="http://localhost:1/api/catalog",
        credential=SecretStr(credential),
        warehouse="w",
        oauth2_server_uri=None,
        scope=None,
        warehouse_storage=None,
        warehouse_prefix=None,
    )


def test_duckdb_rejects_a_secret_only_credential() -> None:
    with pytest.raises(FloeConfigError, match="needs client_id:client_secret"):
        open_duckdb(rest_catalog("only-a-secret"))


def test_duckdb_without_s3_storage_creates_no_s3_secret() -> None:
    statements = _duckdb_setup(rest_catalog("id:secret"), None)
    assert not any("TYPE s3" in s for s in statements)


def test_open_duckdb_sets_the_adr_limits_and_cleans_the_spill_dir(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def no_statements(*_: object) -> list[str]:  # no catalog to attach
        return []

    monkeypatch.setattr("floe.iceberg.catalog._duckdb_setup", no_statements)
    con = open_duckdb(rest_catalog("id:secret"))
    memory, spill, size = con.execute(
        "SELECT current_setting('memory_limit'), current_setting('temp_directory'), current_setting('max_temp_directory_size')"
    ).fetchone() or ("", "", "")
    assert (memory, size) == (
        "3.7 GiB",
        "46.5 GiB",
    )  # 4GB and 50GB, not 80% of RAM and 90% of the disk
    assert Path(spill).is_dir()
    con.close()
    del con
    assert not Path(spill).exists()


class RacedNamespace:
    """A catalog whose namespace appears between `namespace_exists` and `create_namespace`, as when another entity creates it first."""

    created = False

    def namespace_exists(self, _: object) -> bool:
        return False

    def create_namespace(self, _: object) -> None:
        raise NamespaceAlreadyExistsError("exists")

    def table_exists(self, _: object) -> bool:
        return False

    def create_table(self, identifier: object, *_: object, **__: object) -> str:
        self.created = True
        return "table"


def test_a_namespace_created_by_another_entity_is_not_an_error() -> None:
    catalog = RacedNamespace()
    table, added = ensure_table(catalog, "ns", "t", schema(ID), "e")  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType] -- a double with the four calls ensure_table makes
    assert catalog.created and added == [] and table == "table"  # pyright: ignore[reportUnnecessaryComparison] -- the double returns a str
