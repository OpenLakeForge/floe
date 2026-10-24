"""Messages come from `iceberg/schema.rs`; no catalog involved."""

import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType, TimestampType

from floe.errors import FloeRunError
from floe.iceberg.catalog import (
    _credential_props,  # pyright: ignore[reportPrivateUsage] -- Rust tests its private `expand_env_refs` the same way
    additive_columns,
    build_partition_spec,
    check_partition_spec_matches,
    check_schema_matches,
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
        schema(ID, NAME, TS), [(" id ", "Identity"), ("ts", " DAY")], "e"
    )
    assert [(f.source_id, f.name, transform_name(f)) for f in spec.fields] == [
        (1, "id", "identity"),
        (3, "ts_day", "day"),
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
