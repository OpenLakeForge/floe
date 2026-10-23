"""Messages come from the recorded `expected/validate.json` of the `parse_*`, `prof_*` and `d1_*` conformance cases."""

import pytest

from floe.config.storage import (
    CatalogDefinition,
    StorageDefinition,
    parse_catalogs,
    parse_storages,
)
from floe.errors import FloeConfigError

LOCAL = {"name": "l", "type": "local"}
REST = {"name": "r", "type": "rest", "uri": "u"}


def storages(definitions: object, **rest: object) -> dict[str, object]:
    return {"definitions": definitions, **rest}


@pytest.mark.parametrize(
    ("value", "message"),
    [
        # parse_storages_missing_definitions
        ({"default": "a"}, "missing required field storages.definitions"),
        # prof_storages_not_map
        (["x"], "expected map at storages"),
        # prof_storages_unknown_key
        (storages([], extra=1), "unknown field storages.extra"),
        # prof_storages_definitions_not_array
        (storages("x"), "expected array at storages.definitions"),
        # prof_storages_default_not_string
        (storages([LOCAL], default=1), "expected string at storages.default"),
        # prof_storages_definition_not_map
        (
            storages(["x"]),
            "storages.definitions[0]: expected map at storages.definitions",
        ),
        # parse_storages_definition_wrapper, prof_storages_definition_name_missing
        (
            storages([{"type": "local"}]),
            "storages.definitions[0]: missing required field storages.definitions.name",
        ),
        # prof_storages_definition_type_missing
        (
            storages([{"name": "l"}]),
            "storages.definitions[0]: missing required field storages.definitions.type",
        ),
        # prof_storages_definition_unknown_key
        (
            storages([{**LOCAL, "mode": "x"}]),
            "storages.definitions[0]: unknown field storages.definitions.mode",
        ),
        # prof_storages_path_style_not_bool
        (
            storages([{**LOCAL, "path_style_access": "yes"}]),
            "storages.definitions[0]: expected boolean at storages.definitions.path_style_access",
        ),
        # the index is the failing definition's
        (
            storages([LOCAL, {"name": 5, "type": "local"}]),
            "storages.definitions[1]: expected string at storages.definitions.name",
        ),
    ],
)
def test_parse_storages_errors(value: object, message: str) -> None:
    with pytest.raises(FloeConfigError) as err:
        parse_storages(value)
    assert str(err.value) == message


@pytest.mark.parametrize(
    ("value", "message"),
    [
        # parse_catalogs_missing_definitions
        ({"default": "r"}, "missing required field catalogs.definitions"),
        # parse_catalogs_definition_wrapper
        (
            {"definitions": [{"type": "rest"}]},
            "catalogs.definitions[0]: missing required field catalogs.definitions.name",
        ),
        # parse_catalogs_type_unsupported
        (
            {"definitions": [{"name": "c", "type": "hive"}]},
            "catalogs.definitions[0]: catalogs.definitions name=c has unsupported type=hive (supported: glue, rest, unity)",
        ),
    ],
)
def test_parse_catalogs_errors(value: object, message: str) -> None:
    with pytest.raises(FloeConfigError) as err:
        parse_catalogs(value)
    assert str(err.value) == message


@pytest.mark.parametrize(
    ("value", "message"),
    [
        # prof_catalogs_not_map
        (["x"], "expected map at profile.catalogs"),
        # prof_catalogs_unknown_key
        ({"definitions": [REST], "extra": 1}, "unknown field profile.catalogs.extra"),
        # prof_catalogs_definitions_missing
        ({"default": "r"}, "missing required field profile.catalogs.definitions"),
        # prof_catalogs_definitions_not_array
        ({"definitions": "x"}, "expected array at profile.catalogs.definitions"),
        # prof_catalogs_definition_not_map
        (
            {"definitions": ["x"]},
            "profile.catalogs.definitions[0]: expected map at profile.catalogs.definitions",
        ),
        # prof_catalogs_definition_name_missing
        (
            {"definitions": [{"type": "rest", "uri": "u"}]},
            "profile.catalogs.definitions[0]: missing required field profile.catalogs.definitions.name",
        ),
        # prof_catalogs_definition_type_missing
        (
            {"definitions": [{"name": "r"}]},
            "profile.catalogs.definitions[0]: missing required field profile.catalogs.definitions.type",
        ),
        # prof_catalogs_definition_type_unsupported
        (
            {"definitions": [{"name": "r", "type": "hive"}]},
            "profile.catalogs.definitions[0]: profile.catalogs.definitions name=r has unsupported type=hive (supported: glue, rest, unity)",
        ),
        # prof_catalogs_definition_unknown_key
        (
            {"definitions": [{**REST, "extra": 1}]},
            "profile.catalogs.definitions[0]: unknown field profile.catalogs.definitions.extra",
        ),
        # prof_catalogs_rest_uri_missing
        (
            {"definitions": [{"name": "r", "type": "rest"}]},
            "profile.catalogs.definitions[0]: missing required field profile.catalogs.definitions.uri",
        ),
    ],
)
def test_parse_catalogs_errors_use_the_given_context(
    value: object, message: str
) -> None:
    with pytest.raises(FloeConfigError) as err:
        parse_catalogs(value, "profile.catalogs")
    assert str(err.value) == message


@pytest.mark.parametrize(
    ("value", "message"),
    [
        # d1_storage_adls
        (
            storages(
                [{"name": "a", "type": "adls", "account": "acc", "container": "c"}]
            ),
            "storages.definitions name=a type=adls is not supported in 0.8",
        ),
        # d1_storage_gcs
        (
            storages([{"name": "g", "type": "gcs", "bucket": "b"}]),
            "storages.definitions name=g type=gcs is not supported in 0.8",
        ),
    ],
)
def test_d1_storages_are_rejected(value: object, message: str) -> None:
    with pytest.raises(FloeConfigError) as err:
        parse_storages(value)
    assert str(err.value) == message


@pytest.mark.parametrize(
    ("definition", "message"),
    [
        # d1_catalog_glue
        (
            {"name": "g", "type": "glue", "region": "eu-west-1", "database": "db"},
            "catalogs.definitions name=g type=glue is not supported in 0.8",
        ),
        # d1_catalog_unity
        (
            {
                "name": "u",
                "type": "unity",
                "host": "h",
                "catalog": "c",
                "schema": "s",
                "token": "t",
            },
            "catalogs.definitions name=u type=unity is not supported in 0.8",
        ),
    ],
)
def test_d1_catalogs_are_rejected(definition: object, message: str) -> None:
    with pytest.raises(FloeConfigError) as err:
        parse_catalogs({"definitions": [definition]})
    assert str(err.value) == message


def test_parse_storages_reads_s3_with_endpoint_and_path_style() -> None:
    config = parse_storages(
        storages(
            [
                LOCAL,
                {
                    "name": "lake",
                    "type": "s3",
                    "bucket": "b",
                    "region": "r",
                    "prefix": "p",
                    "endpoint": "http://minio:9000",
                    "path_style_access": True,
                },
            ],
            default="lake",
        )
    )
    assert config.default == "lake"
    assert config.definitions[0] == StorageDefinition(
        name="l",
        type="local",
        bucket=None,
        region=None,
        account=None,
        container=None,
        prefix=None,
        endpoint=None,
        path_style_access=None,
    )
    assert config.definitions[1].endpoint == "http://minio:9000"
    assert config.definitions[1].path_style_access is True


def test_parse_storages_keeps_empty_definitions_and_null_default() -> None:
    # `must not be empty` and `default is required` are validate rules, not parse rules
    config = parse_storages(storages([], default=None))
    assert config.default is None
    assert config.definitions == []


def test_parse_catalogs_reads_rest_and_hides_the_credential() -> None:
    config = parse_catalogs(
        {
            "default": "r",
            "definitions": [
                {
                    **REST,
                    "credential": "id:secret",
                    "warehouse": "w",
                    "oauth2_server_uri": "http://auth",
                    "scope": "PRINCIPAL_ROLE:ALL",
                    "warehouse_storage": "lake",
                    "warehouse_prefix": "p",
                }
            ],
        }
    )
    definition = config.definitions[0]
    assert isinstance(definition, CatalogDefinition)
    assert config.default == "r"
    assert (definition.uri, definition.warehouse_storage) == ("u", "lake")
    assert definition.credential is not None
    assert definition.credential.get_secret_value() == "id:secret"
    assert "secret" not in repr(config)
