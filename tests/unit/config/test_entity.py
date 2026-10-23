"""Messages are the inner text of the recorded `conformance/config/<case>` errors (the `entities[0] (entity.name=orders): ` prefix belongs to the root parser)."""

import copy

import pytest

from floe.config.entity import parse_entity
from floe.errors import FloeConfigError
from floe.pii import PiiColumn

BASE: dict[object, object] = {
    "name": "orders",
    "source": {"format": "csv", "path": "in/orders.csv"},
    "sink": {"accepted": {"format": "parquet", "path": "out/accepted"}},
    "policy": {"severity": "warn"},
    "schema": {
        "columns": [
            {"name": "id", "type": "string"},
            {"name": "amt", "type": "number"},
        ]
    },
}


def entity(**overrides: object) -> dict[object, object]:
    value = copy.deepcopy(BASE)
    for key, new in overrides.items():
        if new is ...:
            del value[key]
        else:
            value[key] = new
    return value


def message(value: object) -> str:
    with pytest.raises(FloeConfigError) as err:
        parse_entity(value)
    return str(err.value)


def test_minimal_entity_keeps_source_and_sink_raw() -> None:
    parsed = parse_entity(entity())
    assert (parsed.name, parsed.id, parsed.incremental_mode) == (
        "orders",
        "orders",
        "none",
    )
    assert parsed.source == BASE["source"]
    assert parsed.sink == BASE["sink"]
    assert parsed.policy.severity == "warn"
    assert [c.column_type for c in parsed.schema_.columns] == ["string", "number"]
    assert (
        parsed.metadata
        is parsed.state
        is parsed.pii
        is parsed.schema_.schema_evolution
        is None
    )


def test_full_entity() -> None:
    parsed = parse_entity(
        entity(
            metadata={"owner": "me", "tags": ["a", "b"]},
            domain="sales",
            incremental_mode=" File ",  # parse_incremental_mode_case_insensitive
            state={"path": "s"},
            schema={
                "normalize_columns": {"enabled": True, "strategy": "snake_case"},
                "mismatch": {"missing_columns": "fill_nulls"},
                "schema_evolution": {"mode": "Add_Columns"},
                "primary_key": ["id"],
                "unique_keys": [["id", "amt"]],
                "columns": [
                    {
                        "name": "id",
                        "source": "$.id",
                        "type": "uint8",
                        "width": 3,
                        "trim": True,
                    }
                ],
            },
            pii={
                "columns": [
                    {"name": "id", "strategy": " Mask ", "mask_pattern": "{first2}"}
                ]
            },
        )
    )
    assert parsed.incremental_mode == "file"
    assert parsed.domain == "sales"
    assert parsed.metadata is not None
    assert parsed.metadata.tags == ["a", "b"]
    assert parsed.state is not None
    assert parsed.state.path == "s"
    schema = parsed.schema_
    assert schema.normalize_columns is not None
    assert schema.normalize_columns.strategy == "snake_case"
    assert schema.schema_evolution is not None
    assert (schema.schema_evolution.mode, schema.schema_evolution.on_incompatible) == (
        "add_columns",
        "fail",
    )
    assert schema.unique_keys == [["id", "amt"]]
    assert schema.columns[0].width == 3
    assert parsed.pii is not None
    assert parsed.pii.columns == [PiiColumn("id", "mask", mask_pattern="{first2}")]


def test_null_optionals_are_absent() -> None:  # parse_yaml_optional_null_is_absent
    parsed = parse_entity(entity(domain=None, incremental_mode=None))
    assert parsed.domain is None
    assert parsed.incremental_mode == "none"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ([], "expected map at entity"),  # parse_include_entity_file_not_a_map
        (
            entity(name=...),
            "missing required field entity.name",
        ),  # parse_entity_without_name
        (entity(name=5), "expected string at entity.name"),  # parse_yaml_name_is_int
        (
            entity(source=...),
            "missing required field entity.source",
        ),  # parse_missing_entity_source
        (entity(sink=...), "missing required field entity.sink"),
        (entity(policy=...), "missing required field entity.policy"),
        (entity(schema=...), "missing required field entity.schema"),
        (
            entity(domain=5),
            "expected string at entity.domain",
        ),  # parse_include_entity_file_domain_not_string
        (entity(bogus=1), "unknown field entity.bogus"),
        ({1: "x"}, "expected string key at entity"),
        (entity(metadata=None), "expected map at entity.metadata"),
        (entity(metadata={"tags": "a"}), "expected array at entity.metadata.tags"),
        (entity(metadata={"tags": [1]}), "expected string at entity.metadata.tags[0]"),
        (entity(state={"bogus": 1}), "unknown field entity.state.bogus"),
        (entity(state=[]), "expected map at entity.state"),
    ],
)
def test_entity_errors(value: object, expected: str) -> None:
    assert message(value) == expected


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        # parse_incremental_mode_unsupported
        (
            {"incremental_mode": "daily"},
            "unsupported value at entity.incremental_mode: daily (allowed: none, archive, file, row)",
        ),
        # parse_policy_severity_unsupported / parse_policy_severity_case_sensitive
        (
            {"policy": {"severity": "ignore"}},
            "policy.severity=ignore is unsupported (allowed: warn, reject, abort)",
        ),
        (
            {"policy": {"severity": "Warn"}},
            "policy.severity=Warn is unsupported (allowed: warn, reject, abort)",
        ),
        ({"policy": {}}, "missing required field policy.severity"),
        ({"policy": {"x": 1}}, "unknown field policy.x"),
        # parse_schema_evolution_mode_unsupported / parse_schema_evolution_on_incompatible_unsupported
        (
            {"schema": {"columns": [], "schema_evolution": {"mode": "drop_columns"}}},
            "unsupported value at schema.schema_evolution.mode: drop_columns (allowed: strict, add_columns)",
        ),
        (
            {
                "schema": {
                    "columns": [],
                    "schema_evolution": {"on_incompatible": "skip"},
                }
            },
            "unsupported value at schema.schema_evolution.on_incompatible: skip (allowed: fail)",
        ),
        ({"schema": {}}, "missing required field schema.columns"),
        ({"schema": {"columns": {}}}, "expected array at schema.columns"),
        ({"schema": {"columns": [], "x": 1}}, "unknown field schema.x"),
        (
            {"schema": {"columns": [], "mismatch": {"x": 1}}},
            "unknown field schema.mismatch.x",
        ),
        (
            {"schema": {"columns": [], "normalize_columns": {"enabled": "yes"}}},
            "expected boolean at schema.normalize_columns.enabled",
        ),
        (
            {"schema": {"columns": [], "primary_key": "id"}},
            "expected array at schema.primary_key",
        ),
        (
            {"schema": {"columns": [], "unique_keys": [["a"], "b"]}},
            "expected array at schema.unique_keys[1]",
        ),
        (
            {"schema": {"columns": [], "unique_keys": [["a", 1]]}},
            "expected string at schema.unique_keys[0][1]",
        ),
        # parse_schema_column_wrapper
        (
            {
                "schema": {
                    "columns": [{"name": "id", "type": "string"}, {"name": "amt"}]
                }
            },
            "schema.columns[1]: missing required field schema.columns.type",
        ),
        (
            {"schema": {"columns": [{"name": "id", "type": "string", "width": -1}]}},
            "schema.columns[0]: expected positive integer at schema.columns.width",
        ),
        (
            {"schema": {"columns": [{"name": "id", "type": "string", "width": "3"}]}},
            "schema.columns[0]: expected integer at schema.columns.width",
        ),
        (
            {"schema": {"columns": [{"name": "id", "type": "string", "x": 1}]}},
            "schema.columns[0]: unknown field schema.columns.x",
        ),
        (
            {"schema": {"columns": [5]}},
            "schema.columns[0]: expected map at schema.columns",
        ),
        # parse_pii_column_wrapper
        (
            {"pii": {"columns": [{"name": "id"}]}},
            "pii.columns[0]: missing required field pii.columns.strategy",
        ),
        # parse_pii_strategy_unsupported (the message echoes the trimmed, lowercased value)
        (
            {"pii": {"columns": [{"name": "id", "strategy": "encrypt"}]}},
            "pii.columns[0]: pii.columns[name=id].strategy=encrypt is unsupported (allowed: hash, drop, nullify, redact, mask)",
        ),
        (
            {"pii": {"columns": [{"name": "id", "strategy": " ENCRYPT "}]}},
            "pii.columns[0]: pii.columns[name=id].strategy=encrypt is unsupported (allowed: hash, drop, nullify, redact, mask)",
        ),
        ({"pii": {"columns": 1}}, "expected array at pii.columns"),
        ({"pii": {}}, "missing required field pii.columns"),
        ({"pii": {"columns": [], "x": 1}}, "unknown field pii.x"),
        (
            {"pii": {"columns": [{"name": "id", "strategy": "hash", "x": 1}]}},
            "pii.columns[0]: unknown field pii.columns.x",
        ),
    ],
)
def test_block_errors(overrides: dict[str, object], expected: str) -> None:
    assert message(entity(**overrides)) == expected


def test_tokenize_is_parsed_and_left_to_validation() -> None:
    parsed = parse_entity(
        entity(pii={"columns": [{"name": "id", "strategy": "tokenize"}]})
    )
    assert parsed.pii is not None
    assert parsed.pii.columns[0].strategy == "tokenize"


def test_schema_evolution_defaults() -> None:  # parse_schema_evolution_strict_fail_ok
    schema: dict[str, object] = {
        "columns": [],
        "schema_evolution": {"mode": "strict", "on_incompatible": "fail"},
    }
    evolution = parse_entity(entity(schema=schema)).schema_.schema_evolution
    assert evolution is not None
    assert (evolution.mode, evolution.on_incompatible) == ("strict", "fail")
    assert (
        parse_entity(
            entity(schema={"columns": [], "schema_evolution": {}})
        ).schema_.schema_evolution
        is not None
    )
