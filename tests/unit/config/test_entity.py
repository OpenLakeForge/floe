"""Messages are the inner text of the recorded `conformance/config/<case>` errors (the `entities[0] (entity.name=orders): ` prefix belongs to the root parser)."""

import copy

import pytest

from floe.config.entity import parse_entity
from floe.errors import FloeConfigError

BASE: dict[object, object] = {
    "name": "orders",
    "source": {"format": "csv", "path": "in/orders.csv"},
    "sink": {"accepted": {"format": "parquet", "path": "out/accepted"}},
    "policy": {"severity": "warn"},
    "schema": {"columns": [{"name": "id", "type": "string"}]},
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


def test_minimal_entity_keeps_source_sink_and_schema_raw() -> None:
    parsed = parse_entity(entity())
    assert (parsed.name, parsed.id, parsed.incremental_mode) == (
        "orders",
        "orders",
        "none",
    )
    assert parsed.source == BASE["source"]
    assert parsed.sink == BASE["sink"]
    assert parsed.schema_ == BASE["schema"]
    assert parsed.policy.severity == "warn"
    assert parsed.metadata is parsed.state is parsed.pii is None


def test_full_entity() -> None:
    parsed = parse_entity(
        entity(
            metadata={"owner": "me", "tags": ["a", "b"]},
            domain="sales",
            incremental_mode=" File ",  # parse_incremental_mode_case_insensitive
            state={"path": "s"},
        )
    )
    assert parsed.incremental_mode == "file"
    assert parsed.domain == "sales"
    assert parsed.metadata is not None
    assert (parsed.metadata.owner, parsed.metadata.tags) == ("me", ["a", "b"])
    assert parsed.state is not None
    assert parsed.state.path == "s"


def test_choice_trims_rust_white_space_only() -> None:
    assert (
        parse_entity(entity(incremental_mode="\u00a0row\u3000")).incremental_mode
        == "row"
    )
    # U+001C is not White_Space in Rust, so `trim()` keeps it and the value is rejected
    assert message(entity(incremental_mode="\x1crow")) == (
        "unsupported value at entity.incremental_mode: \x1crow (allowed: none, archive, file, row)"
    )


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
        # parse_incremental_mode_unsupported
        (
            entity(incremental_mode="daily"),
            "unsupported value at entity.incremental_mode: daily (allowed: none, archive, file, row)",
        ),
        # parse_policy_severity_unsupported / parse_policy_severity_case_sensitive
        (
            entity(policy={"severity": "ignore"}),
            "policy.severity=ignore is unsupported (allowed: warn, reject, abort)",
        ),
        (
            entity(policy={"severity": "Warn"}),
            "policy.severity=Warn is unsupported (allowed: warn, reject, abort)",
        ),
        (entity(policy={}), "missing required field policy.severity"),
        (entity(policy={"x": 1}), "unknown field policy.x"),
        (entity(policy=[]), "expected map at policy"),
    ],
)
def test_entity_errors(value: object, expected: str) -> None:
    assert message(value) == expected
