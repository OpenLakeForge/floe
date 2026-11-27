from __future__ import annotations

from floe.contract.changes import Change, classify


def entity(*columns: dict[str, object], **schema: object) -> dict[str, object]:
    return {
        "name": "orders",
        "policy": {"severity": "reject"},
        "schema": {"columns": list(columns), **schema},
    }


def col(name: str, type_: str = "string", **extra: object) -> dict[str, object]:
    return {"name": name, "type": type_, **extra}


def kinds(
    old: dict[str, object] | None, new: dict[str, object] | None
) -> list[tuple[str, str]]:
    return [(c.kind, c.path) for c in classify(old, new)]


def test_rename_is_breaking_removal_plus_addition() -> None:
    old = entity(col("amount", "double"))
    new = entity(col("amount_ttc", "double"))
    assert kinds(old, new) == [
        ("breaking", "schema.columns[amount]"),
        ("additive", "schema.columns[amount_ttc]"),
    ]


def test_pk_reorder_is_breaking() -> None:
    old = entity(col("a"), col("b"), primary_key=["a", "b"])
    new = entity(col("a"), col("b"), primary_key=["b", "a"])
    assert kinds(old, new) == [("breaking", "schema.primary_key")]


def test_severity_raise_is_tightening() -> None:
    old = entity(col("a"), col("b"))
    old["policy"] = {"severity": "reject", "rule_severity": {"not_null": "warn"}}
    new = entity(col("a"), col("b"))
    new["policy"] = {"severity": "reject", "rule_severity": {"not_null": "reject"}}
    assert kinds(old, new) == [("tightening", "policy.rule_severity.not_null")]
    assert kinds(new, old) == [("loosening", "policy.rule_severity.not_null")]


def test_identical_is_empty() -> None:
    e = entity(col("a", nullable=False), primary_key=["a"], unique_keys=[["a"]])
    assert classify(e, e) == []


def test_type_change_is_breaking() -> None:
    assert kinds(entity(col("a", "int64")), entity(col("a", "string"))) == [
        ("breaking", "schema.columns[a].type")
    ]


def test_source_resource_change_is_breaking() -> None:
    old = entity(col("a"))
    old["source"] = {"resource": "x"}
    new = entity(col("a"))
    new["source"] = {"resource": "y"}
    assert kinds(old, new) == [("breaking", "source.resource")]


def test_missing_table_is_breaking_and_new_table_additive() -> None:
    e = entity(col("a"))
    assert classify(e, None) == [
        Change("breaking", "table", "table removed from workbook")
    ]
    assert [c.kind for c in classify(None, e)] == ["additive"]
    assert classify(None, None) == []


def test_nullable_changes() -> None:
    old = entity(col("a", nullable=True))
    new = entity(col("a", nullable=False))
    assert kinds(old, new) == [("tightening", "schema.columns[a].nullable")]
    assert kinds(new, old) == [("loosening", "schema.columns[a].nullable")]


def test_new_column_nullability() -> None:
    old = entity(col("a"))
    assert kinds(old, entity(col("a"), col("b"))) == [("additive", "schema.columns[b]")]
    assert kinds(old, entity(col("a"), col("b", nullable=False))) == [
        ("tightening", "schema.columns[b]")
    ]


def test_bounds_narrowed_and_widened() -> None:
    old = entity(col("a", "int64", min=0, max=10))
    new = entity(col("a", "int64", min=1, max=20))
    assert kinds(old, new) == [
        ("loosening", "schema.columns[a].max"),
        ("tightening", "schema.columns[a].min"),
    ]


def test_check_added_is_tightening_removed_is_loosening() -> None:
    old = entity(col("a"))
    new = entity(col("a", max_length=5))
    assert kinds(old, new) == [("tightening", "schema.columns[a].max_length")]
    assert kinds(new, old) == [("loosening", "schema.columns[a].max_length")]


def test_allowed_values_and_pattern() -> None:
    old = entity(col("a", allowed_values=["x", "y"], pattern="^a"))
    new = entity(col("a", allowed_values=["x"], pattern="^b"))
    assert kinds(old, new) == [
        ("tightening", "schema.columns[a].allowed_values"),
        ("tightening", "schema.columns[a].pattern"),
    ]
    assert kinds(new, entity(col("a", allowed_values=["z"], pattern="^b"))) == [
        ("tightening", "schema.columns[a].allowed_values")
    ]


def test_incomparable_bounds_count_as_tightening() -> None:
    old = entity(col("a", min=1))
    new = entity(col("a", min="x"))
    assert kinds(old, new) == [("tightening", "schema.columns[a].min")]


def test_unique_key_added_tightening_removed_loosening() -> None:
    old = entity(col("a"), col("b"))
    new = entity(col("a"), col("b"), unique_keys=[["a", "b"]])
    assert kinds(old, new) == [("tightening", "schema.unique_keys[a,b]")]
    assert kinds(new, old) == [("loosening", "schema.unique_keys[a,b]")]


def test_policy_and_column_severity() -> None:
    old = entity(col("a", rule_severity={"length": "warn"}))
    old["policy"] = {"severity": "warn"}
    new = entity(col("a", rule_severity={"length": "abort"}))
    new["policy"] = {"severity": "abort"}
    assert kinds(old, new) == [
        ("tightening", "policy.severity"),
        ("tightening", "schema.columns[a].rule_severity.length"),
    ]


def test_metadata_only() -> None:
    old = entity(col("a"))
    old["metadata"] = {"owner": "x", "description": "d", "other": 1}
    new = entity(col("a"))
    new["metadata"] = {"owner": "y", "description": "d", "other": 2}
    assert kinds(old, new) == [("metadata", "metadata.owner")]


def test_allowed_values_reorder_is_not_a_change() -> None:
    old = entity(col("a", "int64", allowed_values=[1, 2]))
    new = entity(col("a", "int64", allowed_values=[2, 1, 1]))
    assert classify(old, new) == []


def test_string_bounds_are_not_compared_lexicographically() -> None:
    old = entity(col("a", "int64", min="9"))
    new = entity(col("a", "int64", min="10"))
    assert kinds(old, new) == [("tightening", "schema.columns[a].min")]


def test_column_override_removal_resolves_entity_rule_override() -> None:
    old = entity(col("a", "int64", rule_severity={"out_of_range": "reject"}))
    new = entity(col("a", "int64"))
    for e in (old, new):
        e["policy"] = {"severity": "reject", "rule_severity": {"out_of_range": "abort"}}
    assert kinds(old, new) == [
        ("tightening", "schema.columns[a].rule_severity.out_of_range")
    ]
