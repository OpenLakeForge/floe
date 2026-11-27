from typing import Any

import pytest

from floe.add_entity import _render  # pyright: ignore[reportPrivateUsage] -- builds the generated file the importer would write
from floe.config.entity import parse_entity
from floe.config.yaml import parse_yaml
from floe.contract import reconcile as module
from floe.contract.reconcile import reconcile
from floe.contract.render import fingerprint, owned


def _entity(**over: Any) -> dict[str, Any]:
    entity: dict[str, Any] = {
        "name": "orders",
        "domain": "sales",
        "metadata": {"owner": "ops", "description": "Orders"},
        "source": {"resource": "orders_2024"},
        "policy": {"severity": "reject", "rule_severity": {"unique": "warn"}},
        "schema": {
            "primary_key": ["id"],
            "columns": [
                {"name": "id", "type": "int64", "nullable": False},
                {"name": "amount", "type": "string"},
            ],
        },
    }
    return {**entity, **over}


def _generated(entity: dict[str, Any]) -> str:
    return f"# floe-contract: sha256:{fingerprint(owned(entity))}\n" + _render(entity)


_SINK: dict[str, Any] = {
    "accepted": {"format": "parquet", "path": "/out"},
    "rejected": {"format": "parquet", "path": "/rej"},
}
ENGINEER = [
    "  options:\n    header: true  # keep the header\n",
    "sink:\n  accepted:\n    format: parquet  # engineer\n    path: /out\n",
    "incremental_mode: file\n",
    "pii:\n  - id\n",
    "  checks:\n    - name: custom\n",
    "  max_rejection_rate: 0.1  # tolerated\n",
]


def _edited(text: str) -> str:
    text = text.replace(
        "  resource: orders_2024\n", "  resource: orders_2024\n" + ENGINEER[0]
    )
    text = text.replace("source:\n", "# who owns this\nsource:\n")
    text = text.replace(
        "policy:\n", ENGINEER[1] + ENGINEER[2] + ENGINEER[3] + "policy:\n"
    )
    text = text.replace("  rule_severity:", ENGINEER[5] + "  rule_severity:")
    return text.replace("schema:\n", "schema:\n" + ENGINEER[4])


def test_engineer_keys_byte_identical() -> None:
    old = _edited(_generated(_entity()))
    columns = [
        {"name": "id", "type": "int64", "nullable": False},
        {"name": "amount", "type": "float64", "nullable": False},
    ]
    new = _entity(schema={**_entity()["schema"], "columns": columns})
    plan = reconcile("orders.yml", old, new)
    assert plan.status == "updated", plan.reason
    assert plan.new_text is not None
    assert "type: float64" in plan.new_text
    assert plan.new_text.startswith(
        f"# floe-contract: sha256:{fingerprint(owned(new))}\n"
    )
    for snippet in [*ENGINEER, "# who owns this\n"]:
        assert snippet in plan.new_text
    # nothing but the header line and the column lines moved
    assert plan.new_text.splitlines()[1:12] == old.splitlines()[1:12]


def test_unchanged_is_noop() -> None:
    text = _edited(_generated(_entity()))
    plan = reconcile("orders.yml", text, _entity())
    assert plan.status == "unchanged"
    assert plan.new_text == text


def test_hand_edited_owned_key_blocks() -> None:
    text = _generated(_entity()).replace("type: int64", "type: string")
    plan = reconcile("orders.yml", text, _entity())
    assert plan.status == "blocked"
    assert plan.new_text is None
    assert "int64" in plan.reason and "string" in plan.reason


def test_unmanaged_file_blocks() -> None:
    text = _render(_entity())
    plan = reconcile("orders.yml", text, _entity())
    assert plan.status == "blocked"
    assert "not created by floe contract import" in plan.reason


def test_self_check_rejects_bad_splice(monkeypatch: pytest.MonkeyPatch) -> None:
    text = _generated(_entity())
    monkeypatch.setattr(module, "_render", lambda _doc: "columns: []\n")  # pyright: ignore[reportUnknownLambdaType, reportUnknownArgumentType] -- a stub for the emitter
    plan = reconcile("orders.yml", text, _entity(source={"resource": "other"}))
    assert plan.status == "blocked"
    assert "self-check" in plan.reason


def test_string_that_reads_as_a_number_round_trips() -> None:
    new = _entity(metadata={"owner": "ops", "description": "0o10"})
    plan = reconcile("orders.yml", _generated(_entity()), new)
    assert plan.status == "updated", plan.reason
    assert plan.new_text is not None and 'description: "0o10"' in plan.new_text


def test_new_keys_are_inserted_and_absent_keys_removed() -> None:
    old = _edited(_generated(_entity()))
    new = _entity(metadata={"owner": "ops"})
    del new["policy"]["rule_severity"]
    new["source"] = {}
    plan = reconcile("orders.yml", old, new)
    assert plan.status == "updated", plan.reason
    assert plan.new_text is not None
    assert "description" not in plan.new_text and "rule_severity" not in plan.new_text
    assert "max_rejection_rate: 0.1  # tolerated\n" in plan.new_text
    again = reconcile(
        "orders.yml",
        plan.new_text,
        _entity(metadata={"owner": "ops"}) | {"policy": new["policy"], "source": {}},
    )
    assert again.status == "unchanged"
    back = reconcile("orders.yml", plan.new_text, _entity())
    assert back.status == "updated", back.reason
    assert back.new_text is not None and "description: Orders" in back.new_text


def test_flow_style_owned_parent_blocks() -> None:
    text = _generated(_entity()).replace(
        "metadata:\n  owner: ops\n  description: Orders\n",
        "metadata: {owner: ops, description: Orders}\n",
    )
    plan = reconcile("orders.yml", text, _entity(metadata={"owner": "new"}))
    assert plan.status == "blocked"
    assert "metadata" in plan.reason


def _no_metadata() -> dict[str, Any]:
    entity = _entity()
    del entity["metadata"]
    return entity


def test_removing_every_child_drops_the_parent() -> None:
    old = _generated(_entity()).replace(
        "metadata:\n  owner: ops\n  description: Orders\n",
        "metadata:\n  owner: ops\n  description: Orders\n",
    )
    plan = reconcile("orders.yml", old, _no_metadata())
    assert plan.status == "updated", plan.reason
    assert plan.new_text is not None and "metadata" not in plan.new_text
    entity = parse_yaml(plan.new_text)[0]
    assert isinstance(entity, dict)
    parse_entity({**entity, "source": {"format": "csv", "path": "/in"}, "sink": _SINK})  # pyright: ignore[reportUnknownArgumentType]
    back = reconcile("orders.yml", plan.new_text, _entity())
    assert back.status == "updated", back.reason
    assert back.new_text is not None and "owner: ops" in back.new_text


def test_parent_with_an_engineer_child_is_kept() -> None:
    old = _generated(_entity()).replace(
        "  description: Orders\n",
        "  description: Orders\n  tags:\n    - gold  # keep\n",
    )
    plan = reconcile("orders.yml", old, _no_metadata())
    assert plan.status == "updated", plan.reason
    assert plan.new_text is not None
    assert "metadata:\n  tags:\n    - gold  # keep\n" in plan.new_text


def test_inserts_at_the_same_offset_keep_children_under_their_parent() -> None:
    bare = _entity()
    del bare["metadata"], bare["schema"]["primary_key"]
    plan = reconcile("orders.yml", _generated(bare), _entity())
    assert plan.status == "updated", plan.reason
    assert plan.new_text is not None
    assert "\nprimary_key" not in plan.new_text  # still indented under schema
    assert "metadata:" in plan.new_text and "  primary_key:" in plan.new_text


def _block_description() -> str:
    entity = _entity(metadata={"owner": "ops", "description": "line1\nline2"})
    text = _generated(entity).replace(
        '  description: "line1\\nline2"\n',
        "  description: |-\n    line1\n    line2\n# engineer note\n",
    )
    assert "# engineer note" in text
    return text


def test_removing_a_block_scalar_keeps_the_comment_after_it() -> None:
    plan = reconcile(
        "orders.yml", _block_description(), _entity(metadata={"owner": "ops"})
    )
    assert plan.status == "updated", plan.reason
    assert plan.new_text is not None
    assert "description" not in plan.new_text
    assert "  owner: ops\n# engineer note\nsource:" in plan.new_text


def test_replacing_a_block_scalar_keeps_the_next_key() -> None:
    new = _entity(metadata={"owner": "ops", "description": "other"})
    plan = reconcile("orders.yml", _block_description(), new)
    assert plan.status == "updated", plan.reason
    assert plan.new_text is not None
    assert "  description: other\n# engineer note\nsource:" in plan.new_text
