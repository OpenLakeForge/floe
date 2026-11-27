import sys
from dataclasses import fields
from pathlib import Path
from typing import Any, cast

import pytest

from floe._cli import main  # pyright: ignore[reportPrivateUsage] -- drives `floe validate` / `floe run` end to end
from floe.config.yaml import parse_yaml
from floe.contract.publish import preflight
from floe.contract.render import (
    build_entity,
    fingerprint,
    owned,
    plan_first_import,
    render_domain_scaffold,
    render_entity,
)
from floe.contract.workbook import Cell, ColumnRow, TableRow, build_rows

LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
SCAFFOLD_TODO = "TODO_"


def _doc(text: str) -> dict[str, Any]:
    return cast("dict[str, Any]", parse_yaml(text)[0])


def _cells(sheet: str, row: int, record: type, **values: str) -> dict[str, Cell]:
    names = [f.name for f in fields(record)][1:]
    return {
        n: Cell(values.get(n), f"{sheet}!{LETTERS[i]}{row}")
        for i, n in enumerate(names)
    }


def _orders() -> tuple[dict[str, object], TableRow, tuple[ColumnRow, ...]]:
    table = _cells(
        "Tables",
        2,
        TableRow,
        target_table="orders",
        source_dataset="orders_2024",
        owner="sales-ops",
        description="Customer orders",
        business_name="Orders",
        grain="one row per order",
        default_severity="reject",
    )
    columns = [
        _cells("Columns", 2, ColumnRow, target_table="orders", target_column="id",
               target_type="string", primary_key="X"),
        _cells("Columns", 3, ColumnRow, target_table="orders", target_column="amount",
               target_type="int64", min="0", value_severity="warn"),
        _cells("Columns", 4, ColumnRow, target_table="orders", target_column="customer_id",
               target_type="string", fk_target_table="customers",
               fk_target_column="customer_id"),
    ]  # fmt: skip
    errors: list[str] = []
    tables, rows, _ = build_rows([table], columns, errors)
    assert errors == []
    entity = build_entity("sales", tables[0], rows, "warn", errors)
    assert errors == []
    return entity, tables[0], rows


def test_render_is_deterministic() -> None:
    entity, table, rows = _orders()
    first = render_entity(entity, table, rows)
    assert first == render_entity(*_orders())
    assert first.startswith("# floe-contract: sha256:")
    assert "customer_id → customers.customer_id" in first
    assert "one row per order" in first
    assert list(_doc(first)) == [
        "name",
        "domain",
        "metadata",
        "source",
        "policy",
        "schema",
    ]
    # check keys land on the column they belong to
    assert {
        "name": "amount",
        "type": "int64",
        "nullable": True,
        "min": 0,
    }.items() <= _doc(first)["schema"]["columns"][1].items()


def test_fingerprint_matches_owned_projection() -> None:
    entity, table, rows = _orders()
    text = render_entity(entity, table, rows)
    parsed = _doc(text)
    digest = text.splitlines()[0].removeprefix("# floe-contract: sha256:")
    assert digest == fingerprint(owned(parsed))
    # engineer keys are outside the projection
    parsed["sink"] = {"accepted": {"format": "parquet"}}
    parsed["policy"]["max_rejection_rate"] = 0.1
    assert fingerprint(owned(parsed)) == digest
    parsed["schema"]["primary_key"] = ["amount"]
    assert fingerprint(owned(parsed)) != digest


def _project(tmp_path: Path, domain_yml: str) -> Path:
    domain = tmp_path / "domains" / "sales"
    domain.mkdir(parents=True)
    entity, table, rows = _orders()
    (domain / "orders.yml").write_text(render_entity(entity, table, rows))
    (domain / "_domain.yml").write_text(domain_yml)
    (tmp_path / "config.yml").write_text(
        'version: "0.8"\ninclude:\n  domains:\n    - "domains/*/_domain.yml"\n'
    )
    return tmp_path / "config.yml"


def _floe(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    *args: str,
) -> tuple[int, str]:
    monkeypatch.setattr(sys, "argv", ["floe", *args])
    code = main()
    out, err = capsys.readouterr()
    return code, out + err


def test_scaffold_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    scaffold = render_domain_scaffold("sales")
    assert _doc(scaffold)["name"] == "sales"
    assert "__TODO" not in scaffold
    config = _project(tmp_path, scaffold)
    code, out = _floe(monkeypatch, capsys, "validate", "-c", str(config))
    assert code != 0
    assert SCAFFOLD_TODO in out


def test_completed_domain_validates_without_entity_edit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config = _project(
        tmp_path,
        """name: sales
defaults:
  source:
    format: csv
    path: "./in/{{resource}}.csv"
    options:
      header: true
  sink:
    accepted:
      format: parquet
      path: "./out/accepted/{{entity.name}}"
    rejected:
      format: csv
      path: "./out/rejected/{{entity.name}}"
""",
    )
    entity_file = tmp_path / "domains" / "sales" / "orders.yml"
    before = entity_file.read_text()
    code, out = _floe(monkeypatch, capsys, "validate", "-c", str(config))
    assert code == 0, out
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "orders_2024.csv").write_text(
        "id,amount,customer_id\n1,5,c1\n2,7,c2\n"
    )
    monkeypatch.chdir(tmp_path)
    code, out = _floe(monkeypatch, capsys, "run", "-c", str(config))
    assert code == 0, out
    assert (tmp_path / "out" / "accepted" / "orders").exists()
    assert entity_file.read_text() == before


def test_plan_first_import_only_creates_absent_files(tmp_path: Path) -> None:
    (tmp_path / "orders.yml").write_text("name: orders\n")
    (tmp_path / "_domain.yml").write_text("name: sales\n")
    plan = plan_first_import(
        str(tmp_path), "sales", {"orders": "a\n", "customers": "b\n"}
    )
    assert [(p.path, p.status) for p in plan] == [("customers.yml", "created")]
    fresh = tmp_path / "new"
    plan = plan_first_import(str(fresh), "sales", {"orders": "a\n"})
    assert [p.path for p in plan] == ["_domain.yml", "orders.yml"]
    assert {p.status for p in plan} == {"created"}
    assert preflight(str(fresh), plan) == []


def test_fk_comment_cannot_break_the_yaml() -> None:
    columns = [
        _cells("Columns", 2, ColumnRow, target_table="orders", target_column="a\nb: c",
               target_type="string", fk_target_table="x", fk_target_column="y"),
    ]  # fmt: skip
    table = _cells("Tables", 2, TableRow, target_table="orders")
    errors: list[str] = []
    tables, rows, _ = build_rows([table], columns, errors)
    entity = build_entity("sales", tables[0], rows, "warn", errors)
    assert _doc(render_entity(entity, tables[0], rows))["name"] == "orders"


@pytest.mark.parametrize("domain", ["2024", "0o10", "true"])
def test_scaffold_name_round_trips_as_a_string(domain: str) -> None:
    assert _doc(render_domain_scaffold(domain))["name"] == domain


def _uint64(**kw: str) -> dict[str, Any]:
    columns = [
        _cells("Columns", 2, ColumnRow, target_table="orders", target_column="n",
               target_type="uint64", **kw),
    ]  # fmt: skip
    table = _cells("Tables", 2, TableRow, target_table="orders")
    errors: list[str] = []
    tables, rows, _ = build_rows([table], columns, errors)
    entity = build_entity("sales", tables[0], rows, "warn", errors)
    assert errors == []
    text = render_entity(entity, tables[0], rows)
    return _doc(text)["schema"]["columns"][0]


def test_uint64_bound_above_i64_is_not_rounded_to_a_float() -> None:
    assert _uint64(max="18446744073709551615")["max"] == "18446744073709551615"
    assert _uint64(max="9223372036854775807")["max"] == 9223372036854775807


def test_uint64_allowed_values_above_i64_round_trip() -> None:
    col = _uint64(allowed_values="1|9223372036854775809")
    assert col["allowed_values"] == ["1", "9223372036854775809"]
