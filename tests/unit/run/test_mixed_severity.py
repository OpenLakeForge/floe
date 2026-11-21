"""#1357: each check resolves its own severity; the most severe failure decides the row and the file."""

import json
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from floe.config.parse import parse_config
from floe.config.types import EntityConfig
from floe.delta.sink import check_supported
from floe.errors import FloeRunError
from floe.run import entity as entity_module
from floe.run import file as file_module
from floe.run.entity import run_entity

CONFIG = """\
version: "0.3"
domains:
  - name: d
entities:
  - name: orders
    domain: d
    source:
      format: csv
      path: ./in
      options: {header: true, separator: ","}
    sink:
      accepted: {format: parquet, path: ./out/accepted}
      rejected: {format: csv, path: ./out/rejected}
    policy: {severity: reject}
    schema:
      primary_key: [id]
      columns:
        - {name: id, type: int64}
        - {name: email, type: string, nullable: false}
        - {name: name, type: string, nullable: false}
        - {name: amount, type: float64}
report: {path: ./report}
"""


def entity(
    tmp_path: Path,
    rows: str,
    policy: dict[str, Any] | None = None,
    columns: dict[str, dict[str, str]] | None = None,
) -> EntityConfig:
    """The entity with `rule_severity` set on the models: the YAML keys are parsed in #1359."""
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "orders.csv").write_text(rows)
    (tmp_path / "config.yml").write_text(CONFIG)
    e = parse_config(tmp_path / "config.yml").entities[0]
    cols = [
        c.model_copy(update={"rule_severity": (columns or {}).get(c.name)})
        for c in e.schema_.columns
    ]
    return e.model_copy(
        update={
            "policy": e.policy.model_copy(update=policy or {}),
            "schema_": e.schema_.model_copy(update={"columns": cols}),
        }
    )


def test_a_row_with_only_warn_failures_is_kept_and_a_reject_failure_lists_every_error(
    tmp_path: Path,
) -> None:
    rows = "id,email,name,amount\n1,a@x,a,1.5\n2,,b,2.0\n3,,c,x\n"
    e = entity(tmp_path, rows, columns={"email": {"not_null": "warn"}})
    result = run_entity(e, tmp_path, "0.3")
    assert not result.aborted
    assert pl.read_parquet(tmp_path / "out" / "accepted")["id"].to_list() == [1, 2]
    (part,) = (tmp_path / "out" / "rejected").glob("*.csv")
    rejected = pl.read_csv(part)
    assert rejected["__floe_row_index"].to_list() == [2]
    assert json.loads(rejected["__floe_errors"][0]) == [
        {"rule": "not_null", "column": "email", "message": "required value missing"},
        {"rule": "cast_error", "column": "amount", "message": "invalid value for target type"},
    ]  # fmt: skip
    f = result.report["files"][0]
    assert (f["status"], f["accepted_count"], f["rejected_count"]) == ("rejected", 2, 1)
    assert [(r["rule"], r["severity"], r["violations"]) for r in f["validation"]["rules"]] == [
        ("not_null", "warn", 2),
        ("cast_error", "reject", 1),
    ]  # fmt: skip
    assert (f["validation"]["errors"], f["validation"]["warnings"]) == (1, 2)
    errors = json.loads(Path(f["output"]["errors_path"]).read_text())
    assert [r["row_index"] for r in errors] == [1]


def test_an_abort_level_check_aborts_the_file_while_others_warn(tmp_path: Path) -> None:
    rows = "id,email,name,amount\n1,,a,1.5\n1,b@x,b,2.0\n"
    e = entity(
        tmp_path,
        rows,
        policy={"severity": "warn", "rule_severity": {"unique": "abort"}},
    )
    result = run_entity(e, tmp_path, "0.3")
    assert result.aborted
    f = result.report["files"][0]
    assert (f["status"], f["accepted_count"], f["rejected_count"]) == ("aborted", 0, 2)
    assert (tmp_path / "out" / "rejected" / "orders.csv").read_text() == rows
    assert [(r["rule"], r["severity"]) for r in f["validation"]["rules"]] == [
        ("not_null", "warn"),
        ("unique", "abort"),
    ]
    assert result.report["unique_constraints"][0]["action"] == "abort_run"
    assert result.report["policy"] == {
        "severity": "warn",
        "rule_severity": {"unique": "abort"},
    }


def test_a_rule_is_reported_once_per_resolved_severity(tmp_path: Path) -> None:
    rows = "id,email,name,amount\n1,,,1.5\n2,,b,2.0\n"
    e = entity(tmp_path, rows, columns={"email": {"not_null": "warn"}})
    f = run_entity(e, tmp_path, "0.3").report["files"][0]
    assert f["validation"]["rules"] == [
        {"rule": "not_null", "severity": "reject", "violations": 1, "columns": [{"column": "name", "violations": 1}]},
        {"rule": "not_null", "severity": "warn", "violations": 2, "columns": [{"column": "email", "violations": 2}]},
    ]  # fmt: skip
    assert (f["validation"]["errors"], f["validation"]["warnings"]) == (1, 2)


def test_max_rejection_rate_counts_only_rejected_rows(tmp_path: Path) -> None:
    rows = "id,email,name,amount\n1,,a,1.5\n2,,b,2.0\n3,,c,2.5\n4,d@x,d,x\n"
    e = entity(
        tmp_path,
        rows,
        policy={"max_rejection_rate": 0.5},
        columns={"email": {"not_null": "warn"}},
    )
    report = run_entity(e, tmp_path, "0.3").report
    assert report["results"]["rejected_total"] == 1
    assert report["policy"] == {
        "severity": "reject",
        "max_rejection_rate": 0.5,
        "rejection_rate": 0.25,
        "max_rejection_rate_exceeded": False,
    }


def test_a_warn_merge_key_duplicate_is_still_forced_out(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rows = "id,email,name,amount\n1,a@x,a,1.5\n1,b@x,b,2.0\n2,c@x,c,x\n"
    e = entity(tmp_path, rows, policy={"rule_severity": {"unique": "warn"}})

    def merging(plan: file_module.Plan, *args: Any) -> Any:
        plan.merge_key = "id"  # what a merge sink sets; a Parquet sink cannot merge
        return file_module.run_file(plan, *args)

    monkeypatch.setattr(entity_module, "run_file", merging)
    f = run_entity(e, tmp_path, "0.3").report["files"][0]
    assert (f["status"], f["accepted_count"], f["rejected_count"]) == ("rejected", 1, 2)
    assert pl.read_parquet(tmp_path / "out" / "accepted")["name"].to_list() == ["a"]
    errors = json.loads(Path(f["output"]["errors_path"]).read_text())
    assert [r["row_index"] for r in errors] == [1]


def test_a_delta_append_is_refused_when_any_rule_aborts(tmp_path: Path) -> None:
    e = entity(tmp_path, "id\n", policy={"rule_severity": {"unique": "abort"}})
    accepted = e.sink.accepted.model_copy(update={"format": "delta"})
    e = e.model_copy(
        update={"sink": e.sink.model_copy(update={"write_mode": "append", "accepted": accepted})}
    )  # fmt: skip
    with pytest.raises(FloeRunError) as err:
        check_supported(e)
    assert str(err.value) == "entity.name=orders a rule_severity of abort on a delta append is not supported yet: the retry guard cannot cover it"  # fmt: skip
