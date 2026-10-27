"""Normalization and schema mismatch.

Strategy cases come from Rust's `tests/unit/run/normalize.rs` and `run/schema_mismatch.rs`. The
`mismatch` conformance case is read through the CSV reader and compared with the recorded run.
"""

import json
import textwrap
from pathlib import Path

import polars as pl
import pytest

from floe.checks.mismatch import (
    MismatchOutcome,
    apply_mismatch,
    declared_names,
    json_input_columns,
    plan_mismatch,
)
from floe.checks.normalize import (
    normalize_columns,
    normalize_name,
    output_column_mapping,
    source_names,
)
from floe.config.parse import parse_config
from floe.config.types import EntityConfig
from floe.errors import FloeConfigError, FloeIoError
from floe.read.csv import read_csv

CONFORMANCE = Path(__file__).parents[3] / "conformance" / "mismatch"


def entity(
    tmp_path: Path,
    mismatch: str = "{}",
    severity: str = "reject",
    columns: str = "[{name: id, type: string}, {name: name, type: string}]",
    normalize: str = "{enabled: false}",
    fmt: str = "csv",
) -> EntityConfig:
    path = tmp_path / "config.yml"
    path.write_text(
        textwrap.dedent(f"""
        version: "0.3"
        entities:
        - name: customer
          source: {{format: {fmt}, path: in.csv}}
          sink: {{accepted: {{format: parquet, path: out}}}}
          policy: {{severity: {severity}}}
          schema: {{columns: {columns}, mismatch: {mismatch}, normalize_columns: {normalize}}}
        """)
    )
    return parse_config(path).entities[0]


@pytest.mark.parametrize(
    ("value", "strategy", "expected"),
    [
        ("Customer ID", "snake_case", "customer_id"),
        ("createdAt", "snake_case", "created_at"),
        ("HTTPServer2Go", "snake_case", "http_server2_go"),
        ("a--b__c  d", "snake_case", "a_b_c_d"),
        ("héllo wörld", "snake_case", "h_llo_w_rld"),
        ("CustomerID", "lower", "customerid"),
        ("ÉCOLE", "lower", "École"),
        ("customer_id", "camel_case", "customerId"),
        ("Customer ID", "camel_case", "customerId"),
        ("Customer ID", "Camel-Case", "customerId"),
        ("---", "camel_case", ""),
        ("Customer ID", "none", "Customer ID"),
        ("Customer ID", None, "Customer ID"),
    ],
)
def test_normalize_name(value: str, strategy: str | None, expected: str) -> None:
    assert normalize_name(value, strategy) == expected


def test_source_names_use_source_and_detect_collisions(tmp_path: Path) -> None:
    ent = entity(tmp_path, columns="[{name: userId, source: USER-ID, type: string}]")
    columns = ent.schema_.columns
    assert source_names(columns, "snakecase") == ["user_id"]
    assert output_column_mapping(columns, "snakecase") == {"user_id": "userId"}
    dup = entity(
        tmp_path,
        columns="[{name: a, source: A B, type: string}, {name: b, source: a_b, type: string}]",
    ).schema_.columns
    with pytest.raises(
        FloeConfigError, match="column source collision: A B and b -> a_b"
    ):
        source_names(dup, "snakecase")


def test_normalize_columns_renames_and_detects_collisions() -> None:
    df = pl.DataFrame({"Customer ID": [1], "createdAt": [2]})
    assert normalize_columns(df, "snakecase").columns == ["customer_id", "created_at"]
    with pytest.raises(FloeConfigError, match="collision: A B and a_b -> a_b"):
        normalize_columns(pl.DataFrame({"A B": [1], "a_b": [2]}), "snakecase")


def plan(ent: EntityConfig, inputs: list[str]) -> MismatchOutcome:
    return plan_mismatch(ent, declared_names(ent), inputs)


def test_missing_fill_nulls_accepts(tmp_path: Path) -> None:
    ent = entity(tmp_path, "{missing_columns: fill_nulls}", severity="warn")
    out = plan(ent, ["id"])
    assert out.report["mismatch_action"] == "filled_nulls"
    assert not out.rejected and out.errors == 0
    df = apply_mismatch(pl.DataFrame({"id": ["1"]}), out)
    assert df["name"].null_count() == 1 and df["name"].dtype == pl.String


def test_missing_reject_file_rejects(tmp_path: Path) -> None:
    out = plan(entity(tmp_path, "{missing_columns: reject_file}"), ["id"])
    assert (out.rejected, out.aborted, out.errors) == (True, False, 1)
    assert out.report["mismatch_action"] == "rejected_file"
    assert out.report["error"] == {
        "rule": "schema_mismatch",
        "message": "entity.name=customer schema mismatch: missing=1 extra=0",
    }


def test_extra_ignore_drops(tmp_path: Path) -> None:
    out = plan(entity(tmp_path, "{extra_columns: ignore}"), ["id", "name", "z", "a"])
    assert out.report["mismatch_action"] == "ignored_extras"
    assert out.report["extra_columns"] == ["a", "z"]
    df = pl.DataFrame({"id": ["1"], "name": ["n"], "z": ["x"], "a": ["y"]})
    assert apply_mismatch(df, out).columns == ["id", "name"]


def test_extra_reject_file_aborts(tmp_path: Path) -> None:
    ent = entity(tmp_path, "{extra_columns: reject_file}", severity="abort")
    out = plan(ent, ["id", "name", "z"])
    assert (out.rejected, out.aborted) == (False, True)
    assert out.report["mismatch_action"] == "aborted"


def test_reject_file_on_warn_continues_with_warning(tmp_path: Path) -> None:
    ent = entity(tmp_path, "{missing_columns: reject_file}", severity="warn")
    out = plan(ent, ["id"])
    assert (out.rejected, out.warnings, out.errors) == (False, 1, 0)
    assert out.report["mismatch_action"] == "filled_nulls"
    assert "entity.name=customer" in out.report["warning"]


def test_no_mismatch_and_normalized_inputs(tmp_path: Path) -> None:
    ent = entity(tmp_path, normalize="{enabled: true}")
    out = plan(ent, ["ID", "Name"])
    assert out.report["mismatch_action"] == "none"
    assert out.report["missing_columns"] == out.report["extra_columns"] == []


def test_report_lists_are_capped(tmp_path: Path) -> None:
    out = plan(entity(tmp_path), ["id", "name", *[f"x{i:03}" for i in range(60)]])
    assert len(out.report["extra_columns"]) == 50 and len(out.extra) == 60


def test_json_top_level_selectors_only(tmp_path: Path) -> None:
    cols = "[{name: id, type: string}, {name: city, source: a.city, type: string}]"
    assert declared_names(entity(tmp_path, columns=cols, fmt="json")) == ["id"]
    dup = "[{name: a, source: X, type: string}, {name: b, source: x, type: string}]"
    with pytest.raises(FloeConfigError, match="duplicate top-level column selector: x"):
        declared_names(
            entity(
                tmp_path,
                columns=dup,
                fmt="json",
                normalize="{enabled: true, strategy: lower}",
            )
        )


def test_json_probe(tmp_path: Path) -> None:
    f = tmp_path / "in.json"
    f.write_text('[{"b": 1, "a": 2}, {"c": 3}]')
    assert json_input_columns(f) == ["a", "b"]
    f.write_text('\nnot json\n{"z": 1, "y": 2}\n{"q": 1}\n')
    assert json_input_columns(f, "ndjson") == ["y", "z"]
    f.write_text("not json\n")
    with pytest.raises(FloeIoError, match="json parse error at line 1"):
        json_input_columns(f, "ndjson")
    for body, message in (
        ("[]", "json array is empty"),
        ("{}", "expected json array at root"),
        ("[1]", "expected json object at index 0"),
    ):
        f.write_text(body)
        with pytest.raises(FloeIoError, match=message):
            json_input_columns(f)


def test_mismatch_case_matches_rust_report() -> None:
    ent = parse_config(CONFORMANCE / "config.yml").entities[0]
    chunk = next(read_csv(CONFORMANCE / "in" / "customers.csv", ent.source.options))
    out = plan_mismatch(ent, declared_names(ent), chunk.df.columns)
    run = CONFORMANCE / "expected/reports/report/spike/run_ID/spike/customers/run.json"
    expected = json.loads(run.read_text())["files"][0]["mismatch"]
    assert out.report == expected
    df = apply_mismatch(chunk.df, out)
    assert df.columns == ["tenant", "code", "phone", "score", "signup", "missing_col"]
    assert df["missing_col"].null_count() == df.height
