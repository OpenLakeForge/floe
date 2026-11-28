from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from floe import build_manifest
from floe._cli import main  # pyright: ignore[reportPrivateUsage] -- drives `floe contract import` end to end

ROOT = Path(__file__).resolve().parents[3]
EXAMPLE = ROOT / "example/business-contract"
PROJECT = EXAMPLE / "project"
CASE = ROOT / "conformance/business_contract"


def test_example_reimport_is_noop(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    args = ["floe", "contract", "import", str(EXAMPLE / "domain-template.xlsx")]
    monkeypatch.setattr(
        sys, "argv", [*args, "--output-dir", str(PROJECT / "domains/sales"), "--check"]
    )
    assert main() == 0, capsys.readouterr().out


def test_golden_case_matches_example_project() -> None:
    """The golden case is a copy of the example (the suite runs from a temp copy of the case)."""
    for part in ("config.yml", "domains", "in"):
        mine, theirs = PROJECT / part, CASE / part
        files = (
            [mine]
            if mine.is_file()
            else sorted(p for p in mine.rglob("*") if p.is_file())
        )
        for f in files:
            twin = theirs if mine.is_file() else theirs / f.relative_to(mine)
            assert twin.read_bytes() == f.read_bytes(), f"{twin} differs from {f}"


def test_example_manifest_carries_keys_and_severities(tmp_path: Path) -> None:
    shutil.copytree(PROJECT, tmp_path / "p")
    manifest: dict[str, Any] = build_manifest(str(tmp_path / "p/config.yml"))
    schema = ROOT / "orchestrators/schemas/floe.manifest.schema.json"
    Draft202012Validator(json.loads(schema.read_text())).validate(manifest)  # pyright: ignore[reportUnknownMemberType] -- jsonschema ships no types
    by_name = {e["name"]: e for e in manifest["entities"]}
    assert sorted(by_name) == ["sales.customers", "sales.order_lines", "sales.orders"]
    assert by_name["sales.customers"]["schema"]["primary_key"] == ["customer_id"]
    assert by_name["sales.customers"]["schema"]["unique_keys"] == [
        ["email"],
        ["full_name", "birth_date"],
    ]
    assert by_name["sales.order_lines"]["schema"]["primary_key"] == [
        "order_id",
        "line_no",
    ]
    assert by_name["sales.orders"]["policy_rule_severity"] == {
        "unique": "abort",
        "cast_error": "warn",
    }
    assert by_name["sales.order_lines"]["policy_rule_severity"] == {"not_null": "warn"}
    assert "policy_rule_severity" not in by_name["sales.customers"]
    country = next(
        c
        for c in by_name["sales.customers"]["schema"]["columns"]
        if c["name"] == "country_code"
    )
    assert country["rule_severity"] == {"length": "warn", "not_allowed": "warn"}
