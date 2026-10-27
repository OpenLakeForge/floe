"""`floe validate` (Rust: `crates/floe-cli/tests/validate_output.rs`)."""

import os
import sys
from pathlib import Path

import pytest

from floe._cli import main

REPO = Path(__file__).resolve().parents[2]
CONFIG = REPO / "example/standalone/config.yml"
PROFILE = REPO / "example/profiles/dev.yml"


def floe(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], *args: str
) -> tuple[int, str, str]:
    monkeypatch.setattr(sys, "argv", ["floe", *args])
    code = main()
    out, err = capsys.readouterr()
    return code, out, err


def test_validate_default_output_is_human_text(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    code, out, _ = floe(monkeypatch, capsys, "validate", "-c", str(CONFIG))
    assert code == 0
    assert out == (
        f"Config valid: {os.path.realpath(CONFIG)}\n"
        "Version: 0.8\n"
        "Report: ./report\n"
        "Entities: 1\n"
        "Entity: orders\n"
        "  Source: csv (./in/orders.csv)\n"
        "  Sink accepted: parquet (./out/accepted/orders)\n"
        "  Sink rejected: csv (./out/rejected/orders)\n"
        "  Severity: reject\n"
        f"Next: floe run -c {os.path.realpath(CONFIG)}\n"
    )


def test_validate_with_profile_includes_profile_in_next_command(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    code, out, _ = floe(
        monkeypatch, capsys, "validate", "-c", str(CONFIG), "--profile", str(PROFILE)
    )
    assert code == 0
    assert out.endswith(
        f"Next: floe run -c {os.path.realpath(CONFIG)} -p {os.path.realpath(PROFILE)}\n"
    )


def test_validate_with_output_flag_is_rejected(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    code, _, err = floe(
        monkeypatch, capsys, "validate", "-c", str(CONFIG), "--output", "json"
    )
    assert code == 2
    assert err.startswith("error: unexpected argument '--output' found\n")


def test_validate_invalid_config_sets_exit_1_and_error_text(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    (tmp_path / "invalid.yml").write_text('version: "0.2"\nentities: []\n')
    code, _, err = floe(monkeypatch, capsys, "validate", "-c", str(tmp_path / "invalid.yml"))  # fmt: skip
    assert code == 1
    assert err.startswith("Error: ")


def test_validate_profile_only_output_is_human_text(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    code, out, _ = floe(monkeypatch, capsys, "validate", "--profile", str(PROFILE))
    assert code == 0
    assert (
        out == "Profile valid: dev\nSchema: floe/v1/EnvironmentProfile\nCatalogs: 0\n"
    )


def test_validate_without_config_or_profile_fails(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    code, _, err = floe(monkeypatch, capsys, "validate")
    assert code == 1
    assert err == "Error: floe validate requires --config unless --profile is provided\n"  # fmt: skip


def test_validate_checks_the_entity_selection(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    code, _, err = floe(
        monkeypatch, capsys, "validate", "-c", str(CONFIG), "--entities", "orders,zz"
    )
    assert code == 1
    assert err == "Error: entities not found: zz\n"


def test_validate_accepts_the_tutorial_example_config(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    code, _, err = floe(
        monkeypatch, capsys, "validate", "-c", str(REPO / "example/config.yml")
    )
    assert (code, err) == (0, "")
