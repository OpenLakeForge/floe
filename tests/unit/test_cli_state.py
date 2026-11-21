"""`floe state inspect|reset` (Rust: `crates/floe-cli/tests/state.rs`)."""

import json
import sys
from pathlib import Path

import pytest

from floe._cli import main

V1_STATE = """{
  "schema": "floe.state.file-ingest.v1",
  "entity": "sales",
  "updated_at": "2026-04-22T09:00:00Z",
  "files": {
    "local:///tmp/incoming/sales.csv": {
      "processed_at": "2026-04-22T08:59:00Z",
      "size": 42,
      "mtime": "2026-04-22T08:00:00Z"
    }
  }
}"""


def write_config(tmp_path: Path, source: str = "{incoming}") -> tuple[Path, Path]:
    """The config and its entity's default state file (not created)."""
    (tmp_path / "incoming").mkdir()
    config = tmp_path / "config.yml"
    path = source.format(incoming=tmp_path / "incoming")
    config.write_text(f"""\
version: "0.1"
entities:
  - name: "sales"
    incremental_mode: "file"
    source: {{format: "csv", path: "{path}"}}
    sink: {{accepted: {{format: "parquet", path: "{tmp_path / "out"}"}}}}
    policy: {{severity: "warn"}}
    schema: {{columns: [{{name: "id", type: "string"}}]}}
""")
    return config, tmp_path / "incoming/.floe/state/sales/state.json"


def floe(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], *args: str
) -> tuple[int, str, str]:
    monkeypatch.setattr(sys, "argv", ["floe", "state", *args])
    code = main()
    out, err = capsys.readouterr()
    return code, out, err


def test_state_inspect_prints_state_summary_and_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config, state = write_config(tmp_path)
    state.parent.mkdir(parents=True)
    state.write_text(V1_STATE)
    code, out, _ = floe(monkeypatch, capsys, "inspect", "-c", str(config), "--entity", "sales")  # fmt: skip
    assert code == 0
    head, body = out.split("\n\n", 1)
    assert head.splitlines() == [
        "Entity: sales",
        "Incremental mode: file",
        f"State path: local://{state}",
        "State exists: yes",
        "Tracked files: 1",
        "Active claims: 0",
        "Updated at: 2026-04-22T09:00:00Z",
    ]
    assert '"entity": "sales"' in body
    assert '"size": 42' in body


def test_state_inspect_without_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config, state = write_config(tmp_path)
    code, out, _ = floe(monkeypatch, capsys, "inspect", "-c", str(config), "--entity", "sales")  # fmt: skip
    assert code == 0
    assert out.splitlines()[2:] == [
        f"State path: local://{state}",
        "State exists: no",
        "Tracked files: 0",
        "Active claims: 0",
    ]


def test_state_commands_do_not_require_a_runner_profile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config, state = write_config(tmp_path)
    config.write_text(
        config.read_text().replace(
            '    incremental_mode: "file"',
            '    runner: large\n    incremental_mode: "file"',
        )
    )
    state.parent.mkdir(parents=True)
    state.write_text(V1_STATE)
    assert (
        floe(monkeypatch, capsys, "inspect", "-c", str(config), "--entity", "sales")[0]
        == 0
    )
    assert (
        floe(
            monkeypatch,
            capsys,
            "reset",
            "-c",
            str(config),
            "--entity",
            "sales",
            "--yes",
        )[0]
        == 0
    )
    assert json.loads(state.read_text())["files"] == {}


def test_state_reset_requires_explicit_yes_flag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config, _ = write_config(tmp_path)
    code, _, err = floe(monkeypatch, capsys, "reset", "-c", str(config), "--entity", "sales")  # fmt: skip
    assert code == 1
    assert "rerun with --yes to confirm" in err


@pytest.mark.parametrize(
    "body",
    [
        '{"schema":"floe.state.file-ingest.v1","entity":"sales","updated_at":null,"files":{"a":{"processed_at":"t","size":1,"mtime":null}}}',
        "{not valid json",
        '{"schema":"wrong.schema","entity":"other","updated_at":null,"files":{}}',
    ],
    ids=["existing", "malformed", "mismatched"],
)
def test_state_reset_empties_the_state_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    body: str,
) -> None:
    config, state = write_config(tmp_path)
    state.parent.mkdir(parents=True)
    state.write_text(body)
    args = ("reset", "-c", str(config), "--entity", "sales", "--yes")
    code, out, _ = floe(monkeypatch, capsys, *args)
    assert code == 0
    assert out.splitlines() == [
        "Entity: sales",
        f"State path: local://{state}",
        "State reset: removed state object",
    ]
    assert "files" in state.read_text() and "not valid" not in state.read_text()
    assert floe(monkeypatch, capsys, *args)[1].endswith("State reset: no state object found\n")  # fmt: skip


def test_state_inspect_applies_the_profile_variables(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config, state = write_config(tmp_path, "{{{{INCOMING}}}}")
    profile = tmp_path / "profile.yml"
    profile.write_text(
        "apiVersion: floe/v1\nkind: EnvironmentProfile\nmetadata: {name: dev}\n"
        f"variables: {{INCOMING: {tmp_path / 'incoming'}}}\n"
    )
    args = ("inspect", "-c", str(config), "-p", str(profile), "--entity", "sales")
    code, out, _ = floe(monkeypatch, capsys, *args)
    assert code == 0
    assert f"State path: local://{state}\n" in out


def run_floe(monkeypatch: pytest.MonkeyPatch, config: Path) -> int:
    monkeypatch.setattr(sys, "argv", ["floe", "run", "-c", str(config)])
    return main()


def reports(tmp_path: Path) -> str:
    return "".join(p.read_text() for p in tmp_path.rglob("run*.json"))


def test_state_forget_requeues_a_changed_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config, state = write_config(tmp_path)
    config.write_text(
        config.read_text().replace(
            "sink: {accepted", 'sink: {write_mode: "append", accepted'
        )
    )
    csv = tmp_path / "incoming/sales.csv"
    csv.write_text("id\n1\n")
    assert run_floe(monkeypatch, config) == 0
    csv.write_text("id\n1\n2\n3\n")
    assert run_floe(monkeypatch, config) == 0
    assert "already_ingested_changed" in reports(tmp_path)
    uri = f"local://{csv}"
    capsys.readouterr()
    args = ("forget", "-c", str(config), "--entity", "sales", "--file", uri, "--yes")
    code, out, _ = floe(monkeypatch, capsys, *args)
    assert code == 0
    assert f"Forgot: {uri} (size=5," in out
    assert "old rows stay" in out
    assert json.loads(state.read_text())["files"] == {}
    assert run_floe(monkeypatch, config) == 0
    assert json.loads(state.read_text())["files"][uri]["size"] == 9


def test_state_forget_requires_yes_and_a_file_mode_entity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config, _ = write_config(tmp_path)
    base = ("forget", "-c", str(config), "--entity", "sales", "--file", "x")
    code, _, err = floe(monkeypatch, capsys, *base)
    assert code == 1 and "rerun with --yes to confirm" in err
    config.write_text(config.read_text().replace('"file"', '"none"'))
    code, _, err = floe(monkeypatch, capsys, *base, "--yes")
    assert code == 1 and "needs incremental_mode: file (is none)" in err


def test_state_forget_refuses_an_unknown_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config, state = write_config(tmp_path)
    state.parent.mkdir(parents=True)
    state.write_text(V1_STATE)
    args = ("forget", "-c", str(config), "--entity", "sales", "--yes", "--file")
    code, _, err = floe(monkeypatch, capsys, *args, "sales.csv")
    assert code == 1 and "not in the incremental state" in err
    assert "local:///tmp/incoming/sales.csv" in state.read_text()
