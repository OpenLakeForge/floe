"""Incremental `file` mode in `run_entity`: skip, release, `row` parity, state location, renewal."""

import json
import time
from pathlib import Path
from typing import Any

import pytest

import floe.run.entity as entity_module
from floe.config.parse import parse_config
from floe.errors import FloeConfigError, FloeRunError
from floe.run import run
from floe.run.entity import EntityRun, run_entity
from floe.run.events import Emitter

CONFIG = """\
version: "0.3"
domains:
  - name: d
entities:
  - name: orders
    domain: d
    incremental_mode: {mode}
    {state}
    source: {{format: csv, path: {src}, options: {{header: true, separator: ","}}}}
    sink:
      accepted: {{format: parquet, path: ./out/accepted}}
      rejected: {{format: csv, path: ./out/rejected}}
    policy: {{severity: {severity}}}
    schema:
      columns:
        - {{name: id, type: int64}}
report: {{path: ./report}}
"""


def setup(
    tmp_path: Path,
    mode: str = "file",
    severity: str = "reject",
    src: str = "./in",
    state: str = "",
) -> Any:
    (tmp_path / "in").mkdir(exist_ok=True)
    (tmp_path / "in" / "orders.csv").write_text("id\n1\n2\n")
    (tmp_path / "config.yml").write_text(
        CONFIG.format(mode=mode, severity=severity, src=src, state=state)
    )
    return parse_config(tmp_path / "config.yml").entities[0]


def go(entity: Any, tmp_path: Path, run_id: str) -> EntityRun:
    return run_entity(entity, tmp_path, "0.3", emit=Emitter(None, run_id))


def state(tmp_path: Path) -> dict[str, Any]:
    return json.loads((tmp_path / "in/.floe/state/d/orders/state.json").read_text())


def test_second_run_skips_the_ingested_file(tmp_path: Path) -> None:
    entity = setup(tmp_path)
    first = go(entity, tmp_path, "r1").report
    assert first["results"]["files_skipped"] == 0
    second = go(entity, tmp_path, "r2").report
    assert second["results"]["files_skipped"] == 1
    assert second["files"][0]["status"] == "skipped"
    assert second["files"][0]["skip_reason"] == "already_ingested"
    assert second["accepted_output"]["files_written"] == 0
    assert list(state(tmp_path)["files"]) == [first["files"][0]["input_file"]]


def test_failed_run_releases_its_claims(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    entity = setup(tmp_path)

    def boom(*_: Any) -> None:
        raise FloeRunError("boom")

    monkeypatch.setattr(entity_module, "_run_file", boom)
    with pytest.raises(FloeRunError):
        go(entity, tmp_path, "r1")
    assert state(tmp_path)["claims"] == {}
    monkeypatch.undo()
    assert go(entity, tmp_path, "r2").report["results"]["files_skipped"] == 0


def test_aborted_run_releases_its_claims(tmp_path: Path) -> None:
    entity = setup(tmp_path, severity="abort")
    (tmp_path / "in/orders.csv").write_text("id\nx\n")
    assert go(entity, tmp_path, "r1").aborted
    assert state(tmp_path)["files"] == {}
    assert go(entity, tmp_path, "r2").report["results"]["files_skipped"] == 0


def test_row_mode_reprocesses_like_rust_0_7(tmp_path: Path) -> None:
    entity = setup(tmp_path, mode="row")
    go(entity, tmp_path, "r1")
    report = go(entity, tmp_path, "r2").report
    assert report["results"]["files_skipped"] == 0
    assert "skip_reason" not in report["files"][0]
    assert not (tmp_path / "in/.floe").exists()


@pytest.mark.parametrize(
    ("src", "expected"),
    [
        ("./in", "in/.floe/state/d/orders/state.json"),
        ("./in/orders.csv", "in/.floe/state/d/orders/state.json"),
        ("./in/*.csv", "in/.floe/state/d/orders/state.json"),
        ("in/sub/*.csv", "in/sub/.floe/state/d/orders/state.json"),
        ("./*.csv", ".floe/state/d/orders/state.json"),
    ],
)
def test_default_state_location(tmp_path: Path, src: str, expected: str) -> None:
    entity = setup(tmp_path, src=src)
    assert entity_module.state_uri(entity, tmp_path) == f"local://{tmp_path / expected}"


def test_state_path_is_local_and_relative_to_the_config(tmp_path: Path) -> None:
    entity = setup(tmp_path, state="state: {path: s/x.json}")
    assert (
        entity_module.state_uri(entity, tmp_path) == f"local://{tmp_path / 's/x.json'}"
    )
    remote = setup(tmp_path, state="state: {path: 's3://b/x.json'}")
    with pytest.raises(FloeConfigError, match="must be a local path"):
        entity_module.state_uri(remote, tmp_path)


def test_claims_are_renewed_while_the_entity_runs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    entity = setup(tmp_path)
    renewed: list[str] = []

    def fake_renew(_client: object, _uri: str, run_id: str, _claimed: object) -> None:
        renewed.append(run_id)

    monkeypatch.setattr(entity_module, "RENEW_INTERVAL_S", 0.01)
    monkeypatch.setattr(entity_module, "renew", fake_renew)
    real = entity_module._run_file  # pyright: ignore[reportPrivateUsage]

    def slow(*a: Any) -> Any:
        time.sleep(0.2)
        return real(*a)

    monkeypatch.setattr(entity_module, "_run_file", slow)
    go(entity, tmp_path, "r1")
    assert set(renewed) == {"r1"}


def test_state_files_are_never_inputs_on_a_repeated_recursive_run(
    tmp_path: Path,
) -> None:
    """D23: with a recursive `*` the default state under `.floe/` must not be read as an input."""
    entity = setup(tmp_path, src="./in/**/*")
    (tmp_path / "in/orders.csv").write_text("id\n1\n")
    go(entity, tmp_path, "r1")
    assert (tmp_path / "in/.floe/state/d/orders/state.json").exists()
    second = go(entity, tmp_path, "r2").report
    assert [f["status"] for f in second["files"]] == ["skipped"]
    assert all(".floe" not in f for f in second["source"]["resolved_inputs"]["files"])


def test_full_refresh_refuses_incremental_file(tmp_path: Path) -> None:
    setup(tmp_path)
    root = parse_config(tmp_path / "config.yml")
    with pytest.raises(FloeConfigError, match="#787"):
        run(tmp_path / "config.yml", root, full_refresh=True)


def test_glob_with_a_parent_directory_component_resolves(tmp_path: Path) -> None:
    entity = setup(tmp_path, src="./in/../in/*.csv")
    assert go(entity, tmp_path, "r1").report["results"]["files_total"] == 1


def test_bare_source_filename_roots_the_state_in_the_config_dir(tmp_path: Path) -> None:
    """#813: `orders.csv` gets the same default state as `./orders.csv`, not one under the file."""
    entity = setup(tmp_path, src="orders.csv")
    (tmp_path / "orders.csv").write_text("id\n1\n")
    go(entity, tmp_path, "r1")
    assert (tmp_path / ".floe/state/d/orders/state.json").exists()
    assert (
        go(entity, tmp_path, "r2").report["files"][0]["skip_reason"]
        == "already_ingested"
    )
