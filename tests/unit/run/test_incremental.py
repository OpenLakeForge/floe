"""Incremental `file` mode in `run_entity`: skip, release, `row` parity, state location, renewal."""

import json
import time
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from floe.config.parse import parse_config
from floe.errors import FloeConfigError, FloeRunError
from floe.run import entity as entity_module  # `floe.run` is the public function
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
    assert not (tmp_path / "in/.floe/state/d/orders/state.json").exists()
    monkeypatch.undo()
    assert go(entity, tmp_path, "r2").report["results"]["files_skipped"] == 0


def test_a_failure_after_the_sink_commit_promotes_the_written_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    """#915: once the rows are in the sink, a later failure must not release the claims (a rerun would append them again)."""
    setup(tmp_path)
    config = tmp_path / "config.yml"
    config.write_text(config.read_text().replace("    sink:", "    sink:\n      write_mode: append"))  # fmt: skip
    entity = parse_config(config).entities[0]

    def boom(*_: Any, **__: Any) -> None:
        raise FloeRunError("report failed")

    monkeypatch.setattr(entity_module, "build_report", boom)
    with pytest.raises(FloeRunError, match="report failed"):
        go(entity, tmp_path, "r1")
    after = state(tmp_path)
    assert list(after["files"]) == [f"local://{tmp_path / 'in/orders.csv'}"]
    assert after["claims"] == {}
    parts = sorted((tmp_path / "out/accepted").iterdir())
    monkeypatch.undo()
    rerun = go(entity, tmp_path, "r2").report
    assert rerun["results"]["files_skipped"] == 1
    assert sorted((tmp_path / "out/accepted").iterdir()) == parts


def test_aborted_run_releases_its_claims(tmp_path: Path) -> None:
    entity = setup(tmp_path, severity="abort")
    (tmp_path / "in/orders.csv").write_text("id\nx\n")
    assert go(entity, tmp_path, "r1").aborted
    assert not (tmp_path / "in/.floe/state/d/orders/state.json").exists()
    assert go(entity, tmp_path, "r2").report["results"]["files_skipped"] == 0


def test_an_abort_promotes_the_files_written_before_it(tmp_path: Path) -> None:
    """#918 (#545 S5): a's rows are written before b aborts; once b is fixed, the rerun writes a's rows no second time."""
    setup(tmp_path, severity="abort")
    config = tmp_path / "config.yml"
    config.write_text(config.read_text().replace("    sink:", "    sink:\n      write_mode: append"))  # fmt: skip
    entity = parse_config(config).entities[0]
    (tmp_path / "in/orders.csv").unlink()
    (tmp_path / "in/a.csv").write_text("id\n1\n2\n")
    (tmp_path / "in/b.csv").write_text("id\nx\n")
    assert go(entity, tmp_path, "r1").aborted
    after = state(tmp_path)
    assert list(after["files"]) == [f"local://{tmp_path / 'in/a.csv'}"]
    assert after["claims"] == {}
    (tmp_path / "in/b.csv").write_text("id\n3\n")
    rerun = go(entity, tmp_path, "r2").report
    assert [f.get("skip_reason") for f in rerun["files"]] == ["already_ingested", None]
    accepted = pl.read_parquet(tmp_path / "out/accepted/*.parquet")
    assert sorted(accepted["id"].to_list()) == [1, 2, 3]


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
    assert (
        entity_module.state_uri(entity, tmp_path)[0] == f"local://{tmp_path / expected}"
    )


def test_state_path_is_local_and_relative_to_the_config(tmp_path: Path) -> None:
    entity = setup(tmp_path, state="state: {path: s/x.json}")
    assert (
        entity_module.state_uri(entity, tmp_path)[0]
        == f"local://{tmp_path / 's/x.json'}"
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


def test_full_refresh_reprocesses_and_resets_the_state(tmp_path: Path) -> None:
    """Rust `claim_all` + `promote_full_refresh`: nothing is skipped and the state holds only this run's files."""
    setup(tmp_path)
    (tmp_path / "in/old.csv").write_text("id\n9\n")
    config = tmp_path / "config.yml"
    run(config, parse_config(config))
    (tmp_path / "in/old.csv").unlink()
    out = run(config, parse_config(config), full_refresh=True).summary
    assert out["results"]["files_total"] == 1 and out["results"]["files_skipped"] == 0
    assert [Path(u).name for u in state(tmp_path)["files"]] == ["orders.csv"]
    assert state(tmp_path)["claims"] == {}
    assert run(config, parse_config(config)).summary["results"]["files_skipped"] == 1


def test_glob_with_a_parent_directory_component_resolves(tmp_path: Path) -> None:
    entity = setup(tmp_path, src="./in/../in/*.csv")
    assert go(entity, tmp_path, "r1").report["results"]["files_total"] == 1


@pytest.mark.parametrize(("mode", "moved"), [("archive", True), ("none", True), ("file", False), ("row", False)])  # fmt: skip
def test_archive_follows_rust_archive_enabled(tmp_path: Path, mode: str, moved: bool) -> None:  # fmt: skip
    """#824: only `archive`, or `none` with `sink.archive`, moves the source (Rust `archive_enabled()`)."""
    setup(tmp_path, mode=mode)
    config = tmp_path / "config.yml"
    config.write_text(config.read_text().replace("      rejected:", "      archive: {path: ./arch}\n      rejected:"))  # fmt: skip
    archived = go(parse_config(config).entities[0], tmp_path, "r1").report["files"][0]["output"]["archived_path"]  # fmt: skip
    assert (tmp_path / "in/orders.csv").exists() is not moved
    if moved:
        assert Path(archived).parent == tmp_path / "arch/orders"
        assert Path(archived).name.startswith("orders__run-r1__src-")
    else:
        assert archived is None


def test_a_failed_parquet_write_leaves_the_input_unarchived(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    """#914: archiving waits for the sink commit, so a failed write loses no rows."""
    setup(tmp_path, mode="archive")
    config = tmp_path / "config.yml"
    config.write_text(config.read_text().replace("      rejected:", "      archive: {path: ./arch}\n      rejected:"))  # fmt: skip

    def boom(*_: Any) -> None:
        raise FloeRunError("commit failed")

    monkeypatch.setattr(entity_module, "write_accepted", boom)
    with pytest.raises(FloeRunError, match="commit failed"):
        go(parse_config(config).entities[0], tmp_path, "r1")
    assert (tmp_path / "in/orders.csv").exists()
    assert not (tmp_path / "arch").exists()


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
