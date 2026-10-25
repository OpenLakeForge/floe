"""`floe run`: exit code, run reports and the stop after an aborted entity."""

import json
import sys
from pathlib import Path
from typing import Any

import pytest

from floe._cli import main

ENTITY = """\
  - name: {name}
    domain: d
    source:
      format: csv
      path: ./in/{name}.csv
      options: {{header: true, separator: ","}}
    sink:
      accepted: {{format: parquet, path: ./out/accepted/{name}}}
      rejected: {{format: csv, path: ./out/rejected/{name}}}
    policy: {{severity: abort}}
    schema:
      columns:
        - {{name: id, type: int64}}
"""


def run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rows: dict[str, str]) -> int:
    (tmp_path / "in").mkdir()
    for name, text in rows.items():
        (tmp_path / "in" / f"{name}.csv").write_text(text)
    config = 'version: "0.3"\ndomains:\n  - name: d\nentities:\n'
    config += "".join(ENTITY.format(name=n) for n in rows)
    (tmp_path / "config.yml").write_text(config + "report: {path: ./report}\n")
    monkeypatch.setattr(
        sys, "argv", ["floe", "run", "-c", str(tmp_path / "config.yml")]
    )
    return main()


def summary(tmp_path: Path) -> dict[str, Any]:
    (path,) = (tmp_path / "report").glob("run_*/run.summary.json")
    return json.loads(path.read_text())


def test_clean_run_exits_zero_and_writes_both_reports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert run(tmp_path, monkeypatch, {"a": "id\n1\n"}) == 0
    assert summary(tmp_path)["run"]["status"] == "success"
    assert list((tmp_path / "report").glob("run_*/d/a/run.json"))


def test_abort_exits_two_and_skips_the_later_entities(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert run(tmp_path, monkeypatch, {"a": "id\nx\n", "b": "id\n1\n"}) == 2
    assert [e["name"] for e in summary(tmp_path)["entities"]] == ["d.a"]
    assert not (tmp_path / "out" / "accepted" / "b").exists()


def test_a_failing_entity_keeps_the_reports_of_the_ones_before_it_and_writes_no_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "a.csv").write_text("id\n1\n")  # b.csv is missing
    config = 'version: "0.3"\ndomains:\n  - name: d\nentities:\n'
    config += "".join(ENTITY.format(name=n) for n in ("a", "b"))
    (tmp_path / "config.yml").write_text(config + "report: {path: ./report}\n")
    monkeypatch.setattr(
        sys, "argv", ["floe", "run", "-c", str(tmp_path / "config.yml")]
    )
    assert main() == 1
    assert not list(
        (tmp_path / "report").glob("run_*/run.summary.json")
    )  # Rust: no summary
    assert len(list((tmp_path / "report").glob("run_*/d/a/run.json"))) == 1
    assert (tmp_path / "out" / "accepted" / "a").exists()


STORAGES = (
    "storages:\n  default: local\n  definitions:\n    - {name: local, type: local}\n    - {name: lake, type: s3, bucket: b, region: eu-west-1}\n"
    "    - {name: other, type: local}\n"
)


def run_with(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, old: str, new: str
) -> int:
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "a.csv").write_text("id\n1\n")
    entity = ENTITY.format(name="a").replace(old, new, 1)
    (tmp_path / "config.yml").write_text(
        f'version: "0.3"\n{STORAGES}domains:\n  - name: d\nentities:\n{entity}'
    )
    monkeypatch.setattr(
        sys, "argv", ["floe", "run", "-c", str(tmp_path / "config.yml")]
    )
    return main()


@pytest.mark.parametrize(
    ("old", "new", "field"),
    [
        (
            "path: ./out/accepted/a}",
            "path: ./out/accepted/a, storage: lake}",
            "sink.accepted.storage",
        ),
        (
            "path: ./out/rejected/a}",
            "path: ./out/rejected/a, storage: lake}",
            "sink.rejected.storage",
        ),
    ],
)
def test_a_remote_entity_storage_is_refused_before_any_io(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    old: str,
    new: str,
    field: str,
) -> None:
    assert run_with(tmp_path, monkeypatch, old, new) == 1
    assert f"{field}=lake (type s3) is not supported yet" in capsys.readouterr().err
    assert not (tmp_path / "out").exists()


def test_a_local_named_storage_still_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    old = "path: ./in/a.csv\n"
    assert run_with(tmp_path, monkeypatch, old, old + "      storage: other\n") == 0
    assert (tmp_path / "out" / "accepted" / "a").exists()


def write(tmp_path: Path, rows: dict[str, str]) -> Path:
    (tmp_path / "in").mkdir()
    for name, text in rows.items():
        (tmp_path / "in" / f"{name}.csv").write_text(text)
    config = 'version: "0.3"\ndomains:\n  - name: d\nentities:\n'
    config += "".join(ENTITY.format(name=n) for n in rows)
    (tmp_path / "config.yml").write_text(config + "report: {path: ./report}\n")
    return tmp_path / "config.yml"


def floe(monkeypatch: pytest.MonkeyPatch, *args: str) -> int:
    monkeypatch.setattr(sys, "argv", ["floe", *args])
    return main()


def test_run_id_names_the_run_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = write(tmp_path, {"a": "id\n1\n"})
    assert floe(monkeypatch, "run", "-c", str(config), "--run-id", "r1") == 0
    assert summary(tmp_path)["run"]["run_id"] == "r1"
    assert (tmp_path / "report" / "run_r1" / "run.summary.json").exists()


def test_entities_restricts_the_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = write(tmp_path, {"a": "id\n1\n", "b": "id\n1\n", "c": "id\n1\n"})
    assert floe(monkeypatch, "run", "-c", str(config), "--entities", "a", "--entities", "d.c") == 0  # fmt: skip
    assert [e["name"] for e in summary(tmp_path)["entities"]] == ["d.a", "d.c"]


def test_unknown_entity_exits_one(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config = write(tmp_path, {"a": "id\n1\n"})
    assert floe(monkeypatch, "run", "-c", str(config), "--entities", "a,zz") == 1
    assert capsys.readouterr().err == "Error: entities not found: zz\n"


def test_full_refresh_overwrites_an_append_sink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = write(tmp_path, {"a": "id\n1\n"})
    config.write_text(config.read_text().replace("    sink:\n", "    sink:\n      write_mode: append\n"))  # fmt: skip
    for _ in range(2):
        assert floe(monkeypatch, "run", "-c", str(config)) == 0
    assert len(list((tmp_path / "out/accepted/a").glob("*.parquet"))) == 2
    assert floe(monkeypatch, "run", "-c", str(config), "-F") == 0
    assert len(list((tmp_path / "out/accepted/a").glob("*.parquet"))) == 1


def test_profile_variables_reach_the_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = write(tmp_path, {"a": "id\n1\n"})
    config.write_text(config.read_text().replace("./out/accepted/a", "'./out/{{ZONE}}/a'"))  # fmt: skip
    (tmp_path / "p.yml").write_text(
        "apiVersion: floe/v1\nkind: EnvironmentProfile\nmetadata: {name: dev}\nvariables: {ZONE: bronze}\n"
    )
    assert floe(monkeypatch, "run", "-c", str(config), "-p", str(tmp_path / "p.yml")) == 0  # fmt: skip
    assert (tmp_path / "out" / "bronze" / "a").exists()


@pytest.mark.parametrize(
    ("args", "error"),
    [
        ([], "Error: one of --config or --manifest is required\n"),
        (["--manifest", "m.json"], "Error: floe run --manifest is not supported yet\n"),
    ],
)
def test_run_needs_a_config(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    args: list[str],
    error: str,
) -> None:
    assert floe(monkeypatch, "run", *args) == 1
    assert capsys.readouterr().err == error
