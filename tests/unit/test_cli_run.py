"""`floe run`: exit code, run reports, and the run going on past an aborted entity (D36)."""

import importlib
import json
import sys
from pathlib import Path
from typing import Any

import pytest

from floe._cli import main
from floe.config.types import EntityConfig
from floe.errors import FloeStorageError
from floe.run.entity import EntityRun

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
    (path,) = (tmp_path / "report").glob("d/run_*/run.summary.json")
    return json.loads(path.read_text())


def test_clean_run_exits_zero_and_writes_both_reports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert run(tmp_path, monkeypatch, {"a": "id\n1\n"}) == 0
    assert summary(tmp_path)["run"]["status"] == "success"
    assert list((tmp_path / "report").glob("d/run_*/d/a/run.json"))


def test_d36_an_abort_fails_only_its_entity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rows = {"a": "id\n1\n", "b": "id\nx\n", "c": "id\n1\n"}
    assert run(tmp_path, monkeypatch, rows) == 2
    entities = summary(tmp_path)["entities"]
    assert [(e["name"], e["status"]) for e in entities] == [
        ("d.a", "success"),
        ("d.b", "aborted"),
        ("d.c", "success"),
    ]
    assert summary(tmp_path)["run"]["status"] == "aborted"
    assert (tmp_path / "out/accepted/a").exists() and (tmp_path / "out/accepted/c").exists()  # fmt: skip
    assert list((tmp_path / "report").glob("d/run_*/d/c/run.json"))


def test_d36_a_storage_failure_still_stops_the_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_pkg = importlib.import_module(
        "floe.run"
    )  # `floe.run` the attribute is the run function
    real = run_pkg.run_entity

    def failing(entity: EntityConfig, *args: Any) -> EntityRun:
        if entity.name == "b":
            raise FloeStorageError("storage down")
        return real(entity, *args)

    monkeypatch.setattr(run_pkg, "run_entity", failing)
    assert run(tmp_path, monkeypatch, {"a": "id\n1\n", "b": "id\n1\n", "c": "id\n1\n"}) == 1  # fmt: skip
    assert (tmp_path / "out/accepted/a").exists()
    assert not (tmp_path / "out/accepted/c").exists()
    assert not list((tmp_path / "report").glob("d/run_*/run.summary.json"))


def test_an_unresolved_input_fails_before_any_entity_runs(
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
    # Rust resolves every entity's inputs first (#864): `a` never runs, nothing is written
    assert not (tmp_path / "report").exists()
    assert not (tmp_path / "out").exists()


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
    assert (tmp_path / "report" / "d" / "run_r1" / "run.summary.json").exists()


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


@pytest.mark.parametrize(
    ("entities", "run_dir", "domain"),
    [(["a", "b"], "sales/run_r", "sales"), (["a", "b", "h"], "run_r", None)],
)
def test_d37_a_single_domain_run_reports_under_its_domain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    entities: list[str],
    run_dir: str,
    domain: str | None,
) -> None:
    config = write(tmp_path, dict.fromkeys(entities, "id\n1\n"))
    text = config.read_text().replace("domain: d", "domain: sales")
    text = text.replace("  - name: d\n", "  - name: sales\n  - name: hr\n")
    config.write_text(text.replace("  - name: h\n    domain: sales", "  - name: h\n    domain: hr"))  # fmt: skip
    assert floe(monkeypatch, "run", "-c", str(config), "--run-id", "r") == 0
    summary = json.loads((tmp_path / "report" / run_dir / "run.summary.json").read_text())  # fmt: skip
    assert summary["run"].get("domain") == domain
    assert summary["report"]["report_file"].endswith(f"/report/{run_dir}/run.summary.json")  # fmt: skip
    assert (tmp_path / "report" / run_dir / "sales" / "a" / "run.json").exists()
