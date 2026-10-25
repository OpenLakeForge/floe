"""`floe run --manifest`: a run from a generated manifest matches the config run (Rust: `run_with_manifest_path`)."""

import json
import re
import shutil
import sys
from pathlib import Path
from typing import Any, cast

import polars as pl
import pytest

from floe._cli import main
from floe.manifest_run import load_manifest

CONFORMANCE = Path(__file__).parents[2] / "conformance"
# D22: a manifest run reports the manifest spec_version (the Floe version), a config run the config version
VOLATILE = {
    "started_at",
    "finished_at",
    "duration_ms",
    "elapsed_ms",
    "spec_version",
    "version",
}
ARCHIVE_NAME = re.compile(
    r"__run-.+?__src-[0-9a-f]{16}"
)  # hashes the absolute source URI


def floe(monkeypatch: pytest.MonkeyPatch, *argv: str) -> int:
    monkeypatch.setattr(sys, "argv", ["floe", *argv])
    return main()


def _mask(value: Any, root: str) -> Any:
    if isinstance(value, dict):
        doc = cast(dict[str, Any], value)
        return {k: "<volatile>" if k in VOLATILE else _mask(v, root) for k, v in doc.items()}  # fmt: skip
    if isinstance(value, list):
        return [_mask(v, root) for v in cast(list[Any], value)]
    if isinstance(value, str):
        return ARCHIVE_NAME.sub("__run-<id>", value.replace(root, "<ROOT>"))
    return value


def outputs(work: Path) -> dict[str, Any]:
    """Reports (masked), accepted rows and rejected files of a finished run."""
    out: dict[str, Any] = {}
    for p in sorted(work.glob("report/**/*.json")):
        out[p.relative_to(work).as_posix()] = _mask(json.loads(p.read_text()), str(work))  # fmt: skip
    for p in sorted(work.glob("out/**/*.parquet")):
        out[p.parent.relative_to(work).as_posix()] = pl.read_parquet(p).to_dicts()
    for p in sorted(work.glob("out/**/*.csv")):
        out[re.sub(r"part-[0-9a-f-]{36}", "part", p.relative_to(work).as_posix())] = p.read_text()  # fmt: skip
    return out


def copy_case(case: str, dest: Path) -> Path:
    shutil.copytree(CONFORMANCE / case, dest, ignore=shutil.ignore_patterns("expected*"))  # fmt: skip
    return dest


@pytest.mark.parametrize(
    "case",
    [
        "severity_warn",
        "mismatch_abort",
        "mismatch_reject_file",
        "archive_mode",
        "xml",
        "example/standalone",
    ],
)
def test_a_run_from_the_generated_manifest_matches_the_config_run(
    case: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    by_config, by_manifest = copy_case(case, tmp_path / "c"), copy_case(case, tmp_path / "m")  # fmt: skip
    monkeypatch.chdir(by_config)
    code = floe(monkeypatch, "run", "-c", "config.yml", "--run-id", "r")
    monkeypatch.chdir(by_manifest)
    assert floe(monkeypatch, "manifest", "generate", "-c", "config.yml", "-o", "m.json") == 0  # fmt: skip
    assert floe(monkeypatch, "run", "--manifest", "m.json", "--run-id", "r") == code
    expected, actual = outputs(by_config), outputs(by_manifest)
    summary = "report/run_r/run.summary.json"
    assert expected[summary]["config"].pop("path") == "<ROOT>/config.yml"
    assert actual[summary]["config"].pop("path") == "m.json"  # as typed, as Rust
    assert actual == expected


@pytest.mark.parametrize(
    ("case", "flags"),
    [
        ("archive_mode", ["--log-format", "json"]),
        ("archive_mode", ["--log-format", "text", "--verbose"]),
        ("archive_mode", ["--quiet"]),
        ("archive_mode", ["--dry-run", "--log-format", "json"]),
        # #880: a manifest run also lists every input before the first entity_started
        ("unresolved_inputs", ["--log-format", "json"]),
    ],
)
def test_events_and_output_match_the_config_run(
    case: str,
    flags: list[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def console(work: Path, *source: str) -> tuple[int, str, str]:
        monkeypatch.chdir(work)
        code = floe(monkeypatch, "run", *source, "--run-id", "r", *flags)
        out, err = capsys.readouterr()
        out, err = (s.replace(str(work), "<ROOT>") for s in (out, err))
        timing = re.compile(r'(ts_ms|elapsed_ms)(":|=)\d+')
        return code, timing.sub(r"\1\2T", out), timing.sub(r"\1\2T", err)

    by_config = copy_case(case, tmp_path / "c")
    expected = console(by_config, "-c", "config.yml")
    by_manifest = copy_case(case, tmp_path / "m")
    monkeypatch.chdir(by_manifest)
    assert floe(monkeypatch, "manifest", "generate", "-c", "config.yml", "-o", "m.json") == 0  # fmt: skip
    capsys.readouterr()
    actual = console(by_manifest, "--manifest", "m.json")
    config = "<ROOT>/config.yml"  # Rust logs the manifest path as typed
    assert actual == tuple(
        x.replace(config, "m.json") if isinstance(x, str) else x for x in expected
    )
    logged = (
        "--log-format" in flags and "--dry-run" not in flags
    )  # a dry run emits no events
    assert ("run_started" in expected[1]) == logged


NAMED = """\
version: "0.3"
metadata: {project: shop, owner: data, tags: [bronze]}
sources:
  - {name: drop, format: csv, path: "./in/{{resource}}.csv", options: {header: true}}
domains: [{name: spike}]
entities:
  - name: orders
    domain: spike
    source: {ref: drop, resource: orders}
    sink: {accepted: {format: parquet, path: ./out/accepted/orders}}
    policy: {severity: warn}
    schema: {columns: [{name: id, type: int64}, {name: name, type: string}, {name: qty, type: int64}]}
report: {path: ./report}
"""


def test_an_image_runtime_manifest_with_sources_and_metadata_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """D20: the manifest's `sources` and `metadata` blocks and work-root paths load and run."""
    by_config, by_manifest = copy_case("severity_warn", tmp_path / "c"), copy_case("severity_warn", tmp_path / "m")  # fmt: skip
    for work in (by_config, by_manifest):
        (work / "config.yml").write_text(NAMED)
    monkeypatch.chdir(by_config)
    assert floe(monkeypatch, "run", "-c", "config.yml", "--run-id", "r") == 0
    monkeypatch.chdir(by_manifest)
    monkeypatch.setenv("FLOE_WORK_ROOT", str(by_manifest))
    argv = ("-c", "config.yml", "-o", "m.json", "--runtime", "image")
    assert floe(monkeypatch, "manifest", "generate", *argv) == 0
    m = json.loads((by_manifest / "m.json").read_text())
    assert m["sources"][0]["name"] == "drop" and m["metadata"]["project"] == "shop"
    assert m["entities"][0]["source"]["uri"] == f"local://{by_manifest}/in/orders.csv"
    assert floe(monkeypatch, "run", "--manifest", "m.json", "--run-id", "r") == 0
    expected, actual = outputs(by_config), outputs(by_manifest)
    summary = "report/run_r/run.summary.json"
    expected[summary]["config"].pop("metadata")  # ponytail: carried over by #873
    assert expected[summary]["config"].pop("path") == "<ROOT>/config.yml"
    assert actual[summary]["config"].pop("path") == "m.json"
    assert actual == expected


def test_manifest_paths_resolve_against_its_own_directory_and_entities_select(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rust `run_with_manifest_file_executes_entity`."""
    work = copy_case("severity_warn", tmp_path / "w")
    monkeypatch.chdir(work)
    assert floe(monkeypatch, "manifest", "generate", "-c", "config.yml", "-o", "m.json") == 0  # fmt: skip
    monkeypatch.chdir(tmp_path)
    assert floe(monkeypatch, "run", "--manifest", "w/m.json", "--entities", "orders") == 0  # fmt: skip
    assert list((work / "out/accepted/orders").glob("*.parquet"))
    assert floe(monkeypatch, "run", "--manifest", "w/m.json", "--entities", "nope") == 1


def manifest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, config: str) -> Path:
    (tmp_path / "config.yml").write_text(config)
    out = tmp_path / "m.json"
    argv = ("manifest", "generate", "-c", str(tmp_path / "config.yml"), "-o", str(out))
    assert floe(monkeypatch, *argv) == 0
    return out


QUALIFIED = """\
version: "0.3"
storages:
  default: local
  definitions:
    - {name: local, type: local}
    - {name: bronze, type: s3, bucket: example-bronze, region: eu-west-1}
sources:
  - {name: crm, format: csv, path: "./bronze/{{resource}}.csv"}
domains: [{name: sales}, {name: finance}]
entities:
  - name: accounts
    domain: sales
    source: {ref: crm, resource: crm_orders}
    sink:
      accepted: {format: parquet, storage: bronze, path: sales/accounts}
      rejected: {format: csv, path: ./out/rejected}
    policy: {severity: reject}
    schema: {columns: [{name: id, type: string}]}
  - name: accounts
    domain: finance
    source: {format: csv, path: ./in/a.csv}
    sink: {accepted: {format: parquet, path: ./out/a}}
    policy: {severity: warn}
    schema: {columns: [{name: id, type: string}]}
lineage: {url: "http://localhost:1", namespace: ns}
report: {path: ./report}
"""


def test_reconstruct_keeps_ids_bare_names_refs_storages_and_lineage_cloud_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rust `v03_manifest_names_are_qualified_and_replay_recovers_bare_names`,
    `manifest_records_source_ref_and_effective_resource` and `manifest_embeds_config_level_storages_for_replay`.
    """
    root, observed = load_manifest(manifest(tmp_path, monkeypatch, QUALIFIED))
    assert [(e.id, e.name, e.domain) for e in root.entities] == [
        ("finance.accounts", "accounts", "finance"),
        ("sales.accounts", "accounts", "sales"),
    ]
    sales = root.entities[1]
    assert (sales.source.source_ref, sales.source.resource) == ("crm", "crm_orders")
    assert sales.policy.severity == "reject"
    assert root.storages and root.storages.definitions[1].bucket == "example-bronze"
    assert root.lineage and root.lineage.namespace == "ns"
    assert root.report.path == str(tmp_path / "report")
    assert sales.sink.accepted.path == "sales/accounts"  # the run resolves the raw path
    assert observed[1].sink.accepted.path == "s3://example-bronze/sales/accounts"
    assert (
        observed[0].sink.accepted.path == "./out/a"
    )  # local paths stay as reconstructed


def test_a_legacy_dotted_bare_name_is_kept_whole(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = manifest(tmp_path, monkeypatch, QUALIFIED)
    doc = json.loads(path.read_text())
    doc["entities"][0]["asset_key"] = ["finance", "finance.accounts"]
    path.write_text(json.dumps(doc))
    entity = load_manifest(path)[0].entities[0]
    assert (entity.id, entity.name) == ("finance.accounts", "finance.accounts")


@pytest.mark.parametrize(
    ("edit", "error"),
    [
        ({"storages": "nope"}, "Error: manifest storages block is malformed: "),
        (
            {"spec_version": "9.9"},
            "Error: manifest spec_version=9.9 is unsupported (newer than Floe 0.",
        ),
        ({"entities": None}, "Error: manifest parse error: "),
        (
            {"report_base_uri": "s3://b/report"},
            "Error: report_base_uri=s3://b/report: remote reports are not supported yet (#834)",
        ),
    ],
)
def test_a_bad_manifest_fails_before_the_run(
    edit: dict[str, object],
    error: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Rust `manifest_reconstruct_rejects_malformed_storages_block`; D22 for `spec_version`."""
    path = manifest(tmp_path, monkeypatch, QUALIFIED)
    path.write_text(json.dumps(json.loads(path.read_text()) | edit))
    capsys.readouterr()
    assert floe(monkeypatch, "run", "--manifest", str(path)) == 1
    assert capsys.readouterr().err.startswith(error)
    assert not (tmp_path / "report").exists()


@pytest.mark.parametrize(
    ("location", "error"),
    [
        (
            "s3://b/m.json",
            "Error: remote manifest is not supported yet (#834): s3://b/m.json\n",
        ),
        ("missing.json", "Error: No such file or directory (os error 2)\n"),
    ],
)
def test_an_unreadable_manifest_location_fails(
    location: str,
    error: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    assert floe(monkeypatch, "run", "--manifest", location) == 1
    assert capsys.readouterr().err == error


def test_a_missing_manifest_is_logged_after_run_started_with_code_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    argv = ("run", "--manifest", "nope.json", "--run-id", "r", "--log-format", "text")
    assert floe(monkeypatch, *argv) == 1
    assert capsys.readouterr().out.splitlines()[:2] == [
        "run_started run_id=r config=nope.json report_base=disabled",
        "log level=error code=error No such file or directory (os error 2)",
    ]  # Rust 0.7, byte for byte
