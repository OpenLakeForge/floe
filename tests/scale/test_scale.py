"""#558: a generated 200-entity project through validate, manifest generate and runs, with runtime and peak RSS printed.

Run with `pytest -m scale tests/scale -rP` (nightly, `.github/workflows/python-integration.yml`); excluded from
`pytest` by default. `FLOE_SCALE_ENTITIES` changes N. Every `floe` call is a child process, so the peak RSS
(`VmHWM`: `ru_maxrss` would carry over pytest's own peak through the fork) is the command's own, not pytest's.
"""

import json
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any, NamedTuple

import polars as pl
import pytest
from scale_project import (
    ACCEPTED,
    DOMAINS,
    FILES,
    REJECTED,
    ROWS,
    generate,
    write_input,
)

pytestmark = [
    pytest.mark.scale,
    pytest.mark.skipif(
        sys.platform != "linux", reason="VmHWM is read from /proc, Linux only"
    ),
]

N = int(os.environ.get("FLOE_SCALE_ENTITIES", "200"))
WORKERS = 4
# ponytail: a fixed ceiling, ~3x the measured peak of a 200-entity run (support-matrix.md); revisit when RSS is bounded
MAX_RSS_MIB = 600
PROFILE = f"""\
apiVersion: floe/v1
kind: EnvironmentProfile
metadata: {{name: scale}}
execution:
  runner: {{type: local}}
  orchestration: {{strategy: parallel, max_concurrent_entities: {WORKERS}}}
"""
CHILD = """\
import sys
from floe._cli import main
sys.argv[0] = "floe"
try:
    code = main()
except SystemExit as stop:  # --version and argparse errors exit this way
    code = stop.code
peak = next(line.split()[1] for line in open("/proc/self/status") if line.startswith("VmHWM"))
print("PEAK_RSS_KIB", peak, file=sys.stderr)
sys.exit(code)
"""


class Result(NamedTuple):
    code: int
    seconds: float
    peak_mib: float
    out: str
    err: str


def floe(cwd: Path, *args: str) -> Result:
    started = time.monotonic()
    done = subprocess.run([sys.executable, "-c", CHILD, *args], check=False, cwd=cwd, capture_output=True, text=True)  # fmt: skip
    seconds = time.monotonic() - started
    *lines, peak = done.stderr.rstrip().splitlines() or ["PEAK_RSS_KIB 0"]
    return Result(
        done.returncode,
        seconds,
        int(peak.split()[-1]) / 1024,
        done.stdout,
        "\n".join(lines),
    )


@pytest.fixture(scope="module")
def timings(request: pytest.FixtureRequest) -> Iterator[dict[str, Result]]:
    """What each step cost, printed after the module (`-rP` or `-s` to see it)."""
    steps: dict[str, Result] = {}
    yield steps
    reporter = request.config.pluginmanager.get_plugin("terminalreporter")
    lines = [
        f"{label:<34}{r.seconds:8.2f} s{r.peak_mib:9.0f} MiB peak RSS"
        for label, r in steps.items()
    ]
    if reporter is not None:
        reporter.write_line(
            f"\nscale run summary, {N} entities in {len(DOMAINS)} domains, {WORKERS} workers"
        )
        for line in lines:
            reporter.write_line(line)


@pytest.fixture(scope="module")
def project(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, list[str]]:
    root = tmp_path_factory.mktemp("scale")
    ids = generate(root, N)
    (root / "profile.yml").write_text(PROFILE)
    return root, ids


def summary_of(root: Path, run_id: str) -> dict[str, Any]:
    return json.loads(
        (root / "report" / f"run_{run_id}" / "run.summary.json").read_text()
    )


def manifest(root: Path, *args: str) -> dict[str, Any]:
    done = floe(
        root,
        "manifest",
        "generate",
        "-c",
        "config.yml",
        "-o",
        "-",
        "--deterministic",
        *args,
    )
    assert done.code == 0, done.err
    return json.loads(done.out)


@pytest.fixture(scope="module")
def first_run(
    project: tuple[Path, list[str]], timings: dict[str, Result]
) -> dict[str, Any]:
    root, _ = project
    done = floe(
        root,
        "run",
        "-c",
        "config.yml",
        "-p",
        "profile.yml",
        "--run-id",
        "first",
        "--quiet",
    )
    timings[f"run all {N} (workers={WORKERS})"] = done
    assert done.code == 0, done.err
    return summary_of(root, "first")


def test_the_config_digest_is_the_same_for_the_same_project(tmp_path: Path) -> None:
    a, b = tmp_path / "a", tmp_path / "b"
    generate(a, N)
    generate(b, N)
    files = sorted(p.relative_to(a) for p in a.rglob("*") if p.is_file())
    assert (
        len(files) > 2 * N
    )  # a root, sources, domain and entity files, and the inputs
    assert files == sorted(p.relative_to(b) for p in b.rglob("*") if p.is_file())
    assert [p for p in files if p.suffix in (".yml", ".json") and (a / p).read_bytes() != (b / p).read_bytes()] == []  # fmt: skip
    first, second = manifest(a), manifest(b)
    assert first["config_checksum"] == second["config_checksum"]
    assert first["config_files"] == second["config_files"]
    assert (
        len(first["config_files"]) == 2 + len(DOMAINS) + N
    )  # config, sources, domains, entities
    assert first == manifest(
        a
    )  # same project, same checkout: the whole manifest repeats


def test_validate_accepts_the_project_and_a_slice(
    project: tuple[Path, list[str]], timings: dict[str, Result]
) -> None:
    root, ids = project
    timings[f"validate all {N}"] = done = floe(root, "validate", "-c", "config.yml")
    assert done.code == 0, done.err
    done = floe(root, "validate", "-c", "config.yml", "--entities", ",".join(ids[:3]))
    assert done.code == 0, done.err
    bad = floe(root, "validate", "-c", "config.yml", "--entities", "sales.nope")
    assert bad.code != 0 and "sales.nope" in bad.err + bad.out


def test_manifest_generate_for_all_entities_and_for_slices(project: tuple[Path, list[str]], timings: dict[str, Result]) -> None:  # fmt: skip
    root, ids = project
    full = manifest(root)
    timings[f"manifest generate all {N}"] = floe(
        root, "manifest", "generate", "-c", "config.yml", "-o", "-"
    )
    assert [e["name"] for e in full["entities"]] == ids
    assert [d["name"] for d in full["domains"]] == sorted(DOMAINS)
    # a slice by entity: the three entities, in config order
    picked = [ids[100], ids[0], ids[7]]
    assert [e["name"] for e in manifest(root, "--entities", ",".join(picked))["entities"]] == [ids[0], ids[7], ids[100]]  # fmt: skip
    # a slice by domain: every entity of `hr`
    hr = [i for i in ids if i.startswith("hr.")]
    sliced = manifest(root, "--entities", ",".join(hr))
    assert [e["name"] for e in sliced["entities"]] == hr
    assert {e["domain"] for e in sliced["entities"]} == {"hr"}
    assert len(hr) == N // len(DOMAINS)


def test_a_parallel_run_reports_every_entity(
    project: tuple[Path, list[str]], first_run: dict[str, Any]
) -> None:
    root, ids = project
    assert (
        first_run["run"]["status"] == "rejected"
    )  # every entity has rejects, none fails
    assert first_run["run"]["exit_code"] == 0
    assert [
        e["name"] for e in first_run["entities"]
    ] == ids  # config order, whatever the threads did
    for entity in first_run["entities"]:
        assert entity["results"] == {
            "files_total": FILES, "files_skipped": 0, "rows_total": FILES * ROWS,
            "accepted_total": ACCEPTED, "rejected_total": REJECTED, "warnings_total": 0, "errors_total": REJECTED,
        }, entity["name"]  # fmt: skip
    total = first_run["results"]
    assert (total["files_total"], total["rows_total"]) == (N * FILES, N * FILES * ROWS)
    assert (total["accepted_total"], total["rejected_total"]) == (
        N * ACCEPTED,
        N * REJECTED,
    )
    accepted = sum(
        pl.read_parquet(p).height for p in (root / "out/accepted").rglob("*.parquet")
    )
    assert accepted == N * ACCEPTED
    rejected = sum(
        pl.read_csv(p).height for p in (root / "out/rejected").rglob("*.csv")
    )
    assert rejected == N * REJECTED


def test_the_run_stays_under_the_memory_ceiling(
    first_run: dict[str, Any], timings: dict[str, Result]
) -> None:
    assert first_run["run"]["status"] == "rejected"
    assert timings[f"run all {N} (workers={WORKERS})"].peak_mib < MAX_RSS_MIB


def test_a_restart_skips_ingested_files_and_takes_only_new_ones(
    project: tuple[Path, list[str]],
    first_run: dict[str, Any],
    timings: dict[str, Result],
) -> None:
    root, ids = project
    assert first_run["results"]["files_total"] == N * FILES
    done = floe(
        root,
        "run",
        "-c",
        "config.yml",
        "-p",
        "profile.yml",
        "--run-id",
        "restart",
        "--quiet",
    )
    timings[f"restart, nothing new ({N})"] = done
    assert done.code == 0, done.err
    again = summary_of(root, "restart")["results"]
    assert (again["files_total"], again["files_skipped"]) == (N * FILES, N * FILES)
    assert (again["rows_total"], again["accepted_total"], again["rejected_total"]) == (
        0,
        0,
        0,
    )
    # a retry after three new drops: only those files are read, and nothing else is appended twice
    fresh = {0: "csv", 1: "json", 2: "parquet"}
    for k, fmt in fresh.items():
        write_input(root / "in" / fmt / f"{fmt}_{k:03d}" / f"part-{FILES}.{fmt}", fmt, list(range(9000 + k * 10, 9000 + k * 10 + ROWS)))  # fmt: skip
    done = floe(
        root,
        "run",
        "-c",
        "config.yml",
        "-p",
        "profile.yml",
        "--run-id",
        "retry",
        "--quiet",
    )
    timings[f"retry, 3 new files ({N})"] = done
    assert done.code == 0, done.err
    retry = summary_of(root, "retry")
    assert retry["results"]["files_skipped"] == N * FILES
    assert (retry["results"]["rows_total"], retry["results"]["accepted_total"]) == (
        3 * ROWS,
        3 * ROWS,
    )
    touched = {e["name"] for e in retry["entities"] if e["results"]["rows_total"]}
    assert touched == {
        i for i in ids if i.split(".")[1] in {"csv_000", "json_001", "parquet_002"}
    }
    accepted = sum(
        pl.read_parquet(p).height for p in (root / "out/accepted").rglob("*.parquet")
    )
    assert accepted == N * ACCEPTED + 3 * ROWS


def test_start_up_costs_what_a_one_entity_run_costs(project: tuple[Path, list[str]], timings: dict[str, Result]) -> None:  # fmt: skip
    root, ids = project
    timings["floe validate -c 1-entity"] = floe(
        root, "validate", "-c", "config.yml", "--entities", ids[0]
    )
    timings["floe --version (cold start)"] = version = floe(root, "--version")
    assert version.code == 0
    scratch = root.parent / "slice"
    shutil.rmtree(scratch, ignore_errors=True)
    shutil.copytree(
        root, scratch, ignore=shutil.ignore_patterns("out", "report", ".floe")
    )
    timings["run 1 entity, project of N"] = one = floe(scratch, "run", "-c", "config.yml", "--entities", ids[3], "--quiet")  # fmt: skip
    assert one.code == 0, one.err
