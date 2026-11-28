"""#1458: what an independent reader of a Parquet output directory sees when a run dies, and what a late policy abort leaves.

Parquet staging is **not** atomic (docs/sinks/parquet.md): parts are staged as `_staging-*`, then copied over their
final names one by one, then the old parts are deleted. These tests pin what is observable at each step and name the
operator-facing outcome of every case, in the vocabulary of #1458:

- `safe to retry`: the retry converges to the data a clean run would give.
- `may temporarily expose partial data`: a reader of `part-*.parquet` sees rows that no run intended to publish
  together, until a retry (or an operator) repairs the directory.
- `needs manual intervention`: a retry does not repair it (it duplicates rows, skips them, or the run is lost).

The kill is a `Crash` (a `BaseException`) raised at an existing seam, as in `test_crash_retry.py`, whose helpers are
reused. Storage is the real local client in a temp directory; only a Floe or `LocalClient` call is made to raise.
"""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportMissingTypeStubs=false
# pyright: reportPrivateUsage=false

import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parent))  # the sibling test module's helpers  # fmt: skip
from test_crash_retry import (  # pyright: ignore[reportMissingImports] -- a sibling test module, put on the path above
    NEW,
    OLD,
    Crash,
    Pipeline,
    arm,
    dead,
)

from floe._cli import main
from floe.run.incremental import Incremental
from floe.storage.local import LocalClient

OLD3 = "id,name\n100,z\n101,y\n102,x\n"  # three parts at one row each: a later, shorter output leaves a stale tail
STAGING = "_staging-"


def project(
    tmp_path: Path, mode: str, incremental: str, max_size: int | None = None
) -> Pipeline:
    """The `orders` Parquet entity of `test_crash_retry`, with `incremental_mode: file` or none, one row per part with `max_size`."""
    p = Pipeline(tmp_path, "parquet", mode)
    text = (tmp_path / "config.yml").read_text()
    if incremental == "none":
        text = text.replace("    incremental_mode: file\n", "")
    if max_size:
        text = text.replace("path: ./out/accepted}", f"path: ./out/accepted, options: {{max_size_per_file: {max_size}}}}}")  # fmt: skip
    (tmp_path / "config.yml").write_text(text)
    return p


def visible(p: Pipeline) -> list[int]:
    """The accepted ids a reader of `part-*.parquet` sees: the only files Floe and its docs call output."""
    return sorted(
        i
        for f in (p.dir / "out/accepted").glob("part-*.parquet")
        for i in pl.read_parquet(f)["id"]
    )


def staged(p: Pipeline) -> list[str]:
    """Orphaned staging files: not `part-*`, so hidden from a reader of the parts (and from engines that skip `_` names)."""
    return sorted(
        f.name for f in (p.dir / "out/accepted").iterdir() if f.name.startswith(STAGING)
    )


def run_reports(p: Pipeline, run_id: str) -> list[Path]:
    return list((p.dir / "report").rglob(f"run_{run_id}/**/run.json"))


def seed(p: Pipeline, old: str = OLD) -> list[int]:
    (p.dir / "in/old.csv").write_text(old)
    p.run("seed")
    held = visible(p)
    (p.dir / "in/new.csv").write_text(NEW)
    return held


def kill(monkeypatch: pytest.MonkeyPatch, p: Pipeline, die_on: Callable[[Any], None] | None = None, keep_staging: bool = False) -> None:  # fmt: skip
    """Run `dead` and let the run die: no `Incremental` cleanup; with `keep_staging`, no `_parts` cleanup either (a SIGKILL)."""
    with monkeypatch.context() as dying:
        for cleanup in ("release", "finish"):
            dying.setattr(Incremental, cleanup, dead(getattr(Incremental, cleanup)))
        if keep_staging:
            real = LocalClient.delete

            def sigkill_keeps_staging(self: LocalClient, uri: str) -> None:
                if not (STAGING in uri and isinstance(sys.exc_info()[1], Crash)):
                    real(
                        self, uri
                    )  # only the unwinding cleanup of the dying run is skipped

            dying.setattr(LocalClient, "delete", sigkill_keeps_staging)
        if die_on:
            die_on(dying)
        p.crash("dead")
    if (p.dir / "in/.floe").exists():
        p.expire_claims()


def second_copy_dies(dying: pytest.MonkeyPatch) -> None:
    """The publish loop copies the last part first (#1423); it dies on the second copy, after one part is visible."""
    copy, copies = LocalClient.copy, list[str]()

    def wrapped(self: LocalClient, src: str, dst: str) -> None:
        copies.append(dst)
        if len(copies) > 1:
            raise Crash
        copy(self, src, dst)

    dying.setattr(LocalClient, "copy", wrapped)


def first_old_delete_dies(dying: pytest.MonkeyPatch) -> None:
    """Every new part is published; the run dies on the first delete of an old part that no new part replaced."""
    delete = LocalClient.delete

    def wrapped(self: LocalClient, uri: str) -> None:
        if "/out/accepted/part-" in uri:
            raise Crash
        delete(self, uri)

    dying.setattr(LocalClient, "delete", wrapped)


# phase 1: before staging completes


@pytest.mark.parametrize(("mode", "incremental", "retried"), [
    ("overwrite", "file", [1, 2]),  # the retry skips old.csv: it is in the state
    ("overwrite", "none", [1, 2, 100]),  # no state: every input is read again
    ("append", "file", [1, 2, 100]),
    ("append", "none", [1, 2, 100, 100]),  # no marker by contract: `none` appends the whole input set on every run
])  # fmt: skip
def test_a_crash_before_staging_completes_leaves_the_output_untouched_and_the_retry_converges(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str, incremental: str, retried: list[int]) -> None:  # fmt: skip
    """`safe to retry`. One part is staged, the process is killed: the staged file is an orphan no reader of `part-*` sees."""
    p = project(tmp_path, mode, incremental, max_size=1)
    held = seed(p)
    kill(
        monkeypatch,
        p,
        lambda dying: arm(dying, "parquet", "partial_flush"),
        keep_staging=True,
    )
    assert visible(p) == held == [100]  # nothing of the dead run is published
    assert (
        len(staged(p)) == 1
    )  # the orphan is named `_staging-<id>-part-...`, safe to delete
    assert run_reports(p, "dead") == []  # no report: the entity never finished
    p.run("retry")
    assert visible(p) == retried
    assert p.rejected() == [
        "x"
    ]  # the dead run's rejected part is named from its input: rewritten, not duplicated


# phase 2: after the first published part, before all parts are published


@pytest.mark.parametrize(("mode", "incremental", "exposed", "retried", "outcome"), [
    # overwrite: the new part-00001 replaced old part-00001 while old parts 00000 and 00002 stay: a mix of both outputs
    ("overwrite", "file", [2, 100, 102], [1, 2], "may temporarily expose partial data; a retry restores a clean directory"),
    ("overwrite", "none", [100, 101, 102, 102], [1, 2, 100, 101, 102], "may temporarily expose partial data; a retry restores a clean directory"),
    # append: the last part of the batch is published first, so row 1 is missing; a retry appends the whole batch again
    ("append", "file", [2, 100, 101, 102], [1, 2, 2, 100, 101, 102], "may temporarily expose partial data; needs manual intervention (row 2 twice)"),
    ("append", "none", [100, 101, 102, 102], [1, 2, 100, 100, 101, 101, 102, 102, 102], "may temporarily expose partial data; needs manual intervention (duplicates)"),
])  # fmt: skip
def test_a_crash_between_published_parts_exposes_a_partial_output(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str, incremental: str, exposed: list[int], retried: list[int], outcome: str) -> None:  # fmt: skip
    """Parquet staging is not atomic: the dead run's parts become visible one at a time, and `outcome` names what that means."""
    p = project(tmp_path, mode, incremental, max_size=1)
    seed(p, OLD3)
    assert visible(p) == [100, 101, 102]
    kill(monkeypatch, p, second_copy_dies, keep_staging=True)
    assert visible(p) == exposed, outcome  # neither the old output nor the new one
    assert staged(
        p
    )  # the unpublished staged copies are orphans: hidden, not cleaned up
    assert run_reports(p, "dead") == []
    p.run("retry")
    assert visible(p) == retried, outcome


def test_an_overwrite_that_dies_before_deleting_its_old_parts_exposes_a_stale_tail(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    """`may temporarily expose partial data`: every new part is published, one old part nothing replaced is still there."""
    p = project(
        tmp_path, "overwrite", "file", max_size=1
    )  # `none` re-reads the old input too, so no old part is left over
    seed(p, OLD3)
    kill(monkeypatch, p, first_old_delete_dies, keep_staging=True)
    assert visible(p) == [1, 2, 102]  # the new output plus the old part-00002
    p.run("retry")
    assert visible(p) == [1, 2]  # `safe to retry`: the retry deletes the stale tail


# phase 3: after the accepted commit, before the report and the state


@pytest.mark.parametrize(("mode", "incremental", "exposed", "retried", "outcome"), [
    ("overwrite", "file", [1, 2], [1, 2], "safe to retry"),
    ("overwrite", "none", [1, 2, 100], [1, 2, 100], "safe to retry"),
    ("append", "file", [1, 2, 100], [1, 1, 2, 2, 100], "needs manual intervention: Parquet has no snapshot to carry the input hash, so rows are duplicated"),
    ("append", "none", [1, 2, 100, 100], [1, 1, 2, 2, 100, 100, 100], "needs manual intervention: duplicates, as for every `none` rerun"),
])  # fmt: skip
def test_a_crash_after_the_accepted_commit_has_published_the_rows_but_no_report(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str, incremental: str, exposed: list[int], retried: list[int], outcome: str) -> None:  # fmt: skip
    """The accepted and rejected outputs are complete and visible; the report and the state are not written."""
    p = project(tmp_path, mode, incremental)
    seed(p)
    kill(monkeypatch, p, lambda dying: arm(dying, "parquet", "after_commit"))
    assert visible(p) == exposed
    assert p.rejected() == [
        "x"
    ]  # the rejected part is written per file, before the accepted sink
    assert run_reports(p, "dead") == []
    failure = json.loads(
        next((p.dir / "report").rglob("run_dead/run.failure.json")).read_text()
    )["entities"][0]
    assert failure["sinks"] == {"rejected": "committed", "accepted": "unknown"}  # the kill came before the write returned  # fmt: skip
    if incremental == "file":
        assert p.state()[
            "claims"
        ]  # the dead run's claim, expired by `kill`, not a processed file
    p.run("retry")
    assert visible(p) == retried, outcome
    assert p.rejected() == ["x"]


# phase 4: after state promotion, before the report


@pytest.mark.parametrize("mode", ["overwrite", "append"])
def test_a_crash_after_state_promotion_loses_the_report_but_a_retry_writes_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str) -> None:  # fmt: skip
    """`safe to retry`, with the dead run's report lost for good (D18): the retry skips the promoted file."""
    p = project(tmp_path, mode, "file")
    seed(p)
    kill(monkeypatch, p, lambda dying: arm(dying, "parquet", "after_promote"))
    before = visible(p)
    assert before == ([1, 2] if mode == "overwrite" else [1, 2, 100])
    assert {Path(u).name for u in p.state()["files"]} == {"old.csv", "new.csv"}
    assert p.state()["claims"] == {}
    assert (
        run_reports(p, "dead") == []
    )  # there is no run.json to find; the data and the state say it ran
    report = p.run("retry")
    assert {Path(f["input_file"]).name: f.get("skip_reason") for f in report["files"]} == {"old.csv": "already_ingested", "new.csv": "already_ingested"}  # fmt: skip
    assert visible(p) == before
    assert (
        len(run_reports(p, "retry")) == 1
    )  # the retry's own report exists, and says nothing was ingested


# policy.min_rows and policy.max_rejection_rate: checked after the accepted commit, so an abort is not a rollback

POLICY = """\
version: "0.3"
entities:
  - name: orders
    incremental_mode: file
    source: {{format: csv, path: ./in, options: {{header: true, separator: ","}}}}
    sink:
      write_mode: overwrite
      accepted: {{format: parquet, path: ./out/accepted}}
      rejected: {{format: csv, path: ./out/rejected}}
    policy: {policy}
    schema:
      columns:
        - {{name: id, type: int64}}
        - {{name: name, type: string}}
report: {{path: ./report}}
"""


def floe_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[int, dict[str, Any], dict[str, Any]]:
    """`floe run`: its exit code, and the newest run's `run.summary.json` and the entity's `run.json`."""
    monkeypatch.setattr(
        sys, "argv", ["floe", "run", "-c", str(tmp_path / "config.yml")]
    )
    code = main()
    summary = max((tmp_path / "report").rglob("run.summary.json"))
    entity = max((tmp_path / "report").rglob("orders/run.json"))
    return code, json.loads(summary.read_text())["run"], json.loads(entity.read_text())


def accepted_ids(tmp_path: Path) -> list[int]:
    return sorted(
        i
        for f in (tmp_path / "out/accepted").glob("part-*.parquet")
        for i in pl.read_parquet(f)["id"]
    )


@pytest.mark.parametrize(("policy", "key", "violated"), [
    ("{severity: reject, max_rejection_rate: 0.25}", "max_rejection_rate_exceeded", "rejection_rate"),
    ("{severity: reject, min_rows: 10}", "min_rows_violated", "min_rows"),
])  # fmt: skip
def test_a_late_policy_abort_exits_2_but_leaves_the_committed_rows_and_a_rerun_does_not_retry_them(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, policy: str, key: str, violated: str) -> None:  # fmt: skip
    """`needs manual intervention`: the entity is `aborted` (exit 2) after its rows were committed, and `overwrite` already
    replaced the earlier output. The file is promoted, so a rerun is a clean `success` that changes nothing."""
    (tmp_path / "in").mkdir()
    (tmp_path / "config.yml").write_text(POLICY.format(policy="{severity: reject}"))
    (tmp_path / "in/a.csv").write_text("id,name\n1,a\n2,b\n3,c\n")
    assert floe_run(tmp_path, monkeypatch)[0] == 0
    assert accepted_ids(tmp_path) == [1, 2, 3]
    (tmp_path / "config.yml").write_text(POLICY.format(policy=policy))
    (tmp_path / "in/b.csv").write_text(
        "id,name\n7,a\nx,b\ny,c\n9,d\n"
    )  # 2 of 4 rejected, and 4 rows < 10
    code, run, report = floe_run(tmp_path, monkeypatch)
    assert (code, run["status"], run["exit_code"]) == (2, "aborted", 2)
    assert report["policy"][key] is True and violated in report["policy"]
    assert (report["results"]["rows_total"], report["results"]["accepted_total"], report["results"]["rejected_total"]) == (4, 2, 2)  # fmt: skip
    assert accepted_ids(tmp_path) == [
        7,
        9,
    ]  # visible, and the earlier output is gone: no rollback
    assert sorted(i for f in (tmp_path / "out/rejected").glob("*.csv") for i in pl.read_csv(f)["id"].cast(pl.String)) == ["x", "y"]  # fmt: skip
    code, run, report = floe_run(tmp_path, monkeypatch)  # the same inputs again
    assert (code, run["status"]) == (0, "success")
    assert {f["skip_reason"] for f in report["files"]} == {"already_ingested"}
    assert report["policy"][key] is not True  # not evaluated: nothing new was read
    assert accepted_ids(tmp_path) == [7, 9]


def test_a_min_rows_warning_exits_0_with_the_rows_committed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`safe to retry`: at `warn` the entity is `success_with_warnings` and the run exits 0."""
    (tmp_path / "in").mkdir()
    (tmp_path / "config.yml").write_text(
        POLICY.format(policy="{severity: warn, min_rows: 10}")
    )
    (tmp_path / "in/a.csv").write_text("id,name\n1,a\n2,b\n")
    code, run, report = floe_run(tmp_path, monkeypatch)
    assert (code, run["status"], run["exit_code"]) == (0, "success_with_warnings", 0)
    assert (
        report["policy"]["min_rows"] == 10
        and report["policy"]["min_rows_violated"] is True
    )
    assert accepted_ids(tmp_path) == [1, 2]
