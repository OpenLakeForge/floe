"""#1056: accepted chunks go to the sink as they are read; memory follows the chunk, not the entity."""

import gc
from collections.abc import Callable, Iterable, Iterator
from functools import partial
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from floe.config.parse import parse_config
from floe.errors import FloeIoError, FloeRunError
from floe.read.csv import CsvChunk, read_csv
from floe.run import entity as entity_module  # `floe.run` is the public function
from floe.run.entity import run_entity

CHUNK = 100
CONFIG = """\
version: "0.3"
domains:
  - name: d
entities:
  - name: orders
    domain: d
    source:
      format: csv
      path: ./in
      options: {{header: true, separator: ","}}
    sink:
      write_mode: {mode}
      accepted:
        format: parquet
        path: ./out/accepted
        options: {{max_size_per_file: {part_bytes}}}
      rejected: {{format: csv, path: ./out/rejected}}
    policy: {{severity: {severity}}}
    schema:
      columns:
        - {{name: id, type: int64, nullable: false}}
        - {{name: name, type: string}}
        - {{name: price, type: float64}}
        - {{name: day, type: date}}
report: {{path: ./report}}
"""


def csv_rows(ids: Iterable[int]) -> str:
    return "id,name,price,day\n" + "".join(f"{i},n{i},{i}.5,2026-01-{i % 28 + 1:02}\n" for i in ids)  # fmt: skip


def run(tmp_path: Path, files: dict[str, str], mode: str = "overwrite", severity: str = "reject", part_bytes: int = 256 * 1024 * 1024) -> dict[str, Any]:  # fmt: skip
    (tmp_path / "in").mkdir(parents=True, exist_ok=True)
    for name, body in files.items():
        (tmp_path / "in" / name).write_text(body)
    (tmp_path / "config.yml").write_text(CONFIG.format(mode=mode, severity=severity, part_bytes=part_bytes))  # fmt: skip
    entity = parse_config(tmp_path / "config.yml").entities[0]
    return run_entity(entity, tmp_path, "0.3").report


def accepted(tmp_path: Path) -> pl.DataFrame:
    return pl.read_parquet(tmp_path / "out" / "accepted").sort("id")


def live_rows() -> int:
    """Rows of the frames that carry `name`: the unique tracker's key frames (id only) grow with the keys, as designed (#1057)."""
    return sum(o.height for o in gc.get_objects() if type(o) is pl.DataFrame and "name" in o.columns)  # fmt: skip


class Probe:
    """The most rows held in DataFrames at once, sampled whenever the reader or the sink asks for more."""

    def __init__(self) -> None:
        self.peak = 0

    def sample(self) -> None:
        self.peak = max(self.peak, live_rows())

    def parts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Also sample as each Parquet part is written."""
        write = pl.DataFrame.write_parquet

        def tapped(df: pl.DataFrame, *args: Any, **kwargs: Any) -> Any:
            self.sample()
            return write(df, *args, **kwargs)

        monkeypatch.setattr(pl.DataFrame, "write_parquet", tapped)

    def reader(self, chunk_rows: int = CHUNK) -> Callable[..., Iterator[CsvChunk]]:
        def read(*args: Any) -> Iterator[CsvChunk]:
            for chunk in read_csv(*args, chunk_rows=chunk_rows):
                self.sample()
                yield chunk

        return read


def test_peak_rows_in_memory_follow_the_chunk_not_the_entity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    probe = Probe()
    monkeypatch.setattr(entity_module, "read_csv", probe.reader())
    probe.parts(monkeypatch)
    report = run(tmp_path, {"orders.csv": csv_rows(range(100 * CHUNK))}, part_bytes=CHUNK * 24)  # fmt: skip
    assert report["results"]["accepted_total"] == 100 * CHUNK
    assert accepted(tmp_path).height == 100 * CHUNK
    assert probe.peak <= 10 * CHUNK


def test_a_multi_chunk_run_writes_what_a_single_chunk_run_writes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    files = {"a.csv": csv_rows(range(250)), "b.csv": csv_rows(range(250, 400)).replace("\n300,n300", "\n,n300")}  # fmt: skip
    whole = run(tmp_path / "whole", files)
    monkeypatch.setattr(entity_module, "read_csv", partial(read_csv, chunk_rows=7))
    chunked = run(tmp_path / "chunked", files)
    assert_frame_equal(accepted(tmp_path / "chunked"), accepted(tmp_path / "whole"))
    assert chunked["results"] == whole["results"]
    assert chunked["accepted_output"]["files_written"] == whole["accepted_output"]["files_written"] == 1  # fmt: skip


def test_rows_of_a_file_that_fails_late_are_not_written(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    def read(path: Path, *args: Any) -> Iterator[CsvChunk]:
        for n, chunk in enumerate(read_csv(path, *args, chunk_rows=CHUNK)):
            if path.name == "b.csv" and n == 2:
                raise FloeIoError("csv read failed: boom")
            yield chunk

    monkeypatch.setattr(entity_module, "read_csv", read)
    files = {"a.csv": csv_rows(range(150)), "b.csv": csv_rows(range(1000, 1500))}
    report = run(tmp_path, files)
    assert accepted(tmp_path)["id"].to_list() == list(range(150))
    assert [(f["status"], f["accepted_count"]) for f in report["files"]] == [("success", 150), ("rejected", 0)]  # fmt: skip


def test_rows_of_an_aborted_file_are_not_written(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    monkeypatch.setattr(entity_module, "read_csv", partial(read_csv, chunk_rows=CHUNK))
    bad = csv_rows(range(1000, 1500)) + "x,n,1.5,2026-01-01\n"
    report = run(tmp_path, {"a.csv": csv_rows(range(150)), "b.csv": bad, "c.csv": csv_rows(range(2000, 2010))}, severity="abort")  # fmt: skip
    assert accepted(tmp_path)["id"].to_list() == list(range(150))
    assert [(f["status"], f["row_count"]) for f in report["files"]] == [("success", 150), ("aborted", 501)]  # fmt: skip


@pytest.mark.parametrize("mode", ["overwrite", "append"])
def test_a_failure_after_parts_were_written_leaves_none_and_keeps_its_message(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str) -> None:  # fmt: skip
    def read(path: Path, *args: Any) -> Iterator[CsvChunk]:
        for n, chunk in enumerate(read_csv(path, *args, chunk_rows=CHUNK)):
            if path.name == "b.csv" and n == 1:
                raise FloeRunError("boom")
            yield chunk

    monkeypatch.setattr(entity_module, "read_csv", read)
    files = {"a.csv": csv_rows(range(400)), "b.csv": csv_rows(range(1000, 1500))}
    with pytest.raises(FloeRunError, match="^boom$"):
        run(tmp_path, files, mode=mode, part_bytes=CHUNK * 24)
    assert not list((tmp_path / "out" / "accepted").glob("*"))
