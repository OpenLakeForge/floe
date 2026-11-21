"""`floe run` on `type: gcs`, against obstore's in-memory store in place of the GCSStore."""

import io
import json
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from obstore.store import MemoryStore

from floe.config.parse import parse_config
from floe.run import run

ROOT = "gs://b"
CONFIG = """\
version: "0.3"
storages:
  default: lake
  definitions:
    - {name: lake, type: gcs, bucket: b, prefix: base}
domains:
  - name: d
entities:
  - name: orders
    domain: d
    source: {format: csv, path: in/, options: {header: true, separator: ","}}
    sink:
      accepted: {format: parquet, path: out/accepted}
      rejected: {format: csv, path: out/rejected}
      archive: {path: arch}
    policy: {severity: reject}
    schema:
      columns:
        - {name: id, type: int64}
report: {path: runs}
"""


def test_a_gcs_source_is_ingested_and_outputs_land_on_gcs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    store = MemoryStore()

    def gcs(*_: object, **__: object) -> MemoryStore:
        return store

    monkeypatch.setattr("obstore.store.GCSStore", gcs)
    store.put("base/in/a.csv", b"id\n1\nx\n")
    (tmp_path / "config.yml").write_text(CONFIG)
    out: Any = run(tmp_path / "config.yml", parse_config(tmp_path / "config.yml"), run_id="r1")  # fmt: skip
    assert out.report_base_path == f"{ROOT}/base/runs"
    accepted = bytes(store.get("base/out/accepted/part-00000.parquet").bytes())
    assert pl.read_parquet(io.BytesIO(accepted))["id"].to_list() == [1]
    report = json.loads(bytes(store.get("base/runs/d/run_r1/d/orders/run.json").bytes()))  # fmt: skip
    assert [f["input_file"] for f in report["files"]] == [f"{ROOT}/base/in/a.csv"]
    names = [m["path"] for chunk in store.list("base/out/rejected") for m in chunk]
    assert names, "the rejected row is written to gcs"
    assert any(
        m["path"].startswith("base/arch/") for c in store.list("base/arch") for m in c
    )  # archived
