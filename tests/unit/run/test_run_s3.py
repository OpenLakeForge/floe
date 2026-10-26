"""`floe run` on `storage: s3`, against an in-memory S3 client (the SeaweedFS run is in tests/integration)."""

import hashlib
import io
import json
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import pytest

import floe
import floe.storage.resolve as resolve_module
from floe.config.parse import parse_config
from floe.errors import FloeRunError
from floe.run import run
from floe.storage.paths import parse_s3_uri
from floe.storage.s3 import temp_path_for_key
from floe.storage.types import ObjectRef, StoredObject
from floe.write.outputs import write_accepted


class MemS3:
    """`S3Client` over a dict of `s3://` URIs; keys are listed by prefix, as S3 does."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def resolve_uri(self, path: str) -> str:
        return f"s3://b/{path.lstrip('/')}"

    def list(self, prefix: str) -> list[ObjectRef]:
        keys = sorted(parse_s3_uri(u)[1] for u in self.objects)
        return [ObjectRef(f"s3://b/{k}", k, "2026-01-01T00:00:00Z", len(self.objects[f"s3://b/{k}"])) for k in keys if k.startswith(prefix)]  # fmt: skip

    def download(self, uri: str, temp_dir: Path) -> Path:
        dest = temp_path_for_key(temp_dir, parse_s3_uri(uri)[1])
        dest.write_bytes(self.objects[uri])
        return dest

    def write(self, uri: str, body: bytes) -> None:
        self.objects[uri] = body

    def copy(self, src_uri: str, dst_uri: str) -> None:
        self.objects[dst_uri] = self.objects[src_uri]

    def delete(self, uri: str) -> None:
        self.objects.pop(uri, None)

    def read(self, uri: str) -> StoredObject | None:
        body = self.objects.get(uri)
        return None if body is None else StoredObject(body, hashlib.md5(body).hexdigest())  # fmt: skip

    def write_conditional(self, uri: str, expected_version: str | None, body: bytes) -> str | None:  # fmt: skip
        current = self.read(uri)
        if (current and current.version) != expected_version:
            return None
        self.objects[uri] = body
        return hashlib.md5(body).hexdigest()

    def delete_conditional(self, uri: str, expected_version: str | None) -> bool:
        current = self.read(uri)
        if (current and current.version) != expected_version:
            return False
        self.objects.pop(uri, None)
        return True


CONFIG = """\
version: "0.3"
storages:
  default: local
  definitions:
    - {name: local, type: local}
    - {name: lake, type: s3, bucket: b, region: us-east-1}
domains:
  - name: d
entities:
  - name: orders
    domain: d
    source: {format: csv, path: in/, storage: lake, options: {header: true, separator: ","}}
    sink:
      accepted: {format: parquet, path: ./out/accepted}
      rejected: {format: csv, path: ./out/rejected}
    policy: {severity: reject}
    schema:
      columns:
        - {name: id, type: int64}
report: {path: runs, storage: lake}
"""


@pytest.fixture
def s3(monkeypatch: pytest.MonkeyPatch) -> MemS3:
    store = MemS3()

    def client(*_: object) -> MemS3:
        return store

    monkeypatch.setattr(resolve_module, "S3Client", client, raising=False)
    return store


def go(tmp_path: Path, config: str = CONFIG) -> Any:
    (tmp_path / "config.yml").write_text(config)
    return run(tmp_path / "config.yml", parse_config(tmp_path / "config.yml"), run_id="r1")  # fmt: skip


def test_an_s3_source_is_ingested_and_the_reports_land_on_s3(tmp_path: Path, s3: MemS3) -> None:  # fmt: skip
    s3.write("s3://b/in/a.csv", b"id\n1\n2\n")
    s3.write("s3://b/in/.floe/x.csv", b"id\n9\n")  # D23
    out = go(tmp_path)
    assert out.report_base_path == "s3://b/runs"
    assert pl.read_parquet(tmp_path / "out/accepted").height == 2
    report = json.loads(s3.objects["s3://b/runs/run_r1/d/orders/run.json"])
    assert report["source"]["path"] == "s3://b/in/"
    assert [f["input_file"] for f in report["files"]] == ["s3://b/in/a.csv"]
    summary = json.loads(s3.objects["s3://b/runs/run_r1/run.summary.json"])
    assert summary["report"]["report_file"] == "s3://b/runs/run_r1/run.summary.json"


def test_a_rejected_s3_file_is_copied_under_its_source_name(tmp_path: Path, s3: MemS3) -> None:  # fmt: skip
    s3.write("s3://b/in/a.csv", b"other\n1\n")
    go(tmp_path, CONFIG.replace("{name: id, type: int64}", "{name: id, type: int64, nullable: false}\n      mismatch: {missing_columns: reject_file}"))  # fmt: skip
    assert (tmp_path / "out/rejected/a.csv").read_bytes() == b"other\n1\n"


def test_dry_run_lists_the_s3_inputs(tmp_path: Path, s3: MemS3) -> None:
    s3.write("s3://b/in/a.csv", b"id\n1\n")
    (tmp_path / "config.yml").write_text(CONFIG)
    out = run(tmp_path / "config.yml", parse_config(tmp_path / "config.yml"), dry_run=True)  # fmt: skip
    assert out.dry_run == {"d.orders": ["s3://b/in/a.csv"]}


SINKS = CONFIG.replace("path: ./out/accepted}", "path: out/accepted, storage: lake}").replace("path: ./out/rejected}", "path: out/rejected, storage: lake}")  # fmt: skip


def parts(s3: MemS3, prefix: str) -> list[str]:
    return sorted(u for u in s3.objects if u.startswith(prefix))


def test_parquet_and_rejected_parts_are_written_to_s3(tmp_path: Path, s3: MemS3) -> None:  # fmt: skip
    s3.write("s3://b/in/a.csv", b"id\n1\nx\n")
    s3.write("s3://b/out/accepted/part-00007.parquet", b"old")
    s3.write("s3://b/out/accepted/keep/part-00001.parquet", b"nested")
    go(tmp_path, SINKS)
    assert parts(s3, "s3://b/out/accepted/") == ["s3://b/out/accepted/keep/part-00001.parquet", "s3://b/out/accepted/part-00000.parquet"]  # fmt: skip
    accepted = pl.read_parquet(io.BytesIO(s3.objects["s3://b/out/accepted/part-00000.parquet"]))  # fmt: skip
    assert accepted["id"].to_list() == [1]
    [rejected] = parts(s3, "s3://b/out/rejected/")
    report = json.loads(s3.objects["s3://b/runs/run_r1/d/orders/run.json"])
    assert report["sink"]["accepted"]["path"] == "s3://b/out/accepted"
    assert report["sink"]["rejected"]["path"] == "s3://b/out/rejected"
    assert report["files"][0]["output"]["rejected_path"] == rejected


def test_append_seeds_unique_keys_from_the_s3_parts(tmp_path: Path, s3: MemS3) -> None:  # fmt: skip
    s3.write("s3://b/in/a.csv", b"id\n1\n")
    config = SINKS.replace("    sink:\n", "    sink:\n      write_mode: append\n").replace("{name: id, type: int64}", "{name: id, type: int64, unique: true}")  # fmt: skip
    go(tmp_path, config)
    out = go(tmp_path, config)
    assert out.summary["results"]["rejected_total"] == 1
    assert len(parts(s3, "s3://b/out/accepted/")) == 2


def test_abort_copies_the_raw_file_and_errors_to_s3(tmp_path: Path, s3: MemS3) -> None:  # fmt: skip
    s3.write("s3://b/in/a.csv", b"id\nx\n")
    go(tmp_path, SINKS.replace("severity: reject", "severity: abort"))
    assert s3.objects["s3://b/out/rejected/a.csv"] == b"id\nx\n"
    assert json.loads(s3.objects["s3://b/out/rejected/a_reject_errors.json"])[0]["row_index"] == 0  # fmt: skip


def test_file_mode_keeps_its_state_on_s3_and_skips_the_ingested_file(tmp_path: Path, s3: MemS3) -> None:  # fmt: skip
    s3.write("s3://b/in/a.csv", b"id\n1\n")
    config = SINKS.replace("    domain: d\n", "    domain: d\n    incremental_mode: file\n", 1)  # fmt: skip
    go(tmp_path, config)
    state = json.loads(s3.objects["s3://b/in/.floe/state/d/orders/state.json"])
    assert list(state["files"]) == ["s3://b/in/a.csv"]
    out = go(tmp_path, config)
    report = json.loads(s3.objects["s3://b/runs/run_r1/d/orders/run.json"])
    assert out.summary["results"]["files_skipped"] == 1
    assert report["files"][0]["skip_reason"] == "already_ingested"


ARCHIVE = SINKS.replace("    domain: d\n", "    domain: d\n    incremental_mode: archive\n", 1).replace("      accepted:", "      archive: {path: archive/, storage: lake}\n      accepted:")  # fmt: skip


def test_archive_copies_then_deletes_the_s3_input(tmp_path: Path, s3: MemS3) -> None:  # fmt: skip
    s3.write("s3://b/in/a.csv", b"id\n1\n")
    go(tmp_path, ARCHIVE)
    [archived] = parts(s3, "s3://b/archive/")
    assert archived.startswith("s3://b/archive/orders/a__run-r1__src-")
    assert "s3://b/in/a.csv" not in s3.objects
    report = json.loads(s3.objects["s3://b/runs/run_r1/d/orders/run.json"])
    assert report["files"][0]["output"]["archived_path"] == archived


def full_refresh(tmp_path: Path, config: str) -> Any:
    (tmp_path / "config.yml").write_text(config)
    return run(tmp_path / "config.yml", parse_config(tmp_path / "config.yml"), run_id="r2", full_refresh=True)  # fmt: skip


def test_full_refresh_reads_the_s3_archive_then_the_source(tmp_path: Path, s3: MemS3) -> None:  # fmt: skip
    s3.write("s3://b/archive/orders/old.csv", b"id\n1\n")
    s3.write("s3://b/in/new.csv", b"id\n2\n")
    out = full_refresh(tmp_path, ARCHIVE)
    assert out.summary["results"]["accepted_total"] == 2


def test_full_refresh_fails_on_a_source_listing_error(tmp_path: Path, s3: MemS3) -> None:  # fmt: skip
    """#828: only "no inputs matched" means empty; never rebuild from the archive alone on another error."""
    s3.write("s3://b/archive/orders/old.csv", b"id\n1\n")
    with pytest.raises(FloeRunError, match="non-empty literal prefix"):
        full_refresh(tmp_path, ARCHIVE.replace("path: in/,", "path: '*.csv',"))


def test_overwrite_at_the_bucket_root_clears_the_old_parts(s3: MemS3) -> None:
    df = pl.DataFrame({"id": [1]})
    write_accepted(df, s3, "s3://b", "append")
    write_accepted(df, s3, "s3://b", "overwrite")
    assert parts(s3, "s3://b/") == ["s3://b/part-00000.parquet"]


def test_append_without_unique_keys_never_reads_the_history(tmp_path: Path, s3: MemS3, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    s3.write("s3://b/in/a.csv", b"id\n1\n")
    config = SINKS.replace("    sink:\n", "    sink:\n      write_mode: append\n")
    go(tmp_path, config)

    def no_read(uri: str) -> None:
        raise AssertionError(f"read {uri}")

    monkeypatch.setattr(s3, "read", no_read)
    go(tmp_path, config)
    assert len(parts(s3, "s3://b/out/accepted/")) == 2


def test_full_refresh_reads_an_archive_nested_in_the_source_once(tmp_path: Path, s3: MemS3) -> None:  # fmt: skip
    s3.write("s3://b/in/a.csv", b"id\n1\n")
    config = ARCHIVE.replace("path: archive/,", "path: in/archive/,")
    go(tmp_path, config)
    out = full_refresh(tmp_path, config)
    assert out.summary["results"]["accepted_total"] == 1


@pytest.mark.parametrize("mode", ["archive", "none"])
def test_d31_a_normal_run_never_lists_a_nested_s3_archive(tmp_path: Path, s3: MemS3, mode: str) -> None:  # fmt: skip
    """D31: the source prefix also lists `in/archive/`; those objects are not input again."""
    config = ARCHIVE.replace("path: archive/,", "path: in/archive/,").replace("incremental_mode: archive", f"incremental_mode: {mode}")  # fmt: skip
    config = config.replace("    sink:\n", "    sink:\n      write_mode: append\n")
    s3.write("s3://b/in/a.csv", b"id\n1\n")
    go(tmp_path, config)
    s3.write("s3://b/in/b.csv", b"id\n2\n")
    out = go(tmp_path, config)
    report = json.loads(s3.objects["s3://b/runs/run_r1/d/orders/run.json"])
    assert [f["input_file"] for f in report["files"]] == ["s3://b/in/b.csv"]
    assert report["source"]["resolved_inputs"]["files"] == ["s3://b/in/b.csv"]
    assert out.summary["results"]["accepted_total"] == 1


def test_d31_full_refresh_reads_a_nested_s3_archive_and_the_source_once(tmp_path: Path, s3: MemS3) -> None:  # fmt: skip
    config = ARCHIVE.replace("path: archive/,", "path: in/archive/,")
    s3.write("s3://b/in/a.csv", b"id\n1\n")
    go(tmp_path, config)
    s3.write("s3://b/in/b.csv", b"id\n2\n")
    out = full_refresh(tmp_path, config)
    assert out.summary["results"]["accepted_total"] == 2


def test_inspect_entity_state_reads_the_s3_state(tmp_path: Path, s3: MemS3) -> None:
    s3.write("s3://b/in/a.csv", b"id\n1\n")
    config = SINKS.replace("    domain: d\n", "    domain: d\n    incremental_mode: file\n", 1)  # fmt: skip
    go(tmp_path, config)
    got = floe.inspect_entity_state(str(tmp_path / "config.yml"), "orders")
    assert got["path_uri"] == "s3://b/in/.floe/state/d/orders/state.json"
    assert got["path_local"] is None
    assert list(got["state"]["files"]) == ["s3://b/in/a.csv"]


def test_the_s3_source_is_listed_once_per_run(tmp_path: Path, s3: MemS3, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    """#864: the pre-check lists the inputs before the first entity_started; run_entity reuses them."""
    s3.write("s3://b/in/a.csv", b"id\n1\n")
    prefixes: list[str] = []
    real = s3.list

    def counted(prefix: str) -> list[ObjectRef]:
        prefixes.append(prefix)
        return real(prefix)

    monkeypatch.setattr(s3, "list", counted)
    go(tmp_path)
    assert prefixes == ["in/"]


def test_d28_a_partitioned_s3_source_loads_by_date_with_derived_columns(tmp_path: Path, s3: MemS3) -> None:  # fmt: skip
    config = CONFIG.replace('separator: ","}}', 'separator: ","}, partitioning: {pattern: "{yyyy}/{MM}/{dd}/{batch}", date_column: day, batch_column: batch}}')  # fmt: skip
    s3.write("s3://b/in/2026/10/25/aa/x.csv", b"id\n1\n")
    s3.write("s3://b/in/2026/10/24/bb/x.csv", b"id\n2\n")
    go(tmp_path, config)
    got = pl.read_parquet(tmp_path / "out/accepted")
    assert got.rows() == [(2, date(2026, 10, 24), "bb"), (1, date(2026, 10, 25), "aa")]
    report = json.loads(s3.objects["s3://b/runs/run_r1/d/orders/run.json"])
    assert report["source"]["resolved_inputs"]["files"] == ["s3://b/in/2026/10/24/bb/x.csv", "s3://b/in/2026/10/25/aa/x.csv"]  # fmt: skip


def test_d29_the_s3_archive_keeps_the_partition_path_for_a_full_refresh(tmp_path: Path, s3: MemS3) -> None:  # fmt: skip
    config = ARCHIVE.replace('separator: ","}}', 'separator: ","}, partitioning: {pattern: "{yyyy}/{MM}/{dd}/{batch}", date_column: day}}')  # fmt: skip
    s3.write("s3://b/in/2026/10/24/a1b2/x.csv", b"id\n1\n")
    go(tmp_path, config)
    [archived] = parts(s3, "s3://b/archive/")
    assert archived.startswith("s3://b/archive/orders/2026/10/24/a1b2/x__run-r1__src-")
    full_refresh(tmp_path, config)
    [part] = parts(s3, "s3://b/out/accepted/")
    assert pl.read_parquet(s3.objects[part]).rows() == [(1, date(2026, 10, 24))]
