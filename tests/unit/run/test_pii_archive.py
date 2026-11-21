"""#1363: a PII entity can archive; the archive is a move, so rejected rows stay masked and no raw copy is added."""

from pathlib import Path

import polars as pl

from floe.config.parse import parse_config
from floe.config.validate import validate_entities
from floe.run.entity import run_entity
from floe.run.events import Emitter

CONFIG = """\
version: "0.3"
domains:
  - name: d
entities:
  - name: people
    domain: d
    incremental_mode: archive
    source: {format: csv, path: ./in, options: {header: true, separator: ","}}
    sink:
      accepted: {format: parquet, path: ./out/accepted}
      rejected: {format: csv, path: ./out/rejected}
      archive: {path: ./arch}
    policy: {severity: reject}
    schema:
      columns:
        - {name: id, type: int64}
        - {name: email, type: string}
    pii:
      columns:
        - {name: email, strategy: redact, redact_value: "***"}
report: {path: ./report}
"""


def test_pii_entity_archives_the_source_and_rejects_masked_rows(tmp_path: Path) -> None:
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "people.csv").write_text("id,email\n1,a@x.io\nbad,secret@x.io\n")
    (tmp_path / "config.yml").write_text(CONFIG)
    config = parse_config(tmp_path / "config.yml")
    validate_entities(config)
    entity = config.entities[0]
    out = run_entity(entity, tmp_path, "0.3", emit=Emitter(None, "r1"))
    archived = Path(out.report["files"][0]["output"]["archived_path"])
    assert archived.read_text().endswith("bad,secret@x.io\n")  # the original, moved
    assert not (tmp_path / "in/people.csv").exists()
    rejected = sorted((tmp_path / "out/rejected").rglob("*.csv"))
    assert len(rejected) == 1  # no raw copy next to the rejected rows
    assert pl.read_csv(rejected[0])["email"].to_list() == ["***"]
    assert "secret@x.io" not in rejected[0].read_text()
