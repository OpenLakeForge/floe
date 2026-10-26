"""The template pass in `parse_config` (cases: `conformance/config/parse_template_*`)."""

from pathlib import Path

from floe.config.parse import parse_config

CONFIG = """version: "0.3"
env:
  file: env.yml
  vars: {B: inline}
domains:
  - {name: sales, incoming_dir: "{{ ROOT }}/sales"}
report: {path: "{{ROOT}}/report"}
lineage: {url: "{{ROOT}}", namespace: ns, api_key: "{{ B }}"}
storages:
  definitions: [{name: s, type: s3, bucket: "{{ B }}"}]
entities:
  - name: orders
    domain: sales
    source: {format: csv, path: "{{ domain.incoming_dir }}/{{ entity.name }}/{{ domain.name }}"}
    sink: {accepted: {format: parquet, path: "{{ ROOT }}/out"}, archive: {path: "{{ B }}"}}
    policy: {severity: warn}
    schema: {columns: [{name: id, type: string}]}
"""


def test_template_pass_fills_every_site(tmp_path: Path) -> None:
    (tmp_path / "env.yml").write_text("ROOT: /data\nB: file\n")
    (tmp_path / "config.yml").write_text(CONFIG)
    config = parse_config(tmp_path / "config.yml")
    entity = config.entities[0]
    assert config.domains[0].resolved_incoming_dir == "/data/sales"
    assert config.report.path == "/data/report"
    assert config.lineage is not None
    assert config.lineage.url == "/data"
    assert config.lineage.api_key is not None
    assert config.lineage.api_key.get_secret_value() == "inline"
    assert config.storages is not None
    assert config.storages.definitions[0].bucket == "inline"
    assert entity.source.path == "/data/sales/orders/sales"
    assert entity.sink.accepted.path == "/data/out"
    assert entity.sink.archive is not None
    assert entity.sink.archive.path == "inline"
