"""A missing optional extra fails with the install hint: at validation, and at import for a manifest run."""

import importlib
import sys
from pathlib import Path

import pytest

from floe.config.parse import parse_config
from floe.config.validate import validate_entities
from floe.errors import FloeConfigError

XLSX = (
    "entity.name=orders source.format=xlsx needs the floe[xlsx] extra: "
    "pip install 'floe-python[xlsx]'"
)


def test_validate_names_the_xlsx_extra(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    config = tmp_path / "config.yml"
    config.write_text("""
version: "0.2"
entities:
- name: orders
  source: {format: xlsx, path: in.xlsx}
  sink: {accepted: {format: parquet, path: out}}
  policy: {severity: warn}
  schema: {columns: [{name: id, type: string}]}
""")
    monkeypatch.setitem(
        sys.modules, "fastexcel", None
    )  # `import fastexcel` raises ImportError
    with pytest.raises(FloeConfigError) as err:
        validate_entities(parse_config(config))
    assert str(err.value) == XLSX


def test_xlsx_reader_import_names_the_xlsx_extra(monkeypatch: pytest.MonkeyPatch) -> None:  # fmt: skip
    monkeypatch.setitem(sys.modules, "fastexcel", None)
    monkeypatch.delitem(sys.modules, "floe.read.xlsx", raising=False)
    with pytest.raises(FloeConfigError) as err:
        importlib.import_module("floe.read.xlsx")
    assert str(err.value) == "source.format=xlsx needs the floe[xlsx] extra: pip install 'floe-python[xlsx]'"  # fmt: skip
