"""Reader failures carry a typed code and keep their reader `rule` (#1260)."""

from pathlib import Path

import pytest

from floe.config.types import SourceOptions
from floe.errors import FloeConfigError, FloeIoError
from floe.read.json import json_records
from floe.read.xlsx import read_xlsx
from floe.read.xml import read_xml

BOOK = Path(__file__).parents[3] / "conformance" / "xlsx" / "in" / "orders.xlsx"


def test_xlsx_missing_sheet_is_an_invalid_option_and_keeps_its_rule() -> None:
    with pytest.raises(FloeIoError) as e:
        read_xlsx(BOOK, SourceOptions.model_validate({"sheet": "nope"}))
    assert (e.value.code, e.value.rule) == ("io_invalid_option", "xlsx_sheet_error")


def test_xlsx_bounds_error_is_a_read_error_and_keeps_its_rule() -> None:
    options = SourceOptions.model_validate({"sheet": "orders", "header_row": 9})
    with pytest.raises(FloeIoError) as e:
        read_xlsx(BOOK, options)
    assert (e.value.code, e.value.rule) == ("io_read_error", "xlsx_read_error")


def test_json_parse_error_is_a_read_error(tmp_path: Path) -> None:
    path = tmp_path / "a.json"
    path.write_text("[{", encoding="utf-8")
    with pytest.raises(FloeIoError) as e:
        list(json_records(path, "array"))
    assert e.value.code == "io_read_error"


def test_xml_without_row_tag_is_a_missing_config_key(tmp_path: Path) -> None:
    path = tmp_path / "a.xml"
    path.write_text("<a/>", encoding="utf-8")
    with pytest.raises(FloeConfigError) as e:
        next(read_xml(path, SourceOptions.model_validate({}), []))
    assert e.value.code == "config_missing_key"
