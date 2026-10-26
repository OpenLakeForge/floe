"""XLSX reader: the conformance input against Rust's output, source options and error messages."""

from pathlib import Path

import polars as pl
import pytest

from floe.config.types import SourceOptions
from floe.errors import FloeIoError
from floe.read.xlsx import read_xlsx

CASE = Path(__file__).parents[3] / "conformance" / "xlsx"
BOOK = CASE / "in" / "orders.xlsx"


def opts(**kwargs: object) -> SourceOptions:
    return SourceOptions.model_validate(kwargs)


def test_conformance_input_matches_rust_rows() -> None:
    df = read_xlsx(BOOK, opts(sheet="orders", header_row=2))
    expected = pl.read_parquet(CASE / "expected" / "accepted" / "orders.parquet")
    assert df.columns == [*expected.columns[:-1], "id_dup_2"]
    assert df.rename({"id_dup_2": expected.columns[-1]}).equals(expected)
    # numbers and booleans as Rust renders them; a blank cell is null
    assert df["amount"].to_list() == ["12.5", "3", "abc"]
    assert df["active"].to_list() == ["true", "false", None]


def test_defaults_to_first_sheet_and_first_row_header() -> None:
    df = read_xlsx(BOOK, opts())
    assert df.columns == ["ignored"]
    assert df.height == 0


def test_data_row_skips_rows_after_the_header() -> None:
    df = read_xlsx(BOOK, opts(sheet="orders", header_row=2, data_row=4))
    assert df["id"].to_list() == ["2", "x3"]


def test_dates_render_as_iso_text_d14() -> None:
    # Rust 0.7 renders the Excel serial ("45293", "45293.12783564815"); fastexcel renders ISO text.
    df = read_xlsx(BOOK, opts(sheet="dates"))
    assert df.row(0) == ("2024-01-02 00:00:00", "2024-01-02 03:04:05")


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        (
            {"sheet": "nope"},
            f"xlsx_sheet_error: sheet nope not found in {BOOK} (available: cover, orders, dates)",
        ),
        (
            {"sheet": "orders", "header_row": 9},
            f"xlsx_read_error: header_row=9 is outside sheet bounds in {BOOK}",
        ),
    ],
)
def test_option_errors(kwargs: dict[str, object], message: str) -> None:
    with pytest.raises(FloeIoError) as err:
        read_xlsx(BOOK, opts(**kwargs))
    assert str(err.value) == message


def test_missing_and_invalid_files(tmp_path: Path) -> None:
    bad = tmp_path / "bad.xlsx"
    bad.write_text("not a zip")
    for path in (tmp_path / "none.xlsx", bad):
        with pytest.raises(
            FloeIoError, match=f"^xlsx_read_error: failed to open xlsx at {path}"
        ):
            read_xlsx(path, opts())
