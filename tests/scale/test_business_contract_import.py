"""#1455: a 200-table / 10,000-column domain workbook imports into a blank directory, deterministically.

Run with `pytest -m scale tests/scale/test_business_contract_import.py -rP`. The workbook is generated here, not
committed. The wall time is printed only: no threshold, no RSS (the ingestion scale suite owns those).
"""

import importlib
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import pytest

from floe._cli import main
from floe.config.yaml import parse_yaml

sys.path.insert(0, str(Path(__file__).parents[1] / "unit/contract"))
# the workbook builders live with the parser tests; loaded by name so pyright need not see that folder
_wb: Any = importlib.import_module("test_workbook_xlsx")
_xlsx: Any = importlib.import_module("xlsx_book")
GUIDE, col, columns, tables = _wb.GUIDE, _wb.col, _wb.columns, _wb.tables
write_book: Callable[..., Path] = _xlsx.write_book

pytestmark = pytest.mark.scale

TABLES = 200
COLUMNS = 50  # per table: 10,000 in all
TYPES = ("int64", "string", "float64", "date", "boolean")


def generate(path: Path) -> Path:
    """Pure function of the constants: a PK, renamed sources, unique keys, not-null and value checks in every table."""
    table_rows = [
        {"target_table": f"t{t:03d}", "source_dataset": f"export_{t:03d}", "owner": f"owner{t % 7}"}
        for t in range(TABLES)
    ]  # fmt: skip
    column_rows: list[dict[str, Any]] = []
    for t in range(TABLES):
        name = f"t{t:03d}"
        column_rows.append(col(name, "id", target_type="int64", primary_key="X"))
        for c in range(1, COLUMNS):
            kind = TYPES[c % len(TYPES)]
            extra: dict[str, Any] = {}
            if c % 3 == 0:
                extra["source_column"] = f"SRC_{c:02d}"
            if c % 4 == 0:
                extra["not_null"] = "X"
            if c == 5:
                extra["unique"] = "X"
            if c in (6, 7):
                extra["unique_group"] = "U1"
            if kind == "string" and c % 2:
                extra.update(min_length="1", max_length="40")
            if kind == "int64":
                extra.update(min="0", max="1000")
            column_rows.append(col(name, f"c{c:02d}", target_type=kind, **extra))
    return write_book(
        path,
        {
            "Guide": GUIDE,
            "Tables": tables(*table_rows),
            "Columns": columns(*column_rows),
        },
    )


def test_200_table_domain_import_without_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    book = generate(tmp_path / "sales.xlsx")
    seconds: list[float] = []

    def run(out: Path) -> None:
        monkeypatch.setattr(sys, "argv", ["floe", "contract", "import", str(book), "--output-dir", str(out)])  # fmt: skip
        started = time.monotonic()
        code = main()
        seconds.append(time.monotonic() - started)
        assert (code, capsys.readouterr().err) == (0, "")

    first, second = tmp_path / "one/sales", tmp_path / "two/sales"
    run(first)
    run(second)
    files = sorted(p.name for p in first.iterdir())
    assert len(files) == TABLES + 1  # one file per table and the `_domain.yml` scaffold
    assert files == sorted(p.name for p in second.iterdir())
    assert not list(tmp_path.glob("**/config.yml"))  # no project config anywhere
    for name in files:
        text = (first / name).read_bytes()
        assert text == (second / name).read_bytes()  # two runs, identical bytes
        doc = cast("dict[str, Any]", parse_yaml(text.decode("utf-8"))[0])
        if name != "_domain.yml":
            assert len(doc["schema"]["columns"]) == COLUMNS
    print(f"\nimported {TABLES} tables / {TABLES * COLUMNS} columns in {seconds[0]:.2f} s (second run {seconds[1]:.2f} s)")  # fmt: skip
