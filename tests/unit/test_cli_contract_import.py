"""`floe contract import`: workbook → editable entity YAML, offline."""

import importlib
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).parent / "contract"))
# the workbook builders live next to the parser tests; loaded by name so pyright need not see that folder
_wb: Any = importlib.import_module("test_workbook_xlsx")
_xlsx: Any = importlib.import_module("xlsx_book")
GUIDE, col, columns, tables = _wb.GUIDE, _wb.col, _wb.columns, _wb.tables
write_book: Callable[..., Path] = _xlsx.write_book

from floe._cli import main

Floe = tuple[int, str, str]
Run = Callable[..., Floe]


@pytest.fixture
def floe(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> Run:
    def call(*args: str | Path) -> Floe:
        monkeypatch.setattr(sys, "argv", ["floe", *map(str, args)])
        code = main()
        out, err = capsys.readouterr()
        return code, out, err

    return call


def workbook(
    tmp: Path, *column_names: str, key: str = "id", table: str = "orders"
) -> Path:
    cols = [
        col(table, n, **({"primary_key": "X"} if n == key else {}))
        for n in column_names
    ]
    return write_book(
        tmp / "sales.xlsx",
        {
            "Guide": GUIDE,
            "Tables": tables({"target_table": table}),
            "Columns": columns(*cols),
        },
    )


def imp(floe: Run, book: Path, out: Path, *args: str | Path) -> Floe:
    return floe("contract", "import", book, "--output-dir", out, *args)


def test_import_without_project_config(floe: Run, tmp_path: Path) -> None:
    out = tmp_path / "domains/sales"
    code, stdout, _ = imp(floe, workbook(tmp_path, "id", "amount"), out)
    assert code == 0
    assert sorted(p.name for p in out.iterdir()) == ["_domain.yml", "orders.yml"]
    assert "2 created" in stdout
    assert "include.domains" in stdout
    code, stdout, _ = imp(floe, workbook(tmp_path, "id", "amount"), out)
    assert (code, "0 created, 0 updated, 1 unchanged" in stdout) == (0, True)


def test_dry_run_no_writes(floe: Run, tmp_path: Path) -> None:
    out = tmp_path / "sales"
    code, stdout, _ = imp(floe, workbook(tmp_path, "id"), out, "--dry-run")
    assert code == 0
    assert "2 created" in stdout
    assert not out.exists()


def test_check_exits_nonzero_on_pending_change(floe: Run, tmp_path: Path) -> None:
    out = tmp_path / "sales"
    book = workbook(tmp_path, "id")
    assert imp(floe, book, out, "--check")[0] == 1
    assert not out.exists()
    imp(floe, book, out)
    assert imp(floe, book, out, "--check")[0] == 0
    book = workbook(tmp_path, "id", "amount")  # additive
    assert imp(floe, book, out, "--check")[0] == 1


def test_breaking_change_needs_flag(floe: Run, tmp_path: Path) -> None:
    out = tmp_path / "sales"
    imp(floe, workbook(tmp_path, "id", "amount"), out)
    before = (out / "orders.yml").read_text()
    renamed = workbook(tmp_path, "id", "total")
    code, stdout, _ = imp(floe, renamed, out)
    assert code == 1
    assert "breaking" in stdout
    assert (out / "orders.yml").read_text() == before
    assert imp(floe, renamed, out, "--accept-breaking")[0] == 0
    assert "total" in (out / "orders.yml").read_text()


def test_removed_table_is_breaking_and_never_deleted(floe: Run, tmp_path: Path) -> None:
    out = tmp_path / "sales"
    imp(floe, workbook(tmp_path, "id"), out)
    other = write_book(
        tmp_path / "sales.xlsx",
        {
            "Guide": GUIDE,
            "Tables": tables({"target_table": "items"}),
            "Columns": columns(col("items", "id", primary_key="X")),
        },
    )
    assert imp(floe, other, out)[0] == 1
    assert imp(floe, other, out, "--accept-breaking")[0] == 0
    assert (out / "orders.yml").exists()


def test_reimport_preserves_engineer_keys(floe: Run, tmp_path: Path) -> None:
    out = tmp_path / "sales"
    imp(floe, workbook(tmp_path, "id"), out)
    path = out / "orders.yml"
    path.write_text(path.read_text() + "sink:\n  write_mode: merge_scd1  # engineer\n")
    code, stdout, _ = imp(floe, workbook(tmp_path, "id", "amount"), out)
    assert code == 0
    assert "1 updated" in stdout
    text = path.read_text()
    assert "write_mode: merge_scd1  # engineer" in text
    assert "amount" in text


def test_hand_edited_owned_key_blocks(floe: Run, tmp_path: Path) -> None:
    out = tmp_path / "sales"
    imp(floe, workbook(tmp_path, "id"), out)
    path = out / "orders.yml"
    path.write_text(path.read_text().replace("name: id", "name: pk"))
    code, stdout, _ = imp(floe, workbook(tmp_path, "id", "amount"), out)
    assert code == 1
    assert "blocked" in stdout


def test_invalid_workbook_exits_without_changes(floe: Run, tmp_path: Path) -> None:
    out = tmp_path / "sales"
    bad = write_book(
        tmp_path / "bad.xlsx",
        {
            "Guide": GUIDE,
            "Tables": tables({"target_table": "orders"}),
            "Columns": columns(col("orders", "id", target_type="nope")),
        },
    )
    code, _, err = imp(floe, bad, out)
    assert code == 1
    assert "Columns" in err
    assert not out.exists()


def test_domain_guard(floe: Run, tmp_path: Path) -> None:
    out = tmp_path / "sales"
    code, _, err = imp(floe, workbook(tmp_path, "id"), out, "--domain", "hr")
    assert code == 1
    assert "hr" in err
    assert not out.exists()


DOMAIN = """name: sales
defaults:
  source:
    format: csv
    path: "./in/{{resource}}.csv"
  sink:
    accepted:
      format: parquet
      path: "./out/accepted/{{entity.name}}"
    rejected:
      format: csv
      path: "./out/rejected/{{entity.name}}"
"""


def project(tmp: Path, domain_yml: str | None = None) -> Path:
    (tmp / "domains/sales").mkdir(parents=True)
    if domain_yml:
        (tmp / "domains/sales/_domain.yml").write_text(domain_yml)
    (tmp / "config.yml").write_text(
        'version: "0.8"\ninclude:\n  domains:\n    - "domains/*/_domain.yml"\n'
    )
    return tmp / "config.yml"


def test_config_with_scaffold_reports_next_step(floe: Run, tmp_path: Path) -> None:
    config = project(tmp_path)
    out = tmp_path / "domains/sales"
    code, stdout, _ = imp(floe, workbook(tmp_path, "id"), out, "-c", config)
    assert code == 0
    assert "complete _domain.yml, then run floe validate" in stdout
    assert (out / "orders.yml").exists()


def test_config_blocks_rename_that_breaks_engineer_keys(floe: Run, tmp_path: Path) -> None:  # fmt: skip
    config = project(tmp_path, DOMAIN)
    out = tmp_path / "domains/sales"
    assert imp(floe, workbook(tmp_path, "id", "amount"), out, "-c", config)[0] == 0
    path = out / "orders.yml"
    path.write_text(path.read_text() + "pii:\n  columns:\n    - name: amount\n      strategy: hash\n")  # fmt: skip
    before = path.read_text()
    code, _, err = imp(floe, workbook(tmp_path, "id", "total"), out, "-c", config, "--accept-breaking")  # fmt: skip
    assert code == 1
    assert "amount" in err
    assert path.read_text() == before


def test_config_prints_effective_overrides(floe: Run, tmp_path: Path) -> None:
    config = project(tmp_path, DOMAIN)
    out = tmp_path / "domains/sales"
    imp(floe, workbook(tmp_path, "id"), out)
    path = out / "orders.yml"
    path.write_text(path.read_text() + "source:\n  cast_mode: coerce\n")
    code, stdout, _ = imp(floe, workbook(tmp_path, "id", "amount"), out, "-c", config)
    assert code == 0
    assert "override: orders: source.cast_mode" in stdout


def test_blank_severities_fail_closed(floe: Run, tmp_path: Path) -> None:
    guide = [[*r[:1], None] if r[0] == "default_severity" else r for r in GUIDE]
    book = write_book(
        tmp_path / "blank.xlsx",
        {"Guide": guide, "Tables": tables({"target_table": "orders"}), "Columns": columns(col("orders", "id", primary_key="X"))},
    )  # fmt: skip
    out = tmp_path / "sales"
    code, _, err = imp(floe, book, out)
    assert code == 1
    assert "Guide.default_severity is blank and table orders sets none" in err
    assert not out.exists()


def test_config_todo_in_entity_name_is_not_the_scaffold(
    floe: Run, tmp_path: Path
) -> None:
    config = project(tmp_path, DOMAIN)
    out = tmp_path / "domains/sales"
    book = workbook(tmp_path, "id", "amount", table="TODO_orders")
    assert imp(floe, book, out, "-c", config)[0] == 0
    path = out / "TODO_orders.yml"
    path.write_text(
        path.read_text()
        + "pii:\n  columns:\n    - name: amount\n      strategy: hash\n"
    )
    before = path.read_text()
    book = workbook(tmp_path, "id", "total", table="TODO_orders")
    code, _, err = imp(floe, book, out, "-c", config, "--accept-breaking")
    assert code == 1
    assert "would not validate" in err
    assert path.read_text() == before


def test_crlf_file_reimports_unchanged(floe: Run, tmp_path: Path) -> None:
    out = tmp_path / "sales"
    book = workbook(tmp_path, "id")
    imp(floe, book, out)
    path = out / "orders.yml"
    path.write_bytes(path.read_bytes().replace(b"\n", b"\r\n"))
    before = path.read_bytes()
    code, stdout, _ = imp(floe, book, out)
    assert (code, "1 unchanged" in stdout) == (0, True)
    assert path.read_bytes() == before


def test_config_absolute_include_is_refused(floe: Run, tmp_path: Path) -> None:
    config = project(tmp_path, DOMAIN)
    config.write_text(f'version: "0.8"\ninclude:\n  domains:\n    - "{tmp_path}/domains/*/_domain.yml"\n')  # fmt: skip
    out = tmp_path / "domains/sales"
    code, _, err = imp(floe, workbook(tmp_path, "id"), out, "-c", config)
    assert code == 1
    assert "relative include.domains" in err
    assert not (out / "orders.yml").exists()
