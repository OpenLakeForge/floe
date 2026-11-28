"""#1455: workbook → entity files → engineer completion → existing `floe validate` / `floe run` → reimport."""

import difflib
import hashlib
import importlib
import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).parent))
# the workbook builders live next to the parser tests; loaded by name so pyright need not see that folder
_wb: Any = importlib.import_module("test_workbook_xlsx")
_xlsx: Any = importlib.import_module("xlsx_book")
GUIDE, col, columns, tables = _wb.GUIDE, _wb.col, _wb.columns, _wb.tables
write_book: Callable[..., Path] = _xlsx.write_book

from floe._cli import main

Floe = tuple[int, str, str]
Run = Callable[..., Floe]
GOLDEN = Path(__file__).parent / "golden"
# an engineer's `_domain.yml` for local Parquet: every table reads ./in/<resource>.csv
DOMAIN = """name: sales
defaults:
  source:
    format: csv
    path: "./in/{{resource}}.csv"
    options: {header: true, separator: ","}
  sink:
    write_mode: append
    accepted:
      format: parquet
      path: "./out/accepted/{{entity.name}}"
    rejected:
      format: csv
      path: "./out/rejected/{{entity.name}}"
"""
CONFIG = 'version: "0.9"\nreport:\n  path: ./report\ninclude:\n  domains:\n    - "domains/*/_domain.yml"\n'
# the engineer's additions to orders.yml: technical keys and comments only
ENGINEER = (
    (
        "policy:\n  severity: reject\n",
        "  max_rejection_rate: 0.5  # engineer: tolerate half\n",
    ),
    (
        "schema:\n",
        "  checks:  # engineer: a row predicate\n    - {name: qty_positive, left: qty, op: ge, value: 0}\n",
    ),
)
TAIL = (
    "sink:\n  write_mode: overwrite  # engineer: full reload\n"
    "pii:\n  columns:\n    - {name: phone, strategy: hash}  # engineer: personal data\n"
)


@pytest.fixture
def floe(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> Run:
    def call(*args: str | Path) -> Floe:
        monkeypatch.setattr(sys, "argv", ["floe", *map(str, args)])
        code = main()
        out, err = capsys.readouterr()
        return code, out, err

    return call


def book(
    tmp: Path,
    *,
    phone: str = "phone",
    qty_type: str = "int64",
    qty_not_null: str | None = None,
    guide: Any = GUIDE,
    extra: dict[str, Any] | None = None,
) -> Path:
    """The golden workbook: renames, a composite PK, independent unique keys, severities and FK comments."""
    orders = {
        "target_table": "orders",
        "source_dataset": "orders_export",
        "owner": "ann",
        "description": "Customer orders",
        "business_name": "Orders",
        "grain": "one row per order line",
        "default_severity": "reject",
        "unique_severity": "warn",
        "not_null_severity": "warn",
        "cast_severity": "reject",
    }
    sheets = {
        "Guide": guide,
        "Tables": tables(orders, {"target_table": "lines"}),
        "Columns": columns(
            col(
                "orders", "id", source_column="ID", target_type="int64", primary_key="X"
            ),
            col(
                "orders",
                "sku",
                primary_key="X",
                unique_group="U1",
                min_length="2",
                max_length="9",
                value_severity="warn",
            ),
            col(
                "orders",
                "email",
                unique_group="U1",
                not_null="X",
                not_null_severity="reject",
                pattern="^[^@]+@[^@]+$",
            ),
            col("orders", phone, source_column="phone"),
            col(
                "orders",
                "qty",
                unique="X",
                target_type=qty_type,
                not_null=qty_not_null,
                min="0",
                max="100",
            ),
            col(
                "orders",
                "status",
                unique_group="U2",
                allowed_values="new|paid",
                fk_target_table="lines",
                fk_target_column="id",
            ),
            col("orders", "region", unique_group="U2"),
            col("lines", "id", target_type="int64", primary_key="X"),
        ),
        **(extra or {}),
    }
    return write_book(tmp / "sales.xlsx", sheets)


def imp(floe: Run, wb: Path, out: Path, *args: str | Path) -> Floe:
    return floe("contract", "import", wb, "--output-dir", out, *args)


def hashes(directory: Path) -> dict[str, str]:
    return {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(directory.iterdir())
    }


def tree(root: Path) -> dict[str, bytes | None]:
    """Every file and directory under `root` (`None` for a directory)."""
    return {
        str(p.relative_to(root)): p.read_bytes() if p.is_file() else None
        for p in sorted(root.rglob("*"))
    }


def project(tmp: Path) -> tuple[Path, Path]:
    """An engineer's project with a config and no entity files yet: (config, domain directory)."""
    root = tmp / "proj"
    out = root / "domains/sales"
    out.mkdir(parents=True)
    (root / "config.yml").write_text(CONFIG)
    return root / "config.yml", out


def engineer_edits(path: Path) -> None:
    text = path.read_text()
    for anchor, addition in ENGINEER:
        assert anchor in text
        text = text.replace(anchor, anchor + addition, 1)
    path.write_text(text + TAIL)


def test_golden_example(floe: Run, tmp_path: Path) -> None:
    out = tmp_path / "domains/sales"
    assert imp(floe, book(tmp_path), out)[0] == 0
    assert sorted(p.name for p in out.iterdir()) == sorted(
        p.name for p in GOLDEN.iterdir()
    )
    for golden in GOLDEN.iterdir():
        assert (out / golden.name).read_text() == golden.read_text(), golden.name


def test_missing_rules_sheet_is_fine(floe: Run, tmp_path: Path) -> None:
    with_rules = book(tmp_path, extra={"Rules": [["legacy"], ["ignored"]]})
    out = tmp_path / "domains/sales"
    assert imp(floe, with_rules, out)[0] == 0
    assert (out / "orders.yml").read_text() == (GOLDEN / "orders.yml").read_text()


def run_report(root: Path, run_id: str) -> dict[str, Any]:
    path = root / "report/sales" / f"run_{run_id}" / "run.summary.json"
    return json.loads(path.read_text())


def test_engineer_completion_is_validated_by_existing_cli(
    floe: Run, tmp_path: Path
) -> None:
    config, out = project(tmp_path)
    assert imp(floe, book(tmp_path), out)[0] == 0
    generated = hashes(out)
    generated.pop("_domain.yml")  # the engineer completes it

    code, _, err = floe("validate", "-c", config)  # an untouched scaffold fails closed
    assert code == 1
    assert "TODO_" in err

    (out / "_domain.yml").write_text(DOMAIN)
    (tmp_path / "proj/in").mkdir()
    (tmp_path / "proj/in/orders_export.csv").write_text(
        "ID,sku,email,phone,qty,status,region\n"
        "1,ab,a@x.io,111,5,new,eu\n"
        "2,cd,b@x.io,222,7,paid,us\n"
        "3,ef,c@x.io,333,-1,new,asia\n"  # below min: rejected
    )
    (tmp_path / "proj/in/lines.csv").write_text("id\n1\n2\n")
    code, stdout, err = floe("validate", "-c", config)
    assert (code, err) == (0, "")
    assert "Entities: 2" in stdout

    code, _, err = floe("run", "-c", config, "--run-id", "r1")
    assert code == 0, err
    report = run_report(tmp_path / "proj", "r1")
    orders = next(e for e in report["entities"] if e["name"] == "sales.orders")
    assert report["run"]["status"] == "rejected"  # rows rejected, exit code 0
    assert (
        orders["results"]["accepted_total"],
        orders["results"]["rejected_total"],
    ) == (2, 1)
    assert {
        k: v for k, v in hashes(out).items() if k != "_domain.yml"
    } == generated  # no edit to the entity files


def test_single_table_change_preserves_other_tables_and_engineer_keys(
    floe: Run, tmp_path: Path
) -> None:
    config, out = project(tmp_path)
    wb = book(tmp_path)
    assert imp(floe, wb, out, "-c", config)[0] == 0
    (out / "_domain.yml").write_text(DOMAIN)
    engineer_edits(out / "orders.yml")
    before = hashes(out)
    edited = (out / "orders.yml").read_text()
    assert floe("validate", "-c", config)[0] == 0

    code, stdout, _ = imp(floe, wb, out, "-c", config)  # identical workbook
    assert (code, hashes(out)) == (0, before)
    assert "0 created, 0 updated, 2 unchanged" in stdout

    changed = book(
        tmp_path, qty_type="float64", qty_not_null="X"
    )  # one type, one not_null
    code, stdout, _ = imp(floe, changed, out, "-c", config, "--accept-breaking")
    assert code == 0
    after = hashes(out)
    assert {n for n in after if after[n] != before[n]} == {"orders.yml"}
    diff = difflib.unified_diff(
        edited.splitlines(),
        (out / "orders.yml").read_text().splitlines(),
        lineterm="",
        n=0,
    )
    changes = [d for d in diff if d[0] in "+-" and not d.startswith(("+++", "---"))]
    assert [c[0] + c[1:].strip() for c in changes if "floe-contract" not in c] == [
        "-type: int64",
        "-nullable: true",
        "+type: float64",
        "+nullable: false",
    ]
    assert (
        sum("floe-contract" in c for c in changes) == 2
    )  # only the fingerprint line moves
    for kept in (*(a for _, a in ENGINEER), *TAIL.splitlines(keepends=True)):
        assert kept in (out / "orders.yml").read_text()
    assert floe("validate", "-c", config)[0] == 0


def test_rename_breaks_pii_reference_and_blocks(floe: Run, tmp_path: Path) -> None:
    config, out = project(tmp_path)
    assert imp(floe, book(tmp_path), out, "-c", config)[0] == 0
    (out / "_domain.yml").write_text(DOMAIN)
    engineer_edits(out / "orders.yml")
    before = tree(tmp_path / "proj")

    renamed = book(tmp_path, phone="mobile")
    code, stdout, _ = imp(floe, renamed, out, "-c", config)
    assert code == 1
    assert "breaking" in stdout
    assert tree(tmp_path / "proj") == before

    code, _, err = imp(floe, renamed, out, "-c", config, "--accept-breaking")
    assert code == 1  # -c validation sees `pii.columns` naming the old column
    assert "the project would not validate after the import" in err
    assert "phone" in err
    assert tree(tmp_path / "proj") == before

    assert (
        imp(floe, renamed, out, "--accept-breaking")[0] == 0
    )  # without -c the rename is the engineer's to fix
    assert floe("validate", "-c", config)[0] == 1


def lines_of(sheet: list[list[Any]], label: str, value: str) -> list[list[Any]]:
    return [[*r[:1], value] if r[0] == label else r for r in sheet]


def bad(tmp: Path, **cols: Any) -> Path:
    """A one-table workbook whose Columns rows are `cols['rows']`; `Tables` and `Guide` may be replaced too."""
    return write_book(
        tmp / "bad.xlsx",
        {
            "Guide": cols.get("guide", GUIDE),
            "Tables": cols.get("tables", tables({"target_table": "orders"})),
            "Columns": columns(*cols["rows"]),
        },
    )


FAILURES: dict[str, tuple[dict[str, Any], str]] = {
    "unknown table": ({"rows": [col("ghost", "id")]}, "unknown target_table ghost"),
    "duplicate target column": (
        {"rows": [col("orders", "id"), col("orders", "id")]},
        "duplicate target_column id",
    ),
    "bad X": (
        {"rows": [col("orders", "id", primary_key="yes")]},
        "expected X or blank, got yes",
    ),
    "bad type": ({"rows": [col("orders", "id", target_type="nope")]}, "nope"),
    "bad severity": (
        {"rows": [col("orders", "id", not_null="X", not_null_severity="loud")]},
        "severity must be one of",
    ),
    "bad group": (
        {"rows": [col("orders", "id", primary_key="X", unique_group="U1")]},
        "identical to the primary key",
    ),
}


@pytest.mark.parametrize("name", FAILURES)
def test_failure_writes_nothing(floe: Run, tmp_path: Path, name: str) -> None:
    kwargs, message = FAILURES[name]
    out = tmp_path / "domains/sales"
    code, _, err = imp(floe, bad(tmp_path, **kwargs), out)
    assert code == 1
    assert message in err
    assert not out.exists()

    assert (
        imp(floe, book(tmp_path), out)[0] == 0
    )  # the same failure on a populated directory
    before = tree(out)
    code, _, err = imp(floe, bad(tmp_path, **kwargs), out)
    assert (code, tree(out)) == (1, before)
    assert message in err


def test_hand_edited_owned_key_writes_nothing(floe: Run, tmp_path: Path) -> None:
    out = tmp_path / "domains/sales"
    assert imp(floe, book(tmp_path), out)[0] == 0
    path = out / "lines.yml"
    path.write_text(path.read_text().replace("type: int64", "type: string"))
    before = tree(out)
    code, stdout, _ = imp(
        floe, book(tmp_path, qty_type="float64"), out, "--accept-breaking"
    )
    assert code == 1
    assert "blocked" in stdout
    assert tree(out) == before  # not even orders.yml, which was fine


def test_unmanaged_file_writes_nothing(floe: Run, tmp_path: Path) -> None:
    out = tmp_path / "domains/sales"
    out.mkdir(parents=True)
    (out / "orders.yml").write_text("name: orders\n# written by hand\n")
    before = tree(out)
    code, stdout, _ = imp(floe, book(tmp_path), out)
    assert code == 1
    assert "blocked" in stdout
    assert tree(out) == before
