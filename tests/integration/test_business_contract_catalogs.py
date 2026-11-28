"""An imported business-contract domain, end to end on a real Polaris catalog and a real S3 store (#1481).

`floe contract import` writes the entity, the engineer completes `_domain.yml` once, `floe run` ingests CSV
that hits the business checks. Iceberg tests need `FLOE_POLARIS_URI` and `FLOE_S3_ENDPOINT` (`-m polaris`),
the Delta test needs `FLOE_S3_ENDPOINT` (`-m s3`); harness: `.github/workflows/python-integration.yml`.
"""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
# ^ pyiceberg, deltalake and pyarrow ship no complete type stubs

import importlib
import os
import sys
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from deltalake import DeltaTable
from pyiceberg.catalog import Catalog

from floe.config.parse import parse_config
from floe.contract.importer import run_import
from floe.contract.workbook import COLUMNS_HEADERS, TABLES_HEADERS
from floe.delta.sink import storage_options
from floe.iceberg.catalog import open_catalog
from floe.run import RunOutcome, run

sys.path.insert(0, str(Path(__file__).parents[1] / "unit" / "contract"))
# the workbook builder lives next to the importer's unit tests; loaded by name so pyright need not see that folder
write_book: Callable[..., Path] = importlib.import_module("xlsx_book").write_book

URI = os.environ.get("FLOE_POLARIS_URI")
S3 = os.environ.get("FLOE_S3_ENDPOINT")
BUCKET = os.environ.get("FLOE_S3_BUCKET", "warehouse")
needs_polaris = pytest.mark.skipif(
    URI is None or S3 is None, reason="FLOE_POLARIS_URI or FLOE_S3_ENDPOINT is not set"
)
needs_s3 = pytest.mark.skipif(S3 is None, reason="FLOE_S3_ENDPOINT is not set")

ROOT = """\
version: "0.3"
storages:
  default: local
  definitions:
    - {{name: local, type: local}}
    - {{name: lake, type: s3, bucket: {bucket}, region: us-east-1, endpoint: "{s3}", path_style_access: true, prefix: {prefix}}}
{catalogs}include:
  domains:
    - "domains/*/_domain.yml"
report: {{path: ./report}}
"""
CATALOGS = """\
catalogs:
  default: polaris
  definitions:
    - {{name: polaris, type: rest, uri: "{uri}", warehouse: spike, credential: "spike-client:spike-secret", scope: "PRINCIPAL_ROLE:ALL"}}
"""
# the engineer's one-time completion of the scaffold; `{evolution}` is the schema_evolution line, or nothing
ICEBERG_DOMAIN = """\
name: {tag}
defaults:
  source: {{format: csv, path: "./in/{{{{resource}}}}.csv", options: {{header: true, separator: ","}}}}
  sink:
    write_mode: append
    accepted: {{format: iceberg, path: "tables/{{{{entity.name}}}}", storage: lake, iceberg: {{catalog: polaris}}}}
    rejected: {{format: iceberg, path: "tables/{{{{entity.name}}}}_rejected", storage: lake}}
{evolution}"""
EVOLUTION = "  schema: {schema_evolution: {mode: add_columns}}\n"
DELTA_DOMAIN = """\
name: {tag}
defaults:
  source: {{format: csv, path: "./in/{{{{resource}}}}.csv", options: {{header: true, separator: ","}}}}
  sink:
    write_mode: append
    accepted: {{format: delta, path: "tables/{{{{entity.name}}}}", storage: lake}}
    rejected: {{format: csv, path: "./out/rejected/{{{{entity.name}}}}"}}
"""

# order_id + line_no are the composite key. Per row: ok, ok, `code` too long (warn), `status` not allowed,
# order_id null, repeats the key of the first row.
BATCH = """\
order_id,line_no,status,code,note
1,1,open,abc,a
1,2,closed,ab,b
2,1,open,abcd,c
3,1,pending,abc,d
,1,open,abc,e
1,1,open,abc,f
"""
CLEAN = "order_id,line_no,status,code,note\n1,1,open,abc,a\n1,2,closed,ab,b\n2,1,open,abc,c\n"


def _cells(headers: tuple[str, ...], **cells: str) -> list[str | None]:
    return [cells.get(h) for h in headers]


def workbook(
    tmp: Path, tag: str, *, line_no_type: str = "int64", discount: bool = False
) -> Path:
    """One `orders` table: composite primary key, an allowed-values column, a max-length column."""

    def col(name: str, type_: str = "string", **kw: str) -> list[str | None]:
        return _cells(
            COLUMNS_HEADERS,
            target_table="orders",
            target_column=name,
            target_type=type_,
            **kw,
        )

    rows = [
        col("order_id", "int64", primary_key="X"),
        col("line_no", line_no_type, primary_key="X"),
        col("status", allowed_values="open|closed", value_severity="reject"),
        col("code", max_length="3", value_severity="warn"),
        col("note"),
    ] + ([col("discount", "int64")] if discount else [])
    table = _cells(
        TABLES_HEADERS,
        target_table="orders",
        source_dataset="orders",
        default_severity="reject",
        unique_severity="reject",
        not_null_severity="reject",
    )
    return write_book(
        tmp / "sales.xlsx",
        {
            "Guide": [
                ["domain", tag],
                ["template_version", "1"],
                ["default_severity", "reject"],
            ],
            "Tables": [list(TABLES_HEADERS), table],
            "Columns": [list(COLUMNS_HEADERS), *rows],
        },
    )


class Project:
    """A project directory: the workbook, `config.yml`, the domain folder and the CSV input."""

    def __init__(
        self, tmp: Path, tag: str, domain: str, prefix: str, catalogs: str = ""
    ) -> None:
        self.tmp, self.tag, self.domain = tmp, tag, domain
        self.out = tmp / "domains" / tag
        self.config = tmp / "config.yml"
        self.config.write_text(
            ROOT.format(
                bucket=BUCKET,
                s3=S3,
                prefix=prefix,
                catalogs=catalogs.format(uri=URI) if catalogs else "",
            )
        )
        (tmp / "in").mkdir()

    def import_book(
        self, book: Path, *, accept_breaking: bool = False, config: bool = False
    ) -> int:
        return run_import(
            str(book),
            str(self.out),
            None,
            False,
            False,
            accept_breaking,
            str(self.config) if config else None,
        )

    def complete_domain(self, evolution: str = "") -> None:
        """The engineer's one-time edit of the scaffold the first import left."""
        assert "TODO_source" in (self.out / "_domain.yml").read_text()  # fail closed until completed  # fmt: skip
        self.write_domain(evolution)

    def write_domain(self, evolution: str = "") -> None:
        text = self.domain.format(tag=self.tag, evolution=evolution)
        (self.out / "_domain.yml").write_text(text)

    def source(self, csv: str) -> None:
        (self.tmp / "in" / "orders.csv").write_text(csv)

    def run(self) -> RunOutcome:
        return run(self.config, parse_config(self.config))

    def entity(self) -> str:
        return (self.out / "orders.yml").read_text()


def rules(report: dict[str, Any]) -> dict[str, tuple[str, int]]:
    """`{rule: (severity, violations)}` over the files of an entity report."""
    found: dict[str, tuple[str, int]] = {}
    for f in report["files"]:
        for r in f["validation"]["rules"]:
            found[r["rule"]] = (r["severity"], r["violations"])
    return found


# not_null, allowed_values and the composite key reject; max_length only warns (the row is kept)
BATCH_RULES = {
    "not_null": ("reject", 1),
    "not_allowed": ("reject", 1),
    "length": ("warn", 1),
    "unique": ("reject", 1),
}


def assert_batch_counts(report: dict[str, Any]) -> None:
    results = report["results"]
    assert (results["rows_total"], results["accepted_total"], results["rejected_total"]) == (6, 3, 3)  # fmt: skip
    assert rules(report) == BATCH_RULES


@pytest.fixture(autouse=True)
def credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "spike")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "spike-s3-secret")


@pytest.fixture
def tag() -> str:
    return f"bc{uuid.uuid4().hex[:8]}"


@pytest.fixture
def iceberg(tmp_path: Path, tag: str) -> Iterator[tuple[Project, Catalog]]:
    """The v1 workbook imported and its domain completed, on a real Polaris; drops what the test created."""
    p = Project(
        tmp_path, tag, ICEBERG_DOMAIN, f"spike/{tag}", CATALOGS
    )  # the `spike` warehouse allows tables under spike/<namespace>
    assert p.import_book(workbook(tmp_path, tag)) == 0
    p.complete_domain()
    catalog = open_catalog(parse_config(p.config).catalogs.definitions[0], None)  # pyright: ignore[reportOptionalMemberAccess] -- the config declares a catalog
    try:
        yield p, catalog
    finally:
        for name in ("orders", "orders_rejected"):
            if catalog.table_exists((tag, name)):
                catalog.drop_table((tag, name))
        if catalog.namespace_exists((tag,)):
            catalog.drop_namespace((tag,))


def pairs(table: Any, *columns: str) -> list[tuple[Any, ...]]:
    rows = table.scan().to_arrow().to_pylist()
    return sorted(tuple(r[c] for c in columns) for r in rows)


@pytest.mark.polaris
@needs_polaris
def test_imported_domain_runs_on_iceberg(iceberg: tuple[Project, Catalog]) -> None:
    p, catalog = iceberg
    p.source(BATCH)
    report = p.run().entity_reports[0]
    assert_batch_counts(report)
    out = report["accepted_output"]
    assert (out["iceberg_namespace"], out["iceberg_table"]) == (p.tag, "orders")
    table = catalog.load_table((p.tag, "orders"))
    assert pairs(table, "order_id", "line_no", "code") == [
        (1, 1, "abc"),
        (1, 2, "ab"),
        (2, 1, "abcd"),
    ]
    assert catalog.load_table((p.tag, "orders_rejected")).scan().to_arrow().num_rows == 3  # fmt: skip


@pytest.mark.polaris
@needs_polaris
def test_additive_reimport_evolves_iceberg_table(iceberg: tuple[Project, Catalog]) -> None:  # fmt: skip
    p, catalog = iceberg
    p.source(CLEAN)
    p.run()
    assert [f.name for f in catalog.load_table((p.tag, "orders")).schema().fields] == [
        "order_id", "line_no", "status", "code", "note",
    ]  # fmt: skip
    assert p.import_book(workbook(p.tmp, p.tag, discount=True), config=True) == 0
    assert "name: discount" in p.entity()
    p.write_domain(EVOLUTION)
    p.source(
        "order_id,line_no,status,code,note,discount\n4,1,open,abc,x,10\n4,2,closed,ab,y,20\n"
    )
    report = p.run().entity_reports[0]
    assert report["schema_evolution"]["added_columns"] == ["discount"]
    assert report["results"]["accepted_total"] == 2
    table = catalog.load_table((p.tag, "orders"))
    assert "discount" in [f.name for f in table.schema().fields]
    assert pairs(table, "order_id", "line_no", "discount") == [
        (1, 1, None),
        (1, 2, None),
        (2, 1, None),
        (4, 1, 10),
        (4, 2, 20),
    ]


@pytest.mark.polaris
@needs_polaris
def test_breaking_reimport_refused_table_untouched(
    iceberg: tuple[Project, Catalog], capsys: pytest.CaptureFixture[str]
) -> None:
    p, catalog = iceberg
    p.source(CLEAN)
    p.run()
    before = catalog.load_table((p.tag, "orders"))
    entity = p.entity()
    capsys.readouterr()
    assert p.import_book(workbook(p.tmp, p.tag, line_no_type="string"), config=True) == 1  # fmt: skip
    stdout = capsys.readouterr().out
    assert "breaking" in stdout and "--accept-breaking" in stdout
    assert p.entity() == entity  # nothing was written
    after = catalog.load_table((p.tag, "orders"))
    assert after.metadata_location == before.metadata_location
    assert after.schema() == before.schema()
    assert pairs(after, "order_id", "line_no") == [(1, 1), (1, 2), (2, 1)]


@pytest.mark.s3
@needs_s3
def test_imported_domain_runs_on_delta(tmp_path: Path, tag: str) -> None:
    p = Project(tmp_path, tag, DELTA_DOMAIN, f"it/{tag}")
    assert p.import_book(workbook(tmp_path, tag)) == 0
    p.complete_domain()
    p.source(BATCH)
    report = p.run().entity_reports[0]
    assert_batch_counts(report)
    storages = parse_config(p.config).storages
    assert storages is not None
    lake = next(s for s in storages.definitions if s.name == "lake")
    table = DeltaTable(f"s3://{BUCKET}/it/{tag}/tables/orders", storage_options=storage_options(lake))  # fmt: skip
    rows = table.to_pyarrow_table().to_pylist()
    assert sorted((r["order_id"], r["line_no"], r["code"]) for r in rows) == [
        (1, 1, "abc"),
        (1, 2, "ab"),
        (2, 1, "abcd"),
    ]
    (rejected,) = (tmp_path / "out" / "rejected" / "orders").rglob("*.csv")
    assert len(rejected.read_text().splitlines()) - 1 == 3  # the header, then 3 rows
