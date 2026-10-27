"""#952: experimental compaction folds the MERGE position deletes in one REPLACE commit.

DuckDB and PyIceberg talk to the in-process REST catalog of `test_merge_commit_retry.py`.
"""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs

from collections.abc import Callable

import pyarrow as pa
import pyiceberg
import pytest
from pyiceberg.table import Table
from pyiceberg.table.snapshots import Operation
from test_merge_commit_retry import (  # pyright: ignore[reportMissingImports] -- a sibling test module, on pytest's path
    ARROW,
    RestCatalog,
    new_table,
    reader,
    server,  # noqa: F401  # pyright: ignore[reportUnusedImport] -- the fixture, used by name
)

from floe.config.storage import CatalogDefinition
from floe.errors import FloeRunError
from floe.iceberg.catalog import open_catalog, open_duckdb
from floe.iceberg.compact import compact
from floe.iceberg.merge import merge_scd1


def files(table: Table) -> tuple[int, int]:
    """(data files, delete files) of the current snapshot."""
    tasks = list(table.scan().plan_files())
    return len(tasks), len({d.file_path for t in tasks for d in t.delete_files})


def rows(table: Table) -> list[tuple[object, ...]]:
    return sorted(tuple(r.values()) for r in table.scan().to_arrow().to_pylist())


def test_compaction_folds_the_merge_deletes_in_one_commit(
    server: tuple[RestCatalog, CatalogDefinition],  # noqa: F811 -- the imported fixture
) -> None:
    catalog, definition = server
    table = new_table(definition)
    with open_duckdb(definition) as con:
        merge_scd1(con, table, reader(*((i, "a") for i in range(10))), ["id"])
        table.refresh()
        merge_scd1(con, table, reader((1, "b"), (2, "b"), (10, "b")), ["id"])
        table.refresh()
        merge_scd1(con, table, reader((2, "c"), (3, "c")), ["id"])
        table.refresh()
        before, commits = rows(table), len(catalog.commits)
        assert files(table)[1] > 0  # the merges left position deletes

        result = compact(table, con)

    table.refresh()
    assert files(table) == (1, 0)
    assert rows(table) == before and len(before) == 11
    assert len(catalog.commits) == commits + 1
    snapshot = table.current_snapshot()
    assert snapshot is not None and snapshot.summary is not None
    assert snapshot.summary.operation == Operation.REPLACE
    assert snapshot.summary["total-delete-files"] == "0"
    assert result["added_data_files"] == 1 and result["removed_delete_files"] > 0


def merged(definition: CatalogDefinition) -> Table:
    """A table after a load and one MERGE that left position deletes."""
    table = new_table(definition)
    with open_duckdb(definition) as con:
        merge_scd1(con, table, reader(*((i, "a") for i in range(10))), ["id"])
        table.refresh()
        merge_scd1(con, table, reader((1, "b"), (2, "b")), ["id"])
    table.refresh()
    return table


def before_commit(monkeypatch: pytest.MonkeyPatch, table: Table, write: Callable[[], object]) -> None:  # fmt: skip
    """Runs `write` once, as another writer, when the compaction reloads the head after its rewrite."""
    refresh = table.refresh

    def racing() -> Table:
        monkeypatch.setattr(table, "refresh", refresh)
        write()
        return refresh()

    monkeypatch.setattr(table, "refresh", racing)


def test_an_append_racing_the_compaction_is_kept(
    server: tuple[RestCatalog, CatalogDefinition],  # noqa: F811 -- the imported fixture
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, definition = server
    table = merged(definition)
    other = open_catalog(definition).load_table(("ns", "t"))
    before_commit(monkeypatch, table, lambda: other.append(pa.Table.from_pylist([{"id": 99, "v": "z"}], ARROW)))  # fmt: skip
    with open_duckdb(definition) as con:
        compact(table, con)
    table.refresh()
    assert files(table) == (2, 0)
    assert rows(table) == [(0, "a"), (1, "b"), (2, "b"), *((i, "a") for i in range(3, 10)), (99, "z")]  # fmt: skip


def test_a_merge_racing_the_compaction_fails_it(
    server: tuple[RestCatalog, CatalogDefinition],  # noqa: F811 -- the imported fixture
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, definition = server
    table = merged(definition)
    other = open_catalog(definition).load_table(("ns", "t"))

    def merge() -> None:
        with open_duckdb(definition) as con:
            merge_scd1(con, other, reader((3, "m")), ["id"])

    before_commit(monkeypatch, table, merge)
    with open_duckdb(definition) as con, pytest.raises(FloeRunError, match="compaction commit failed"):  # fmt: skip
        compact(table, con)
    table.refresh()
    assert (3, "m") in rows(table) and files(table)[1] > 0


def test_compaction_of_an_empty_table_commits_nothing(
    server: tuple[RestCatalog, CatalogDefinition],  # noqa: F811 -- the imported fixture
) -> None:
    catalog, definition = server
    table = new_table(definition)
    with open_duckdb(definition) as con:
        assert compact(table, con) == {"rewritten_data_files": 0, "removed_delete_files": 0, "added_data_files": 0}  # fmt: skip
    assert catalog.commits == []


def test_pyiceberg_is_the_version_compaction_was_written_against() -> None:
    """`floe.iceberg.compact` subclasses private PyIceberg internals: re-check them before bumping."""
    assert pyiceberg.__version__ == "0.12.0", "re-verify floe/iceberg/compact.py against the new PyIceberg internals"  # fmt: skip
