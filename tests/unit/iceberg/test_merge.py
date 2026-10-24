"""The SCD1 MERGE statement on a plain DuckDB table; Iceberg runs in `tests/integration/`."""

from pathlib import Path
from typing import Any

import duckdb
import pytest

from floe.iceberg.merge import scd1_sql

COLUMNS = ["id", "name", "amount", "tier"]
# #598's golden fixture, without the date and region columns that only serve partitioning
TARGET = [(1, "a", 10.0, None), (2, "b", 20.0, None), (3, "c", 30.0, None), (4, "d", 40.0, None), (6, "f", None, None), (7, "g", None, None)]  # fmt: skip
BATCH = [(2, "b2", 20.0, None), (3, "c", 30.0, None), (4, "d", 40.0, "gold"), (5, "e", 50.0, "silver"), (6, "f", None, None), (7, "g", 70.0, None)]  # fmt: skip
EXPECTED = [(1, "a", 10.0, None), (2, "b2", 20.0, None), (3, "c", 30.0, None), (4, "d", 40.0, "gold"), (5, "e", 50.0, "silver"), (6, "f", None, None), (7, "g", 70.0, None)]  # fmt: skip


@pytest.fixture
def con(tmp_path: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    for name, rows in (("t", TARGET), ("b", BATCH)):
        con.execute(f"CREATE TABLE {name} (id BIGINT, name VARCHAR, amount DOUBLE, tier VARCHAR)")  # fmt: skip
        con.executemany(f"INSERT INTO {name} VALUES (?, ?, ?, ?)", rows)
    con.execute(f"COPY b TO '{tmp_path / 'batch.parquet'}'")
    return con


def merge(con: duckdb.DuckDBPyConnection, tmp_path: Path, **kw: Any) -> int:
    sql = scd1_sql('"t"', COLUMNS, ["id"], kw.get("ignore", ()))
    row = con.execute(sql, [str(tmp_path / "batch.parquet")]).fetchone()
    assert row is not None
    return row[0]


def rows(con: duckdb.DuckDBPyConnection) -> list[tuple[Any, ...]]:
    return con.execute("SELECT * FROM t ORDER BY id").fetchall()


def test_golden_then_rerun_changes_nothing(
    con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    assert merge(con, tmp_path) == 4  # 3 updates (2, 4, 7) and 1 insert (5)
    assert rows(con) == EXPECTED
    assert merge(con, tmp_path) == 0
    assert rows(con) == EXPECTED


def test_ignored_columns_keep_target_values(
    con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    merge(con, tmp_path, ignore={"name", "amount", "tier"})
    assert rows(con) == [*TARGET[:4], (5, "e", 50.0, "silver"), *TARGET[4:]]


def test_null_keys_never_match(con: duckdb.DuckDBPyConnection, tmp_path: Path) -> None:
    con.execute("INSERT INTO t VALUES (NULL, 'x', 1.0, NULL)")
    con.execute(f"COPY (SELECT * FROM t WHERE id IS NULL) TO '{tmp_path / 'batch.parquet'}'")  # fmt: skip
    assert merge(con, tmp_path) == 1
    assert len(rows(con)) == len(TARGET) + 2
