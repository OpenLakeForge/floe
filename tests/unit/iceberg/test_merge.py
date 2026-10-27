"""The SCD1 and SCD2 MERGE statements on a plain DuckDB table; Iceberg runs in `tests/integration/`."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs

import datetime as dt
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import BucketTransform
from pyiceberg.types import LongType, NestedField

from floe.iceberg.merge import SCD2_COLUMNS, run_merge, scd1_sql, scd2_sql, stage_batch

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


def merge(con: duckdb.DuckDBPyConnection, tmp_path: Path, **kw: Any) -> tuple[int, int]:
    """(inserted, updated) from the count, checked against the rows the MERGE touched."""
    count, merge = scd1_sql('"t"', COLUMNS, ["id"], kw.get("ignore", ()))
    source = [str(tmp_path / "batch.parquet")]
    counts = con.execute(count, source).fetchone()
    affected = con.execute(merge, source).fetchone()
    assert counts is not None and affected is not None
    assert counts[0] + counts[1] == affected[0]
    return counts[0], counts[1]


def rows(con: duckdb.DuckDBPyConnection) -> list[tuple[Any, ...]]:
    return con.execute("SELECT * FROM t ORDER BY id").fetchall()


def test_golden_then_rerun_changes_nothing(
    con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    assert merge(con, tmp_path) == (1, 3)  # insert 5; update 2, 4, 7
    assert rows(con) == EXPECTED
    assert merge(con, tmp_path) == (0, 0)
    assert rows(con) == EXPECTED


def test_ignored_columns_keep_target_values(
    con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    assert merge(con, tmp_path, ignore={"name", "amount", "tier"}) == (1, 0)
    assert rows(con) == [*TARGET[:4], (5, "e", 50.0, "silver"), *TARGET[4:]]


def test_null_keys_never_match(con: duckdb.DuckDBPyConnection, tmp_path: Path) -> None:
    con.execute("INSERT INTO t VALUES (NULL, 'x', 1.0, NULL)")
    con.execute(f"COPY (SELECT * FROM t WHERE id IS NULL) TO '{tmp_path / 'batch.parquet'}'")  # fmt: skip
    assert merge(con, tmp_path) == (1, 0)
    assert len(rows(con)) == len(TARGET) + 2


# SCD2: #598's golden fixture, with a closed historical version of key 1 that must never match
SCD2_TABLE = "CREATE TABLE t2 (id BIGINT, name VARCHAR, amount DOUBLE, tier VARCHAR, __floe_is_current BOOLEAN, __floe_valid_from TIMESTAMP, __floe_valid_to TIMESTAMP)"  # fmt: skip
T0, T_HIST, T = dt.datetime(2026, 1, 1), dt.datetime(2025, 12, 1), dt.datetime(2026, 2, 1, 12, 30, 0, 123456)  # fmt: skip
SCD2_TARGET = [(*r, True, T0, None) for r in TARGET] + [(1, "a-old", 1.0, None, False, T_HIST, T0)]  # fmt: skip
SCD2_EXPECTED = sorted([
    (1, "a-old", 1.0, None, False, T_HIST, T0),
    (1, "a", 10.0, None, True, T0, None),
    (3, "c", 30.0, None, True, T0, None),
    (6, "f", None, None, True, T0, None),
    (2, "b", 20.0, None, False, T0, T),
    (4, "d", 40.0, None, False, T0, T),
    (7, "g", None, None, False, T0, T),
    (2, "b2", 20.0, None, True, T, None),
    (4, "d", 40.0, "gold", True, T, None),
    (5, "e", 50.0, "silver", True, T, None),
    (7, "g", 70.0, None, True, T, None),
], key=repr)  # fmt: skip


@pytest.fixture
def scd2(con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    con.execute(SCD2_TABLE)
    con.executemany("INSERT INTO t2 VALUES (?, ?, ?, ?, ?, ?, ?)", SCD2_TARGET)
    return con


def scd2_statements(ignore: tuple[str, ...] = ()) -> tuple[str, str, str]:
    compare = [c for c in COLUMNS if c != "id" and c not in ignore]
    return scd2_sql('"t2"', COLUMNS, ["id"], compare, SCD2_COLUMNS, T)


def scd2_rows(con: duckdb.DuckDBPyConnection) -> list[tuple[Any, ...]]:
    return sorted(con.execute("SELECT * FROM t2").fetchall(), key=repr)


def batch_reader(con: duckdb.DuckDBPyConnection) -> pa.RecordBatchReader:
    return con.execute("SELECT * FROM b").to_arrow_reader(batch_size=2)


def test_scd2_golden_then_rerun_changes_nothing(
    scd2: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    count, changed, merge = scd2_statements()
    source = [str(tmp_path / "batch.parquet"), str(tmp_path / "changed.parquet")]

    def run() -> tuple[Any, ...] | None:
        counts = scd2.execute(count, source[:1]).fetchone()
        scd2.execute(changed, source)
        affected = scd2.execute(merge, source).fetchone()
        assert counts is not None and affected == (counts[0] + counts[1],)
        return counts

    assert run() == (4, 3, 2)  # insert 2, 4, 5, 7; close 2, 4, 7; 3 and 6 unchanged
    assert scd2_rows(scd2) == SCD2_EXPECTED
    assert run() == (0, 0, 6)
    assert scd2_rows(scd2) == SCD2_EXPECTED


def test_scd2_ignored_columns_are_not_compared(
    scd2: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    count, changed, merge = scd2_statements(ignore=("name", "amount", "tier"))
    files = [str(tmp_path / "batch.parquet")]
    assert run_merge(scd2, count, merge, files, changed) == (1, 0, 5)
    new = (5, "e", 50.0, "silver", True, T, None)
    assert scd2_rows(scd2) == sorted([*SCD2_TARGET, new], key=repr)


def test_scd2_per_bucket_merges_match_the_golden_fixture(
    scd2: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    spec = PartitionSpec(PartitionField(1, 1000, BucketTransform(4), "id_bucket"))
    schema = Schema(NestedField(1, "id", LongType(), required=True))
    staging = tmp_path / "staging"
    staging.mkdir()
    files = stage_batch(staging, batch_reader(scd2), spec, schema)
    assert len(files) > 1
    assert sorted(pq.read_table(files).column("id").to_pylist()) == [2, 3, 4, 5, 6, 7]

    count, changed, merge = scd2_statements()
    assert run_merge(scd2, count, merge, files, changed) == (4, 3, 2)
    assert scd2_rows(scd2) == SCD2_EXPECTED
    assert run_merge(scd2, count, merge, files, changed) == (0, 0, 6)
    assert scd2_rows(scd2) == SCD2_EXPECTED


def test_a_failed_merge_rolls_back_the_earlier_ones(
    con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    _, merge = scd1_sql('"t"', COLUMNS, ["id"], ())
    files = [str(tmp_path / "batch.parquet"), str(tmp_path / "missing.parquet")]
    with pytest.raises(duckdb.IOException):
        run_merge(con, "SELECT len($1), 0", merge, files)
    assert rows(con) == TARGET


def test_unpartitioned_batch_stages_one_file(
    con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    files = stage_batch(staging, batch_reader(con), PartitionSpec(), Schema())
    assert files == [str(staging / "batch-0.parquet")]
    assert pq.read_table(files[0]).num_rows == len(BATCH)
