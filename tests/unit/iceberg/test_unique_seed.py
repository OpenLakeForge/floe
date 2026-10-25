"""#600's golden fixture: unique flags against a local Iceberg table match the Rust contract."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs

import math
import random
import struct
from collections.abc import Sequence
from pathlib import Path

import polars as pl
import pytest
from pyiceberg.io import load_file_io
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import CommitTableResponse, Table
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.table.update import (
    TableRequirement,
    TableUpdate,
    update_table_metadata,
)
from pyiceberg.transforms import BucketTransform
from pyiceberg.types import DoubleType, LongType, NestedField, StringType

from floe.checks.unique import UniqueTracker
from floe.iceberg.unique import seed_from_table

SCHEMA = Schema(
    NestedField(1, "id", LongType()),
    NestedField(2, "account", StringType()),
    NestedField(3, "seq", LongType()),
    NestedField(4, "f", DoubleType()),
)
NAMES = ["id", "account", "seq", "f"]
NAN = math.nan
Row = tuple[int | None, str | None, int | None, float | None]

HISTORY: list[Row] = [
    (1, "a", 1, 0.0),
    (2, "a", 2, NAN),
    (2, "a", 2, 1.5),  # duplicate key inside the history
    (None, "a", 3, None),  # NULL id: not seeded for [id], seeded for [account, seq]
    (4, None, 4, 2.5),  # NULL account: not seeded for [account, seq]
]
INCOMING: list[Row] = [
    (1, "a", 1, -0.0),
    (3, "a", 3, NAN),
    (None, "a", 9, None),
    (3, "a", 9, -0.0),
    (4, None, 4, 2.5),
    (5, "A", 1, 0.0),
    (None, None, None, None),
    (None, "a", 9, None),
    (4, "a", 4, 3.5),
    (6, "a", 4, 3.5),
]
T, B = "target", "batch"
EXPECTED: dict[tuple[str, ...], dict[int, str]] = {
    ("id",): {0: T, 3: B, 4: T, 8: T},
    ("account", "seq"): {0: T, 1: T, 3: B, 7: B, 9: B},
    # Rust compares floats by bits: -0.0 is a new key (history has 0.0), NaN matches NaN.
    ("f",): {1: T, 3: B, 4: T, 5: T, 9: B},
}
CONSTRAINTS = [("id",), ("account", "seq"), ("id", "account"), ("f",)]


class _Commits:
    """Just enough catalog for `Table.append` on a local warehouse: metadata stays in memory."""

    def supports_server_side_planning(self) -> bool:
        return False

    def _delete_old_metadata(self, *_: object) -> None:
        pass

    def commit_table(
        self,
        table: Table,
        requirements: tuple[TableRequirement, ...],
        updates: tuple[TableUpdate, ...],
    ) -> CommitTableResponse:
        for requirement in requirements:
            requirement.validate(table.metadata)
        return CommitTableResponse.model_validate(
            {
                "metadata": update_table_metadata(table.metadata, updates),
                "metadata-location": table.metadata_location,
            }
        )


def local_table(tmp: Path, rows: Sequence[Row], partitioned: bool) -> Table:
    spec = (
        PartitionSpec(PartitionField(1, 1000, BucketTransform(4), "id_bucket"))
        if partitioned
        else PartitionSpec()
    )
    metadata = new_table_metadata(
        SCHEMA, spec, UNSORTED_SORT_ORDER, f"file://{tmp}", {}
    )
    table = Table(
        ("ns", "history"),
        metadata,
        f"file://{tmp}/metadata/v1.metadata.json",
        load_file_io(location=str(tmp)),
        _Commits(),  # pyright: ignore[reportArgumentType] -- duck-typed stand-in for a catalog
    )
    if rows:  # no rows: a new table, no snapshot yet
        table.append(frame(rows).to_arrow().cast(SCHEMA.as_arrow()))
    return table


def frame(rows: Sequence[Row]) -> pl.DataFrame:
    schema = {"id": pl.Int64, "account": pl.String, "seq": pl.Int64, "f": pl.Float64}
    return pl.DataFrame(rows, schema=schema, orient="row")


def reference(
    history: Sequence[Row], incoming: Sequence[Row], cols: Sequence[str]
) -> dict[int, str]:
    """Brute force from #600 (`reference.py`): Rust's tracker, floats keyed by their bits."""

    def key(row: Row) -> tuple[object, ...] | None:
        values = [row[NAMES.index(c)] for c in cols]
        if any(v is None for v in values):
            return None
        return tuple(
            struct.pack("<d", v) if isinstance(v, float) else v for v in values
        )

    seeded = {k for row in history if (k := key(row)) is not None}
    seen, flagged = set(seeded), dict[int, str]()
    for idx, row in enumerate(incoming):
        k = key(row)
        if k is None:
            continue
        if k in seen:
            flagged[idx] = T if k in seeded else B
        seen.add(k)
    return flagged


def check(
    table: Table, incoming: Sequence[Row], cols: Sequence[str]
) -> tuple[list[int], int]:
    """The flagged rows and how many of them hit the target, as `UniqueTracker` reports them."""
    tracker = UniqueTracker([list(cols)])
    batch = frame(incoming)
    seed_from_table(tracker, table, batch, {})
    mask = tracker.check(batch)[0].mask
    return mask.arg_true().to_list(), tracker.results()[0].target_duplicates_count


def expect(flagged: dict[int, str]) -> tuple[list[int], int]:
    return sorted(flagged), sum(kind == T for kind in flagged.values())


@pytest.mark.parametrize("partitioned", [False, True])
@pytest.mark.parametrize("cols", list(EXPECTED))
def test_golden_fixture(
    tmp_path: Path, cols: tuple[str, ...], partitioned: bool
) -> None:
    assert reference(HISTORY, INCOMING, cols) == EXPECTED[cols]
    table = local_table(tmp_path, HISTORY, partitioned)
    assert check(table, INCOMING, cols) == expect(EXPECTED[cols])


def random_rows(rng: random.Random, n: int) -> list[Row]:
    def null() -> bool:
        return rng.random() < 0.1

    return [
        (
            None if null() else rng.randrange(500),
            None if null() else rng.choice("abc"),
            None if null() else rng.randrange(200),
            None if null() else rng.choice([0.0, -0.0, NAN, 1.0, 2.0]),
        )
        for _ in range(n)
    ]


@pytest.mark.parametrize("seed", range(3))
def test_random_fixtures_match_the_brute_force(tmp_path: Path, seed: int) -> None:
    rng = random.Random(seed)
    history, incoming = random_rows(rng, 600), random_rows(rng, 400)
    table = local_table(tmp_path, history, partitioned=seed == 0)
    for cols in CONSTRAINTS:
        assert check(table, incoming, cols) == expect(
            reference(history, incoming, cols)
        ), cols


def test_stored_names_are_scanned_and_renamed_back(tmp_path: Path) -> None:
    table = local_table(tmp_path, HISTORY, partitioned=False)
    tracker = UniqueTracker([["ID"]])
    batch = frame(INCOMING).rename({"id": "ID"})
    seed_from_table(tracker, table, batch, {"ID": "id"})
    assert tracker.check(batch)[0].mask.arg_true().to_list() == [0, 3, 4, 8]
    assert tracker.results()[0].target_duplicates_count == 3


def test_an_empty_table_seeds_nothing(tmp_path: Path) -> None:
    table = local_table(tmp_path, [], partitioned=False)
    assert check(table, INCOMING, ["id"]) == ([3, 8], 0)
