"""#600's golden fixture: unique flags against a local Iceberg table match the Rust contract."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs

import math
import random
import struct
import tempfile
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.io import load_file_io
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestFile,
    ManifestWriterV2,
)
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
from pyiceberg.typedef import Record
from pyiceberg.types import (
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

from floe.checks.unique import UniqueTracker, seed_from_history
from floe.errors import FloeRunError
from floe.iceberg.unique import cache_table_keys

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


def local_table(
    tmp: Path, rows: Sequence[Row], partitioned: bool, schema: Schema = SCHEMA
) -> Table:
    spec = (
        PartitionSpec(PartitionField(1, 1000, BucketTransform(4), "id_bucket"))
        if partitioned
        else PartitionSpec()
    )
    metadata = new_table_metadata(
        schema, spec, UNSORTED_SORT_ORDER, f"file://{tmp}", {}
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


def seed_from_table(
    tracker: UniqueTracker, table: Table, batch: pl.DataFrame, stored: Mapping[str, str]
) -> None:
    """What a run does per entity: cache the table's key columns, then seed from the cache."""
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "keys.parquet"
        if cache_table_keys(table, tracker.keys, stored, path):
            seed_from_history(tracker, pl.scan_parquet(path), batch, stored)


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


class _DeletesWriter(ManifestWriterV2):
    """PyIceberg writes no delete files, so the test commits a delete manifest by hand."""

    def content(self) -> ManifestContent:
        return ManifestContent.DELETES

    @property
    def _meta(self) -> dict[str, str]:
        return {**super()._meta, "content": "deletes"}


def add_delete_file(
    table: Table,
    tmp: Path,
    content: DataFileContent,
    rows: pa.Table,
    equality_ids: list[int] | None = None,
) -> None:
    path = tmp / f"delete-{uuid.uuid4()}.parquet"
    pq.write_table(rows, path)
    delete = DataFile.from_args(
        content=content, file_path=str(path), file_format=FileFormat.PARQUET,
        partition=Record(), record_count=rows.num_rows,
        file_size_in_bytes=path.stat().st_size, equality_ids=equality_ids,
        _table_format_version=2,
    )  # fmt: skip
    delete.spec_id = 0
    with table.transaction() as tx, tx.update_snapshot().fast_append() as snap:
        process = snap._process_manifests  # pyright: ignore[reportPrivateUsage] -- the only hook to add a manifest

        def with_deletes(manifests: list[ManifestFile]) -> list[ManifestFile]:
            out = table.io.new_output(str(tmp / f"deletes-{uuid.uuid4()}.avro"))
            snapshot_id = snap._snapshot_id  # pyright: ignore[reportPrivateUsage] -- the snapshot being written
            with _DeletesWriter(PartitionSpec(), SCHEMA, out, snapshot_id, "deflate") as writer:  # fmt: skip
                writer.add(ManifestEntry.from_args(status=ManifestEntryStatus.ADDED, snapshot_id=snapshot_id, data_file=delete))  # fmt: skip
            return process(manifests) + [writer.to_manifest_file()]

        snap._process_manifests = with_deletes  # pyright: ignore[reportPrivateUsage, reportAttributeAccessIssue] -- see above


def _field(name: str, kind: pa.DataType, field_id: int) -> pa.Field:
    return pa.field(name, kind, nullable=False, metadata={"PARQUET:field_id": str(field_id)})  # fmt: skip


def test_rows_removed_by_a_positional_delete_are_not_seeded(tmp_path: Path) -> None:
    """DuckDB merges (#817) write positional deletes: a deleted key is not a target duplicate."""
    table = local_table(tmp_path, [(1, "a", 1, 0.0), (2, "a", 2, 0.0)], partitioned=False)  # fmt: skip
    (task,) = table.scan().plan_files()
    position = pa.schema([_field("file_path", pa.string(), 2147483546), _field("pos", pa.int64(), 2147483545)])  # fmt: skip
    deletes = pa.table({"file_path": [task.file.file_path], "pos": [1]}, schema=position)  # fmt: skip
    add_delete_file(table, tmp_path, DataFileContent.POSITION_DELETES, deletes)
    assert [len(t.delete_files) for t in table.scan().plan_files()] == [1]
    assert check(table, [(1, "b", 9, None), (2, "b", 9, None)], ["id"]) == ([0], 1)


def test_an_equality_delete_fails_instead_of_seeding_deleted_keys(tmp_path: Path) -> None:  # fmt: skip
    table = local_table(tmp_path, [(1, "a", 1, 0.0), (2, "a", 2, 0.0)], partitioned=False)  # fmt: skip
    deletes = pa.table({"id": [2]}, schema=pa.schema([_field("id", pa.int64(), 1)]))
    add_delete_file(table, tmp_path, DataFileContent.EQUALITY_DELETES, deletes, [1])
    with pytest.raises(FloeRunError, match="^iceberg unique seed scan failed: .*equality deletes"):  # fmt: skip
        check(table, [(2, "b", 9, None)], ["id"])


def test_unsigned_batch_keys_meet_the_signed_table_keys(tmp_path: Path) -> None:
    """The sink widens unsigned keys (Iceberg has none); the hits come back in the batch's dtype."""
    table = local_table(tmp_path, HISTORY, partitioned=False)
    tracker = UniqueTracker([["id"]])
    batch = frame(INCOMING).drop_nulls("id").with_columns(pl.col("id").cast(pl.UInt32))
    seed_from_table(tracker, table, batch, {})
    assert tracker.check(batch)[0].mask.arg_true().to_list() == [0, 2, 3, 5]
    assert tracker.results()[0].target_duplicates_count == 3


def test_a_key_column_the_table_lacks_seeds_nothing(tmp_path: Path) -> None:
    """`add_columns` adds the column on write: historical rows hold NULL there, never a duplicate."""
    table = local_table(tmp_path, HISTORY, partitioned=False)
    tracker = UniqueTracker([["qty"], ["id"]])
    batch = frame(INCOMING).with_columns(qty=pl.Series([1, 1] + [None] * 8, dtype=pl.Int32))  # fmt: skip
    seed_from_table(tracker, table, batch, {})
    qty, ident = tracker.check(batch)
    assert qty.mask.arg_true().to_list() == [1]  # the in-batch check still runs
    assert ident.mask.arg_true().to_list() == [0, 3, 4, 8]


class _Recording(UniqueTracker):
    def __init__(self, keys: list[list[str]]) -> None:
        super().__init__(keys)
        self.seeded: list[pl.DataFrame] = []

    def seed(self, df: pl.DataFrame) -> None:
        self.seeded.append(df)
        super().seed(df)


def test_only_distinct_historical_keys_are_collected(tmp_path: Path) -> None:
    history = [(1, "a", i, 0.0) for i in range(50)]
    table = local_table(tmp_path, history, partitioned=False)
    tracker = _Recording([["id"]])
    seed_from_table(tracker, table, frame([(1, "b", 0, None)]), {})
    assert [df.height for df in tracker.seeded] == [1]


def test_millisecond_batch_keys_meet_the_microsecond_table_keys(tmp_path: Path) -> None:
    """The run casts datetimes to ms; Iceberg reads them back as us: the join aligns the units."""
    schema = Schema(NestedField(1, "ts", TimestampType()))
    table = local_table(tmp_path, [], partitioned=False, schema=schema)
    days = pl.Series("ts", ["2024-01-01", "2024-01-02"]).str.to_datetime(time_unit="us")
    table.append(days.to_frame().to_arrow().cast(schema.as_arrow()))
    tracker = UniqueTracker([["ts"]])
    batch = pl.Series("ts", ["2024-01-02", "2024-01-03"]).str.to_datetime(time_unit="ms").to_frame()  # fmt: skip
    seed_from_table(tracker, table, batch, {})
    assert tracker.check(batch)[0].mask.to_list() == [True, False]


def test_a_float_key_named_like_a_temporary_is_seeded(tmp_path: Path) -> None:
    schema = Schema(*SCHEMA.fields[:3], NestedField(4, "_bits0", DoubleType()))
    table = local_table(tmp_path, [], partitioned=False, schema=schema)
    table.append(frame(HISTORY).rename({"f": "_bits0"}).to_arrow().cast(schema.as_arrow()))  # fmt: skip
    tracker = UniqueTracker([["_bits0"]])
    batch = frame(INCOMING).rename({"f": "_bits0"})
    seed_from_table(tracker, table, batch, {})
    flagged = expect(EXPECTED[("f",)])
    assert tracker.check(batch)[0].mask.arg_true().to_list() == flagged[0]
    assert tracker.results()[0].target_duplicates_count == flagged[1]
