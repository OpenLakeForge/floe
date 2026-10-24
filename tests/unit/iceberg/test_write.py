"""Chunking and result helpers; no catalog involved."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
# ^ pyarrow ships no type stubs

import pyarrow as pa

from floe.iceberg.write import (
    _chunks,  # pyright: ignore[reportPrivateUsage] -- the memory bound lives here
    _metadata_version,  # pyright: ignore[reportPrivateUsage] -- Rust tests `parse_metadata_version_from_location` the same way
)


def reader(batch_sizes: list[int]) -> pa.RecordBatchReader:  # pyright: ignore[reportUnknownParameterType] -- pyarrow ships no stubs
    schema = pa.schema([("id", pa.int64())])
    batches = [
        pa.record_batch([pa.array(range(n), pa.int64())], schema=schema)
        for n in batch_sizes
    ]
    return pa.RecordBatchReader.from_batches(schema, batches)


def test_chunks_group_batches_up_to_the_row_threshold() -> None:
    sizes = [chunk.num_rows for chunk in _chunks(reader([4, 4, 4, 4, 1]), 8)]
    assert sizes == [8, 8, 1]


def test_chunks_of_an_empty_stream_yield_nothing() -> None:
    assert list(_chunks(reader([]), 8)) == []


def test_metadata_version_is_the_filename_prefix() -> None:
    assert _metadata_version("s3://w/t/metadata/00007-abc.metadata.json") == 7
    assert _metadata_version("s3://w/t/metadata/weird.json") is None
    assert _metadata_version(None) is None
