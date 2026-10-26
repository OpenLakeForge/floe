"""D28: `source.partitioning` path matching and load order."""

from datetime import date, datetime

import pytest

from floe.config.types import SourcePartitioning
from floe.errors import FloeRunError
from floe.storage.inputs import InputFile, ResolvedInputs
from floe.storage.partitioning import (
    Partition,
    in_load_order,
    partition_of,
    source_root,
)

ROOT = "s3://raw/crm/customer"
STD = SourcePartitioning(
    pattern="{yyyy}/{MM}/{dd}/{batch}", date_column="d", batch_column="b"
)


@pytest.mark.parametrize(
    ("uri", "root"),
    [
        ("s3://raw/crm/customer/", ROOT),
        ("local:///in/*/x/*.csv", "local:///in"),
        ("local:///in/a*.csv", "local:///in"),
    ],
)
def test_source_root(uri: str, root: str) -> None:
    assert source_root(uri) == root


@pytest.mark.parametrize(
    ("pattern", "path", "expected"),
    [
        ("{yyyy}/{MM}/{dd}/{batch}", "2026/10/24/a1b2/x.csv", Partition(date(2026, 10, 24), "a1b2")),
        ("/year={yyyy}/month={MM}/day={dd}/", "year=2026/month=01/day=02/x.csv", Partition(date(2026, 1, 2), None)),
        ("{yyyy}{MM}{dd}/{HH}", "20261024/13/x.csv", Partition(datetime(2026, 10, 24, 13), None)),
    ],
)  # fmt: skip
def test_partition_of(pattern: str, path: str, expected: Partition) -> None:
    p = SourcePartitioning(pattern=pattern, date_column="d", batch_column=None)
    assert partition_of(p, "orders", ROOT, f"{ROOT}/{path}") == expected


@pytest.mark.parametrize(
    "path",
    [
        "2026/10/24/x.csv",  # no batch level
        "2026/10/24/a1b2/extra/x.csv",  # the whole directory must match
        "26/10/24/a1b2/x.csv",
        "x.csv",
    ],
)
def test_a_path_that_does_not_match_fails(path: str) -> None:
    with pytest.raises(FloeRunError) as err:
        partition_of(STD, "orders", ROOT, f"{ROOT}/{path}")
    assert str(err.value) == (
        f"entity.name=orders input {ROOT}/{path} does not match "
        "source.partitioning.pattern={yyyy}/{MM}/{dd}/{batch}"
    )


def test_an_invalid_date_fails() -> None:
    uri = f"{ROOT}/2026/13/01/a/x.csv"
    with pytest.raises(FloeRunError) as err:
        partition_of(STD, "orders", ROOT, uri)
    assert str(err.value) == (
        f"entity.name=orders input {uri} has an invalid source.partitioning date: 2026-13-01"
    )


def file(path: str, mtime: str | None) -> InputFile:
    name = path.rpartition("/")[2]
    return InputFile(f"{ROOT}/{path}", name, name, 1, mtime)


def test_load_order_is_date_then_batch_mtime_then_batch_then_file() -> None:
    files = [
        file("2026/10/25/a/x.csv", "2026-10-25T00:00:00Z"),
        file("2026/10/24/zz/b.csv", "2026-10-24T09:00:00.5Z"),
        file("2026/10/24/aa/x.csv", "2026-10-24T10:00:00Z"),
        file("2026/10/24/zz/a.csv", "2026-10-24T09:00:00.000000001Z"),
        file(
            "2026/10/24/mm/x.csv", "2026-10-24T09:00:00Z"
        ),  # earlier than zz's .000000001
        file(
            "2026/10/24/ab/x.csv", "2026-10-24T10:00:00Z"
        ),  # same mtime as aa: by name
    ]
    got = in_load_order(STD, "orders", ROOT, ResolvedInputs(files, [], "directory"))
    order = [f.source_uri.removeprefix(f"{ROOT}/") for f in got.files]
    assert order == [
        "2026/10/24/mm/x.csv",
        "2026/10/24/zz/a.csv",
        "2026/10/24/zz/b.csv",
        "2026/10/24/aa/x.csv",
        "2026/10/24/ab/x.csv",
        "2026/10/25/a/x.csv",
    ]
    assert got.listed == [f.source_uri for f in got.files]
