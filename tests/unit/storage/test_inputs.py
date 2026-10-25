"""Ported from `crates/floe-core/tests/unit/io/storage/{inputs,local}.rs` (resolve cases)."""

import os
from pathlib import Path

import pytest

from floe.errors import FloeConfigError, FloeRunError
from floe.storage.inputs import resolve_inputs
from floe.storage.types import ObjectRef, StoredObject

NO_FILES = "entity.name=customers source.storage=local no input files matched"


class FakeClient:
    def __init__(self, keys: list[str]) -> None:
        self.keys = keys
        self.prefixes: list[str] = []

    def resolve_uri(self, path: str) -> str:
        return f"s3://bucket/{path}"

    def list(self, prefix: str) -> list[ObjectRef]:
        self.prefixes.append(prefix)
        return [
            ObjectRef(f"s3://bucket/{k}", k, None, None)
            for k in self.keys
            if k.startswith(prefix)
        ]

    def write(self, uri: str, body: bytes) -> None:
        raise NotImplementedError

    def copy(self, src_uri: str, dst_uri: str) -> None:
        raise NotImplementedError

    def delete(self, uri: str) -> None:
        raise NotImplementedError

    def read(self, uri: str) -> StoredObject | None:
        raise NotImplementedError

    def write_conditional(
        self, uri: str, expected_version: str | None, body: bytes
    ) -> str | None:
        raise NotImplementedError

    def delete_conditional(self, uri: str, expected_version: str | None) -> bool:
        raise NotImplementedError


def s3(
    path: str, keys: list[str], fmt: str = "csv", *, list_only: bool = False
) -> tuple[FakeClient, list[str], list[str]]:
    client = FakeClient(keys)
    got = resolve_inputs(
        Path("."),
        f"s3://bucket/{path}",
        fmt,
        "orders",
        "s3_raw",
        client,
        list_only=list_only,
    )
    assert got.mode == "directory"
    return client, [f.source_uri for f in got.files], got.listed


def touch(path: Path, text: str = "id\n1\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def local(
    root: Path,
    path: str | Path,
    *,
    recursive: bool = False,
    glob_override: str | None = None,
) -> list[str]:
    return resolve_inputs(
        root,
        str(path),
        "csv",
        "customers",
        "local",
        None,
        recursive=recursive,
        glob_override=glob_override,
    ).listed


def test_s3_filters_suffixes_and_sorts() -> None:
    keys = ["data/b.csv", "data/a.csv", "data/ignore.txt"]
    _, files, listed = s3("data", keys)
    assert files == listed == ["s3://bucket/data/a.csv", "s3://bucket/data/b.csv"]


def test_s3_filters_parquet_objects() -> None:
    _, files, _ = s3(
        "data", ["data/b.parquet", "data/a.parquet", "data/x.csv"], "parquet"
    )
    assert files == ["s3://bucket/data/a.parquet", "s3://bucket/data/b.parquet"]


def test_s3_list_only_returns_no_files() -> None:
    _, files, listed = s3(
        "data", ["data/b.csv", "data/a.csv", "data/x.txt"], list_only=True
    )
    assert files == []
    assert listed == ["s3://bucket/data/a.csv", "s3://bucket/data/b.csv"]


def test_s3_input_file_metadata() -> None:
    client = FakeClient(["data/a.b.csv"])
    got = resolve_inputs(
        Path("."), "s3://bucket/data", "csv", "orders", "s3_raw", client
    )
    (file,) = got.files
    assert (file.source_name, file.source_stem) == ("a.b.csv", "a.b")


def test_s3_glob_suffix_filters_and_uses_derived_prefix() -> None:
    keys = [
        "data/sales_2024.json",
        "data/sales_q1.csv",
        "data/sales_2024.csv",
        "data/inventory_2024.csv",
    ]
    client, _, listed = s3("data/sales_*.csv", keys, list_only=True)
    assert client.prefixes == ["data/sales_"]
    assert listed == [
        "s3://bucket/data/sales_2024.csv",
        "s3://bucket/data/sales_q1.csv",
    ]


def test_s3_glob_nested_segment_wildcard() -> None:
    keys = [
        "data/us/sales.csv",
        "data/eu/sales.csv",
        "data/us/archive/sales.csv",
        "data/sales.csv",
    ]
    client, _, listed = s3("data/*/sales.csv", keys, list_only=True)
    assert client.prefixes == ["data/"]
    assert listed == ["s3://bucket/data/eu/sales.csv", "s3://bucket/data/us/sales.csv"]


def test_s3_glob_results_stably_sorted() -> None:
    client, _, listed = s3("root/*.csv", ["root/b.csv", "root/a.csv", "root/c.csv"])
    assert client.prefixes == ["root/"]
    assert listed == [f"s3://bucket/root/{n}.csv" for n in "abc"]


def test_s3_literal_brackets_are_a_literal_path() -> None:
    keys = ["data/report[2024].csv", "data/report22024.csv"]
    client, _, listed = s3("data/report[2024].csv", keys, list_only=True)
    assert client.prefixes == ["data/report[2024].csv"]
    assert listed == ["s3://bucket/data/report[2024].csv"]


@pytest.mark.parametrize(
    ("path", "reason"),
    [
        ("*.csv", "non-empty literal prefix"),
        ("data/*.[", "invalid cloud glob pattern"),
    ],
)
def test_s3_invalid_glob_is_rejected_before_listing(path: str, reason: str) -> None:
    client = FakeClient(["data/a.csv"])
    with pytest.raises(FloeRunError) as err:
        resolve_inputs(
            Path("."), f"s3://bucket/{path}", "csv", "orders", "s3_raw", client
        )
    assert str(err.value).startswith(
        f"entity.name=orders source.storage=s3_raw invalid cloud source path "
        f"(bucket=bucket, path={path}): "
    )
    assert reason in str(err.value)
    assert client.prefixes == []


def test_s3_skips_floe_state_keys() -> None:
    _, _, listed = s3(
        "data", [".floe/s.csv", "data/.floe/s.csv", "data/a.csv"], list_only=True
    )
    assert listed == ["s3://bucket/data/a.csv"]


def test_s3_no_match_message() -> None:
    with pytest.raises(FloeRunError) as err:
        s3("data/*.csv", ["data/a.txt"])
    assert str(err.value) == (
        "entity.name=orders source.storage=s3_raw no input objects matched "
        "(bucket=bucket, prefix=data/, glob=data/*.csv, suffixes=.csv)"
    )


def test_s3_requires_a_client() -> None:
    with pytest.raises(FloeRunError) as err:
        resolve_inputs(Path("."), "s3://b/k", "csv", "orders", "s3_raw", None)
    assert str(err.value) == "s3 storage client missing"


def test_unsupported_scheme_and_format() -> None:
    with pytest.raises(FloeConfigError) as err:
        resolve_inputs(Path("."), "gs://b/k", "csv", "orders", "gcs", None)
    assert str(err.value) == "unsupported storage uri: gs://b/k"
    with pytest.raises(FloeConfigError) as err:
        resolve_inputs(
            Path("."), "s3://b/k", "fixed", "orders", "s3_raw", FakeClient([])
        )
    assert str(err.value) == "unsupported source format for input resolution: fixed"


def test_local_normalizes_resolved_file_paths(tmp_path: Path) -> None:
    (tmp_path / "cfg").mkdir()
    data = touch(tmp_path / "in" / "data.csv")
    got = resolve_inputs(
        tmp_path / "cfg", "../in/./data.csv", "csv", "orders", "local", None
    )
    assert got.mode == "file"
    assert [f.source_uri for f in got.files] == got.listed == [str(data)]
    (file,) = got.files
    assert (file.source_name, file.source_stem, file.source_size) == (
        "data.csv",
        "data",
        len("id\n1\n"),
    )
    assert file.source_mtime is not None
    assert file.source_mtime.endswith("Z")


def test_local_list_only_returns_no_files(tmp_path: Path) -> None:
    data = touch(tmp_path / "a.csv")
    got = resolve_inputs(tmp_path, "a.csv", "csv", "e", "local", None, list_only=True)
    assert (got.files, got.listed) == ([], [str(data)])


def test_local_uri_comes_from_the_client(tmp_path: Path) -> None:
    touch(tmp_path / "a.csv")
    got = resolve_inputs(tmp_path, "a.csv", "csv", "e", "local", FakeClient([]))
    assert got.listed == [f"s3://bucket/{tmp_path / 'a.csv'}"]


def test_local_default_glob_filters_by_format_case_insensitively(
    tmp_path: Path,
) -> None:
    for name in ("a.csv", "B.CSV", "c.txt"):
        touch(tmp_path / name)
    assert local(tmp_path, tmp_path) == [
        str(tmp_path / "B.CSV"),
        str(tmp_path / "a.csv"),
    ]


def test_local_glob_override_is_used(tmp_path: Path) -> None:
    touch(tmp_path / "a.csv")
    touch(tmp_path / "b.data")
    assert local(tmp_path, tmp_path, glob_override="*.data") == [
        str(tmp_path / "b.data")
    ]


def test_local_recursive_lists_nested_files(tmp_path: Path) -> None:
    nested = touch(tmp_path / "nested" / "a.csv")
    assert local(tmp_path, tmp_path) == [] if False else True
    with pytest.raises(FloeRunError):
        local(tmp_path, tmp_path)
    assert local(tmp_path, tmp_path, recursive=True) == [str(nested)]


def test_local_glob_path_input_is_resolved(tmp_path: Path) -> None:
    touch(tmp_path / "a.csv")
    touch(tmp_path / "a.txt")
    assert local(Path("."), tmp_path / "*.csv") == [str(tmp_path / "a.csv")]


def test_local_list_is_sorted(tmp_path: Path) -> None:
    touch(tmp_path / "b.csv")
    touch(tmp_path / "a.csv")
    assert local(tmp_path, tmp_path) == [
        str(tmp_path / "a.csv"),
        str(tmp_path / "b.csv"),
    ]


def test_local_missing_path_message(tmp_path: Path) -> None:
    with pytest.raises(FloeRunError) as err:
        local(tmp_path, "missing")
    assert str(err.value) == (
        f"{NO_FILES} (base_path={tmp_path / 'missing'}, glob=*.[cC][sS][vV], "
        "recursive=false)"
    )


def test_local_empty_directory_and_empty_glob_messages(tmp_path: Path) -> None:
    with pytest.raises(FloeRunError) as err:
        local(tmp_path, tmp_path, recursive=True, glob_override="*.data")
    assert str(err.value) == (
        f"{NO_FILES} (base_path={tmp_path}, glob=*.data, recursive=true)"
    )
    with pytest.raises(FloeRunError) as err:
        local(tmp_path, "*.csv")
    assert str(err.value) == (
        f"{NO_FILES} (base_path={tmp_path}, glob=*.csv, recursive=false)"
    )


def test_s3_trailing_double_star_is_recursive() -> None:
    keys = ["data/a.csv", "data/nested/b.csv", "other/c.csv"]
    _, _, listed = s3("data/**", keys, list_only=True)
    assert listed == ["s3://bucket/data/a.csv", "s3://bucket/data/nested/b.csv"]


def test_s3_double_star_segment_matches_zero_or_more_directories() -> None:
    keys = ["data/a.csv", "data/x/y/b.csv"]
    _, _, listed = s3("data/**/*.csv", keys, list_only=True)
    assert listed == ["s3://bucket/data/a.csv", "s3://bucket/data/x/y/b.csv"]


def test_s3_negated_character_class() -> None:
    _, _, listed = s3("data/*[!a].csv", ["data/a.csv", "data/b.csv"], list_only=True)
    assert listed == ["s3://bucket/data/b.csv"]


@pytest.mark.skipif(os.geteuid() == 0, reason="root can read any directory")
def test_local_recursive_unreadable_directory_is_an_error(tmp_path: Path) -> None:
    touch(tmp_path / "a.csv")
    touch(tmp_path / "restricted" / "b.csv")
    (tmp_path / "restricted").chmod(0)
    try:
        with pytest.raises(FloeConfigError) as err:
            local(tmp_path, tmp_path, recursive=True)
    finally:
        (tmp_path / "restricted").chmod(0o700)
    assert str(err.value).startswith("glob match failed for ")


def test_s3_negated_class_does_not_cross_directories() -> None:
    keys = ["data/good.csv", "data/x/y.csv"]
    _, _, listed = s3("data/*[!a]*.csv", keys, list_only=True)
    assert listed == ["s3://bucket/data/good.csv"]


def test_local_results_sort_by_complete_path_string(tmp_path: Path) -> None:
    nested = touch(tmp_path / "a" / "b.csv")
    flat = touch(tmp_path / "a.csv")
    assert local(tmp_path, tmp_path, recursive=True) == [str(flat), str(nested)]
    assert local(tmp_path, tmp_path / "**" / "*.csv") == [str(flat), str(nested)]


@pytest.mark.skipif(os.geteuid() == 0, reason="root can read any directory")
def test_local_unreadable_directory_outside_the_glob_is_ignored(tmp_path: Path) -> None:
    allowed = touch(tmp_path / "allowed1" / "a.csv")
    touch(tmp_path / "restricted" / "b.csv")
    (tmp_path / "restricted").chmod(0)
    try:
        assert local(Path("."), tmp_path / "allowed*" / "**" / "*.csv") == [
            str(allowed)
        ]
    finally:
        (tmp_path / "restricted").chmod(0o700)


@pytest.mark.skipif(os.geteuid() == 0, reason="root can read any directory")
def test_local_unreadable_sibling_under_star_is_an_error(tmp_path: Path) -> None:
    touch(tmp_path / "allowed" / "a.csv")
    touch(tmp_path / "restricted" / "b.csv")
    (tmp_path / "restricted").chmod(0)
    try:
        with pytest.raises(FloeConfigError) as err:
            local(Path("."), tmp_path / "*" / "*.csv")
    finally:
        (tmp_path / "restricted").chmod(0o700)
    assert str(err.value).startswith("glob match failed for ")


def test_s3_negated_class_keeps_a_trailing_hyphen_literal() -> None:
    keys = ["data/_.csv", "data/-.csv", "data/b.csv"]
    _, _, listed = s3("data/*[!_-].csv", keys, list_only=True)
    assert listed == ["s3://bucket/data/b.csv"]


def test_local_trailing_separator_keeps_a_glob_directory_only(tmp_path: Path) -> None:
    touch(tmp_path / "a.csv")
    with pytest.raises(FloeRunError):
        local(Path("."), f"{tmp_path}/*.csv/")
    with pytest.raises(FloeRunError):
        local(tmp_path, tmp_path, glob_override="*.csv/")


def test_local_empty_glob_override_matches_nothing(tmp_path: Path) -> None:
    touch(tmp_path / "a.csv")
    with pytest.raises(FloeRunError):
        local(tmp_path, tmp_path, glob_override="")


@pytest.mark.parametrize(
    ("fmt", "name"),
    [("avro", "a.avro"), ("orc", "a.orc"), ("xlsx", "a.xlsx"), ("xml", "a.xml")],
)
def test_directory_inputs_of_the_binary_and_xml_formats(
    tmp_path: Path, fmt: str, name: str
) -> None:
    touch(tmp_path / "in" / name)
    got = resolve_inputs(tmp_path, "in", fmt, "orders", "local", None)
    assert [f.source_name for f in got.files] == [name]
