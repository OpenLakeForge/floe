"""Expected messages come from Rust 0.7 `floe validate`, except D7 (#584 S1, S2) and D10 (`time`)."""

import textwrap
from pathlib import Path

import pytest

from floe.config.parse import parse_config
from floe.config.validate import validate_entities
from floe.errors import FloeConfigError

ALLOWED = "string, number, boolean, datetime, date"


def check(
    tmp_path: Path,
    schema: str,
    version: str = "0.2",
    fmt: str = "csv",
    sink: str = "{accepted: {format: parquet, path: out}}",
) -> None:
    path = tmp_path / "config.yml"
    path.write_text(
        textwrap.dedent(f"""
        version: "{version}"
        entities:
        - name: orders
          source: {{format: {fmt}, path: in.csv}}
          sink: {sink}
          policy: {{severity: warn}}
          schema: {schema}
        """)
    )
    validate_entities(parse_config(path))


COLS = "columns: [{name: id, type: string}, {name: amt, type: number}]"


@pytest.mark.parametrize(
    ("schema", "message"),
    [
        (
            "{columns: [{name: id, type: 'string '}]}",
            f"schema.columns[0].type=string  is unsupported for column id (allowed: {ALLOWED})",
        ),
        (
            "{columns: [{name: id, type: time}]}",
            f"schema.columns[0].type=time is unsupported for column id (allowed: {ALLOWED})",
        ),
        (
            "{columns: [{name: id, type: string}], normalize_columns: {strategy: ' lower'}}",
            (
                "schema.normalize_columns.strategy= lower is unsupported "
                "(allowed: snake_case, lower, camel_case, none)"
            ),
        ),
        (
            f"{{{COLS}, mismatch: {{extra_columns: fill_nulls}}}}",
            (
                "schema.mismatch.extra_columns=fill_nulls is unsupported "
                "(allowed: reject_file, ignore)"
            ),
        ),
        (f"{{{COLS}, primary_key: []}}", "schema.primary_key must not be empty"),
        (
            f"{{{COLS}, primary_key: [id, id]}}",
            "schema.primary_key has duplicate column id",
        ),
        (
            f"{{{COLS}, primary_key: [zz]}}",
            "schema.primary_key[0]=zz references unknown schema column",
        ),
        (
            "{columns: [{name: id, type: string, nullable: true}], primary_key: [id]}",
            "schema.primary_key column id cannot set nullable=true",
        ),
        (
            f"{{{COLS}, unique_keys: [[id], []]}}",
            "schema.unique_keys[1] must not be empty",
        ),
        (
            f"{{{COLS}, unique_keys: [[id, id]]}}",
            "schema.unique_keys[0] has duplicate column id",
        ),
        (
            f"{{{COLS}, unique_keys: [[id, zz]]}}",
            "schema.unique_keys[0][1]=zz references unknown schema column",
        ),
        (
            f"{{{COLS}, schema_evolution: {{mode: add_columns}}}}",
            (
                "schema.schema_evolution.mode=add_columns requires sink.accepted.format=delta "
                "(Delta-only additive schema evolution in this phase)"
            ),
        ),
    ],
)
def test_schema_errors(tmp_path: Path, schema: str, message: str) -> None:
    with pytest.raises(FloeConfigError) as err:
        check(tmp_path, schema)
    assert str(err.value) == f"entity.name=orders {message}"


def test_evolution_old_version(tmp_path: Path) -> None:
    with pytest.raises(FloeConfigError, match='requires root.version >= "0.2"'):
        check(tmp_path, f"{{{COLS}, schema_evolution: {{mode: strict}}}}", "0.1")


def test_json_column_limit(tmp_path: Path) -> None:
    columns = ", ".join(f"{{name: c{i}, type: string}}" for i in range(1025))
    with pytest.raises(FloeConfigError, match="has 1025 entries"):
        check(tmp_path, f"{{columns: [{columns}]}}", fmt="json")


def test_valid_spellings(tmp_path: Path) -> None:
    check(
        tmp_path,
        "{columns: [{name: id, type: Int-64}, {name: t, type: TIMESTAMP}],"
        " normalize_columns: {strategy: Snake-Case}, primary_key: [' id '],"
        " unique_keys: [[id, t]]}",
    )


def test_add_columns_on_iceberg(tmp_path: Path) -> None:
    check(
        tmp_path,
        f"{{{COLS}, schema_evolution: {{mode: add_columns}}}}",
        sink="{accepted: {format: iceberg, path: out}}",
    )


def test_d2_iceberg_accepts_merge(tmp_path: Path) -> None:
    check(
        tmp_path,
        f"{{{COLS}, primary_key: [id]}}",
        sink="{write_mode: merge_scd2, accepted: {format: iceberg, path: out, merge: {scd2: {}}}}",
    )


@pytest.mark.parametrize(
    ("sink", "message"),
    [
        (
            "{accepted: {format: delta, path: out}}",
            "sink.accepted.format=delta is not supported in 0.8",
        ),
        (
            "{accepted: {format: duckdb, duckdb: {connection: 'md:x'}}}",
            "sink.accepted.format=duckdb is not supported in 0.8",
        ),
        (
            "{write_mode: merge_scd1, accepted: {format: parquet, path: out}}",
            "sink.write_mode=merge_scd1 is not supported by sink.accepted.format=parquet",
        ),
        (
            "{accepted: {format: iceberg, path: out, partition_spec: [{column: id, transform: bucket}]}}",
            (
                "sink.accepted.partition_spec[0].transform=bucket is unsupported "
                "(allowed: identity, year, month, day, hour)"
            ),
        ),
    ],
)
def test_sink_errors(tmp_path: Path, sink: str, message: str) -> None:
    with pytest.raises(FloeConfigError) as err:
        check(tmp_path, f"{{{COLS}, primary_key: [id]}}", sink=sink)
    assert str(err.value) == f"entity.name=orders {message}"
