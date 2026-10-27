"""Expected messages come from Rust 0.7 `floe validate`, except D7 (#584 S1, S2), D10 (`time`) and D12 (`bucket`)."""

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
    catalogs: str = "",
    partitioning: str = "",
) -> None:
    path = tmp_path / "config.yml"
    path.write_text(
        catalogs
        + textwrap.dedent(f"""
        version: "{version}"
        entities:
        - name: orders
          source: {{format: {fmt}, path: in.csv{partitioning}}}
          sink: {sink}
          policy: {{severity: warn}}
          schema: {schema}
        """)
    )
    validate_entities(parse_config(path))


COLS = "columns: [{name: id, type: string}, {name: amt, type: number}]"
CATALOG = (
    "storages: {default: s, definitions: [{name: s, type: s3, bucket: b, region: r}]}\n"
    "catalogs: {default: c, definitions: [{name: c, type: rest, uri: u}]}"
)


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
            "schema.schema_evolution.mode=add_columns requires sink.accepted.format=iceberg",
        ),
    ],
)
def test_schema_errors(tmp_path: Path, schema: str, message: str) -> None:
    with pytest.raises(FloeConfigError) as err:
        check(tmp_path, schema)
    assert str(err.value) == f"entity.name=orders {message}"


@pytest.mark.parametrize(
    ("pii", "message"),
    [
        ("{name: zz, strategy: nullify}", "pii.columns: zz is not a schema column"),
        (
            "{name: amt, strategy: hash}",
            (
                "pii.columns[name=amt].strategy=hash can only be applied to string columns "
                "(declared type=number); use strategy=nullify or strategy=drop instead"
            ),
        ),
        (
            "{name: id, strategy: redact}",
            (
                "pii.columns[name=id].strategy=redact cannot be applied to a unique-key column: "
                "the strategy collapses or removes values, violating uniqueness constraints"
            ),
        ),
    ],
)
def test_pii_errors(tmp_path: Path, pii: str, message: str) -> None:
    path = tmp_path / "config.yml"
    path.write_text(
        textwrap.dedent(f"""
        version: "0.2"
        entities:
        - name: orders
          source: {{format: csv, path: in.csv}}
          sink: {{accepted: {{format: parquet, path: out}}}}
          policy: {{severity: warn}}
          schema: {{{COLS}, primary_key: [id]}}
          pii: {{columns: [{pii}]}}
        """)
    )
    with pytest.raises(FloeConfigError) as err:
        validate_entities(parse_config(path))
    assert str(err.value) == f"entity.name=orders {message}"


def test_evolution_old_version(tmp_path: Path) -> None:
    with pytest.raises(FloeConfigError, match='requires root.version >= "0.2"'):
        check(tmp_path, f"{{{COLS}, schema_evolution: {{mode: strict}}}}", "0.1")


def test_d21_version_follows_floe(tmp_path: Path) -> None:
    check(tmp_path, f"{{{COLS}}}", "0.8")
    with pytest.raises(FloeConfigError) as err:
        check(tmp_path, f"{{{COLS}}}", "0.9")
    assert str(err.value) == "root.version=0.9 is unsupported (newer than Floe 0.8)"


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
        sink="{accepted: {format: iceberg, path: out, iceberg: {}}}",
        catalogs=CATALOG,
    )


def test_d2_iceberg_accepts_merge(tmp_path: Path) -> None:
    check(
        tmp_path,
        f"{{{COLS}, primary_key: [id]}}",
        sink="{write_mode: merge_scd2, accepted: {format: iceberg, path: out, iceberg: {}, merge: {scd2: {}}}}",
        catalogs=CATALOG,
    )


def test_scd2_empty_column_is_not_defaulted(tmp_path: Path) -> None:
    sink = "{write_mode: merge_scd2, accepted: {format: iceberg, path: out, merge: {scd2: {valid_to_column: ''}}}}"
    with pytest.raises(FloeConfigError, match="scd2.valid_to_column must not be empty"):
        check(tmp_path, f"{{{COLS}, primary_key: [id]}}", sink=sink)


def test_empty_iceberg_catalog_is_not_defaulted(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    path.write_text(
        textwrap.dedent(f"""
        version: "0.2"
        catalogs: {{default: c, definitions: [{{name: c, type: rest, uri: u}}]}}
        entities:
        - name: orders
          source: {{format: csv, path: in.csv}}
          sink: {{accepted: {{format: iceberg, path: out, iceberg: {{catalog: ''}}}}}}
          policy: {{severity: warn}}
          schema: {{{COLS}}}
        """)
    )
    with pytest.raises(FloeConfigError, match="references unknown catalog $"):
        validate_entities(parse_config(path))


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
            "{accepted: {format: iceberg, path: out, partition_spec: [{column: id, transform: 'bucket(16)'}]}}",
            (
                "sink.accepted.partition_spec[0].transform=bucket(16) is unsupported "
                "(allowed: identity, year, month, day, hour, bucket)"
            ),
        ),
        (
            "{accepted: {format: iceberg, path: out, partition_spec: [{column: id, transform: bucket}]}}",
            "sink.accepted.partition_spec[0].num_buckets is required for transform=bucket",
        ),
        (
            "{accepted: {format: iceberg, path: out, partition_spec: [{column: id, transform: Bucket, num_buckets: 0}]}}",
            "sink.accepted.partition_spec[0].num_buckets must be greater than 0",
        ),
        (
            "{accepted: {format: iceberg, path: out, partition_spec: [{column: id, transform: day, num_buckets: 4}]}}",
            "sink.accepted.partition_spec[0].num_buckets is only supported with transform=bucket",
        ),
        (
            "{accepted: {format: iceberg, path: out, partition_spec: [{column: id, num_buckets: 4}]}}",
            "sink.accepted.partition_spec[0].num_buckets is only supported with transform=bucket",
        ),
    ],
)
def test_sink_errors(tmp_path: Path, sink: str, message: str) -> None:
    with pytest.raises(FloeConfigError) as err:
        check(tmp_path, f"{{{COLS}, primary_key: [id]}}", sink=sink)
    assert str(err.value) == f"entity.name=orders {message}"


def test_scd2_column_collides_with_normalised_schema_name(tmp_path: Path) -> None:
    sink = "{write_mode: merge_scd2, accepted: {format: iceberg, path: out, merge: {scd2: {valid_to_column: valid_to}}}}"
    schema = "{columns: [{name: Valid To, type: string}], normalize_columns: {enabled: true}, primary_key: [Valid To]}"
    with pytest.raises(FloeConfigError, match="valid_to_column=valid_to collides"):
        check(tmp_path, schema, sink=sink)


def test_bucket_transform_ok(tmp_path: Path) -> None:
    sink = "{accepted: {format: iceberg, path: out, iceberg: {}, partition_spec: [{column: id, transform: bucket, num_buckets: 16}]}}"
    check(tmp_path, f"{{{COLS}}}", sink=sink, catalogs=CATALOG)


def test_d26_iceberg_requires_catalog(tmp_path: Path) -> None:
    with pytest.raises(FloeConfigError) as err:
        check(tmp_path, f"{{{COLS}}}", sink="{accepted: {format: iceberg, path: out}}")
    assert str(err.value) == (
        "entity.name=orders sink.accepted.format=iceberg requires "
        "sink.accepted.iceberg with a catalog in 0.8"
    )


PART = (
    ", partitioning: {pattern: 'raw/{yyyy}/{MM}/{dd}/{batch}',"
    " date_column: ingest_date, batch_column: batch_id}"
)


def test_d28_derived_columns_are_schema_columns(tmp_path: Path) -> None:
    sink = (
        "{write_mode: merge_scd1, accepted: {format: iceberg, path: out, iceberg: {},"
        " partition_spec: [{column: ingest_date, transform: day}, {column: batch_id}]}}"
    )
    schema = f"{{{COLS}, primary_key: [id, ingest_date], unique_keys: [[batch_id]]}}"
    check(tmp_path, schema, sink=sink, catalogs=CATALOG, partitioning=PART)


def test_d28_derived_columns_are_not_json_selectors(tmp_path: Path) -> None:
    columns = ", ".join(f"{{name: c{i}, type: string}}" for i in range(1024))
    part = ", partitioning: {pattern: '{yyyy}/{MM}/{dd}', date_column: 'ingest]date'}"
    check(tmp_path, f"{{columns: [{columns}]}}", fmt="json", partitioning=part)


def test_d28_derived_columns_need_partitioning(tmp_path: Path) -> None:
    sink = "{accepted: {format: iceberg, path: out, iceberg: {}, partition_spec: [{column: ingest_date, transform: day}]}}"
    with pytest.raises(FloeConfigError, match="column=ingest_date references unknown"):
        check(tmp_path, f"{{{COLS}}}", sink=sink, catalogs=CATALOG)


@pytest.mark.parametrize(
    ("block", "message"),
    [
        (
            "{pattern: '{yyyy}/{MM}/{dd}/{mm}', date_column: d}",
            (
                "source.partitioning.pattern={yyyy}/{MM}/{dd}/{mm} has unknown token {mm} "
                "(allowed: {yyyy}, {MM}, {dd}, {HH}, {batch})"
            ),
        ),
        (
            "{pattern: '{yyyy}/{MM}/{dd}/{batch', date_column: d}",
            "source.partitioning.pattern={yyyy}/{MM}/{dd}/{batch has an unmatched brace",
        ),
        (
            "{pattern: '{yyyy}/{MM}/{batch}/{batch}/{dd}', date_column: d}",
            "source.partitioning.pattern={yyyy}/{MM}/{batch}/{batch}/{dd} repeats token {batch}",
        ),
        (
            "{pattern: '{yyyy}/{MM}/{batch}', date_column: d}",
            "source.partitioning.pattern={yyyy}/{MM}/{batch} must contain {yyyy}, {MM} and {dd}",
        ),
        (
            "{pattern: '{yyyy}/{MM}/{dd}', date_column: ' '}",
            "source.partitioning.date_column must not be empty",
        ),
        (
            "{pattern: '{yyyy}/{MM}/{dd}/{batch}', date_column: d, batch_column: ''}",
            "source.partitioning.batch_column must not be empty",
        ),
        (
            "{pattern: '{yyyy}/{MM}/{dd}', date_column: d, batch_column: b}",
            "source.partitioning.batch_column requires {batch} in source.partitioning.pattern",
        ),
        (
            "{pattern: '{yyyy}/{MM}/{dd}/{batch}', date_column: d, batch_column: d}",
            "source.partitioning.batch_column=d must differ from source.partitioning.date_column",
        ),
        (
            "{pattern: '{yyyy}/{MM}/{dd}', date_column: amt}",
            "source.partitioning.date_column=amt is already a column in schema.columns",
        ),
        (
            "{pattern: '{yyyy}/{MM}/{dd}/{batch}', date_column: d, batch_column: id}",
            "source.partitioning.batch_column=id is already a column in schema.columns",
        ),
    ],
)
def test_d28_partitioning_errors(tmp_path: Path, block: str, message: str) -> None:
    with pytest.raises(FloeConfigError) as err:
        check(tmp_path, f"{{{COLS}}}", partitioning=f", partitioning: {block}")
    assert str(err.value) == f"entity.name=orders {message}"


def test_d28_a_derived_column_clashes_with_a_normalised_schema_column(tmp_path: Path) -> None:  # fmt: skip
    """#913: `Ingest Date` normalises to `ingest_date`, the derived column's name."""
    schema = "{columns: [{name: id, type: string}, {name: Ingest Date, type: string}], normalize_columns: {enabled: true}}"  # fmt: skip
    part = ", partitioning: {pattern: '{yyyy}/{MM}/{dd}', date_column: ingest_date}"
    with pytest.raises(FloeConfigError) as err:
        check(tmp_path, schema, partitioning=part)
    assert str(err.value) == (
        "entity.name=orders source.partitioning.date_column=ingest_date "
        "is already a column in schema.columns"
    )


def test_decimal_with_precision_and_scale_is_accepted(tmp_path: Path) -> None:
    check(
        tmp_path,
        "{columns: [{name: amt, type: 'Decimal(38, 10)'}, {name: b, type: 'decimal(1,0)'}]}",
    )


@pytest.mark.parametrize(
    ("kind", "message"),
    [
        ("decimal", "type=decimal needs a precision and scale"),
        ("decimal(39,2)", "needs 1 <= p <= 38"),
        ("decimal(0,0)", "needs 1 <= p <= 38"),
        ("decimal(5,6)", "0 <= s <= p"),
        ("decimal(5)", "is unsupported"),
        ("decimal(4,-2)", "is unsupported"),
        ("decimal(-4,2)", "is unsupported"),
    ],
)
def test_bare_or_invalid_decimal_is_rejected(
    tmp_path: Path, kind: str, message: str
) -> None:
    with pytest.raises(FloeConfigError, match=message.replace("(", r"\(")):
        check(tmp_path, f"{{columns: [{{name: amt, type: '{kind}'}}]}}")
