"""Config models, ported from `crates/floe-core/src/config/types.rs` (entity-level fields)."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from floe.pii import PiiColumn

IncrementalMode = Literal["none", "archive", "file", "row"]
PolicySeverity = Literal["warn", "reject", "abort"]
SchemaEvolutionMode = Literal["strict", "add_columns"]
SchemaEvolutionIncompatibleAction = Literal["fail"]
WriteMode = Literal["overwrite", "append", "merge_scd1", "merge_scd2"]


class _Model(BaseModel):
    model_config = ConfigDict(frozen=True)


class EntityMetadata(_Model):
    data_product: str | None
    domain: str | None
    owner: str | None
    description: str | None
    tags: list[str] | None


class EntityStateConfig(_Model):
    path: str | None


class PolicyConfig(_Model):
    severity: PolicySeverity


class NormalizeColumnsConfig(_Model):
    enabled: bool | None
    strategy: str | None


class SchemaMismatchConfig(_Model):
    missing_columns: str | None
    extra_columns: str | None


class SchemaEvolutionConfig(_Model):
    mode: SchemaEvolutionMode = "strict"
    on_incompatible: SchemaEvolutionIncompatibleAction = "fail"


class ColumnConfig(_Model):
    name: str
    source: str | None
    column_type: str
    nullable: bool | None
    unique: bool | None
    width: int | None
    trim: bool | None


class SchemaConfig(_Model):
    normalize_columns: NormalizeColumnsConfig | None
    mismatch: SchemaMismatchConfig | None
    schema_evolution: SchemaEvolutionConfig | None
    primary_key: list[str] | None
    unique_keys: list[list[str]] | None
    columns: list[ColumnConfig]


class SourceOptions(_Model):
    header: bool | None = True
    separator: str | None = ";"
    encoding: str | None = "UTF8"
    null_values: list[str] | None = [""]
    recursive: bool | None = False
    glob: str | None = None
    json_mode: str | None = None
    sheet: str | None = None
    header_row: int | None = 1
    data_row: int | None = None
    row_tag: str | None = None
    namespace: str | None = None
    value_tag: str | None = None


class SourcePartitioning(_Model):  # D28
    pattern: str
    date_column: str
    batch_column: str | None
    lookback_days: int = 1  # D30: file mode lists from the watermark minus these days


class SourceConfig(_Model):
    format: str
    path: str
    storage: str | None
    options: SourceOptions
    cast_mode: str
    source_ref: str | None
    resource: str | None
    partitioning: SourcePartitioning | None = None


class SinkOptions(_Model):
    compression: str | None
    row_group_size: int | None
    max_size_per_file: int | None


class MergeScd2Options(_Model):
    current_flag_column: str | None
    valid_from_column: str | None
    valid_to_column: str | None


class MergeOptions(_Model):
    ignore_columns: list[str] | None
    compare_columns: list[str] | None
    scd2: MergeScd2Options | None


class IcebergSinkOptions(_Model):
    catalog: str | None
    namespace: str | None
    table: str | None
    location: str | None


class IcebergPartitionField(_Model):
    column: str
    transform: str
    num_buckets: int | None


class SinkTarget(_Model):
    format: str
    path: str
    storage: str | None
    options: SinkOptions | None
    merge: MergeOptions | None
    iceberg: IcebergSinkOptions | None
    delta: object = None  # raw: Delta is dropped (D1), validation rejects it
    duckdb: object = None
    partition_by: list[str] | None
    partition_spec: list[IcebergPartitionField] | None
    write_mode: WriteMode


class ArchiveTarget(_Model):
    path: str
    storage: str | None


class SinkConfig(_Model):
    write_mode: WriteMode
    accepted: SinkTarget
    rejected: SinkTarget | None
    archive: ArchiveTarget | None


class PiiConfig(_Model):
    columns: list[PiiColumn]


class EntityConfig(_Model):
    name: str
    id: str
    metadata: EntityMetadata | None
    domain: str | None
    incremental_mode: IncrementalMode
    state: EntityStateConfig | None
    source: SourceConfig
    sink: SinkConfig
    policy: PolicyConfig
    # `schema` shadows a BaseModel attribute, so the field is `schema_` and the key stays `schema`.
    schema_: SchemaConfig = Field(alias="schema")
    pii: PiiConfig | None


def with_partition_columns(entity: EntityConfig) -> EntityConfig:
    """D28: the columns `source.partitioning` derives count as schema columns."""
    p = entity.source.partitioning
    if p is None:
        return entity
    date_type = "datetime" if "{HH}" in p.pattern else "date"
    derived = [(p.date_column, date_type), (p.batch_column, "string")]
    columns = [
        ColumnConfig(name=n, source=n, column_type=t, nullable=False, unique=None, width=None, trim=None)
        for n, t in derived
        if n is not None
    ]  # fmt: skip  # `source` keeps the name out of normalize_columns
    schema = entity.schema_.model_copy(
        update={"columns": [*entity.schema_.columns, *columns]}
    )
    return entity.model_copy(update={"schema_": schema})
