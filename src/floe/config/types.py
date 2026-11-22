"""Config models (entity-level fields)."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from floe.pii import PiiColumn

IncrementalMode = Literal["none", "archive", "file"]
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
    max_rejection_rate: float | None = None
    min_rows_per_file: int | None = Field(default=None, ge=1)
    rule_severity: dict[str, PolicySeverity] | None = None


_LEVELS: tuple[PolicySeverity, ...] = ("warn", "reject", "abort")


def rule_level(
    entity: "EntityConfig", rule: str, column: "ColumnConfig | None" = None
) -> PolicySeverity:
    """The severity of one check: the column's `rule_severity`, else the entity's, else `policy.severity`."""
    for levels in (column and column.rule_severity, entity.policy.rule_severity):
        if levels and rule in levels:
            return levels[rule]
    return entity.policy.severity


def max_level(entity: "EntityConfig") -> PolicySeverity:
    """The most severe level any check of the entity can resolve to."""
    levels: list[PolicySeverity] = [
        entity.policy.severity,
        *(entity.policy.rule_severity or {}).values(),
    ]
    for c in entity.schema_.columns:
        levels += (c.rule_severity or {}).values()
    levels.extend(p.severity for p in entity.schema_.checks or [] if p.severity)
    return max(levels, key=_LEVELS.index)


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
    rule_severity: dict[str, PolicySeverity] | None = None
    # #1251: the raw YAML literals; `floe.checks.values.literal` converts them per column type
    min: int | float | str | None = None
    max: int | float | str | None = None
    allowed_values: list[int] | list[str] | None = None
    pattern: str | None = None
    min_length: int | None = None
    max_length: int | None = None


PredicateOp = Literal["eq", "ne", "lt", "le", "gt", "ge", "is_null", "not_null"]


class PredicateCondition(_Model):
    left: str
    op: PredicateOp
    right: str | None = None
    # #1252: the raw literal; `floe.checks.values.literal` converts it with the type of `left`
    value: int | float | str | None = None


class PredicateCheck(PredicateCondition):
    name: str
    when: PredicateCondition | None = None
    severity: PolicySeverity | None = None


class SchemaConfig(_Model):
    normalize_columns: NormalizeColumnsConfig | None
    mismatch: SchemaMismatchConfig | None
    schema_evolution: SchemaEvolutionConfig | None
    primary_key: list[str] | None
    unique_keys: list[list[str]] | None
    columns: list[ColumnConfig]
    checks: list[PredicateCheck] | None = None


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


def catalog_ident(value: str) -> str:
    """Normalize a catalog identifier."""
    out = "".join(c.lower() if c.isascii() and (c.isalnum() or c in "_-") else "_" for c in value)  # fmt: skip
    return out.strip("_") or "default"


class DeltaSinkOptions(_Model):
    """The Unity Catalog binding of a Delta sink; the keys are checked in validation."""

    catalog: str | None
    # `schema` shadows a BaseModel attribute, so the field is `schema_` and the key stays `schema`.
    schema_: str | None = Field(alias="schema")
    table: str | None


NO_ICEBERG = IcebergSinkOptions(catalog=None, namespace=None, table=None, location=None)  # fmt: skip


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
    delta: DeltaSinkOptions | None = None
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
    runner: str | None = None
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
        ColumnConfig(name=n, source=n, column_type=t, nullable=False)
        for n, t in derived
        if n is not None
    ]  # fmt: skip  # `source` keeps the name out of normalize_columns
    schema = entity.schema_.model_copy(
        update={"columns": [*entity.schema_.columns, *columns]}
    )
    return entity.model_copy(update={"schema_": schema})
