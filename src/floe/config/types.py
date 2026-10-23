"""Config models, ported from `crates/floe-core/src/config/types.rs` (entity-level fields)."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from floe.pii import PiiColumn

IncrementalMode = Literal["none", "archive", "file", "row"]
PolicySeverity = Literal["warn", "reject", "abort"]
SchemaEvolutionMode = Literal["strict", "add_columns"]
SchemaEvolutionIncompatibleAction = Literal["fail"]


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


class PiiConfig(_Model):
    columns: list[PiiColumn]


class EntityConfig(_Model):
    name: str
    id: str
    metadata: EntityMetadata | None
    domain: str | None
    incremental_mode: IncrementalMode
    state: EntityStateConfig | None
    source: object  # ponytail: raw mapping until #710 parses it
    sink: object  # ponytail: raw mapping until #710 parses it
    policy: PolicyConfig
    # `schema` shadows a BaseModel attribute, so the field is `schema_` and the key stays `schema`.
    schema_: SchemaConfig = Field(alias="schema")
    pii: PiiConfig | None
