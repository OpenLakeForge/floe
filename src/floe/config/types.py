"""Config models, ported from `crates/floe-core/src/config/types.rs` (entity-level fields)."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

IncrementalMode = Literal["none", "archive", "file", "row"]
PolicySeverity = Literal["warn", "reject", "abort"]


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
    schema_: object = Field(alias="schema")  # ponytail: raw until #714 parses it
    pii: object  # ponytail: raw (None when absent) until #714 parses it
