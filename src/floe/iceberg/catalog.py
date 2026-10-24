"""Polaris REST catalog access, ported from `io/write/iceberg/rest.rs` and `iceberg/schema.rs`."""

import os
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from urllib.parse import urlsplit

import duckdb
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import parse_transform
from pyiceberg.types import NestedField

from floe.config.storage import CatalogDefinition, StorageDefinition
from floe.config.types import SchemaEvolutionMode
from floe.errors import FloeRunError

# (column, transform) pairs, as in the entity's `sink.accepted.partition_spec`
PartitionFields = Sequence[tuple[str, str]]

_TRANSFORMS = frozenset({"identity", "year", "month", "day", "hour"})
# first id of a partition field, as in the Iceberg spec
_PARTITION_FIELD_ID_START = 1000
DUCKDB_CATALOG = "floe_catalog"


def transform_name(field: PartitionField) -> str:
    return str(field.transform)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType] -- PyIceberg leaves Transform unparametrized


@contextmanager
def run_error(context: str) -> Generator[None]:
    try:
        yield
    except Exception as e:  # PyIceberg has no common base exception
        raise FloeRunError(f"{context}: {e}") from e


def _expand_env_part(part: str, catalog: str) -> str:
    if not part.startswith("${"):
        return part
    if not part.endswith("}"):
        raise FloeRunError(
            f"rest iceberg catalog {catalog} credential has unclosed env placeholder"
        )
    name = part[2:-1]
    if not name or "{" in name or "}" in name:
        raise FloeRunError(
            f"rest iceberg catalog {catalog} credential has invalid env placeholder"
        )
    value = os.environ.get(name)
    if value is None:
        raise FloeRunError(
            f"rest iceberg catalog {catalog} credential references env var {name} which is not set"
        )
    return value


def _credential_props(catalog: str, credential: str) -> dict[str, str]:
    """`token:<v>` is a bearer token; `client_credentials:<id>:<secret>` and a plain value are OAuth2."""
    if "${" in credential:
        credential = ":".join(
            _expand_env_part(p, catalog) for p in credential.split(":")
        )
    if credential.startswith("token:"):
        return {"token": credential.removeprefix("token:")}
    return {"credential": credential.removeprefix("client_credentials:")}


def open_catalog(
    definition: CatalogDefinition, storage: StorageDefinition | None = None
) -> Catalog:
    """Credentials for S3 come from the environment; the catalog never vends them (as in Rust)."""
    props = {
        "type": "rest",
        "uri": definition.uri,
        "header.X-Iceberg-Access-Delegation": "",
    }
    if definition.warehouse is not None:
        props["warehouse"] = definition.warehouse
    if definition.credential is not None:
        props.update(
            _credential_props(definition.name, definition.credential.get_secret_value())
        )
    if definition.oauth2_server_uri is not None:
        props["oauth2-server-uri"] = definition.oauth2_server_uri
    if definition.scope is not None:
        props["scope"] = definition.scope
    if storage is not None:
        if storage.endpoint is not None:
            props["s3.endpoint"] = storage.endpoint
        if storage.region is not None:
            props["s3.region"] = storage.region
        if storage.path_style_access is not None:
            props["s3.force-virtual-addressing"] = str(
                not storage.path_style_access
            ).lower()
    with run_error("rest catalog init failed"):
        return load_catalog(definition.name, **props)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_options(options: dict[str, str]) -> str:
    return ", ".join(f"{k} {_sql_literal(v)}" for k, v in options.items())


def open_duckdb(
    definition: CatalogDefinition, storage: StorageDefinition | None = None
) -> duckdb.DuckDBPyConnection:
    """DuckDB with the catalog attached as `DUCKDB_CATALOG`; S3 credentials come from the AWS chain, as for PyIceberg."""
    auth: dict[str, str] = {}
    if definition.credential is not None:
        props = _credential_props(
            definition.name, definition.credential.get_secret_value()
        )
        if "token" in props:
            auth["TOKEN"] = props["token"]
        else:
            client_id, _, secret = props["credential"].partition(":")
            auth |= {
                "CLIENT_ID": client_id,
                "CLIENT_SECRET": secret,
                "OAUTH2_SERVER_URI": definition.oauth2_server_uri
                or f"{definition.uri.rstrip('/')}/v1/oauth/tokens",
            }
            if definition.scope is not None:
                auth["OAUTH2_SCOPE"] = definition.scope
    s3 = {"PROVIDER": "credential_chain"}
    if storage is not None:
        if storage.region is not None:
            s3["REGION"] = storage.region
        if storage.endpoint is not None:
            endpoint = urlsplit(storage.endpoint)
            s3["ENDPOINT"] = endpoint.netloc or endpoint.path
            s3["USE_SSL"] = str(endpoint.scheme != "http").lower()
        if storage.path_style_access:
            s3["URL_STYLE"] = "path"
    # no access delegation, as in `open_catalog`
    attach = {"TYPE": "iceberg", "ENDPOINT": definition.uri, "ACCESS_DELEGATION_MODE": "none"}  # fmt: skip
    with run_error("duckdb catalog attach failed"):
        # ponytail: DuckDB's default memory_limit and spill dir; set the ADR's limits when the run wires merges
        con = duckdb.connect()
        con.execute(f"CREATE SECRET (TYPE s3, {_sql_options(s3)})")
        if auth:
            con.execute(f"CREATE SECRET floe_auth (TYPE iceberg, {_sql_options(auth)})")  # fmt: skip
            attach["SECRET"] = "floe_auth"
        else:
            attach["AUTHORIZATION_TYPE"] = "none"
        warehouse = _sql_literal(definition.warehouse or "")
        con.execute(f"ATTACH {warehouse} AS {DUCKDB_CATALOG} ({_sql_options(attach)})")  # fmt: skip
    return con


def build_partition_spec(
    schema: Schema, fields: PartitionFields, entity: str
) -> PartitionSpec:
    """Bucket is rejected here until #703 (D12)."""
    spec_fields: list[PartitionField] = []
    for index, (column, transform) in enumerate(fields):
        column = column.strip()
        try:
            source = schema.find_field(column)
        except ValueError:
            raise FloeRunError(
                f"entity.name={entity} iceberg partition_spec column {column} was not found in runtime schema"
            ) from None
        transform = transform.strip().lower()
        if transform not in _TRANSFORMS:
            raise FloeRunError(
                f"entity.name={entity} iceberg partition_spec column {column} has unsupported runtime transform {transform}"
            )
        spec_fields.append(
            PartitionField(
                source_id=source.field_id,
                field_id=_PARTITION_FIELD_ID_START + index,
                transform=parse_transform(transform),
                name=column if transform == "identity" else f"{column}_{transform}",
            )
        )
    return PartitionSpec(*spec_fields)


def _describe_field(field: NestedField) -> str:
    required = "required" if field.required else "optional"
    return f"{field.name}:{field.field_type}:{required}"


def check_schema_matches(existing: Schema, expected: Schema, entity: str) -> None:
    """Rust has no Iceberg schema evolution: any difference fails (see the questions on #650)."""
    if len(existing.fields) != len(expected.fields):
        raise FloeRunError(
            f"entity.name={entity} iceberg schema evolution is not supported "
            f"(column count differs: existing={len(existing.fields)}, incoming={len(expected.fields)})"
        )
    for index, (old, new) in enumerate(zip(existing.fields, expected.fields)):
        if (old.name, old.required, old.field_type) != (
            new.name,
            new.required,
            new.field_type,
        ):
            raise FloeRunError(
                f"entity.name={entity} iceberg schema evolution is not supported "
                f"(column {index} differs: existing={_describe_field(old)} incoming={_describe_field(new)})"
            )


def additive_columns(
    existing: Schema, expected: Schema, entity: str
) -> list[NestedField]:
    """Rust's Delta rules (`merge/shared.rs`) for `add_columns`: new columns only."""
    incoming = {f.name: f for f in expected.fields}
    incompatible: list[str] = []
    for old in existing.fields:
        new = incoming.get(old.name)
        if new is None:
            incompatible.append(f"missing existing column {old.name}")
            continue
        if new.field_type != old.field_type:
            incompatible.append(
                f"column {old.name} type changed from {old.field_type} to {new.field_type}"
            )
        if old.required and not new.required:
            incompatible.append(
                f"column {old.name} nullability changed from non-nullable to nullable"
            )
    if incompatible:
        raise FloeRunError(
            f"entity.name={entity} iceberg schema evolution failed: add_columns supports additive changes only; "
            f"incompatible changes detected: {'; '.join(incompatible)}"
        )
    known = {f.name for f in existing.fields}
    return [f for f in expected.fields if f.name not in known]


def _describe_spec(spec: PartitionSpec) -> str:
    if not spec.fields:
        return "unpartitioned"
    return ",".join(
        f"{f.source_id}:{f.name}:{transform_name(f).capitalize()}" for f in spec.fields
    )


def _key(spec: PartitionSpec) -> list[tuple[int, str, str]]:
    return [(f.source_id, f.name, transform_name(f)) for f in spec.fields]


def check_partition_spec_matches(
    existing: PartitionSpec, expected: PartitionSpec | None, entity: str
) -> None:
    if expected is None:
        return
    if _key(existing) != _key(expected):
        raise FloeRunError(
            f"entity.name={entity} iceberg partition spec evolution is not supported "
            f"(existing={_describe_spec(existing)} incoming={_describe_spec(expected)})"
        )


def ensure_table(
    catalog: Catalog,
    namespace: str,
    name: str,
    schema: Schema,
    entity: str,
    location: str | None = None,
    partition_fields: PartitionFields | None = None,
    schema_evolution: SchemaEvolutionMode = "strict",
) -> Table:
    """Create the namespace and table if missing; otherwise check the table matches (Rust's append path).

    With `add_columns`, columns new in `schema` are added as optional (D13); partition changes stay rejected.
    """
    ident = (namespace,)
    with run_error("rest catalog namespace_exists check failed"):
        namespace_exists = catalog.namespace_exists(ident)
    if not namespace_exists:
        with run_error("rest catalog namespace create failed"):
            catalog.create_namespace(ident)
    identifier = (namespace, name)
    with run_error("rest catalog table_exists check failed"):
        exists = catalog.table_exists(identifier)
    if exists:
        with run_error("rest catalog load_table failed"):
            table = catalog.load_table(identifier)
        added: list[NestedField] = []
        if schema_evolution == "add_columns":
            added = additive_columns(table.schema(), schema, entity)
            # field ids of `schema` may differ from the table's once columns are added
            check_schema = table.schema()
        else:
            check_schema_matches(table.schema(), schema, entity)
            check_schema = schema
        check_partition_spec_matches(
            table.spec(),
            None
            if partition_fields is None
            else build_partition_spec(check_schema, partition_fields, entity),
            entity,
        )
        if added:
            with run_error("rest catalog update_schema failed"):
                with table.update_schema() as update:
                    for field in added:
                        update.add_column(field.name, field.field_type, required=False)
                table.refresh()
        return table
    spec = (
        None
        if partition_fields is None
        else build_partition_spec(schema, partition_fields, entity)
    )
    with run_error("rest catalog create_table failed"):
        return catalog.create_table(  # pyright: ignore[reportUnknownMemberType] -- PyIceberg's `schema` is `Schema | Unknown`
            identifier,
            schema,
            location=location,
            partition_spec=spec if spec is not None else PartitionSpec(),
        )
