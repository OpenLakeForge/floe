"""Polaris REST catalog access, ported from `io/write/iceberg/rest.rs` and `iceberg/schema.rs`."""

from collections.abc import Generator, Sequence
from contextlib import contextmanager

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import parse_transform
from pyiceberg.types import NestedField

from floe.config.storage import CatalogDefinition, StorageDefinition
from floe.errors import FloeRunError

# (column, transform) pairs, as in the entity's `sink.accepted.partition_spec`
PartitionFields = Sequence[tuple[str, str]]

_TRANSFORMS = frozenset({"identity", "year", "month", "day", "hour"})
# first id of a partition field, as in the Iceberg spec
_PARTITION_FIELD_ID_START = 1000


def transform_name(field: PartitionField) -> str:
    return str(field.transform)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType] -- PyIceberg leaves Transform unparametrized


@contextmanager
def _run_error(context: str) -> Generator[None]:
    try:
        yield
    except Exception as e:  # PyIceberg has no common base exception
        raise FloeRunError(f"{context}: {e}") from e


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
        props["credential"] = definition.credential.get_secret_value()
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
    with _run_error("rest catalog init failed"):
        return load_catalog(definition.name, **props)


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
) -> Table:
    """Create the namespace and table if missing; otherwise check the table matches (Rust's append path)."""
    spec = (
        None
        if partition_fields is None
        else build_partition_spec(schema, partition_fields, entity)
    )
    with _run_error("rest catalog namespace create failed"):
        catalog.create_namespace_if_not_exists(namespace)
    identifier = (namespace, name)
    with _run_error("rest catalog table_exists check failed"):
        exists = catalog.table_exists(identifier)
    if exists:
        with _run_error("rest catalog load_table failed"):
            table = catalog.load_table(identifier)
        check_schema_matches(table.schema(), schema, entity)
        check_partition_spec_matches(table.spec(), spec, entity)
        return table
    with _run_error("rest catalog create_table failed"):
        return catalog.create_table(  # pyright: ignore[reportUnknownMemberType] -- PyIceberg's `schema` is `Schema | Unknown`
            identifier,
            schema,
            location=location,
            partition_spec=spec if spec is not None else PartitionSpec(),
        )
