"""Registers a Delta table as an external table in Unity Catalog (#1314), over the REST API with `urllib` only."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportMissingTypeStubs=false
# ^ pyarrow and deltalake ship no type stubs

import json
import os
import re
import urllib.error
import urllib.request
from typing import Any

import pyarrow as pa
from deltalake import DeltaTable

from floe.config.storage import CatalogDefinition
from floe.errors import FloeRunError

_API = "/api/2.1/unity-catalog"
_TIMEOUT_S = 30
_ENV_REF = re.compile(r"\$\{([^{}]+)\}")
_SIMPLE = {"int8": ("tinyint", "BYTE"), "int16": ("smallint", "SHORT"), "int32": ("int", "INT"), "int64": ("bigint", "LONG"), "float": ("float", "FLOAT"), "double": ("double", "DOUBLE"), "bool": ("boolean", "BOOLEAN"), "date32[day]": ("date", "DATE"), "binary": ("binary", "BINARY")}  # fmt: skip


def spark_type(kind: pa.DataType) -> tuple[str, str]:
    """`(type_text, type_name)` of an Arrow type; a type Floe has no column type for is refused."""
    if kind in (pa.string(), pa.large_string(), pa.string_view()):
        return "string", "STRING"
    if pa.types.is_timestamp(kind):
        return (
            ("timestamp", "TIMESTAMP")
            if kind.tz
            else ("timestamp_ntz", "TIMESTAMP_NTZ")
        )
    if pa.types.is_decimal(kind):
        return f"decimal({kind.precision},{kind.scale})", "DECIMAL"
    if (found := _SIMPLE.get(str(kind))) is None:
        raise FloeRunError(f"unity catalog registration cannot map the Arrow type {kind} to a Spark type", code="run_not_supported")  # fmt: skip
    return found


def columns(table: DeltaTable) -> list[dict[str, Any]]:
    """The `columns` of the create request, from the table's own schema (so a widened unsigned integer shows as the signed type)."""
    out: list[dict[str, Any]] = []
    for i, f in enumerate(pa.schema(table.schema().to_arrow())):
        text, name = spark_type(f.type)
        spark = {"INT": "integer", "DECIMAL": text}.get(
            name, name.lower()
        )  # the JSON type names of Spark's `DataType.json`
        column: dict[str, Any] = {"name": f.name, "type_text": text, "type_name": name, "type_json": json.dumps({"name": f.name, "type": spark, "nullable": f.nullable, "metadata": {}}), "position": i, "nullable": f.nullable}  # fmt: skip
        if pa.types.is_decimal(f.type):
            column |= {"type_precision": f.type.precision, "type_scale": f.type.scale}
        out.append(column)
    return out


def _token(definition: CatalogDefinition) -> str:
    """`token` is an env reference like `${DATABRICKS_TOKEN}`, expanded here and never logged."""
    raw = definition.token.get_secret_value() if definition.token else ""
    if (ref := _ENV_REF.fullmatch(raw)) is None:
        return raw
    value = os.environ.get(ref[1])
    if not value:
        raise FloeRunError(f"unity catalog {definition.name} token references env var {ref[1]} which is not set", code="run_invalid_setup")  # fmt: skip
    return value


def _call(definition: CatalogDefinition, token: str, method: str, path: str, body: dict[str, Any] | None = None) -> tuple[int, dict[str, Any]]:  # fmt: skip
    """One request; 2xx, 404 and 409 come back as `(status, json)`, anything else is a `run_catalog_error` without the token."""
    request = urllib.request.Request(f"{(definition.host or '').rstrip('/')}{_API}{path}", method=method, data=None if body is None else json.dumps(body).encode(), headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})  # fmt: skip
    try:
        with urllib.request.urlopen(request, timeout=_TIMEOUT_S) as response:
            return response.status, json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as err:
        try:
            detail = json.loads(err.read() or b"{}")
        except ValueError:
            detail = {}
        if err.code in (404, 409):
            return err.code, detail
        raise FloeRunError(f"unity catalog {definition.name} {method} {path} failed with HTTP {err.code} ({detail.get('error_code')}: {detail.get('message')})", code="run_catalog_error") from None  # fmt: skip
    except (urllib.error.URLError, OSError, ValueError) as err:
        raise FloeRunError(f"unity catalog {definition.name} {method} {path} failed: {getattr(err, 'reason', err)}", code="run_catalog_error") from None  # fmt: skip


def register(
    definition: CatalogDefinition,
    schema: str,
    name: str,
    location: str,
    table: DeltaTable,
) -> None:
    """Make `location` visible as the external Delta table `<catalog>.<schema>.<name>`; idempotent, so it runs after every commit.

    An existing table must be the external Delta table at `location`; its columns are never updated, since Unity reads
    an external table's schema from the Delta log. A failure leaves the Delta commit in place and a retry registers it.
    """
    token, catalog = _token(definition), definition.catalog
    full = f"{catalog}.{schema}.{name}"
    who = f"unity catalog {definition.name} table {full}"
    status, found = _call(definition, token, "GET", f"/tables/{full}")
    if status == 404:
        if definition.create_schema_if_missing and _call(definition, token, "GET", f"/schemas/{catalog}.{schema}")[0] == 404:  # fmt: skip
            _call(definition, token, "POST", "/schemas", {"name": schema, "catalog_name": catalog})  # 409: a racing run created it  # fmt: skip
        body = {"name": name, "catalog_name": catalog, "schema_name": schema, "table_type": "EXTERNAL", "data_source_format": "DELTA", "storage_location": location, "properties": {}, "columns": columns(table)}  # `properties` is required by the API  # fmt: skip
        status, found = _call(definition, token, "POST", "/tables", body)
        if status == 409:  # a racing run registered it first: check what it registered
            status, found = _call(definition, token, "GET", f"/tables/{full}")
    if status != 200:
        raise FloeRunError(f"{who} could not be registered or read back (HTTP {status}: {found.get('error_code')})", code="run_catalog_error")  # fmt: skip
    if (found.get("table_type"), found.get("data_source_format")) != (
        "EXTERNAL",
        "DELTA",
    ):
        raise FloeRunError(f"{who} exists but is {found.get('table_type')} {found.get('data_source_format')}, not an EXTERNAL DELTA table", code="run_state_conflict")  # fmt: skip
    if (found.get("storage_location") or "").rstrip("/") != location.rstrip("/"):
        raise FloeRunError(f"{who} exists with storage_location {found.get('storage_location')}, not {location}", code="run_state_conflict")  # fmt: skip
