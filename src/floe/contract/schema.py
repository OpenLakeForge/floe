"""One table's records → the business-owned identity and schema keys of a Floe entity.

Pure: no I/O. Problems are appended to `errors`, each with its Excel cell.
"""

from typing import Any

from floe.checks.cast import decimal_type
from floe.config.validate import _TYPE_NAMES, _canonical  # type: ignore[reportPrivateUsage]  # one type list, not two
from floe.contract.workbook import ColumnRow, TableRow


def _text(value: str | None) -> str | None:
    return value if value is not None and value.strip() else None


def _type_error(column: ColumnRow) -> str | None:
    kind = _canonical(column.target_type)
    try:
        exact = decimal_type(column.target_type)
    except ValueError as err:
        return f"is invalid: {err}"
    if kind == "decimal":
        return "needs a precision and scale, e.g. decimal(18,2)"
    if exact is None and kind not in _TYPE_NAMES:
        return "is not a Floe type"
    return None


def compile_schema(
    domain: str,
    table: TableRow,
    columns: tuple[ColumnRow, ...] | list[ColumnRow],
    errors: list[str],
) -> dict[str, Any]:
    """Entity keys the business owns: name, domain, metadata, source.resource, schema."""
    rows = [c for c in columns if c.target_table == table.target_table]
    if not rows:
        errors.append(f"{table.refs['target_table']}: {table.target_table} has no Columns rows")  # fmt: skip
    primary_key = [c.target_column for c in rows if c.primary_key]
    keys: dict[tuple[str, str], list[ColumnRow]] = {}  # first appearance wins the order
    for c in rows:
        if (label := _text(c.unique_group)) is not None:
            if c.unique:
                errors.append(
                    f"{c.refs['unique']}: {c.target_column} is already in unique_group {label} "
                    f"({c.refs['unique_group']}); drop unique"
                )
            keys.setdefault(("group", label), []).append(c)
        elif c.unique:
            keys[("column", c.target_column)] = [c]
    unique_keys: list[list[str]] = []
    seen = {frozenset(primary_key): "the primary key"} if primary_key else {}
    for members in keys.values():
        names = [c.target_column for c in members]
        ref = members[0].refs["unique" if members[0].unique else "unique_group"]
        if (same := seen.get(frozenset(names))) is not None:
            errors.append(f"{ref}: unique key {names} is identical to {same}")
        else:
            seen[frozenset(names)] = f"unique key {names}"
        unique_keys.append(names)
    schema_columns: list[dict[str, Any]] = []
    for c in rows:
        if (problem := _type_error(c)) is not None:
            errors.append(
                f"{c.refs['target_type']}: target_type {c.target_type} {problem}"
            )
        entry: dict[str, Any] = {"name": c.target_column, "type": c.target_type}
        if (source := _text(c.source_column)) is not None and source != c.target_column:
            entry["source"] = source
        entry["nullable"] = not (c.not_null or c.primary_key)
        schema_columns.append(entry)
    schema: dict[str, Any] = {"columns": schema_columns}
    if primary_key:
        schema["primary_key"] = primary_key
    if unique_keys:
        schema["unique_keys"] = unique_keys
    out: dict[str, Any] = {"name": table.target_table, "domain": domain}
    metadata = {
        k: v
        for k, v in (
            ("owner", _text(table.owner)),
            ("description", _text(table.description)),
        )
        if v is not None
    }
    if metadata:
        out["metadata"] = metadata
    if (resource := _text(table.source_dataset)) is not None:
        out["source"] = {"resource": resource}
    out["schema"] = schema
    return out
