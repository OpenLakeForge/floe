"""Entity validation, ported from `config/validate.rs` (`validate_sink`, `validate_schema` and their helpers)."""

from floe import __version__
from floe.checks.normalize import output_names
from floe.config.project import RootConfig, config_version
from floe.config.types import EntityConfig, WriteMode
from floe.config.yaml import rust_debug
from floe.errors import FloeConfigError
from floe.pii import token_n
from floe.read.json import parse_selector as parse_json_selector
from floe.read.xml import parse_selector

MIN_VERSION = (0, 1)
QUALIFIED_VERSION = (0, 3)
# D21: root.version is the minimum Floe release (major.minor) the config targets.
FLOE_VERSION = config_version(".".join(__version__.split(".")[:2]))
MAX_JSON_COLUMNS = 1024
CAST_MODES = ("strict", "coerce")
SOURCE_FORMATS = ("csv", "tsv", "parquet", "json", "orc", "avro", "xlsx", "xml")
REMOTE_URIS = tuple(f"{s}://" for s in ("s3", "gs", "gcs", "abfs", "abfss", "az"))
STORAGE_TYPES = ("local", "s3", "adls", "gcs")
NORMALIZE_STRATEGIES = ("snake_case", "lower", "camel_case", "none")
MISMATCH_MISSING = ("reject_file", "fill_nulls")
MISMATCH_EXTRA = ("reject_file", "ignore")
# D10 (#619): Rust 0.7 also lists `time`; it is not supported in 0.8.
COLUMN_TYPES = ("string", "number", "boolean", "datetime", "date")
SINK_FORMATS = ("parquet", "iceberg")
PARQUET_COMPRESSIONS = ("snappy", "gzip", "zstd", "uncompressed")
PARTITION_TRANSFORMS = ("identity", "year", "month", "day", "hour")
# D2 (#619): Iceberg accepts the merge modes; Rust 0.7 allows them for Delta (D1) and DuckDB (D1) only.
SINK_WRITE_MODES: dict[str, tuple[WriteMode, ...]] = {
    "parquet": ("overwrite", "append"),
    "iceberg": ("overwrite", "append", "merge_scd1", "merge_scd2"),
}
SCD2_DEFAULTS = {
    "current_flag_column": "__floe_is_current",
    "valid_from_column": "__floe_valid_from",
    "valid_to_column": "__floe_valid_to",
}
# Same normalisation as `checks/cast.py`, so every accepted type casts.
_TYPE_NAMES = {
    *("string", "str", "text", "boolean", "bool", "datetime", "timestamp", "date"),
    *("number", "float", "float32", "float64", "double", "decimal", "int"),
    *("int8", "int16", "int32", "int64", "integer", "long"),
    *("uint8", "uint16", "uint32", "uint64"),
}  # fmt: skip


def _canonical(value: str) -> str:
    # D7 (#584 S1, S2): no trim, unlike Rust's `normalize_value`; the run-time paths never trimmed.
    return value.lower().replace("-", "").replace("_", "")


def _fail(entity: EntityConfig, message: str) -> FloeConfigError:
    return FloeConfigError(f"entity.name={entity.name} {message}")


def _check_primary_key(entity: EntityConfig) -> None:
    primary_key = entity.schema_.primary_key
    if primary_key is None:
        return
    if not primary_key:
        raise _fail(entity, "schema.primary_key must not be empty")
    columns = {c.name: c for c in reversed(entity.schema_.columns)}
    seen: set[str] = set()
    for index, name in enumerate(primary_key):
        value = name.strip()
        if not value:
            raise _fail(entity, f"schema.primary_key[{index}] must not be empty")
        if value in seen:
            raise _fail(entity, f"schema.primary_key has duplicate column {value}")
        seen.add(value)
        if value not in columns:
            raise _fail(
                entity,
                f"schema.primary_key[{index}]={value} references unknown schema column",
            )
        if columns[value].nullable is True:
            raise _fail(
                entity, f"schema.primary_key column {value} cannot set nullable=true"
            )


def _check_unique_keys(entity: EntityConfig) -> None:
    # ponytail: Rust's override and dedup warnings are not ported; they are not errors.
    unique_keys = entity.schema_.unique_keys
    if unique_keys is None:
        return
    if not unique_keys:
        raise _fail(entity, "schema.unique_keys must not be empty")
    names = {c.name for c in entity.schema_.columns}
    for index, key in enumerate(unique_keys):
        where = f"schema.unique_keys[{index}]"
        if not key:
            raise _fail(entity, f"{where} must not be empty")
        seen: set[str] = set()
        for column_index, name in enumerate(key):
            value = name.strip()
            if not value:
                raise _fail(entity, f"{where}[{column_index}] must not be empty")
            if value not in names:
                raise _fail(
                    entity,
                    f"{where}[{column_index}]={value} references unknown schema column",
                )
            if value in seen:
                raise _fail(entity, f"{where} has duplicate column {value}")
            seen.add(value)


def _check_evolution(entity: EntityConfig, version: str) -> None:
    evolution = entity.schema_.schema_evolution
    if evolution is None:
        return
    if config_version(version) < (0, 2):
        raise _fail(entity, 'schema.schema_evolution requires root.version >= "0.2"')
    # D13 (#619): add_columns is accepted on Iceberg; Rust allows only Delta, which D1 drops.
    if evolution.mode == "add_columns" and entity.sink.accepted.format != "iceberg":
        raise _fail(
            entity,
            f"schema.schema_evolution.mode={evolution.mode} requires "
            "sink.accepted.format=delta (Delta-only additive schema evolution in this phase)",
        )


def _merge_list(entity: EntityConfig, field: str, values: list[str]) -> None:
    names = {c.name for c in entity.schema_.columns}
    primary_key = {c.strip() for c in entity.schema_.primary_key or []}
    where = f"sink.accepted.merge.{field}"
    seen: set[str] = set()
    for index, raw in enumerate(values):
        value = raw.strip()
        if not value:
            raise _fail(entity, f"{where}[{index}] must not be empty")
        if value not in names:
            raise _fail(
                entity, f"{where}[{index}]={value} references unknown schema column"
            )
        if value in seen:
            raise _fail(entity, f"{where} has duplicate column {value}")
        seen.add(value)
    for index, raw in enumerate(values):
        if raw.strip() in primary_key:
            raise _fail(
                entity,
                f"{where}[{index}]={raw.strip()} cannot reference schema.primary_key column",
            )


def _check_merge(entity: EntityConfig) -> None:
    merge = entity.sink.accepted.merge
    if merge is None:
        return
    mode = entity.sink.write_mode
    if not any(
        m.startswith("merge") for m in SINK_WRITE_MODES[entity.sink.accepted.format]
    ):
        raise _fail(
            entity,
            "sink.accepted.merge is only supported when sink.accepted.format supports merge (e.g. delta)",
        )
    if mode not in ("merge_scd1", "merge_scd2"):
        raise _fail(
            entity,
            "sink.accepted.merge is only supported with sink.write_mode=merge_scd1 or merge_scd2",
        )
    for field in ("ignore_columns", "compare_columns"):
        values = getattr(merge, field)
        if values is not None:
            _merge_list(entity, field, values)
    if merge.scd2 is None:
        return
    if mode != "merge_scd2":
        raise _fail(
            entity,
            "sink.accepted.merge.scd2 is only supported with sink.write_mode=merge_scd2",
        )
    nc = (
        entity.schema_.normalize_columns
    )  # raw strategy: an unsupported one is validate_schema's error
    strategy = (nc.strategy or "snake_case") if nc and nc.enabled else None
    names = set(output_names(entity.schema_.columns, strategy))
    resolved = {
        field: (default if (v := getattr(merge.scd2, field)) is None else v).strip()
        for field, default in SCD2_DEFAULTS.items()
    }
    for field, value in resolved.items():
        if not value:
            raise _fail(entity, f"sink.accepted.merge.scd2.{field} must not be empty")
        if value in names:
            raise _fail(
                entity,
                f"sink.accepted.merge.scd2.{field}={value} collides with schema column name",
            )
    if len(set(resolved.values())) != len(resolved):
        raise _fail(entity, "sink.accepted.merge.scd2 column names must be unique")


def _check_write_mode(entity: EntityConfig) -> None:
    mode, fmt = entity.sink.write_mode, entity.sink.accepted.format
    if mode not in SINK_WRITE_MODES[fmt]:
        raise _fail(
            entity,
            f"sink.write_mode={mode} is not supported by sink.accepted.format={fmt}",
        )
    if mode in ("merge_scd1", "merge_scd2"):
        primary_key = entity.schema_.primary_key
        if primary_key is None:
            raise _fail(entity, f"sink.write_mode={mode} requires schema.primary_key")
        if not primary_key:
            raise _fail(
                entity, f"sink.write_mode={mode} requires non-empty schema.primary_key"
            )
    _check_merge(entity)


def _check_partitioning(entity: EntityConfig) -> None:
    accepted = entity.sink.accepted
    if accepted.partition_by is not None and accepted.partition_spec is not None:
        raise _fail(
            entity,
            "sink.accepted.partition_by and sink.accepted.partition_spec are mutually exclusive",
        )
    # ponytail: `partition_by` is Delta-only (D1), so its per-column checks are unreachable.
    if accepted.partition_by is not None:
        raise _fail(
            entity,
            "sink.accepted.partition_by is only supported for sink.accepted.format=delta",
        )
    spec = accepted.partition_spec
    if spec is None:
        return
    where = "sink.accepted.partition_spec"
    if accepted.format != "iceberg":
        raise _fail(
            entity, f"{where} is only supported for sink.accepted.format=iceberg"
        )
    if not spec:
        raise _fail(entity, f"{where} must not be empty")
    names = {c.name for c in entity.schema_.columns}
    seen: set[tuple[str, str]] = set()
    for index, field in enumerate(spec):
        column = field.column.strip()
        if not column:
            raise _fail(entity, f"{where}[{index}].column must not be empty")
        if column not in names:
            raise _fail(
                entity,
                f"{where}[{index}].column={column} references unknown schema column",
            )
        transform = field.transform.strip().lower()
        if transform not in PARTITION_TRANSFORMS:
            raise _fail(
                entity,
                f"{where}[{index}].transform={field.transform} is unsupported "
                f"(allowed: {', '.join(PARTITION_TRANSFORMS)})",
            )
        if (column, transform) in seen:
            raise _fail(entity, f"{where} has duplicate entry ({column}, {transform})")
        seen.add((column, transform))


def _storage_ref(
    config: RootConfig,
    name: str | None,
    field: str,
    who: str,
    profiled: bool = False,
) -> str:
    """`resolve_name` then `validate_reference`; `who` is the `entity.name=X ` prefix, empty for the report."""
    storages = config.storages
    if name is None:
        name = (storages and storages.default) or "local"
    # ponytail: a profile can replace the storages (#635); skip the reference check until it lands.
    if not profiled:
        if storages is None and name != "local":
            raise FloeConfigError(
                f"{who}{field} references unknown storage {name} (no storages block)"
            )
        if storages and name not in {d.name for d in storages.definitions}:
            raise FloeConfigError(f"{who}{field} references unknown storage {name}")
    return name


def _storage_type(config: RootConfig, name: str) -> str:
    definitions = config.storages.definitions if config.storages else []
    return next((d.type for d in definitions if d.name == name), "local")


def _check_iceberg(
    entity: EntityConfig, config: RootConfig, accepted_storage: str
) -> None:
    accepted = entity.sink.accepted
    iceberg = accepted.iceberg
    if iceberg is None:
        return
    if accepted.format != "iceberg":
        raise _fail(
            entity,
            "sink.accepted.iceberg is only supported for sink.accepted.format=iceberg",
        )
    catalogs = config.catalogs
    name = (
        iceberg.catalog
        if iceberg.catalog is not None
        else catalogs and catalogs.default
    )
    if name is None:
        raise _fail(
            entity,
            "sink.accepted.iceberg.catalog is required (or set catalogs.default)",
        )
    definitions = catalogs.definitions if catalogs else []
    definition = next((c for c in definitions if c.name == name), None)
    if definition is None:
        raise _fail(
            entity, f"sink.accepted.iceberg.catalog references unknown catalog {name}"
        )
    # Unity and Glue are rejected at parse (D1); the registry checked `warehouse_storage`.
    if definition.warehouse_storage is None:
        kind = _storage_type(config, accepted_storage)
        if kind not in ("s3", "gcs"):
            raise _fail(
                entity,
                f"sink.accepted.iceberg.catalog requires sink.accepted storage type s3 or gcs (got {kind})",
            )


def _check_sink(entity: EntityConfig, config: RootConfig, profiled: bool) -> None:
    sink, accepted = entity.sink, entity.sink.accepted
    if accepted.format in ("delta", "duckdb"):  # D1 (#619): reported first
        raise _fail(
            entity, f"sink.accepted.format={accepted.format} is not supported in 0.8"
        )
    if accepted.format not in SINK_FORMATS:
        raise _fail(entity, f"sink.accepted.format={accepted.format} is unsupported")
    options = accepted.options
    if options is not None and accepted.format == "parquet":
        if options.compression not in (None, *PARQUET_COMPRESSIONS):
            raise _fail(
                entity,
                f"sink.accepted.options.compression={options.compression} is unsupported "
                f"(allowed: {', '.join(PARQUET_COMPRESSIONS)})",
            )
        for field in ("row_group_size", "max_size_per_file"):
            if getattr(options, field) == 0:
                raise _fail(
                    entity, f"sink.accepted.options.{field} must be greater than 0"
                )
    if entity.policy.severity == "reject" and sink.rejected is None:
        raise _fail(entity, "sink.rejected is required when policy.severity=reject")
    if sink.rejected is not None and sink.rejected.format != "csv":
        raise _fail(
            entity, f"sink.rejected.format={sink.rejected.format} is unsupported"
        )
    who = f"entity.name={entity.name} "
    accepted_storage = _storage_ref(
        config, accepted.storage, "sink.accepted.storage", who, profiled
    )
    # ponytail: a profile can add catalogs and storages, which land with #635; skip the binding.
    if not profiled:
        _check_iceberg(entity, config, accepted_storage)
    _check_partitioning(entity)
    _check_write_mode(entity)
    if sink.rejected is not None:
        _storage_ref(
            config, sink.rejected.storage, "sink.rejected.storage", who, profiled
        )
    archive = sink.archive
    source = _storage_ref(
        config, entity.source.storage, "source.storage", who, profiled
    )
    if archive is not None and archive.storage not in (None, source):
        raise _fail(
            entity, f"sink.archive.storage must match source.storage ({source})"
        )


def validate_schema(entity: EntityConfig, version: str) -> None:
    schema = entity.schema_
    if entity.source.format == "json" and len(schema.columns) > MAX_JSON_COLUMNS:
        raise _fail(
            entity,
            f"schema.columns has {len(schema.columns)} entries which exceeds "
            f"the JSON selector limit of {MAX_JSON_COLUMNS}",
        )
    strategy = schema.normalize_columns and schema.normalize_columns.strategy
    if strategy is not None and _canonical(strategy) not in {
        _canonical(s) for s in NORMALIZE_STRATEGIES
    }:
        raise _fail(
            entity,
            f"schema.normalize_columns.strategy={strategy} is unsupported "
            f"(allowed: {', '.join(NORMALIZE_STRATEGIES)})",
        )
    mismatch = schema.mismatch
    if mismatch is not None:
        for field, policy, allowed in (
            ("missing_columns", mismatch.missing_columns, MISMATCH_MISSING),
            ("extra_columns", mismatch.extra_columns, MISMATCH_EXTRA),
        ):
            if policy is not None and policy not in allowed:
                raise _fail(
                    entity,
                    f"schema.mismatch.{field}={policy} is unsupported "
                    f"(allowed: {', '.join(allowed)})",
                )
    for index, column in enumerate(schema.columns):
        if _canonical(column.column_type) not in _TYPE_NAMES:
            raise _fail(
                entity,
                f"schema.columns[{index}].type={column.column_type} is unsupported "
                f"for column {column.name} (allowed: {', '.join(COLUMN_TYPES)})",
            )
        parse = {"xml": parse_selector, "json": parse_json_selector}.get(
            entity.source.format
        )
        if parse:
            selector = column.source if column.source is not None else column.name
            try:
                parse(selector)
            except ValueError as err:
                raise _fail(
                    entity,
                    f"schema.columns[{index}].source={selector} is invalid: {err}",
                ) from err
    _check_primary_key(entity)
    _check_unique_keys(entity)
    _check_evolution(entity, version)


def _check_source(entity: EntityConfig, config: RootConfig, profiled: bool) -> None:
    source = entity.source
    if source.format == "fixed":  # D1 (#619): fixed-width is dropped
        raise _fail(entity, f"source.format={source.format} is not supported in 0.8")
    if source.format not in SOURCE_FORMATS:
        raise _fail(entity, f"source.format={source.format} is unsupported")
    if source.cast_mode not in CAST_MODES:
        raise _fail(
            entity,
            f"source.cast_mode={source.cast_mode} is unsupported "
            f"(allowed: {', '.join(CAST_MODES)})",
        )
    who = f"entity.name={entity.name} "
    _storage_ref(config, source.storage, "source.storage", who, profiled)
    options = source.options
    mode = options.json_mode or "array"
    if source.format == "json" and mode not in ("array", "ndjson"):
        raise _fail(
            entity,
            f"source.options.json_mode={mode} is unsupported (allowed: array, ndjson)",
        )
    if source.format == "xlsx":
        sheet, header_row = options.sheet, options.header_row
        if sheet is not None and not sheet.strip():
            raise _fail(entity, "source.options.sheet must not be empty")
        header_row = 1 if header_row is None else header_row
        if header_row == 0:
            raise _fail(entity, "source.options.header_row must be greater than 0")
        data_row = header_row + 1 if options.data_row is None else options.data_row
        if data_row <= header_row:
            raise _fail(
                entity,
                "source.options.data_row must be greater than source.options.header_row",
            )
    if source.format == "xml":
        if not (options.row_tag or "").strip():
            raise _fail(entity, "source.options.row_tag is required for xml input")
        for field in ("namespace", "value_tag"):
            value = getattr(options, field)
            if value is not None and not value.strip():
                raise _fail(entity, f"source.options.{field} must not be empty")
    # D7 (#584 S3, S4): Rust 0.7 only rejects these when the reader opens the file.
    if source.format in ("csv", "tsv"):
        separator, encoding = options.separator or "", options.encoding or "utf8"
        if len(separator.encode()) != 1:
            raise _fail(
                entity, f"separator must be a single byte, got {rust_debug(separator)}"
            )
        if _canonical(encoding) not in ("utf8", "lossyutf8"):
            raise _fail(entity, f"unsupported encoding: {encoding}")


def _check_state(entity: EntityConfig) -> None:
    state, archive = entity.state, entity.sink.archive
    if state and state.path is not None and not state.path.strip():
        raise _fail(entity, "entity.state.path must not be empty")
    mode = entity.incremental_mode
    if mode == "archive" and archive is None:
        raise _fail(entity, "incremental_mode=archive requires sink.archive")
    # ponytail: Rust's deprecation warning for `sink.archive` without a mode is not ported.
    if archive is not None and mode in ("file", "row"):
        raise _fail(
            entity,
            f"sink.archive conflicts with incremental_mode={mode}; "
            "use incremental_mode=archive or remove sink.archive",
        )


def _pii_error(entity: EntityConfig, name: str, message: str) -> FloeConfigError:
    return _fail(entity, f"pii.columns[name={name}]{message}")


def _check_pii(entity: EntityConfig) -> None:
    pii = entity.pii
    if pii is None:
        return
    if entity.policy.severity == "abort":
        raise _fail(
            entity,
            "pii: masking is not applied when policy.severity=abort because the raw "
            "file is written to sink.rejected without DataFrame processing",
        )
    if entity.sink.archive is not None:
        raise _fail(
            entity,
            "pii: sink.archive copies the original unmasked source file to the archive "
            "sink; remove sink.archive or disable pii masking",
        )
    schema = entity.schema_
    mismatch = schema.mismatch
    for field, policy, fix in (
        ("missing_columns", mismatch and mismatch.missing_columns, "fill_nulls"),
        ("extra_columns", mismatch and mismatch.extra_columns, "ignore"),
    ):
        if policy == "reject_file":
            raise _fail(
                entity,
                f"pii: schema.mismatch.{field}=reject_file writes the raw unmasked file "
                "to sink.rejected before PII masking can be applied; "
                f"use {field}={fix} instead",
            )
    columns = {c.name: c for c in schema.columns}
    primary_key = set(schema.primary_key or [])
    mode = entity.sink.write_mode
    unique = primary_key | {c for key in schema.unique_keys or [] for c in key}
    if (
        schema.unique_keys is None
    ):  # legacy `unique: true` is ignored once `unique_keys` is set
        unique |= {c.name for c in schema.columns if c.unique is True}
    seen: set[str] = set()
    for col in pii.columns:
        name, strategy = col.name, col.strategy
        label = strategy.capitalize()  # Rust's `{:?}` of the enum
        if name in seen:
            raise _fail(entity, f"pii.columns: column name {name} is duplicated")
        seen.add(name)
        if name not in columns:
            raise _fail(entity, f"pii.columns: {name} is not an unknown schema column")
        if strategy == "tokenize":
            raise _pii_error(entity, name, ".strategy=tokenize is not yet supported")
        declared = columns[name]
        if strategy in ("hash", "redact", "mask") and _canonical(
            declared.column_type
        ) not in ("string", "str", "text"):
            raise _pii_error(
                entity,
                name,
                f".strategy={label} can only be applied to string columns "
                f"(declared type={declared.column_type}); "
                "use strategy=nullify or strategy=drop instead",
            )
        if strategy == "nullify" and declared.nullable is False:
            raise _pii_error(
                entity,
                name,
                ".strategy=nullify cannot be applied to a nullable=false column",
            )
        if strategy == "drop" and name in primary_key:
            raise _pii_error(
                entity, name, ".strategy=drop cannot be applied to a primary_key column"
            )
        if name in primary_key and mode in ("merge_scd1", "merge_scd2"):
            raise _pii_error(
                entity,
                name,
                f": PII strategy cannot be applied to a primary_key column with write_mode={mode}: "
                "masking the merge key corrupts the merge predicate",
            )
        if name in unique and strategy in ("drop", "nullify", "redact", "mask"):
            raise _pii_error(
                entity,
                name,
                f".strategy={label} cannot be applied to a unique-key column: the strategy "
                "collapses or removes values, violating uniqueness constraints",
            )
        if name in unique and strategy == "hash":
            raise _pii_error(
                entity,
                name,
                ".strategy=hash cannot be applied to a unique-key column: uniqueness checks "
                "record raw values in the run report before masking, leaking unmasked PII",
            )
        # ponytail: Rust's warning for an unkeyed `hash` is not ported.
        fmt = entity.sink.accepted.format
        if strategy == "drop" and fmt in ("iceberg", "delta"):
            raise _pii_error(
                entity,
                name,
                f".strategy=drop is not supported with format={fmt} "
                "because the sink enforces the full declared schema",
            )
        if strategy == "mask":
            pattern = col.mask_pattern
            if pattern is None:
                raise _pii_error(entity, name, ".strategy=mask requires mask_pattern")
            if not (token_n(pattern, "{last") or token_n(pattern, "{first")):
                raise _pii_error(
                    entity,
                    name,
                    ".mask_pattern must contain at least one token ({firstN} or {lastN})",
                )


def _check_storages(config: RootConfig) -> None:
    storages, catalogs = config.storages, config.catalogs
    names: set[str] = set()
    if storages is not None:
        if not storages.definitions:
            raise FloeConfigError("storages.definitions must not be empty")
        for d in storages.definitions:
            if d.type not in STORAGE_TYPES:
                raise FloeConfigError(
                    f"storages.definitions name={d.name} type={d.type} is unsupported "
                    f"(allowed: {', '.join(STORAGE_TYPES)})"
                )
            if d.type == "s3" and (d.bucket is None or d.region is None):
                missing = "bucket" if d.bucket is None else "region"
                raise FloeConfigError(
                    f"storages.definitions name={d.name} requires {missing} for type s3"
                )
            if d.name in names:
                raise FloeConfigError(
                    f"storages.definitions name={d.name} is duplicated"
                )
            names.add(d.name)
        if storages.default is None:
            raise FloeConfigError("storages.default is required when storages is set")
        if storages.default not in names:
            raise FloeConfigError(
                f"storages.default={storages.default} does not match any definition"
            )
    if catalogs is None:
        return
    if not catalogs.definitions:
        raise FloeConfigError("catalogs.definitions must not be empty")
    seen: set[str] = set()
    for c in catalogs.definitions:
        wh = c.warehouse_storage
        if wh is not None:
            if wh not in names:
                raise FloeConfigError(
                    f"catalogs.definitions name={c.name} warehouse_storage "
                    f"references unknown storage {wh}"
                )
            if (kind := _storage_type(config, wh)) not in ("s3", "gcs"):
                raise FloeConfigError(
                    f"catalogs.definitions name={c.name} warehouse_storage must "
                    f"reference s3 or gcs storage for rest catalog (got {kind})"
                )
        if c.name in seen:
            raise FloeConfigError(f"catalogs.definitions name={c.name} is duplicated")
        seen.add(c.name)
    if catalogs.default is not None and catalogs.default not in seen:
        raise FloeConfigError(
            f"catalogs.default={catalogs.default} does not match any definition"
        )


def _check_report_lineage(config: RootConfig, profiled: bool) -> None:
    report, lineage = config.report, config.lineage
    name = _storage_ref(config, report.storage, "report.storage", "", profiled)
    if _storage_type(config, name) == "local" and report.path.startswith(REMOTE_URIS):
        raise FloeConfigError(f"report.path must be a local path (got {report.path})")
    # D7 (#584 S5): Rust 0.7 only rejects an unknown formatter when a run starts.
    if report.formatter not in (None, "json", "csv", "text"):
        raise FloeConfigError(f"unsupported report.formatter: {report.formatter}")
    # ponytail: a profile can replace `lineage` (#635); skip its checks until it lands.
    if lineage is None or profiled:
        return
    if not lineage.url.strip():
        raise FloeConfigError("lineage.url must not be empty")
    if not lineage.namespace.strip():
        raise FloeConfigError("lineage.namespace must not be empty")
    if lineage.max_failures == 0:
        raise FloeConfigError("lineage.max_failures must be at least 1")


def validate_entities(config: RootConfig, profiled: bool = False) -> None:
    version = config_version(config.version)
    if version < MIN_VERSION:
        raise FloeConfigError(
            f"root.version={config.version} is unsupported "
            f"(minimum supported version: {MIN_VERSION[0]}.{MIN_VERSION[1]})"
        )
    if version > FLOE_VERSION:
        raise FloeConfigError(
            f"root.version={config.version} is unsupported "
            f"(newer than Floe {FLOE_VERSION[0]}.{FLOE_VERSION[1]})"
        )
    if not config.entities:
        raise FloeConfigError(
            "entities list is empty (at least one entity is required)"
        )
    if not profiled:
        _check_storages(config)
    _check_report_lineage(config, profiled)
    ids: dict[str, int] = {}
    for index, entity in enumerate(config.entities):
        _check_source(entity, config, profiled)
        _check_state(entity)
        _check_sink(entity, config, profiled)
        validate_schema(entity, config.version)
        _check_pii(entity)
        for field, value in (("name", entity.name), ("domain", entity.domain)):
            if (
                version >= QUALIFIED_VERSION
                and value is not None
                and (not value or any(c in value for c in "./\\"))
            ):
                raise _fail(
                    entity,
                    f"entity.{field} must be non-empty without '.', '/' or '\\' "
                    'when root.version >= "0.3"',
                )
        if entity.id in ids:
            raise FloeConfigError(
                f"entity.name={entity.id} is duplicated in config "
                f"(entities[{ids[entity.id]}] and entities[{index}])"
            )
        ids[entity.id] = index
