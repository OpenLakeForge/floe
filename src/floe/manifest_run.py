"""Rebuild the `RootConfig` a run needs from a manifest (Rust: `manifest/reconstruct.rs`).

The manifest was validated and resolved when it was generated: nothing is re-validated here. As in
Rust, a malformed option block is dropped and an unknown enum value falls back to its default.
"""

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar, cast, get_args

from pydantic import BaseModel, ValidationError

from floe.config.project import (
    LineageConfig,
    ProjectMetadata,
    ReportConfig,
    RootConfig,
    config_version,
)
from floe.config.storage import CatalogsConfig, StoragesConfig
from floe.config.types import (
    EntityConfig,
    IcebergSinkOptions,
    MergeOptions,
    NormalizeColumnsConfig,
    PiiConfig,
    SchemaEvolutionConfig,
    SchemaMismatchConfig,
    SinkOptions,
    SinkTarget,
    SourceConfig,
    SourceOptions,
)
from floe.config.validate import FLOE_VERSION
from floe.errors import FloeConfigError, FloeIoError, FloeRunError

Json = dict[str, Any]
_M = TypeVar("_M", bound=BaseModel)
_CLOUD = ("s3://", "gs://", "gcs://", "az://", "abfss://", "abfs://")
_WRITE_MODES = ("overwrite", "append", "merge_scd1", "merge_scd2")


def _omitted_as_none(model: type[BaseModel], value: object) -> object:
    """Rust's `Option`: an omitted nullable field is `None`, in nested blocks too."""
    if not isinstance(value, dict):
        return value
    filled = dict(cast(Json, value))
    for name, field in model.model_fields.items():
        args = get_args(field.annotation)
        nested = next((a for a in args if isinstance(a, type) and issubclass(a, BaseModel)), None)  # fmt: skip
        if name in filled and nested:
            filled[name] = _omitted_as_none(nested, filled[name])
        elif name not in filled and field.is_required() and type(None) in args:
            filled[name] = None
    return filled


def _lenient(model: type[_M], value: object) -> _M | None:
    """`serde_json::from_value(v).ok()`."""
    try:
        return (
            None
            if value is None
            else model.model_validate(_omitted_as_none(model, value))
        )
    except ValidationError:
        return None


def _pick(value: str | None, allowed: tuple[str, ...], default: str) -> str:
    return value if value in allowed else default


def _storage(name: str, named_local: bool) -> str | None:
    """`local` is the implicit storage, unless a definition is named so (Rust drops it either way)."""
    return None if name == "local" and not named_local else name


def _sink(t: Json, write_mode: str, named_local: bool) -> Json:
    return {
        "format": t["format"],
        "path": t["path"],
        "storage": _storage(t["storage"], named_local),
        "options": _lenient(SinkOptions, t.get("options")),
        "merge": _lenient(MergeOptions, t.get("merge")),
        "iceberg": _lenient(IcebergSinkOptions, t.get("iceberg")),
        "partition_by": t.get("partition_by"),
        "partition_spec": None,
        "write_mode": _pick(t.get("write_mode"), _WRITE_MODES, write_mode),
    }


def _schema(s: Json) -> Json:
    keys = ("source", "nullable", "unique", "width", "trim")
    return {
        "columns": [
            {"name": c["name"], "column_type": c["column_type"]}
            | {k: c.get(k) for k in keys}
            for c in s["columns"]
        ],
        "primary_key": s["primary_key"] or None,
        "unique_keys": s["unique_keys"] or None,
        "normalize_columns": _lenient(
            NormalizeColumnsConfig, s.get("normalize_columns")
        ),
        "mismatch": _lenient(SchemaMismatchConfig, s.get("mismatch")),
        "schema_evolution": _lenient(SchemaEvolutionConfig, s.get("schema_evolution")),
    }


def _entity(m: Json, named_local: bool) -> EntityConfig:
    src, sinks = m["source"], m["sinks"]
    write_mode = _pick(m.get("write_mode"), _WRITE_MODES, "overwrite")
    rejected, archive = sinks.get("rejected"), sinks.get("archive")
    source = {
        "format": src["format"],
        "path": src["path"],
        "storage": _storage(src["storage"], named_local),
        "options": _lenient(SourceOptions, src.get("options")) or SourceOptions(),
        "cast_mode": src.get("cast_mode") or "strict",
        "source_ref": src.get("ref"),
        "resource": src.get("resource"),
    }
    sink = {
        "write_mode": write_mode,
        "accepted": _sink(sinks["accepted"], write_mode, named_local),
        "rejected": rejected and _sink(rejected, write_mode, named_local),
        "archive": archive
        and {
            "path": archive["path"],
            "storage": _storage(archive["storage"], named_local),
        },
    }
    incremental = _pick(m.get("incremental_mode"), ("archive", "file", "row"), "none")
    return EntityConfig.model_validate(
        {
            "id": m["name"],
            "name": (m.get("asset_key") or [m["name"]])[-1],
            "metadata": None,
            "domain": m.get("domain"),
            "incremental_mode": incremental,
            "state": {"path": m["state_path"]} if m.get("state_path") else None,
            "source": source,
            "sink": sink,
            "policy": {
                "severity": _pick(m.get("policy_severity"), ("reject", "abort"), "warn")
            },
            "schema": _schema(m["schema"]),
            "pii": _lenient(PiiConfig, m.get("pii")),
        }
    )


def _section(
    model: type[_M], m: Json, name: str, shape: Callable[[Json], Json] | None = None
) -> _M | None:
    """An embedded block, back in config shape; a malformed one is an error, as in Rust."""
    value = m.get(name)
    try:
        if value is not None and shape:
            value = value | {"definitions": [shape(d) for d in value["definitions"]]}
        return None if value is None else model.model_validate(value)
    except (ValidationError, TypeError, KeyError, AttributeError) as e:
        raise FloeConfigError(f"manifest {name} block is malformed: {e}") from e


def _storage_def(d: Json) -> Json:
    return {k: v for k, v in d.items() if k != "fs_type"} | {"type": d["fs_type"]}


def _catalog_def(d: Json) -> Json:
    rest = {k: v for k, v in d["type_config"].items() if k != "type"}  # D1: REST only
    return {k: v for k, v in d.items() if k != "type_config"} | rest


def _report(base: str, storages: StoragesConfig | None) -> ReportConfig:
    if base.startswith(_CLOUD):  # ponytail: S3 in `floe run` is #834; route it then
        raise FloeRunError(f"report_base_uri={base}: remote reports are not supported yet (#834)")  # fmt: skip
    local = storages and next((d.name for d in storages.definitions if d.type == "local"), None)  # fmt: skip
    path = base.removeprefix("local://")
    return ReportConfig(path=path, formatter=None, storage=local)


def _cloud(target: _M, uri: str | None) -> _M:
    """A cloud `uri` replaces the path, so lineage names the dataset by its bucket."""
    is_cloud = uri is not None and uri.startswith(_CLOUD)
    return target.model_copy(update={"path": uri}) if is_cloud else target


def _lineage_entity(entity: EntityConfig, m: Json) -> EntityConfig:
    """Rust `lineage_inputs_from_manifest_json`."""
    sink, sinks = entity.sink, m["sinks"]
    accepted: SinkTarget = _cloud(sink.accepted, sinks["accepted"].get("uri"))
    rejected = sink.rejected and _cloud(sink.rejected, sinks["rejected"].get("uri"))
    source: SourceConfig = _cloud(entity.source, m["source"]["uri"])
    update = {"accepted": accepted, "rejected": rejected}
    return entity.model_copy(update={"source": source, "sink": sink.model_copy(update=update)})  # fmt: skip


def _named_local(m: Json) -> bool:
    try:
        return any(d["name"] == "local" for d in m["storages"]["definitions"])
    except (KeyError, TypeError):  # no storages; a malformed block fails in `_section`
        return False


def load_manifest(path: Path) -> tuple[RootConfig, list[EntityConfig]]:
    """The run's config, and the entities its lineage observes."""
    try:
        text = path.read_text()
    except OSError as e:  # as the CLI's `_realpath`
        raise FloeIoError(f"{e.strerror} (os error {e.errno})") from e
    try:
        m = json.loads(text)
        spec = m["spec_version"]
        entities = [_entity(e, _named_local(m)) for e in m["entities"]]
        report_base = m["report_base_uri"]
        meta = m.get("metadata")  # D20: dumped without its None fields
        metadata = None if meta is None else ProjectMetadata.model_validate(_omitted_as_none(ProjectMetadata, meta))  # fmt: skip
    except (ValueError, KeyError, TypeError, AttributeError) as e:
        why = f"missing field `{e.args[0]}`" if isinstance(e, KeyError) else e
        raise FloeConfigError(f"manifest parse error: {why}") from e
    try:
        supported = config_version(spec) <= FLOE_VERSION
    except FloeConfigError:
        supported = False
    if not supported:
        floe = f"{FLOE_VERSION[0]}.{FLOE_VERSION[1]}"
        raise FloeConfigError(f"manifest spec_version={spec} is unsupported (newer than Floe {floe})")  # fmt: skip
    storages = _section(StoragesConfig, m, "storages", _storage_def)
    root = RootConfig(
        version=spec,
        metadata=metadata,
        storages=storages,
        catalogs=_section(CatalogsConfig, m, "catalogs", _catalog_def),
        env=None,
        domains=[],
        report=_report(report_base, storages),
        lineage=_section(LineageConfig, m, "lineage"),
        entities=entities,
        config_files=[],
    )
    observed = [_lineage_entity(e, me) for e, me in zip(entities, m["entities"])]
    return root, observed
