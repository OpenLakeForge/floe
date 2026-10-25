"""Orchestrator manifest, ported from `manifest/builder.rs` and `manifest/model.rs`.

The manifest JSON is the contract (Dagster and Airflow consume it): key order is the order of the
Rust structs, and every value Rust builds with `serde_json::to_value` has its keys sorted.
"""

import hashlib
import json
import re
import time
from dataclasses import asdict
from itertools import takewhile
from pathlib import Path
from typing import Any, NamedTuple, cast

from pydantic import BaseModel

from floe import __version__
from floe.config.project import ReportConfig, RootConfig
from floe.config.storage import StoragesConfig
from floe.config.types import ArchiveTarget, EntityConfig, SinkTarget, SourceConfig
from floe.storage.paths import normalize_local_path

Json = dict[str, Any]
_REMOTE = ("s3://", "gs://", "gcs://", "abfs://", "abfss://", "az://")


def _sorted(value: object) -> Any:
    """`serde_json::to_value`: objects come out key-sorted."""
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json")
    if isinstance(value, dict):
        return {k: _sorted(v) for k, v in sorted(cast(Json, value).items())}
    if isinstance(value, list):
        return [_sorted(v) for v in cast(list[object], value)]
    return value


def _put(target: Json, **optional: object) -> Json:
    """`skip_serializing_if = "Option::is_none"`."""
    return target | {k: v for k, v in optional.items() if v is not None}


def _sha256(data: bytes) -> str:
    return f"sha256:{hashlib.sha256(data).hexdigest()}"


def _manifest_id(config_uri: str, checksum: str | None) -> str:
    h = 0xCBF29CE484222325
    for byte in config_uri.encode() + b"\0" + (checksum or "").encode():
        h = ((h ^ byte) * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return f"mfv1-{h:016x}"


def _revision(manifest: Json) -> str:
    """SHA-256 of the canonical content, without the volatile `generated_at_ts_ms` and itself."""
    body = {
        k: v
        for k, v in manifest.items()
        if k not in ("generated_at_ts_ms", "manifest_revision")
    }
    return _sha256(
        json.dumps(_sorted(body), separators=(",", ":"), ensure_ascii=False).encode()
    )


class _Context(NamedTuple):
    config_dir: Path
    storages: StoragesConfig | None


def _s3_uri(bucket: str, prefix: str | None, raw: str) -> str | None:
    """`resolve_s3_uri`: the bucket-qualified URI, or None where Rust errors."""
    uri_bucket, _, uri_key = raw.removeprefix("s3://").partition("/")
    if raw.startswith("s3://") and uri_bucket:
        if uri_bucket != bucket:
            return None
        return f"s3://{bucket}/{uri_key}" if uri_key else f"s3://{bucket}"
    key = "/".join(p for p in ((prefix or "").strip("/"), raw.lstrip("/")) if p)
    return f"s3://{bucket}/{key}" if key else f"s3://{bucket}"


class _Target:
    """`resolve_or_raw`: the resolved URI, else the raw path as typed."""

    def __init__(
        self,
        ctx: _Context,
        target: SourceConfig | SinkTarget | ArchiveTarget | ReportConfig,
    ) -> None:
        storages, self.raw = ctx.storages, target.path
        name = target.storage or (storages.default if storages else "local")
        definition = storages and next(
            (d for d in storages.definitions if d.name == name), None
        )
        kind = definition.type if definition else "local" if not storages else None
        uri = None
        if kind == "local" and not self.raw.startswith(_REMOTE):
            uri = f"local://{normalize_local_path(ctx.config_dir / self.raw)}"
        elif kind == "s3" and definition and definition.bucket is not None:
            uri = _s3_uri(definition.bucket, definition.prefix, self.raw)
        self.resolved = uri is not None
        self.storage = name if uri else target.storage or "local"
        self.uri = uri or self.raw

    def json(self, fmt: str | None = None) -> Json:
        fields = {"storage": self.storage, "uri": self.uri, "path": self.raw}
        return ({"format": fmt} if fmt else {}) | fields | {"resolved": self.resolved}


def _redact_key(key: str | None) -> str | None:
    """Only an exact `${VAR}` reference survives: literal HMAC keys never reach a manifest."""
    return key if key and re.fullmatch(r"\$\{[^{}]+\}", key) else None


def _sink(ctx: _Context, target: SinkTarget) -> Json:
    return _put(
        _Target(ctx, target).json(target.format),
        options=_sorted(target.options),
        partition_by=target.partition_by,
        merge=_sorted(target.merge),
        iceberg=_sorted(target.iceberg),
    )


def _entity(entity: EntityConfig, ctx: _Context) -> Json:
    source = _Target(ctx, entity.source)
    sink, meta, schema, domain = (
        entity.sink,
        entity.metadata,
        entity.schema_,
        entity.domain,
    )
    accepted = _Target(ctx, sink.accepted)
    rejected = sink.rejected and _Target(ctx, sink.rejected)
    tags = {
        k: v
        for k, v in (
            ("data_product", meta and meta.data_product),
            ("domain", meta and meta.domain),
            ("owner", meta and meta.owner),
        )
        if v is not None
    }
    pii = entity.pii and _sorted(
        {
            "columns": [
                asdict(c) | {"key": _redact_key(c.key)} for c in entity.pii.columns
            ]
        }
    )
    return _put(
        {
            "name": entity.id,
            "domain": domain,
            "group_name": domain or "default",
            "asset_key": [domain or "default", entity.name],
            "source_format": entity.source.format,
            "accepted_sink_uri": accepted.uri,
            "rejected_sink_uri": rejected and rejected.uri,
            "tags": tags or None,
            "source": _put(
                source.json(entity.source.format),
                ref=entity.source.source_ref,
                resource=entity.source.resource,
            )
            | {
                "cast_mode": entity.source.cast_mode,
                "options": _sorted(entity.source.options),
            },
            "sinks": {
                "accepted": _sink(ctx, sink.accepted),
                "rejected": sink.rejected and _sink(ctx, sink.rejected),
                "archive": sink.archive and _Target(ctx, sink.archive).json(),
            },
            "runner": None,
            "policy_severity": entity.policy.severity,
            "write_mode": sink.write_mode,
            "incremental_mode": entity.incremental_mode,
            "schema": _put(
                {
                    "columns": [
                        _put(
                            {"name": c.name, "column_type": c.column_type},
                            source=c.source,
                            nullable=c.nullable,
                            unique=c.unique,
                            width=c.width,
                            trim=c.trim,
                        )
                        for c in schema.columns
                    ],
                    "primary_key": schema.primary_key or [],
                    "unique_keys": schema.unique_keys or [],
                },
                normalize_columns=_sorted(schema.normalize_columns),
                mismatch=_sorted(schema.mismatch),
                schema_evolution=_sorted(schema.schema_evolution),
            ),
        },
        pii=pii,
        state_path=entity.state and entity.state.path,
    )


def _execution() -> Json:
    return {
        "entrypoint": "floe",
        "base_args": ["run", "--manifest", "{manifest_uri}"]
        + ["--log-format", "json", "--quiet"],
        "per_entity_args": ["--entities", "{entity_name}"],
        "log_format": "json",
        "result_contract": {
            "run_finished_event": True,
            "summary_uri_field": "summary_uri",
            "exit_codes": {
                "0": "success_or_rejected",
                "1": "technical_failure",
                "2": "aborted",
            },
        },
        "defaults": {"env": {}, "workdir": None},
    }


_RUNNER_FIELDS = (
    *("command", "args", "timeout_seconds", "ttl_seconds_after_finished"),
    *("poll_interval_seconds", "secrets", "image", "namespace", "service_account"),
    *("resources", "env", "workspace_url", "existing_cluster_id", "config_uri"),
    *("python_file_uri", "job_name", "auth", "env_parameters"),
)


_OPTIONAL = ("config_checksum", "config_files")


def _config_files(files: list[Path]) -> list[Json] | None:
    """Every file of a multi-file config by path relative to the root's directory, else None."""
    if len(files) < 2:
        return None
    root = files[0].parent.parts
    entries: list[Json] = []
    for file in files:
        parts = file.parts
        common = sum(1 for _ in takewhile(lambda p: p[0] == p[1], zip(parts, root)))
        path = "/".join([".."] * (len(root) - common) + list(parts[common:]))
        entries.append(
            {"path": path, "sha256": hashlib.sha256(file.read_bytes()).hexdigest()}
        )
    return sorted(entries, key=lambda e: e["path"])


def _blocks(root: RootConfig) -> Json:
    """The effective `storages`, `catalogs` and `lineage`, serialized as Rust does (secrets in clear)."""
    blocks: Json = {}
    if root.storages:
        blocks["storages"] = _sorted(
            {
                "default": root.storages.default,
                "definitions": [
                    d.model_dump(exclude={"type"}) | {"fs_type": d.type}
                    for d in root.storages.definitions
                ],
            }
        )
    if root.catalogs:
        blocks["catalogs"] = _sorted(
            {
                "default": root.catalogs.default,
                "definitions": [
                    {
                        "name": c.name,
                        "type_config": {
                            "type": "rest",
                            "uri": c.uri,
                            "credential": c.credential
                            and c.credential.get_secret_value(),
                            "warehouse": c.warehouse,
                            "oauth2_server_uri": c.oauth2_server_uri,
                            "scope": c.scope,
                        },
                        "warehouse_storage": c.warehouse_storage,
                        "warehouse_prefix": c.warehouse_prefix,
                    }
                    for c in root.catalogs.definitions
                ],
            }
        )
    if root.lineage:
        key = root.lineage.api_key
        blocks["lineage"] = _sorted(
            root.lineage.model_dump(exclude={"api_key"})
            | {"api_key": key and key.get_secret_value()}
        )
    return blocks


def build_manifest(
    root: RootConfig,
    config_path: Path,
    deterministic: bool,
    manifest_name: str | None,
) -> Json:
    ctx = _Context(normalize_local_path(config_path.parent), root.storages)
    config_uri = f"local://{config_path}"
    files = _config_files(root.config_files)
    checksum = (
        _sha256("".join(f"{f['path']}\0{f['sha256']}\n" for f in files).encode())
        if files
        else _sha256(config_path.read_bytes())
        if len(root.config_files) == 1
        else None
    )
    report = _Target(ctx, root.report)
    manifest: Json = _put(
        {
            "schema": "floe.manifest.v1",
            "generated_at_ts_ms": 0,
            "floe_version": __version__,
        },
        spec_version=root.version,
        manifest_name=manifest_name,
    )
    manifest |= {
        "manifest_id": _manifest_id(config_uri, checksum),
        "manifest_revision": None,
        "runtime_env": "cli",
        "config_uri": config_uri,
        "config_checksum": checksum,
        "config_files": files,
        "report_base_uri": report.uri,
        "domains": [
            _put(
                {"name": d.name},
                incoming_dir=d.resolved_incoming_dir or d.incoming_dir,
            )
            for d in root.domains
        ],
        "execution": _execution(),
        "runners": {
            "default": "local",
            "definitions": {
                "local": {"type": "local_process"} | dict.fromkeys(_RUNNER_FIELDS)
            },
        },
        "entities": [
            _entity(e, ctx) for e in sorted(root.entities, key=lambda e: e.id)
        ],
    }
    manifest |= _blocks(root)
    manifest = {
        k: v for k, v in manifest.items() if k not in _OPTIONAL or v is not None
    }
    if not deterministic:
        manifest["generated_at_ts_ms"] = time.time_ns() // 1_000_000
    manifest["manifest_revision"] = _revision(manifest)
    return manifest
