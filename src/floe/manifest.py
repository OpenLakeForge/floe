"""Orchestrator manifest, ported from `manifest/builder.rs` and `manifest/model.rs`.

The manifest JSON is the contract (Dagster and Airflow consume it): key order is the order of the
Rust structs, and every value Rust builds with `serde_json::to_value` has its keys sorted.
"""

import hashlib
import json
import os
import re
import time
from dataclasses import asdict
from itertools import takewhile
from pathlib import Path, PurePosixPath
from typing import Any, NamedTuple, cast

from pydantic import BaseModel

from floe import __version__
from floe.config.profile import ProfileConfig
from floe.config.project import ReportConfig, RootConfig
from floe.config.storage import StoragesConfig
from floe.config.types import ArchiveTarget, EntityConfig, SinkTarget, SourceConfig
from floe.config.validate import FLOE_VERSION
from floe.errors import FloeConfigError, FloeError
from floe.pii import PiiColumn
from floe.run import select_entities
from floe.storage.paths import normalize_local_path
from floe.storage.s3 import read_s3

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


class Options(NamedTuple):
    config_arg: str  # `-c` as typed
    deterministic: bool
    manifest_name: str | None
    default_domain: str | None
    path_mode: str
    runtime: str | None  # None: detect
    manifest_uri: str | None = (
        None  # replaces `{manifest_uri}` in `execution.base_args`
    )
    profile_arg: str | None = None  # `-p` as typed
    profile: ProfileConfig | None = None  # its `execution` block shapes the runners


class _Context(NamedTuple):
    config_dir: Path
    storages: StoragesConfig | None
    resolved_uri_paths: bool


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
        # `--manifest-path-mode resolved-uri`: a resolved path is recorded as its URI, minus `local://`
        self.path = (
            self.uri.removeprefix("local://")
            if ctx.resolved_uri_paths and uri
            else self.raw
        )

    def json(self, fmt: str | None = None) -> Json:
        fields = {"storage": self.storage, "uri": self.uri, "path": self.path}
        return ({"format": fmt} if fmt else {}) | fields | {"resolved": self.resolved}


def _pii_key(entity: EntityConfig, column: PiiColumn) -> str | None:
    """Only an exact `${VAR}` reference reaches a manifest (D20: no secrets). A literal hash key is
    refused, not dropped: dropping it makes a manifest run hash with unsalted SHA-256 (#1006)."""
    key = column.key
    if key is None or re.fullmatch(r"\$\{[^{}]+\}", key):
        return key
    if column.strategy != "hash":
        return None  # `key` is ignored by every other strategy
    raise FloeConfigError(
        f"entity.name={entity.name} pii.columns[name={column.name}].key: a manifest never "
        "carries secrets, so a literal hash key cannot be written to it; "
        "set key to a ${ENV_VAR} reference"
    )


def _sink(ctx: _Context, target: SinkTarget) -> Json:
    return _put(
        _Target(ctx, target).json(target.format),
        options=_sorted(target.options),
        partition_by=target.partition_by,
        partition_spec=_sorted(target.partition_spec),  # #911
        merge=_sorted(target.merge),
        iceberg=_sorted(target.iceberg),
    )


def _entity(entity: EntityConfig, ctx: _Context, default_domain: str | None) -> Json:
    source = _Target(ctx, entity.source)
    sink, meta, schema, domain = (
        entity.sink,
        entity.metadata,
        entity.schema_,
        entity.domain if entity.domain is not None else default_domain,
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
                asdict(c) | {"key": _pii_key(entity, c)} for c in entity.pii.columns
            ]
        }
    )
    return _put(
        {
            "name": entity.id,
            "domain": domain,
            "group_name": domain if domain is not None else "default",
            "asset_key": [domain if domain is not None else "default", entity.name],
            "source_format": entity.source.format,
            "accepted_sink_uri": accepted.uri,
            "rejected_sink_uri": rejected and rejected.uri,
            "tags": tags or None,
            "source": _put(
                source.json(entity.source.format),
                ref=entity.source.source_ref,
                resource=entity.source.resource,
            )
            | _put(
                {
                    "cast_mode": entity.source.cast_mode,
                    "options": _sorted(entity.source.options),
                },
                partitioning=_sorted(entity.source.partitioning),  # D28
            ),
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


def _execution(manifest_uri: str | None, profile: ProfileConfig | None) -> Json:
    orchestration = profile and profile.orchestration
    execution: Json = {
        "entrypoint": "floe",
        "base_args": ["run", "--manifest", manifest_uri or "{manifest_uri}"]
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
    if orchestration is not None:  # Rust skips it when the profile sets none
        execution["orchestration"] = orchestration
    return execution


_RUNNER_FIELDS = (
    *("command", "args", "timeout_seconds", "ttl_seconds_after_finished"),
    *("poll_interval_seconds", "secrets", "image", "namespace", "service_account"),
    *("resources", "env", "workspace_url", "existing_cluster_id", "config_uri"),
    *("python_file_uri", "job_name", "auth", "env_parameters"),
)
_PYTHON_RUNNER_FIELDS = ("job_scope", "domain_resources")  # carried only when set


# Rust `runners_contract`: the fields each profile runner type carries into the manifest
_PROFILE_RUNNER_FIELDS = {
    "kubernetes_job": _RUNNER_FIELDS[:11],
    "databricks_job": (*_RUNNER_FIELDS[:3], _RUNNER_FIELDS[4], *_RUNNER_FIELDS[11:]),
}


def _runners(profile: ProfileConfig | None) -> Json:
    """The profile's `kubernetes_job` or `databricks_job` runner as `default`, else `local`."""
    kind = profile and profile.runner.get("type")
    if not isinstance(kind, str) or kind not in _PROFILE_RUNNER_FIELDS:
        local = {"type": "local_process"} | dict.fromkeys(_RUNNER_FIELDS)
        return {"default": "local", "definitions": {"local": local}}
    assert profile is not None
    keep = _PROFILE_RUNNER_FIELDS[kind]
    runner: Json = {"type": kind}
    for k in _RUNNER_FIELDS:
        v = profile.runner.get(k) if k in keep else None
        if k in ("env", "env_parameters") and isinstance(v, dict):  # a BTreeMap in Rust
            v = dict(sorted(cast(dict[str, str], v).items()))
        runner[k] = v
    if kind == "kubernetes_job":
        for k in _PYTHON_RUNNER_FIELDS:
            if (v := profile.runner.get(k)) is not None:
                runner[k] = v
    if kind == "databricks_job":
        runner["job_name"] = runner["job_name"] or "floe-{domain}-{env}"
    return {"default": "default", "definitions": {"default": runner}}


_OPTIONAL = (
    *("work_root", "config_checksum", "config_files"),
    *("profile_uri", "profile_checksum"),
)
_WORK_ROOT = "/work"


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


def _sources(entities: list[Json]) -> list[Json] | None:
    """D20: the named sources' index, derived from the entities that use them."""
    index: dict[str, Json] = {}
    for entity in entities:
        source = entity["source"]
        if (ref := source.get("ref")) is None:
            continue
        # ponytail: an entity overlay can change a named source's format; the first entity's wins
        entry = index.setdefault(
            ref, {"name": ref, "format": source["format"], "uri": source["uri"]}
        )
        pairs = zip(entry["uri"].split("/"), source["uri"].split("/"))
        common = takewhile(lambda p: p[0] == p[1], pairs)
        entry["uri"] = "/".join(a for a, _ in common)
        entry.setdefault("entities", []).append(entity["name"])
    for (
        entry
    ) in index.values():  # consumers on different storages or buckets share no location
        if entry["uri"].rstrip("/").endswith(":") or not entry["uri"]:
            entry["uri"] = None
    return sorted(index.values(), key=lambda s: s["name"]) or None


def detect_runtime() -> str:
    """`FLOE_RUNTIME`, else a container (`/.dockerenv` or cwd `/work`) is `image`, else `cli`."""
    forced = os.environ.get("FLOE_RUNTIME", "").strip().lower()
    if forced in ("image", "cli"):
        return forced
    in_container = Path("/.dockerenv").exists() or Path.cwd() == Path(_WORK_ROOT)
    return "image" if in_container else "cli"


def _config_uri(typed: str, canonical: Path, work_root: str | None) -> str:
    """`cli`: the canonical host path. `image`: the path as typed, rebased under the work-root."""
    if work_root is None:
        return f"local://{canonical}"
    parts = ["" if p.startswith("/") else p for p in PurePosixPath(typed).parts]
    rel = "/".join(parts) or "."
    return (
        f"local://{rel}"
        if rel.startswith("/")
        else f"local://{work_root.rstrip('/')}/{rel}"
    )


def build_manifest(
    root: RootConfig, config_path: Path, options: Options, selectors: list[str]
) -> Json:
    if options.path_mode not in ("default", "resolved-uri"):
        raise FloeError(
            f"unknown --manifest-path-mode value '{options.path_mode}'; expected 'default' or 'resolved-uri'"
        )
    runtime = options.runtime or detect_runtime()
    work_root = None
    if runtime == "image":
        work_root = os.environ.get("FLOE_WORK_ROOT", "")
        work_root = (work_root if work_root.strip() else _WORK_ROOT).replace("\\", "/")
    config_uri = _config_uri(options.config_arg, config_path, work_root)
    ctx = _Context(
        # D20: in `image` runtime local paths resolve beside `config_uri`, never on the host
        Path(config_uri.removeprefix("local://")).parent
        if work_root is not None
        else normalize_local_path(config_path.parent),
        root.storages,
        options.path_mode == "resolved-uri",
    )
    files = _config_files(root.config_files)
    checksum = (
        _sha256("".join(f"{f['path']}\0{f['sha256']}\n" for f in files).encode())
        if files
        else _sha256(config_path.read_bytes())
        if len(root.config_files) == 1
        else None
    )
    report = _Target(ctx, root.report)
    profile_uri = profile_checksum = None
    if options.profile_arg is not None:  # recorded like `config_uri` and its checksum
        # an s3 profile is read from there by the run, so it is recorded as typed
        if options.profile_arg.startswith("s3://"):
            profile_uri = options.profile_arg
            profile_checksum = _sha256(read_s3(profile_uri, "profile"))
        else:
            profile = Path(os.path.realpath(options.profile_arg))
            profile_uri = _config_uri(options.profile_arg, profile, work_root)
            profile_checksum = _sha256(profile.read_bytes())
    manifest: Json = _put(
        {
            "generated_at_ts_ms": 0,
            "floe_version": __version__,
            "spec_version": f"{FLOE_VERSION[0]}.{FLOE_VERSION[1]}",  # D22
        },
        manifest_name=options.manifest_name,
    )
    manifest |= {
        "manifest_id": _manifest_id(config_uri, checksum),
        "manifest_revision": None,
        "runtime_env": runtime,
        "work_root": work_root,
        "config_uri": config_uri,
        "config_checksum": checksum,
        "config_files": files,
        "profile_uri": profile_uri,
        "profile_checksum": profile_checksum,
        "report_base_uri": report.uri,
        "domains": [
            _put(
                {"name": d.name},
                incoming_dir=d.resolved_incoming_dir or d.incoming_dir,
            )
            for d in root.domains
        ],
        "execution": _execution(options.manifest_uri, options.profile),
        "runners": _runners(options.profile),
        "entities": [
            _entity(e, ctx, options.default_domain)
            for e in sorted(select_entities(root, selectors), key=lambda e: e.id)
        ],
    }
    manifest |= _blocks(root)
    manifest = _put(
        manifest,
        metadata=root.metadata and root.metadata.model_dump(exclude_none=True),
        sources=_sources(manifest["entities"]),
    )
    manifest = {
        k: v for k, v in manifest.items() if k not in _OPTIONAL or v is not None
    }
    if not options.deterministic:
        manifest["generated_at_ts_ms"] = time.time_ns() // 1_000_000
    manifest["manifest_revision"] = _revision(manifest)
    return manifest
