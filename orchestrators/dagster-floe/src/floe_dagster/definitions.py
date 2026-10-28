from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Iterable

from dagster import AssetSelection, Definitions, define_asset_job

from .assets import _has_rejected_asset, _rejected_key, build_floe_asset_defs
from .manifest import ManifestExecution, load_manifest_source
from .runner import InProcessRunner, LocalRunner, Runner


def build_runner_from_env() -> Runner:
    """In-process when the `floe` Python API can run a manifest, unless `FLOE_RUNNER=local`."""
    choice = os.environ.get("FLOE_RUNNER", "in_process")
    if choice not in ("in_process", "local"):
        raise ValueError(f"FLOE_RUNNER must be 'in_process' or 'local', got {choice!r}")
    runner = InProcessRunner if choice == "in_process" and _floe_api() else LocalRunner
    return runner(os.environ.get("FLOE_BIN", "floe"), timeout=_timeout_from_env())


def _floe_api() -> bool:
    try:
        import floe
    except ImportError:
        return False
    return hasattr(floe, "run_manifest")  # the Rust `floe-python` package has none


def _timeout_from_env() -> float | None:
    raw = os.environ.get("FLOE_RUN_TIMEOUT_SECONDS")
    if not raw:
        return None
    try:
        timeout = float(raw)
    except ValueError as exc:
        raise ValueError(
            f"FLOE_RUN_TIMEOUT_SECONDS must be a number, got {raw!r}"
        ) from exc
    if timeout <= 0:
        raise ValueError("FLOE_RUN_TIMEOUT_SECONDS must be positive")
    return timeout


def build_definitions(
    *,
    manifest_path: str | None = None,
    manifest_uri: str | None = None,
    entities: list[str] | None = None,
    runner: Runner | None = None,
    with_job: bool = True,
    job_name: str | None = None,
    config_path: str | None = None,
    profile_path: str | None = None,
    manifest_name: str | None = None,
) -> Definitions:
    """From a manifest file, or (`config_path`, `floe` extra) a manifest built in memory."""
    return _build_definitions(
        sources=[
            {
                "manifest_path": manifest_path,
                "config_path": config_path,
                "profile_path": profile_path,
                "manifest_name": manifest_name,
            }
        ],
        manifest_uri=manifest_uri,
        entities=entities,
        runner=runner,
        with_job=with_job,
        job_name=job_name,
    )


def build_definitions_from_manifest_dir(
    *,
    manifest_dir: str,
    pattern: str = "*.manifest.json",
    runner: Runner | None = None,
    with_job: bool = True,
) -> Definitions:
    paths = sorted(
        str(path.resolve())
        for path in Path(manifest_dir).expanduser().resolve().glob(pattern)
        if path.is_file()
    )
    if not paths:
        raise ValueError(
            f"no manifest files found in {manifest_dir!r} using pattern {pattern!r}"
        )
    return build_definitions_from_manifest_paths(
        manifest_paths=paths,
        runner=runner,
        with_job=with_job,
    )


_Source = dict[str, "str | None"]  # load_manifest_source kwargs: manifest_path, config_path, profile_path, manifest_name


def build_definitions_from_manifest_paths(
    *,
    manifest_paths: Iterable[str],
    manifest_uri: str | None = None,
    entities: list[str] | None = None,
    runner: Runner | None = None,
    with_job: bool = True,
    job_name: str | None = None,
) -> Definitions:
    manifest_paths_list = list(manifest_paths)
    if not manifest_paths_list:
        raise ValueError("manifest_paths cannot be empty")
    return _build_definitions(
        sources=[{"manifest_path": path} for path in manifest_paths_list],
        manifest_uri=manifest_uri,
        entities=entities,
        runner=runner,
        with_job=with_job,
        job_name=job_name,
    )


def _build_definitions(
    *,
    sources: list[_Source],
    manifest_uri: str | None,
    entities: list[str] | None,
    runner: Runner | None,
    with_job: bool,
    job_name: str | None,
) -> Definitions:
    if job_name is not None and len(sources) != 1:
        raise ValueError("job_name override is only supported for a single manifest")
    if manifest_uri is not None and len(sources) != 1:
        raise ValueError("manifest_uri override is only supported for a single manifest")

    runner_impl = runner or build_runner_from_env()
    assets_defs: list[object] = []
    jobs = []
    seen_asset_keys: set[tuple[str, ...]] = set()
    seen_job_names: set[str] = set()

    for source in sources:
        manifest, _ = load_manifest_source(**source)
        manifest_path = source.get("manifest_path") or source["config_path"]  # names the job
        selected_entities = None if entities is None else list(entities)
        manifest_assets, manifest_source_assets, manifest_entities = build_floe_asset_defs(
            **source,
            manifest_uri=manifest_uri,
            runner=runner_impl,
            entities=selected_entities,
        )
        assets_defs.extend(manifest_assets)
        assets_defs.extend(manifest_source_assets)
        _validate_no_duplicate_asset_keys(
            seen_asset_keys=seen_asset_keys,
            manifest_path=manifest_path,
            entity_asset_keys=(
                [entity.asset_key for entity in manifest_entities]
                + [list(sa.key.path) for sa in manifest_source_assets]
            ),
        )

        if with_job and manifest_entities:
            manifest_job_name = (
                job_name
                if job_name is not None and len(sources) == 1
                else _default_job_name(manifest_path, manifest.manifest_id)
            )
            if manifest_job_name in seen_job_names:
                raise ValueError(
                    f"duplicate job name {manifest_job_name!r} generated from manifests; "
                    "ensure unique manifest_id values"
                )
            seen_job_names.add(manifest_job_name)
            selection = AssetSelection.assets(
                *[entity.asset_key for entity in manifest_entities],
                # a domain-scope asset is subsettable, so its rejected outputs are selected explicitly
                *[_rejected_key(e) for e in manifest_entities if _has_rejected_asset(e)],
            ).required_multi_asset_neighbors()
            job_kwargs: dict[str, Any] = {
                "name": manifest_job_name,
                "selection": selection,
            }
            run_config = resolve_execution_config(manifest.execution)
            if run_config is not None:
                job_kwargs["config"] = run_config
            jobs.append(define_asset_job(**job_kwargs))

    return Definitions(assets=assets_defs, jobs=jobs)


def _resolve_max_concurrent(orchestration: Any) -> int | None:
    if orchestration is None:
        return None
    # sequential always means one-at-a-time, regardless of max_concurrent_entities
    if orchestration.strategy == "sequential":
        return 1
    if orchestration.max_concurrent_entities is not None:
        return orchestration.max_concurrent_entities
    return None


def resolve_execution_config(
    execution: ManifestExecution | None,
) -> dict[str, Any] | None:
    """Return a Dagster job run-config dict derived from a manifest execution block.

    Returns ``None`` when the manifest carries no concurrency constraint.
    """
    if execution is None:
        return None
    max_concurrent = _resolve_max_concurrent(execution.orchestration)
    if max_concurrent is None:
        return None
    return {"execution": {"config": {"multiprocess": {"max_concurrent": max_concurrent}}}}


def build_job_run_config_from_manifest(
    manifest_path: str | None = None,
    *,
    config_path: str | None = None,
    profile_path: str | None = None,
    manifest_name: str | None = None,
) -> dict[str, Any] | None:
    """Load a Floe manifest (or build it from `config_path`) and return its Dagster job run-config dict.

    Returns ``None`` when the manifest carries no concurrency constraint.
    """
    manifest, _ = load_manifest_source(manifest_path, config_path, profile_path, manifest_name)
    return resolve_execution_config(manifest.execution)


def _default_job_name(manifest_path: str, manifest_id: str) -> str:
    base = manifest_id.strip() or Path(manifest_path).stem
    normalized = re.sub(r"[^a-zA-Z0-9_]+", "_", base).strip("_")
    if not normalized:
        normalized = "manifest"
    if not re.match(r"[a-zA-Z_]", normalized):
        normalized = f"m_{normalized}"
    return f"floe_{normalized}_job"


def _validate_no_duplicate_asset_keys(
    *,
    seen_asset_keys: set[tuple[str, ...]],
    manifest_path: str,
    entity_asset_keys: list[list[str]],
) -> None:
    for asset_key_parts in entity_asset_keys:
        key_tuple = tuple(asset_key_parts)
        if key_tuple in seen_asset_keys:
            key_value = "/".join(asset_key_parts)
            raise ValueError(
                f"duplicate asset key {key_value!r} across manifests; "
                f"conflict found while processing {manifest_path}"
            )
        seen_asset_keys.add(key_tuple)
