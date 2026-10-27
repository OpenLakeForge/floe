"""Manifest discovery helpers for Airflow DAG registration."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re


@dataclass(frozen=True)
class ManifestDagSpec:
    manifest_path: str
    dag_id: str


def discover_manifest_dag_specs(
    manifest_dir: str,
    *,
    dag_prefix: str = "floe",
) -> list[ManifestDagSpec]:
    """Discover manifests in a directory and derive deterministic DAG IDs."""
    directory = Path(manifest_dir)
    if not directory.exists() or not directory.is_dir():
        return []

    manifest_paths = sorted(
        path.resolve()
        for path in directory.glob("*.manifest.json")
        if path.is_file()
    )

    specs: list[ManifestDagSpec] = []
    used_ids: set[str] = set()
    for manifest_path in manifest_paths:
        base_id = _dag_id_from_manifest_path(manifest_path, dag_prefix=dag_prefix)
        dag_id = base_id
        suffix = 2
        while dag_id in used_ids:
            dag_id = f"{base_id}_{suffix}"
            suffix += 1
        used_ids.add(dag_id)
        specs.append(ManifestDagSpec(manifest_path=str(manifest_path), dag_id=dag_id))
    return specs


def _dag_id_from_manifest_path(path: Path, *, dag_prefix: str) -> str:
    filename = path.name
    if filename.endswith(".manifest.json"):
        name = filename[: -len(".manifest.json")]
    elif filename.endswith(".json"):
        name = filename[: -len(".json")]
    else:
        name = path.stem

    normalized = re.sub(r"[^0-9A-Za-z_]+", "_", name).strip("_").lower()
    if not normalized:
        normalized = "manifest"
    return f"{dag_prefix}_{normalized}"
