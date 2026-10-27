"""Hooks for Airflow-Floe connector."""

from __future__ import annotations

from pathlib import Path

from .runtime import DagManifestContext, build_dag_manifest_context_or_empty


def load_manifest_context(
    manifest_path: str,
    *,
    config_override: str | None = None,
    default_config_path: str | None = None,
) -> DagManifestContext:
    return build_dag_manifest_context_or_empty(
        manifest_path=manifest_path,
        config_override=config_override,
        default_config_path=default_config_path,
    )


class FloeManifestHook:
    """Resolve connector runtime context from a Floe Airflow manifest.

    Example::

        hook = FloeManifestHook(manifest_path)
        context = hook.get_context()

        run_task = FloeRunOperator(
            task_id="floe_run",
            config_path=hook.get_config_path(),
            manifest_context=context,
        )
    """

    def __init__(
        self,
        manifest_path: str,
        *,
        config_override: str | None = None,
        default_config_path: str | None = None,
    ) -> None:
        self.manifest_path = manifest_path
        self.config_override = config_override
        self.default_config_path = default_config_path
        self._context: DagManifestContext | None = None

    def get_context(self) -> DagManifestContext:
        if self._context is None:
            self._context = load_manifest_context(
                self.manifest_path,
                config_override=self.config_override,
                default_config_path=self.default_config_path,
            )
        return self._context

    def get_assets(self):
        return list(self.get_context().assets_by_entity.values())

    def get_entities(self) -> list[str]:
        return self.get_context().entity_names

    def get_config_path(self) -> str:
        return self.get_context().config_path

    @staticmethod
    def default_example_paths() -> tuple[str, str]:
        base = Path(__file__).resolve().parents[2]
        manifest = str(base / "example" / "manifest.airflow.json")
        config = str(base / "example" / "config.yml")
        return manifest, config
