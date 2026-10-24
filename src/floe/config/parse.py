"""`parse_config`, ported from `config/parse.rs` and `config/template.rs` (`apply_templates_with_vars`)."""

from collections.abc import Mapping
from pathlib import Path
from typing import Any, TypeVar

from pydantic import BaseModel, SecretStr

from floe.config.include import assemble_includes
from floe.config.project import RootConfig, parse_root
from floe.config.types import EntityConfig
from floe.config.vars import build_env_vars, replace_placeholders
from floe.config.yaml import load_single_doc
from floe.errors import FloeConfigError

Vars = Mapping[str, str]
M = TypeVar("M", bound=BaseModel)


def _fill(model: M, names: tuple[str, ...], variables: Vars, label: str) -> M:
    """Substitute placeholders in the `names` fields that are set; `label` prefixes the field in errors."""
    update: dict[str, Any] = {}
    for name in names:
        value = getattr(model, name)
        secret = isinstance(value, SecretStr)
        if secret:
            value = value.get_secret_value()
        if value is not None:
            filled = replace_placeholders(value, variables, label + name)
            update[name] = SecretStr(filled) if secret else filled
    return model.model_copy(update=update)


def _fill_entity(
    entity: EntityConfig, variables: Vars, domains: Mapping[str, str | None]
) -> EntityConfig:
    context = dict(variables) | {"entity.name": entity.name}
    if entity.domain is not None:
        if entity.domain not in domains:
            raise FloeConfigError(
                f"entity.name={entity.name} references unknown domain {entity.domain}"
            )
        context.pop("domain.incoming_dir", None)
        if (incoming_dir := domains[entity.domain]) is not None:
            context["domain.incoming_dir"] = incoming_dir
        context["domain.name"] = entity.domain

    def sub(path: str, field: str) -> str:
        return replace_placeholders(path, context, field, entity.name)

    sink = entity.sink
    accepted = sink.accepted.model_copy(
        update={"path": sub(sink.accepted.path, "entities.sink.accepted.path")}
    )
    rejected, archive = sink.rejected, sink.archive
    if rejected is not None:
        rejected = rejected.model_copy(
            update={"path": sub(rejected.path, "entities.sink.rejected.path")}
        )
    if archive is not None:
        archive = archive.model_copy(
            update={"path": sub(archive.path, "entities.sink.archive.path")}
        )
    return entity.model_copy(
        update={
            "source": entity.source.model_copy(
                update={"path": sub(entity.source.path, "entities.source.path")}
            ),
            "sink": sink.model_copy(
                update={"accepted": accepted, "rejected": rejected, "archive": archive}
            ),
        }
    )


def _apply_templates(
    config: RootConfig, config_dir: Path, profile_vars: Vars
) -> RootConfig:
    env = config.env
    variables = build_env_vars(
        config_dir, env.file if env else None, env.vars if env else {}, profile_vars
    )
    domains: list[Any] = []
    lookup: dict[str, str | None] = {}
    for domain in config.domains:
        resolved = (
            None
            if domain.incoming_dir is None
            else replace_placeholders(
                domain.incoming_dir, variables, "domains.incoming_dir"
            )
        )
        domains.append(domain.model_copy(update={"resolved_incoming_dir": resolved}))
        if domain.name in lookup:
            raise FloeConfigError(f"duplicate domain name {domain.name}")
        lookup[domain.name] = resolved

    report = _fill(config.report, ("path",), variables, "report.")
    lineage = config.lineage
    if lineage is not None:
        lineage = _fill(
            lineage,
            ("url", "endpoint", "api_key", "dataset_namespace"),
            variables,
            "lineage.",
        )
    storages = config.storages
    if storages is not None:
        names = ("bucket", "region", "account", "container", "prefix", "endpoint")
        definitions = [
            _fill(d, names, variables, f"storages.definitions.{d.name}.")
            for d in storages.definitions
        ]
        storages = storages.model_copy(update={"definitions": definitions})
    return config.model_copy(
        update={
            "domains": domains,
            "report": report,
            "lineage": lineage,
            "storages": storages,
            "entities": [_fill_entity(e, variables, lookup) for e in config.entities],
        }
    )


def parse_config(path: Path, profile_vars: Vars | None = None) -> RootConfig:
    """Load a config and its includes, then apply the template pass."""
    doc = load_single_doc(path)
    config_files = [path]
    origins = assemble_includes(doc, path.parent, config_files)
    config = parse_root(doc, origins).model_copy(update={"config_files": config_files})
    return _apply_templates(config, path.parent, profile_vars or {})
