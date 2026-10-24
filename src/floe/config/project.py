"""Project root, domains and named sources, ported from `config/parse.rs` (`parse_root`, `parse_domains`, `parse_sources`)."""

import re
from pathlib import Path
from typing import cast

from pydantic import BaseModel

from floe.config.entity import parse_entity
from floe.config.storage import (
    CatalogsConfig,
    StoragesConfig,
    parse_catalogs,
    parse_storages,
)
from floe.config.types import EntityConfig
from floe.config.vars import string_map
from floe.config.yaml import (
    get_array,
    get_string,
    merge_yaml,
    opt_string,
    opt_vec_string,
    validate_known_keys,
    yaml_array,
    yaml_hash,
    yaml_string,
)
from floe.errors import FloeConfigError, FloeError

Hash = dict[object, object]
V0_3 = (0, 3)
ROOT_KEYS = (
    *("version", "metadata", "storages", "filesystems", "catalogs", "env"),
    *("domains", "sources", "report", "lineage", "entities"),
)
SOURCE_KEYS = ("format", "path", "storage", "filesystem", "options", "cast_mode")


class ProjectMetadata(BaseModel):
    project: str | None
    description: str | None
    owner: str | None
    tags: list[str] | None


class EnvConfig(BaseModel):
    file: str | None
    vars: dict[str, str]


class DomainConfig(BaseModel):
    name: str
    incoming_dir: str | None
    defaults: Hash | None  # raw; `validate_domains` rejects the keys D7 forbids


class RootConfig(BaseModel):
    # report and lineage stay raw until their parsers are wired in
    version: str
    metadata: ProjectMetadata | None
    storages: StoragesConfig | None
    catalogs: CatalogsConfig | None
    env: EnvConfig | None
    domains: list[DomainConfig]
    report: object
    lineage: object
    entities: list[EntityConfig]
    config_files: list[Path]


_VERSION = re.compile(r"\+?0*([0-9]{1,20})\.\+?0*([0-9]{1,20})")


def config_version(raw: str) -> tuple[int, int]:
    match = _VERSION.fullmatch(raw)
    version = (int(match[1]), int(match[2])) if match else (2**64, 0)
    if max(version) >= 2**64:
        raise FloeConfigError(
            f'root.version={raw} is invalid; expected numeric major.minor format like "0.1"'
        )
    return version


def _field(value: object, key: str) -> object:
    return cast(Hash, value).get(key) if isinstance(value, dict) else None


def _hint(entity: object, key: str) -> str | None:
    value = _field(entity, key)
    return value if isinstance(value, str) else None


def parse_domain(item: object, version: str) -> DomainConfig:
    hash = yaml_hash(item, "domains")
    validate_known_keys(hash, "domains", ("name", "incoming_dir", "defaults"))
    name = get_string(hash, "name", "domains")
    defaults = None
    if "defaults" in hash:
        if config_version(version) < V0_3:
            raise FloeConfigError('domains.defaults requires root.version >= "0.3"')
        defaults = yaml_hash(hash["defaults"], "domains.defaults")
    incoming_dir = opt_string(hash, "incoming_dir", "domains")
    if incoming_dir is None and config_version(version) < V0_3:
        raise FloeConfigError("missing required field domains.incoming_dir")
    return DomainConfig(name=name, incoming_dir=incoming_dir, defaults=defaults)


def parse_sources(value: object, version: str) -> dict[str, Hash]:
    if config_version(version) < V0_3:
        raise FloeConfigError('root.sources requires root.version >= "0.3"')
    sources: dict[str, Hash] = {}
    for item in yaml_array(value, "sources"):
        hash = dict(yaml_hash(item, "sources"))
        validate_known_keys(hash, "sources", ("name", *SOURCE_KEYS))
        name = get_string(hash, "name", "sources")
        del hash["name"]
        if name in sources:
            raise FloeConfigError(f"duplicate source name {name}")
        sources[name] = hash
    return sources


def resolve_source_ref(
    entity: object, sources: dict[str, Hash]
) -> tuple[Hash, str] | None:
    source = _field(entity, "source")
    if not isinstance(source, dict) or "ref" not in source:
        return None
    source = cast(Hash, source)
    overlay = {k: v for k, v in source.items() if k != "ref"}
    name = yaml_string(source["ref"], "source.ref")
    if name not in sources:
        available = ", ".join(sources)
        raise FloeConfigError(
            f"source.ref={name} is not a declared source (available: {available})"
        )
    if "resource" in overlay:
        resource = yaml_string(overlay.pop("resource"), "source.resource")
    else:
        resource = _hint(entity, "name") or ""
    resolved = dict(cast(Hash, entity))
    resolved.pop("source")  # a re-inserted key moves to the back, as in hashlink
    resolved["source"] = merge_yaml(sources[name], overlay)
    return resolved, resource


def _merge_entity(
    entity: object, sources: dict[str, Hash], defaults: dict[str, Hash]
) -> object:
    resource = None
    if resolved := resolve_source_ref(entity, sources):
        entity, resource = resolved
    domain = _hint(entity, "domain")
    merged = merge_yaml(defaults[domain], entity) if domain in defaults else entity
    source = _field(merged, "source") if resource is not None else None
    if isinstance(source, dict) and isinstance(cast(Hash, source).get("path"), str):
        # `{{resource}}` is entity-specific; every other placeholder is left for the template pass
        source = cast(Hash, source)
        source["path"] = re.sub(
            r"\{\{(.*?)\}\}",
            lambda m: str(resource) if m[1].strip() == "resource" else m[0],
            cast(str, source["path"]),
            flags=re.DOTALL,
        )
    return merged


def parse_entities(
    entities: list[object], defaults: dict[str, Hash], sources: dict[str, Hash]
) -> list[EntityConfig]:
    result: list[EntityConfig] = []
    for index, entity in enumerate(entities):
        try:
            result.append(parse_entity(_merge_entity(entity, sources, defaults)))
        except FloeError as error:
            name = _hint(entity, "name")
            who = f"entities[{index}]" + (
                "" if name is None else f" (entity.name={name})"
            )
            raise FloeConfigError(f"{who}: {error}") from error
    return result


def parse_root(doc: object) -> RootConfig:
    root = yaml_hash(doc, "root")
    validate_known_keys(root, "root", ROOT_KEYS)
    version = get_string(root, "version", "root")
    metadata = None
    if "metadata" in root:
        meta = yaml_hash(root["metadata"], "metadata")
        metadata = ProjectMetadata(
            project=opt_string(meta, "project", "metadata"),
            description=opt_string(meta, "description", "metadata"),
            owner=opt_string(meta, "owner", "metadata"),
            tags=opt_vec_string(meta, "tags", "metadata"),
        )
    if "storages" in root and "filesystems" in root:
        raise FloeConfigError(
            "root.storages and root.filesystems are mutually exclusive"
        )
    key = "storages" if "storages" in root else "filesystems"
    storages = parse_storages(root[key]) if key in root else None
    env = None
    if "env" in root:
        env_hash = yaml_hash(root["env"], "env")
        validate_known_keys(env_hash, "env", ("file", "vars"))
        env_vars = (
            string_map(env_hash["vars"], "env.vars") if "vars" in env_hash else {}
        )
        env = EnvConfig(file=opt_string(env_hash, "file", "env"), vars=env_vars)
    catalogs = parse_catalogs(root["catalogs"]) if "catalogs" in root else None
    items = yaml_array(root["domains"], "domains") if "domains" in root else []
    domains = [parse_domain(item, version) for item in items]
    defaults = {d.name: d.defaults for d in domains if d.defaults is not None}
    sources = parse_sources(root["sources"], version) if "sources" in root else {}
    entities = parse_entities(get_array(root, "entities", "root"), defaults, sources)
    try:
        qualified = config_version(version) >= V0_3
    except FloeConfigError:
        qualified = False
    entities = [
        e.model_copy(update={"id": f"{e.domain}.{e.name}"})
        if qualified and e.domain is not None
        else e
        for e in entities
    ]
    return RootConfig(
        version=version,
        metadata=metadata,
        storages=storages,
        catalogs=catalogs,
        env=env,
        domains=domains,
        report=root.get("report"),
        lineage=root.get("lineage"),
        entities=entities,
        config_files=[],
    )


def validate_domains(config: RootConfig) -> None:
    """D7 (#584 S6, S7): Rust 0.7 accepts these at validate time; Python rejects them."""
    qualified = config_version(config.version) >= V0_3
    for domain in config.domains:
        if qualified and (not domain.name or any(c in domain.name for c in "./\\")):
            raise FloeConfigError(
                f"domain.name={domain.name} must be non-empty without '.', '/' or '\\' "
                'when root.version >= "0.3"'
            )
        defaults = domain.defaults or {}
        schema = defaults.get("schema")
        forbidden = ["name"] if "name" in defaults else []
        if isinstance(schema, dict):
            forbidden += [
                f"schema.{k}" for k in ("columns", "primary_key") if k in schema
            ]
        if forbidden:
            raise FloeConfigError(f"unknown field domains.defaults.{forbidden[0]}")
