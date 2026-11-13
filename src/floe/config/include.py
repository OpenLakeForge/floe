"""Include assembly, ported from `config/parse.rs` (`assemble_includes`)."""

import os
from pathlib import Path
from typing import cast

from floe.config.project import V0_3, Hash, IncludeOrigins, at_file, config_version
from floe.config.vars import resolve_local_path
from floe.config.yaml import (
    get_string,
    get_value,
    load_single_doc,
    opt_string,
    validate_known_keys,
    yaml_array,
    yaml_hash,
    yaml_string,
)
from floe.errors import FloeConfigError
from floe.glob import Pattern, glob


def glob_base(directory: Path) -> str:
    """Rust `glob_base`: escaping would turn a Windows verbatim prefix's `?` into `[?]`."""
    text = str(directory)
    if text.startswith("\\\\?\\UNC\\"):
        text = "\\\\" + text[8:]
    else:
        text = text.removeprefix("\\\\?\\")
    return Pattern.escape(text)


def _entity_files(domain: Hash, domain_file: Path) -> list[Path]:
    """Its `entities` glob (#1213) if the domain file sets one, else its sibling `*.yml` files."""
    directory = domain_file.parent
    if "entities" not in domain:
        files = [p for p in directory.iterdir() if p.suffix == ".yml"]
    else:
        pattern = yaml_string(domain.pop("entities"), "domains.entities")
        files = [Path(m) for m in glob(os.path.join(glob_base(directory), pattern))]
        if not files:
            raise FloeConfigError(
                f"domains.entities pattern {pattern!r} matched no file"
            )
    return sorted(p for p in files if p.resolve() != domain_file.resolve())


def assemble_includes(
    doc: object, config_dir: Path, config_files: list[Path]
) -> IncludeOrigins:
    """Splice included sources, domains and entities into `doc` so parsing sees one document."""
    if not isinstance(doc, dict) or "include" not in doc:
        return IncludeOrigins()
    root = cast(Hash, doc)
    include = root.pop("include")
    version = root.get("version")
    try:
        supported = isinstance(version, str) and config_version(version) >= V0_3
    except FloeConfigError:
        supported = False
    if not supported:
        raise FloeConfigError('root.include requires root.version >= "0.3"')
    include = yaml_hash(include, "include")
    validate_known_keys(include, "include", ("domains", "sources"))
    sources_path = None
    if "sources" in include:
        if "sources" in root:
            raise FloeConfigError(
                "root.sources and include.sources are mutually exclusive"
            )
        sources_path = resolve_local_path(
            config_dir, yaml_string(include["sources"], "include.sources")
        )
        with at_file(sources_path):
            sources_root = yaml_hash(load_single_doc(sources_path), "sources")
            validate_known_keys(sources_root, "root", ("sources",))
            root["sources"] = get_value(sources_root, "sources", "root")
        config_files.append(sources_path)
    if "domains" in include:
        patterns = yaml_array(include["domains"], "include.domains")
    elif sources_path is not None:
        patterns = []
    else:
        raise FloeConfigError("missing required field include.domains")
    domains: list[tuple[object, Path]] = []
    entities: list[tuple[object, Path]] = []
    for raw in patterns:
        pattern = yaml_string(raw, "include.domains")
        matches = glob(os.path.join(glob_base(config_dir), pattern))
        if not matches:
            raise FloeConfigError(
                f"include.domains pattern {pattern!r} matched no file "
                "(includes resolve against local files only; a remote project cannot include)"
            )
        for match in matches:
            domain_file = Path(match)
            with at_file(domain_file):
                domain = yaml_hash(load_single_doc(domain_file), "domains")
                name = get_string(domain, "name", "domains")
                entity_files = _entity_files(domain, domain_file)
            config_files.append(domain_file)
            domains.append((domain, domain_file))
            # D4: an entity's name comes from its YAML, never from its file name, so `con.yml` loads like any other
            for entity_file in entity_files:
                with at_file(entity_file):
                    loaded = load_single_doc(entity_file)
                    if not isinstance(loaded, dict):
                        raise FloeConfigError("expected map at entity")
                    entity = cast(Hash, loaded)
                    declared = opt_string(entity, "domain", "entity")
                if declared is None:
                    entity.pop("domain", None)  # a re-inserted key moves to the back
                    entity["domain"] = name
                elif declared != name:
                    raise FloeConfigError(
                        f"{entity_file} has domain {declared!r} but its _domain.yml names {name!r}"
                    )
                config_files.append(entity_file)
                entities.append((entity, entity_file))
    origins = IncludeOrigins(root=config_files[0], sources=sources_path)
    for key, items, item_origins in (
        ("domains", domains, origins.domains),
        ("entities", entities, origins.entities),
    ):
        array = root.setdefault(key, [])
        if not isinstance(array, list):
            raise FloeConfigError(f"expected array at {key}")
        item_origins += [None] * len(cast("list[object]", array))
        for item, file in items:
            cast("list[object]", array).append(item)
            item_origins.append(file)
    return origins
