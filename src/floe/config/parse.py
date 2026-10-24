"""`parse_config`, ported from `config/parse.rs`."""

from pathlib import Path

from floe.config.include import assemble_includes
from floe.config.project import RootConfig, parse_root
from floe.config.yaml import load_single_doc


def parse_config(path: Path) -> RootConfig:
    """Load a config and its includes. ponytail: the template pass is #728."""
    doc = load_single_doc(path)
    config_files = [path]
    origins = assemble_includes(doc, path.parent, config_files)
    config = parse_root(doc, origins)
    return config.model_copy(update={"config_files": config_files})
