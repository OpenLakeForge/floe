"""`parse_config`, ported from `config/parse.rs`."""

from pathlib import Path

from floe.config.project import RootConfig, parse_root
from floe.config.yaml import load_single_doc


def parse_config(path: Path) -> RootConfig:
    """Load a config. ponytail: the template pass (`apply_templates_with_vars`) is not ported yet."""
    config = parse_root(load_single_doc(path))
    return config.model_copy(update={"config_files": [path]})
