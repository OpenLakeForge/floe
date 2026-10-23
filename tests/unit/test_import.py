from importlib.metadata import version

import floe


def test_version_matches_metadata() -> None:
    assert floe.__version__ == version("floe-python")
