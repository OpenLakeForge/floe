from pathlib import Path

import pytest

_NAME = "floe.manifest.schema.json"
_CANONICAL = Path(__file__).resolve().parents[2] / "schemas" / _NAME
_VENDORED = Path(__file__).resolve().parents[1] / "src" / "floe_dagster" / "schemas" / _NAME


@pytest.mark.skipif(not _CANONICAL.is_file(), reason="orchestrators/schemas is not next to this package (sdist)")
def test_vendored_manifest_schema_matches_canonical():
    assert _VENDORED.read_bytes() == _CANONICAL.read_bytes(), (
        f"{_VENDORED} differs from {_CANONICAL}; copy the canonical file over it"
    )
