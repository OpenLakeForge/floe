"""Profile keys that were parsed and never applied are refused (#1009)."""

from pathlib import Path

import pytest

from floe.config.profile import parse_profile
from floe.errors import FloeConfigError


def test_validation_strict_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "profile.yml"
    path.write_text(
        "apiVersion: floe/v1\nkind: EnvironmentProfile\nmetadata: {name: n}\nvalidation: {strict: true}\n"
    )
    with pytest.raises(
        FloeConfigError,
        match=r"profile.validation.strict is not supported \(see CHANGELOG\)",
    ):
        parse_profile(path)
