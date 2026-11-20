"""Profile keys that were parsed and never applied are refused (#1009)."""

from pathlib import Path

import pytest

from floe.config.parse import parse_config
from floe.config.profile import apply_profile, parse_profile
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


PROFILE = "apiVersion: floe/v1\nkind: EnvironmentProfile\nmetadata: {name: n}\n"


def report_under_profile(tmp_path: Path, report: str, profile: str) -> tuple[str, str | None]:  # fmt: skip
    (tmp_path / "config.yml").write_text(f'version: "0.3"\nentities: []\n{report}')
    (tmp_path / "profile.yml").write_text(PROFILE + profile)
    root = parse_config(tmp_path / "config.yml", {})
    fixed = apply_profile(root, parse_profile(tmp_path / "profile.yml")).report
    return fixed.path, fixed.storage


@pytest.mark.parametrize(
    ("report", "profile", "expected"),
    [
        ("report: {path: team_a}\n", "report: {storage: ops, prefix: stage/}\n", ("stage/team_a", "ops")),
        ("report: {path: ./a/b, storage: ops}\n", "report: {storage: ops, prefix: stage}\n", ("stage/./a/b", "ops")),
        ("", "report: {storage: ops, prefix: stage}\n", ("stage/report", "ops")),
        ("report: {path: team_a}\n", "report: {storage: ops}\n", ("team_a", "ops")),
        ("report: {path: /abs, storage: x}\n", "", ("/abs", "x")),
    ],
)  # fmt: skip
def test_1237_profile_report_prefixes_the_project_path(
    tmp_path: Path, report: str, profile: str, expected: tuple[str, str | None]
) -> None:
    assert report_under_profile(tmp_path, report, profile) == expected


@pytest.mark.parametrize(
    ("report", "message"),
    [
        ("report: {path: ../other}\n", "report.path must be a relative path under the profile's report prefix (got ../other)"),
        ("report: {path: a/../../b}\n", "report.path must be a relative path under the profile's report prefix (got a/../../b)"),
        ("report: {path: /abs}\n", "report.path must be a relative path under the profile's report prefix (got /abs)"),
        ("report: {path: 'a\\..\\..\\out'}\n", "report.path must be a relative path under the profile's report prefix (got a\\..\\..\\out)"),
        ("report: {path: 'C:\\out'}\n", "report.path must be a relative path under the profile's report prefix (got C:\\out)"),
        ("report: {path: 'C:out'}\n", "report.path must be a relative path under the profile's report prefix (got C:out)"),
        ("report: {path: 's3://b/k'}\n", "report.path must be a relative path under the profile's report prefix (got s3://b/k)"),
        ("report: {path: r, storage: work}\n", "report.storage=work differs from the profile's report.storage=ops"),
    ],
)  # fmt: skip
def test_1237_project_report_cannot_leave_the_profile_prefix(
    tmp_path: Path, report: str, message: str
) -> None:
    with pytest.raises(FloeConfigError) as err:
        report_under_profile(
            tmp_path, report, "report: {storage: ops, prefix: stage}\n"
        )
    assert str(err.value) == message


@pytest.mark.parametrize(
    ("report", "message"),
    [
        ("report: {prefix: p}\n", "profile.report.storage is required"),
        ("report: {storage: ops, path: p}\n", "profile.report"),
    ],
)
def test_1237_profile_report_keys(tmp_path: Path, report: str, message: str) -> None:
    (tmp_path / "profile.yml").write_text(PROFILE + report)
    with pytest.raises(FloeConfigError, match=message):
        parse_profile(tmp_path / "profile.yml")
