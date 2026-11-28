import subprocess
import sys
from pathlib import Path

SCRIPT = Path(__file__).parents[2] / "scripts" / "assert_tests_ran.py"


def _run(tmp_path: Path, cases: str) -> subprocess.CompletedProcess[str]:
    report = tmp_path / "junit.xml"
    report.write_text(f"<testsuites><testsuite>{cases}</testsuite></testsuites>")
    return subprocess.run(
        [sys.executable, str(SCRIPT), str(report)],
        check=False,
        capture_output=True,
        text=True,
    )


def test_all_skipped_fails(tmp_path: Path) -> None:
    result = _run(tmp_path, '<testcase name="a"><skipped/></testcase>')
    assert result.returncode == 1
    assert "no integration test ran" in result.stdout


def test_none_collected_fails(tmp_path: Path) -> None:
    assert _run(tmp_path, "").returncode == 1


def test_one_ran_passes(tmp_path: Path) -> None:
    result = _run(
        tmp_path, '<testcase name="a"><skipped/></testcase><testcase name="b"/>'
    )
    assert result.returncode == 0
