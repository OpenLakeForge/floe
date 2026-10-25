import subprocess
import sys
from pathlib import Path

EXAMPLE = Path(__file__).parents[2] / "example" / "standalone" / "config.yml"


def test_python_m_floe_matches_the_console_script() -> None:
    args = ["validate", "-c", str(EXAMPLE)]
    module = subprocess.run([sys.executable, "-m", "floe", *args], check=False, capture_output=True, text=True)  # fmt: skip
    script = subprocess.run([str(Path(sys.executable).parent / "floe"), *args], check=False, capture_output=True, text=True)  # fmt: skip
    assert (module.returncode, module.stdout, module.stderr) == (script.returncode, script.stdout, script.stderr)  # fmt: skip
    assert module.returncode == 0, module.stderr
