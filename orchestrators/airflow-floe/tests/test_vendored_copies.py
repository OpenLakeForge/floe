"""airflow-floe vendors two modules from dagster-floe (canonical); they must stay byte-identical."""

from __future__ import annotations

import unittest
from pathlib import Path

_PKG = Path(__file__).resolve().parents[1] / "src" / "airflow_floe"
_CANONICAL_PKG = Path(__file__).resolve().parents[2] / "dagster-floe" / "src" / "floe_dagster"


@unittest.skipUnless(
    _CANONICAL_PKG.is_dir(), "dagster-floe is not next to this package (installed sdist)"
)
class VendoredCopiesTest(unittest.TestCase):
    def test_copies_match_dagster_canonical(self) -> None:
        for name in ("databricks_client.py", "k8s_status.py"):
            with self.subTest(module=name):
                self.assertEqual(
                    (_PKG / name).read_bytes(),
                    (_CANONICAL_PKG / name).read_bytes(),
                    f"{name} differs from the dagster-floe copy; edit the dagster one and copy it here",
                )


if __name__ == "__main__":
    unittest.main()
