from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from airflow_floe.manifest_discovery import discover_manifest_dag_specs


class ManifestDiscoveryTests(unittest.TestCase):
    def test_discover_manifest_dag_specs_returns_sorted_specs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "sales.manifest.json").write_text("{}", encoding="utf-8")
            (base / "hr.manifest.json").write_text("{}", encoding="utf-8")
            (base / "ignore.json").write_text("{}", encoding="utf-8")

            specs = discover_manifest_dag_specs(str(base))

            self.assertEqual([spec.dag_id for spec in specs], ["floe_hr", "floe_sales"])
            self.assertEqual(
                [Path(spec.manifest_path).name for spec in specs],
                ["hr.manifest.json", "sales.manifest.json"],
            )

    def test_discover_manifest_dag_specs_handles_sanitized_id_collisions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "sales-data.manifest.json").write_text("{}", encoding="utf-8")
            (base / "sales_data.manifest.json").write_text("{}", encoding="utf-8")

            specs = discover_manifest_dag_specs(str(base))
            self.assertEqual(
                [spec.dag_id for spec in specs], ["floe_sales_data", "floe_sales_data_2"]
            )

    def test_discover_manifest_dag_specs_returns_empty_for_missing_dir(self) -> None:
        specs = discover_manifest_dag_specs("/definitely/missing/path")
        self.assertEqual(specs, [])


if __name__ == "__main__":
    unittest.main()
