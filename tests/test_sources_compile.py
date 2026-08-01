"""Dependency-free smoke tests for the public code sample."""

from __future__ import annotations

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class RepositorySmokeTests(unittest.TestCase):
    def test_python_sources_compile(self) -> None:
        for source in (ROOT / "dags").glob("*.py"):
            compile(source.read_text(encoding="utf-8"), str(source), "exec")

    def test_expected_dataform_files_exist(self) -> None:
        expected = {
            "dataform/workflow_settings.yaml",
            "dataform/definitions/sources/raw_orders.sqlx",
            "dataform/definitions/staging/stg_orders.sqlx",
            "dataform/definitions/marts/fact_orders.sqlx",
            "dataform/definitions/assertions/assert_no_negative_revenue.sqlx",
        }
        missing = [path for path in expected if not (ROOT / path).is_file()]
        self.assertFalse(missing, f"Missing expected files: {missing}")


if __name__ == "__main__":
    unittest.main()
