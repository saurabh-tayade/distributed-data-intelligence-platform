"""Real Content publisher over synthetic Gold; not Bronze/Silver pipeline acceptance."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("operations_demo_content", Path(__file__).resolve().parents[1] / "local-operations-demo.py")
demo = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(demo)


class ContentDemoNativeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import duckdb
        import pyspark
        if duckdb.__version__ != "1.4.5" or pyspark.__version__ != "4.0.2":
            raise RuntimeError("Run with pinned DuckDB 1.4.5 and PySpark 4.0.2 dependencies")
        original_path = list(sys.path)
        sys.path[:0] = [str(demo.REPO / "processing"), str(demo.REPO / "processing/tests"), str(demo.REPO / "services/content-api/tests")]
        cls.addClassCleanup(lambda: sys.path.__setitem__(slice(None), original_path))
        # Existing literal fixture timestamps require Python and Spark UTC.
        environment = patch.dict(os.environ, TZ="UTC", PYSPARK_PYTHON=sys.executable, SPARK_LOCAL_IP="127.0.0.1")
        environment.start()
        cls.addClassCleanup(lambda: (environment.stop(), time.tzset()))
        time.tzset()
        cls.temporary = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temporary.cleanup)
        cls.root = Path(cls.temporary.name)
        cls.selection = demo.prepare_content_analytics(cls.root)

    def test_all_three_reports_keep_literal_identity_values_pages_and_totals(self):
        before = demo.file_inventory(self.root)
        verified = demo.verify_content_analytics(self.selection)
        self.assertEqual(before, demo.file_inventory(self.root))
        self.assertEqual(verified["inventory"], demo.selection_inventory(self.selection))
        self.assertGreater(verified["unchangedFiles"], 0)
        self.assertEqual(verified, json.loads((self.root / "content-analytics-verification.json").read_text()))
        for scenario in verified["scenarios"]:
            self.assertIn("limit", scenario["query"])
            self.assertEqual(scenario["response"]["meta"]["limit"], scenario["query"]["limit"])
        self.assertEqual(next(row for row in verified["scenarios"] if row["report"] == "catalog" and row["name"] == "source")["query"]["limit"], 100)
        reports = {(row["report"], row["name"]): row["response"] for row in verified["scenarios"]}
        catalog = reports["catalog", "default"]
        self.assertEqual([row["sourceId"] for row in catalog["data"]], ["gamma", "alpha", "beta", "alpha"])
        self.assertEqual(catalog["data"][1]["observationCount"], "2")
        self.assertEqual(catalog["data"][1]["lastObservedAt"], "2026-10-13T00:00:00.000001Z")
        self.assertEqual(catalog["data"][1]["qualityWarnings"], ["INVALID_PUBLISHED_AT", "INVALID_UPDATED_AT"])
        self.assertIsNone(catalog["data"][0]["publisherName"])
        self.assertIsNone(catalog["data"][2]["publisherCategory"])
        self.assertEqual(reports["catalog", "combined"]["filters"]["sourceId"], "alpha")
        self.assertEqual(len(reports["catalog", "inclusive-date"]["data"]), 2)
        for report in ("catalog", "source_discovery_daily", "reference_coverage"):
            self.assertEqual(reports[report, "empty-page"]["data"], [])
            self.assertEqual(reports[report, "empty-filter"]["data"], [])
            self.assertTrue(reports[report, "first-page"]["meta"]["hasMore"])
            self.assertFalse(reports[report, "final-page"]["meta"]["hasMore"])
        daily = reports["source_discovery_daily", "default"]["data"]
        self.assertEqual([row["discoveredArticleCount"] for row in daily], ["2", "1", "1"])
        self.assertEqual([row["discoveryDate"] for row in daily], ["2026-10-12", "2026-10-13", "2026-10-13"])
        self.assertEqual(reports["source_discovery_daily", "inclusive-date"]["data"], [daily[0]])
        coverage = reports["reference_coverage", "default"]
        self.assertEqual([row["referenceUsage"] for row in coverage["data"]], ["USED", "USED", "UNUSED", "ABSENT"])
        self.assertEqual(coverage["totals"]["articleCount"], "4")
        self.assertEqual(coverage["totals"]["matchedArticleCount"], "3")
        self.assertEqual(coverage["totals"]["missingArticleCount"], "1")
        self.assertEqual(coverage["totals"]["unusedReferenceSourceCount"], "1")
        self.assertEqual(coverage["totals"], reports["reference_coverage", "empty-page"]["totals"])
        self.assertEqual(reports["reference_coverage", "empty-filter"]["totals"]["articleCount"], "0")
        self.assertEqual(coverage["data"][1]["missingOptionalAttributes"], ["publisher_category", "declared_language", "country_code"])
        for response in reports.values():
            self.assertEqual(response["publication"], self.selection["publication"])
            self.assertEqual(response["goldPublication"], catalog["goldPublication"])
            self.assertEqual(response["reference"], catalog["reference"])

    def test_wrong_identity_and_damaged_publication_or_dependencies_fail_closed(self):
        import content_metadata as metadata
        for case in ("wrong-identity", "missing-marker", "corrupt-parquet", "missing-gold", "corrupt-reference"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary) / "fixture"
                shutil.copytree(self.root / "content-analytics", directory)
                selected = self.selection | dict(root=str(directory / "output"), goldRoot=str(directory / "gold"), referenceRoot=str(directory / "references"))
                if case == "wrong-identity":
                    selected["publication"] = "0" * 64
                elif case == "missing-marker":
                    next((directory / "output").rglob("complete.json")).unlink()
                elif case == "corrupt-parquet":
                    next((directory / "output").rglob("*.parquet")).write_bytes(b"invalid")
                elif case == "missing-gold":
                    shutil.rmtree(directory / "gold")
                else:
                    next((directory / "references").rglob("reference.csv")).write_bytes(b"invalid")
                before = demo.file_inventory(directory)
                with self.assertRaises(metadata.ContentFailure):
                    demo.verify_content_analytics(selected)
                self.assertEqual(before, demo.file_inventory(directory))

    def test_failed_preparation_never_records_verified_selection(self):
        # Reuse completed bytes only to inject the post-publication verification failure.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch("generate_content_fixtures.completed", return_value=self.selection["publication"]), \
                    patch.object(demo, "verify_content_analytics", side_effect=RuntimeError("verification failed")):
                with self.assertRaisesRegex(RuntimeError, "verification failed"):
                    demo.prepare_content_analytics(root, spark=object())
            self.assertFalse((root / "content-analytics-verification.json").exists())


if __name__ == "__main__":
    unittest.main()
