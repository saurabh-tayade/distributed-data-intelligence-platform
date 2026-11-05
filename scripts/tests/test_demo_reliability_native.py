"""Spark-free demo selection acceptance. Requires pinned requirements-analytics.txt."""
import importlib.util
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("operations_demo_native", Path(__file__).resolve().parents[1] / "local-operations-demo.py")
demo = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(demo)


class ReliabilityDemoNativeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import duckdb
        if duckdb.__version__ != "1.4.5":
            raise RuntimeError("Run with the pinned DuckDB 1.4.5 analytics environment")
        cls.path = list(sys.path)
        sys.path[:0] = [str(demo.REPO / "processing"), str(demo.REPO / "processing/tests")]
        cls.addClassCleanup(lambda: sys.path.__setitem__(slice(None), cls.path))

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.selection = demo.prepare_reliability_analytics(self.root)

    def test_selected_literal_fixture_reports_pages_filters_and_pre_epoch_without_writes(self):
        before = demo.file_inventory(self.root)
        verified = demo.verify_reliability_analytics(self.selection)
        self.assertEqual(before, demo.file_inventory(self.root))
        self.assertGreater(verified["unchangedFiles"], 0)
        reports = {scenario["name"]: scenario["response"] for scenario in verified["scenarios"] if scenario["report"] == "requests_5m"}
        self.assertEqual(list(reports), [name for name, _ in demo.reliability_scenarios()])
        self.assertEqual([row["requestCount"] for row in reports["default"]["data"]], ["2", "1"])
        self.assertTrue(reports["first-page"]["meta"]["hasMore"])
        self.assertFalse(reports["final-page"]["meta"]["hasMore"])
        self.assertIsNone(reports["final-page"]["data"][0]["ownerTeam"])
        self.assertEqual(reports["empty-page"]["data"], [])
        self.assertEqual(reports["empty-filter"]["data"], [])
        self.assertEqual(len(reports["filtered"]["data"]), 1)
        old = reports["pre-epoch"]["data"][0]
        self.assertEqual(old["windowStart"], "1969-12-31T23:55:00.000000Z")
        self.assertEqual(old["referenceStatus"], "MISSING")
        self.assertIsNone(old["serviceName"])
        self.assertTrue(all(report["publication"] == self.selection["publication"] and report["origin"] == "synthetic" for report in reports.values()))

    def test_coverage_reports_preserve_g1_scope_totals_and_reference_edges(self):
        before = demo.file_inventory(self.root)
        verified = demo.verify_reliability_analytics(self.selection)
        self.assertEqual(before, demo.file_inventory(self.root))
        self.assertEqual(verified["inventory"], demo.selection_inventory(self.selection))
        reports = {(row["report"], row["name"]): row["response"] for row in verified["scenarios"]}
        inputs = reports["input_coverage", "default"]
        self.assertEqual(len(inputs["data"]), 1)
        self.assertEqual(inputs["totals"]["physicalOccurrenceCount"], "10")
        self.assertEqual(inputs["totals"]["validOccurrenceCount"], "5")
        self.assertEqual(inputs["totals"]["rejectedOccurrenceCount"], "5")
        self.assertEqual(inputs["totals"]["distinctValidIdentityCount"], "4")
        self.assertEqual(inputs["totals"]["duplicateExcessInScope"], "1")
        self.assertEqual(inputs["data"][0]["rejectedReasonCounts"]["FIELD_NULL"], "2")
        self.assertEqual(inputs["data"][0]["rejectedReasonCounts"]["FIELD_VALUE"], "3")
        for report in ("input_coverage", "reference_coverage"):
            default = reports[report, "default"]
            empty = reports[report, "empty-page"]
            self.assertEqual(default["totals"], empty["totals"])
            self.assertEqual(empty["data"], [])
            self.assertFalse(empty["meta"]["hasMore"])
            for row in [x for x in verified["scenarios"] if x["report"] == report]:
                self.assertEqual(default["publication"], row["response"]["publication"])
                self.assertEqual(default["reference"], row["response"]["reference"])
        self.assertEqual(reports["input_coverage", "empty-source"]["totals"]["inputBatchCount"], "0")
        coverage = reports["reference_coverage", "default"]
        self.assertEqual(coverage["totals"]["eventCount"], "4")
        self.assertEqual(coverage["totals"]["unusedReferenceServiceCount"], "1")
        self.assertEqual([row["referenceUsage"] for row in coverage["data"]], ["UNUSED", "USED", "USED", "ABSENT"])
        optional = reports["reference_coverage", "optional-null"]["data"][0]
        self.assertIsNone(optional["ownerTeam"])
        self.assertEqual(optional["missingOptionalAttributes"], ["owner_team", "service_tier"])
        self.assertIsNone(reports["reference_coverage", "missing"]["data"][0]["serviceName"])
        self.assertEqual(reports["reference_coverage", "empty-filter"]["totals"]["eventCount"], "0")

    def test_wrong_identity_and_damaged_publication_or_dependencies_fail_closed(self):
        import reliability_format_metadata as metadata
        for case in ("wrong-identity", "missing-marker", "corrupt-parquet", "missing-batch", "corrupt-reference"):
            with self.subTest(case=case):
                directory = self.root / case
                shutil.copytree(self.root / "reliability-analytics", directory)
                selected = self.selection | dict(root=str(directory / "output"), batchRoot=str(directory / "batches"), referenceRoot=str(directory / "references"))
                if case == "wrong-identity":
                    selected["publication"] = "0" * 64
                elif case == "missing-marker":
                    next((directory / "output").rglob("complete.json")).unlink()
                elif case == "corrupt-parquet":
                    next((directory / "output").rglob("*.parquet")).write_bytes(b"invalid")
                elif case == "missing-batch":
                    shutil.rmtree(directory / "batches")
                else:
                    next((directory / "references").rglob("complete.json")).write_bytes(b"invalid")
                before = demo.file_inventory(directory)
                with self.assertRaises(metadata.ReliabilityFailure):
                    demo.verify_reliability_analytics(selected)
                self.assertEqual(before, demo.file_inventory(directory))

    def test_failed_preparation_never_records_verified_selection(self):
        failed_root = self.root / "failed"
        failed_root.mkdir()
        with patch.object(demo, "verify_reliability_analytics", side_effect=RuntimeError("verification failed")):
            with self.assertRaisesRegex(RuntimeError, "verification failed"):
                demo.prepare_reliability_analytics(failed_root)
        self.assertFalse((failed_root / "reliability-analytics-verification.json").exists())


if __name__ == "__main__":
    unittest.main()
