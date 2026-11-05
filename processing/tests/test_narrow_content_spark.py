"""Actual Spark producer integration, separate from local lifecycle doubles.

Requires the supported pinned processing CI runtime. Dependencies are direct
native-consumer fixtures, not an end-to-end Bronze/Silver production proof.
One class-scoped Spark session bounds additional startup cost.
"""
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import content_analytics as analytics
import content_dependencies as dependencies
import content_metadata as m
import narrow_content_profile as profile
import narrow_content_publication as publisher
import narrow_content_reports as reports
from content_fixtures import article_rows
from narrow_content_test_fixtures import catalog_row, inputs, snapshot
from publication_metadata import canonical_bytes


@unittest.skipUnless(importlib.util.find_spec('pyspark'), 'requires pinned Spark processing runtime')
class SparkPrecompletionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from silver import session
        import content_publication
        cls.native = content_publication
        cls.spark = session()
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)

    def bounded_inputs(self, extra):
        row = catalog_row(article_rows()[0])
        row['title'] = '\n\t"\\é🙂'
        row['title'] += 'x' * (profile.REPORT_ROW_LIMIT - len(canonical_bytes(analytics.public(row))) + extra)
        return inputs(self.base, change=lambda rows: rows[0].update(title=row['title']))

    def verify(self, args, fingerprint):
        return reports.verify_publication_v1(args['output_root'], fingerprint,
            gold_root=args['gold_root'], reference_root=args['reference_root'])

    def test_exact_boundary_publishes_native_compatible_completion(self):
        args = self.bounded_inputs(0)
        with patch.object(self.native, '_verify', wraps=self.native._verify) as readback:
            result = publisher.publish_v1(self.spark, **args)
        readback.assert_called_once()
        self.assertEqual(result['status'], 'published')
        directory = m.publication_directory(args['output_root'], result['product_fingerprint'])
        self.assertEqual(set(p.name for p in directory.iterdir()), set(analytics.TOP))
        before = snapshot(self.base)
        replay = self.native.publish(self.spark, **args)
        self.assertEqual(dict(replay), dict(result) | {'status': 'replayed'})
        self.assertEqual(self.verify(args, result['product_fingerprint'])['report_counts']['catalog'], 4)
        self.assertEqual(snapshot(self.base), before)
        with analytics.open_publication(args['output_root'], result['product_fingerprint'],
                gold_root=args['gold_root'], reference_root=args['reference_root']) as opened:
            self.assertEqual(max(len(canonical_bytes(row)) for row in opened.report('catalog')['rows']), 61440)

    def test_plus_one_rejects_before_markers_and_legacy_still_accepts(self):
        args = self.bounded_inputs(1)
        dependency = dependencies.verify_dependencies(**args)
        with self.assertRaises(m.ContentFailure) as caught:
            publisher.publish_v1(self.spark, **args)
        self.assertEqual(caught.exception.code, 'RESOURCE_LIMIT')
        directory = m.publication_directory(args['output_root'], dependency.fingerprint)
        self.assertTrue(directory.is_dir())
        self.assertFalse((directory / 'complete.tmp').exists())
        self.assertFalse((directory / 'complete.json').exists())
        legacy_root = self.base / 'legacy'
        legacy_root.mkdir()
        legacy_args = args | {'output_root': legacy_root}
        result = self.native.publish(self.spark, **legacy_args)
        self.assertEqual(result['product_fingerprint'], dependency.fingerprint)
        before = snapshot(self.base)
        for operation in (lambda: publisher.publish_v1(self.spark, **legacy_args),
                          lambda: self.verify(legacy_args, dependency.fingerprint)):
            with self.assertRaises(m.ContentFailure) as caught:
                operation()
            self.assertEqual(caught.exception.code, 'RESOURCE_LIMIT')
        self.assertEqual(snapshot(self.base), before)

    def test_empty_catalog_still_scans_native_reference_only_rows(self):
        args = inputs(self.base, empty=True)
        with patch.object(profile, 'validate_report_row', wraps=profile.validate_report_row) as checked:
            result = publisher.publish_v1(self.spark, **args)
        self.assertEqual(checked.call_count, 3)
        self.assertTrue(all(call.args[0] == 'reference_coverage' and call.args[1]['reference_usage'] == 'UNUSED'
                            for call in checked.call_args_list))
        self.assertEqual(dict(self.verify(args, result['product_fingerprint'])['report_counts']),
                         dict(catalog=0, reference_coverage=3, source_discovery_daily=0))


if __name__ == '__main__':
    unittest.main()
