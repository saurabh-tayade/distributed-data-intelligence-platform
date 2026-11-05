"""Real native v2 producer integration; no stand-in DataFrames or verifiers."""
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path[:0] = [str(Path(__file__).resolve().parent), str(Path(__file__).resolve().parents[1])]
from narrow_content_test_fixtures import snapshot
from publication_metadata import canonical_bytes
import v2_publisher_native_fixtures as fixtures


@unittest.skipUnless(importlib.util.find_spec('pyspark'), 'requires pinned Spark processing runtime')
class NativeV2PublishersTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from silver import session
        os.environ['PYSPARK_PYTHON'] = sys.executable
        cls.spark = session()
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_real_publish_readback_replay_and_reference_only_empty(self):
        import gold_publication
        import content_publication
        import content_analytics
        import managed_gold
        import narrow_gold_publication_v2 as gold
        import narrow_content_publication_v2 as content
        from silver_inputs import load_inputs
        with tempfile.TemporaryDirectory() as directory:
            for empty in (False, True):
                with self.subTest(empty=empty), fixtures.source(self.spark, Path(directory) / str(empty), empty=empty) as case:
                    expected, gc = case['expected'], case['gold']
                    with patch.object(managed_gold, 'silver_check', wraps=managed_gold.silver_check) as checked:
                        gv = gold.publish_v2(self.spark, gc, expectations=expected)
                    self.assertEqual(gv['status'], 'VERIFIED')
                    self.assertTrue(any(len(call.args) > 1 and call.args[1] is self.spark for call in checked.call_args_list))
                    cc, args = fixtures.content_command(case, gv)
                    cv = content.publish_v2(self.spark, cc, expectations=expected)
                    self.assertEqual(cv['status'], 'VERIFIED')
                    self.assertEqual(cv['prepared'], cc.value()['prepared'])
                    self.assertEqual(len(fixtures.envelopes(gc, gv, cc, cv)), 31)
                    with content_analytics.open_publication(cc.value()['content']['root'], cv['prepared']['fingerprint'],
                            gold_root=args['gold_root'], reference_root=args['reference_root']) as opened:
                        catalog = opened.report('catalog')['rows']
                        coverage = opened.report('reference_coverage')['rows']
                        self.assertEqual(len(catalog), 0 if empty else 1)
                        self.assertEqual(len(coverage), 2)
                        if empty:
                            self.assertTrue(all(row['reference_usage'] == 'UNUSED' for row in coverage))
                    before = snapshot(case['base'])
                    self.assertEqual(gold.verify_publication_v2(self.spark, gc, expectations=expected), gv)
                    self.assertEqual(content.verify_publication_v2(self.spark, cc, expectations=expected), cv)
                    self.assertEqual(gold.publish_v2(self.spark, gc, expectations=expected), gv)
                    self.assertEqual(content.publish_v2(self.spark, cc, expectations=expected), cv)
                    selected = load_inputs(self.spark, gc.value()['silver']['root'],
                        markers=[gc.value()['inputs'][0]['qualification']['selection']['relativeAddress']])
                    self.assertEqual(gold_publication.publish(selected, gc.value()['gold']['root'])['status'], 'SKIPPED')
                    self.assertEqual(content_publication.publish(self.spark, output_root=cc.value()['content']['root'], **args)['status'], 'replayed')
                    self.assertEqual(snapshot(case['base']), before)
                    self.assertEqual(case['store'].writes, [])
                    self.assertTrue(case['store'].reads)
                    # Detached complete bodies preserve actual native evidence.
                    for name, _, raw in fixtures.envelopes(gc, gv, cc, cv):
                        self.assertEqual(canonical_bytes(json.loads(raw)), raw, name)


if __name__ == '__main__':
    unittest.main()
