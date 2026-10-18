"""Generate with M2.4 once; stop Spark/remove Silver before any consumer tests."""
from contextlib import redirect_stdout
from datetime import datetime
from hashlib import sha256
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

import duckdb
from gold_analytics import ARTICLE_SCHEMA, DAILY_SCHEMA, main, open_publication
from gold_reader import AnalyticsFailure
from publication_metadata import canonical_bytes


class AnalyticsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from gold_fixtures import create_analytics_publications
        cls.base = tempfile.TemporaryDirectory()
        cls.gold_base, publications = create_analytics_publications(cls.base.name)
        cls.full, cls.first, cls.empty = (publications[k] for k in ('full', 'first', 'empty'))

    @classmethod
    def tearDownClass(cls):
        cls.base.cleanup()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "gold's snapshots"
        shutil.copytree(self.gold_base, self.root)
        self.directory = self.root / self.full

    def marker(self, change):
        path = self.directory / 'complete.json'
        value = json.loads(path.read_text())
        change(value)
        path.write_bytes(canonical_bytes(value))

    def refresh_inventory(self, table):
        files = []
        for path in sorted((self.directory / table).glob('*.parquet')):
            raw = path.read_bytes()
            files.append(dict(path=f'{table}/{path.name}', byte_length=len(raw), sha256=sha256(raw).hexdigest()))
        self.marker(lambda m: m['files'].update({table: files}))

    def rewrite(self, table, projection):
        # All SQL here is test-owned. Remove old CRC files when replacing a dataset.
        con = duckdb.connect()
        try:
            paths = [str(p) for p in (self.directory / table).glob('*.parquet')]
            con.read_parquet(paths, hive_partitioning=False).create_view('original')
            target = Path(self.tmp.name) / 'replacement.parquet'
            con.sql('SELECT ' + projection + ' FROM original').write_parquet(str(target))
            shutil.rmtree(self.directory / table)
            (self.directory / table).mkdir()
            shutil.move(target, self.directory / table / 'replacement.parquet')
        finally:
            con.close()
        self.refresh_inventory(table)

    def test_reports_metrics_order_and_filters(self):
        with open_publication(self.root, self.full) as analytics:
            publishers = analytics.report('publishers')
            self.assertEqual([(r['source_id'], r['article_count'], r['observation_count']) for r in publishers],
                             [('alpha', 2, 4), ('beta', 1, 1)])
            self.assertNotIn('distinct_ingestion_count', publishers[0])
            self.assertEqual(publishers[0]['articles_without_published_at'], 2)
            daily = analytics.report('daily')
            self.assertEqual([(r['source_id'], str(r['ingestion_date']), r['observation_count'],
                               r['distinct_article_count'], r['distinct_ingestion_count'], r['quality_warning_count'])
                              for r in daily], [('alpha', '2026-10-12', 2, 2, 1, 0),
                                               ('alpha', '2026-10-13', 2, 1, 1, 4),
                                               ('beta', '2026-10-13', 1, 1, 1, 0)])
            quality = analytics.report('quality', source='alpha', start='2026-10-13', end='2026-10-13')
            self.assertEqual(quality, [dict(source_id='alpha', observation_count=2,
                                            quality_warning_count=4, warning_codes_per_observation=2.0)])
            self.assertEqual(analytics.report('quality')[0]['warning_codes_per_observation'], 1.0)
            articles = analytics.report('articles', source='alpha')
            self.assertEqual([r['title'] for r in articles], ['Selected A', 'B'])
            self.assertEqual(articles[0]['selected_entry_index'], 1)
            self.assertEqual(articles[0]['selected_ingested_at'], datetime(2026, 10, 13, 0, 0, 0, 1))
            self.assertIsNone(articles[0]['published_at'])
            self.assertEqual(analytics.report('articles', limit=1)[0]['source_id'], 'beta')
            self.assertEqual(analytics.report('articles', source="alpha' OR 1=1 --"), [])
            self.assertEqual(analytics.report('daily'), daily)
            self.assertEqual(len(analytics.report('daily', start='2026-10-13')), 2)

    def test_exact_snapshot_and_empty_selection(self):
        with open_publication(self.root, self.first.rsplit('=', 1)[1]) as analytics:
            self.assertEqual(analytics.report('publishers')[0]['observation_count'], 2)
            articles = analytics.report('articles')
            self.assertEqual([r['article_id'] for r in articles], sorted(r['article_id'] for r in articles))
            self.assertEqual(next(r['published_at'] for r in articles if r['title'] == 'Older A'), datetime(2026, 10, 10))
        with open_publication(self.root, self.empty) as analytics:
            for name in ('publishers', 'daily', 'articles', 'quality'):
                self.assertEqual(analytics.report(name), [])

    def test_missing_marker_manifest_and_data(self):
        for relative, code in [('complete.json', 'INCOMPLETE_OUTPUT'), ('inputs.json', 'INVALID_LAYOUT')]:
            path = self.directory / relative
            raw = path.read_bytes()
            path.unlink()
            with self.assertRaisesRegex(AnalyticsFailure, code):
                open_publication(self.root, self.full)
            path.write_bytes(raw)
        next((self.directory / 'gold_articles').glob('*.parquet')).unlink()
        with self.assertRaisesRegex(AnalyticsFailure, 'INVENTORY_MISMATCH'):
            open_publication(self.root, self.full)

    def test_strict_marker_metadata(self):
        path = self.directory / 'complete.json'
        raw = path.read_bytes()
        for changed in (b'bad', b'[]', b'{"a":1,"a":1}',
                        raw.replace(b'"publication_version":1', b'"publication_version":true'),
                        raw.replace(b'"processor_revision":1', b'"processor_revision":2'),
                        raw.replace(b'"gold_articles":3', b'"gold_articles":3.0'),
                        raw[:-1] + b',"unexpected":1}'):
            with self.subTest(changed=changed[:30]):
                path.write_bytes(changed)
                with self.assertRaises(AnalyticsFailure):
                    open_publication(self.root, self.full)

    def test_manifest_and_parquet_hashes(self):
        path = self.directory / 'inputs.json'
        raw = path.read_bytes()
        path.write_bytes(raw + b' ')
        with self.assertRaisesRegex(AnalyticsFailure, 'MANIFEST_MISMATCH'):
            open_publication(self.root, self.full)
        path.write_bytes(raw)
        parquet = next((self.directory / 'gold_articles').glob('*.parquet'))
        parquet.write_bytes(b'altered')
        with self.assertRaisesRegex(AnalyticsFailure, 'FILE_MISMATCH'):
            open_publication(self.root, self.full)

    def test_paths_symlinks_and_unexpected_files(self):
        for selection in ('../' + self.full, str(self.directory), 'v2/' + self.full[3:], self.full + '/../x', '*'):
            with self.subTest(selection=selection), self.assertRaisesRegex(AnalyticsFailure, 'INVALID_SELECTION'):
                open_publication(self.root, selection)
        for name in ('extra.parquet', '_hidden.parquet'):
            extra = self.directory / 'gold_articles' / name
            extra.write_bytes(b'extra')
            with self.assertRaisesRegex(AnalyticsFailure, 'INVENTORY_MISMATCH'):
                open_publication(self.root, self.full)
            extra.unlink()
        path = next((self.directory / 'gold_articles').glob('*.parquet'))
        outside = Path(self.tmp.name) / 'outside.parquet'
        path.rename(outside)
        path.symlink_to(outside)
        with self.assertRaisesRegex(AnalyticsFailure, 'INVALID_LAYOUT'):
            open_publication(self.root, self.full)

    def test_inventory_traversal(self):
        self.marker(lambda m: m['files']['gold_articles'][0].update(path='../outside.parquet'))
        with self.assertRaises(AnalyticsFailure):
            open_publication(self.root, self.full)

    def test_unsupported_schema_on_one_file(self):
        con = duckdb.connect()
        extra = self.directory / 'gold_articles' / 'extra.parquet'
        con.sql('SELECT 1 AS wrong').write_parquet(str(extra))
        con.close()
        self.refresh_inventory('gold_articles')
        with self.assertRaisesRegex(AnalyticsFailure, 'UNSUPPORTED_SCHEMA'):
            open_publication(self.root, self.full)

    def test_count_and_cross_table_reconciliation(self):
        self.marker(lambda m: m['output_counts'].update(gold_articles=99))
        with self.assertRaisesRegex(AnalyticsFailure, 'COUNT_MISMATCH'):
            open_publication(self.root, self.full)
        self.marker(lambda m: m['output_counts'].update(gold_articles=3))
        self.rewrite('gold_source_daily', "* REPLACE (CASE WHEN source_id = 'alpha' THEN 'other' ELSE source_id END AS source_id)")
        with self.assertRaisesRegex(AnalyticsFailure, 'RECONCILIATION_FAILED'):
            open_publication(self.root, self.full)

    def test_required_values_and_read_only_queries(self):
        before = {str(p): p.read_bytes() for p in self.root.rglob('*') if p.is_file()}
        with open_publication(self.root, self.full) as analytics:
            analytics.report('articles')
        self.assertEqual(before, {str(p): p.read_bytes() for p in self.root.rglob('*') if p.is_file()})
        self.rewrite('gold_articles', '* REPLACE (NULL::VARCHAR AS title)')
        with self.assertRaisesRegex(AnalyticsFailure, 'INVALID_VALUES'):
            open_publication(self.root, self.full)

    def test_changes_during_session_are_rejected(self):
        with open_publication(self.root, self.full) as analytics:
            self.marker(lambda m: m['output_counts'].update(gold_articles=9))
            with self.assertRaisesRegex(AnalyticsFailure, 'PUBLICATION_CHANGED'):
                analytics.report('publishers')

    def test_cli_and_argument_validation(self):
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(main(['--gold-root', str(self.root), '--publication', self.full,
                                   '--report', 'articles', '--source', 'alpha', '--limit', '1']), 0)
        result = json.loads(output.getvalue())
        self.assertEqual(result['rows'][0]['selected_ingested_at'], '2026-10-13T00:00:00.000001Z')
        self.assertIsNone(result['rows'][0]['published_at'])
        for extra in (['--limit', '0'], ['--limit', '1001'], ['--limit', 'bad'],
                      ['--start-date', 'bad'], ['--start-date', '2026-10-14', '--end-date', '2026-10-13']):
            output = io.StringIO()
            with redirect_stdout(output):
                code = main(['--publication', self.full, '--report', 'daily', *extra])
            self.assertEqual(code, 2)
            self.assertEqual(json.loads(output.getvalue())['code'], 'INVALID_ARGUMENT')

    def test_query_process_without_spark_or_silver(self):
        script = '''
import builtins, sys
original = builtins.__import__
def guarded(name, *args, **kwargs):
    if name.split('.')[0] in {'pyspark', 'silver', 'silver_inputs', 'gold', 'gold_publication', 'boto3'}:
        raise AssertionError('consumer imported producer dependency')
    return original(name, *args, **kwargs)
builtins.__import__ = guarded
from gold_analytics import open_publication
with open_publication(sys.argv[1], sys.argv[2]) as analytics:
    assert analytics.report('publishers')[0]['article_count'] == 2
    assert len(analytics.report('daily')) == 3
    assert len(analytics.report('articles')) == 3
    assert analytics.report('quality')[0]['quality_warning_count'] == 4
assert 'pyspark' not in sys.modules
print('standalone OK')
'''
        result = subprocess.run([sys.executable, '-c', script, str(self.root), self.full],
                                env=dict(os.environ, JAVA_HOME='/nonexistent', PYTHONPATH='processing'),
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('standalone OK', result.stdout)

    def test_schema_mapping_matches_spark_contract(self):
        from gold import GOLD_ARTICLES_SCHEMA, GOLD_SOURCE_DAILY_SCHEMA
        types = {'int': 'INTEGER', 'bigint': 'BIGINT', 'string': 'VARCHAR',
                 'timestamp': 'TIMESTAMP', 'date': 'DATE', 'array<string>': 'VARCHAR[]'}
        for spark, duck in ((GOLD_ARTICLES_SCHEMA, ARTICLE_SCHEMA), (GOLD_SOURCE_DAILY_SCHEMA, DAILY_SCHEMA)):
            self.assertEqual([(f.name, types[f.dataType.simpleString()]) for f in spark], duck)


if __name__ == '__main__':
    unittest.main()
