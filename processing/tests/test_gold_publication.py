"""Synthetic local publication/replay; no retained data or external services."""
from contextlib import redirect_stdout, redirect_stderr
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

from pyspark.sql.readwriter import DataFrameWriter

import gold_publication as publication
from bronze_to_silver import process_one
from gold import GOLD_ARTICLES_SCHEMA, GOLD_SOURCE_DAILY_SCHEMA
from silver import SILVER_SCHEMA, session
from silver_inputs import InputFailure, canonical_bytes, load_inputs
from test_bronze import Store, snapshot


def file_bytes(root):
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in root.rglob('*') if p.is_file()}


class PublicationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = session()
        cls.spark.sparkContext.setLogLevel('ERROR')
        cls.base = tempfile.TemporaryDirectory()
        cls.silver_base = Path(cls.base.name) / 'silver'
        cls.gold_base = Path(cls.base.name) / 'gold'
        store = Store()
        for fixture, number in [('rss-business.xml', 1), ('atom-science.xml', 2)]:
            key, _ = snapshot(store, fixture, number)
            process_one(cls.spark, store, 'local', key, cls.silver_base)
        for marker in cls.silver_base.rglob('complete.json'):
            accepted = marker.parent / 'accepted'
            rows = [r.asDict() for r in cls.spark.read.parquet(str(accepted)).collect()]
            if len(rows) == 1:
                rows += [rows[0], dict(rows[0], entry_index=3)]
            else:
                rows[0].update(published_at=None, updated_at=None,
                               quality_warnings=['INVALID_PUBLISHED_AT', 'INVALID_UPDATED_AT'])
            cls.spark.createDataFrame(rows, SILVER_SCHEMA).write.mode('overwrite').parquet(str(accepted))
            content = json.loads(marker.read_text())
            content['counts']['accepted'] = len(rows)
            marker.write_text(json.dumps(content))
        cls.original = publication.run(cls.spark, cls.silver_base, cls.gold_base)
        cls.relative = cls.original['publication']

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.base.cleanup()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.silver = Path(self.tmp.name) / 'silver'
        self.gold = Path(self.tmp.name) / 'gold'
        shutil.copytree(self.silver_base, self.silver)
        shutil.copytree(self.gold_base, self.gold)
        self.directory = self.gold / self.relative

    def replay(self, **kwargs):
        return publication.run(self.spark, self.silver, self.gold, **kwargs)

    def test_explicit_readonly_verification_never_creates_or_repairs(self):
        inputs=load_inputs(self.spark,self.silver)
        before=file_bytes(self.gold)
        self.assertEqual(publication.verify_publication(inputs,self.gold)['output_counts'],self.original['output_counts'])
        self.assertEqual(file_bytes(self.gold),before)
        missing=Path(self.tmp.name)/'absent'
        with self.assertRaises(Exception):publication.verify_publication(inputs,missing)
        self.assertFalse(missing.exists())
        (self.directory/'complete.json').unlink();partial=file_bytes(self.gold)
        with self.assertRaises(Exception):publication.verify_publication(inputs,self.gold)
        self.assertEqual(file_bytes(self.gold),partial)

    def update_marker(self, change):
        path = self.directory / 'complete.json'
        marker = json.loads(path.read_text())
        change(marker)
        path.write_bytes(canonical_bytes(marker))

    def rewrite_gold(self, name, change):
        path = self.directory / name
        schema = publication.SCHEMAS[name]
        rows = [r.asDict() for r in self.spark.read.schema(schema).parquet(str(path)).collect()]
        self.spark.createDataFrame(change(rows), schema).write.mode('overwrite').parquet(str(path))
        # Refresh hashes to prove semantic verification also catches corruption.
        self.update_marker(lambda m: m.update(files=publication.output_inventory(self.directory)))

    def test_success_and_replay_preserve_all_files(self):
        self.assertEqual(self.original['status'], 'PROCESSED')
        marker = json.loads((self.directory / 'complete.json').read_text())
        self.assertEqual(marker['input_counts'], dict(accepted=4, accepted_before_replay_collapse=5, rejected=1))
        self.assertEqual(marker['duplicates'], dict(duplicate_publications=0, duplicate_accepted_rows=1))
        self.assertEqual(marker['output_counts'], dict(gold_articles=3, gold_source_daily=1))
        self.assertEqual(marker['input_manifest_sha256'], marker['input_set_fingerprint'])
        daily, = self.spark.read.parquet(str(self.directory / 'gold_source_daily')).collect()
        self.assertEqual((daily.observation_count, daily.distinct_article_count,
                          daily.distinct_ingestion_count, daily.quality_warning_count), (4, 3, 2, 2))
        before = file_bytes(self.gold)
        result = self.replay(sources=['fictional'])
        self.assertEqual(result['status'], 'SKIPPED')
        markers = [p.relative_to(self.silver).as_posix() for p in self.silver.rglob('complete.json')]
        again = self.replay(markers=list(reversed(markers)) + markers)
        self.assertEqual(again['status'], 'SKIPPED')
        self.assertEqual(again['diagnostics']['duplicate_selections'], 2)
        self.assertEqual(before, file_bytes(self.gold))

    def test_incomplete_is_never_resumed(self):
        (self.directory / 'complete.json').unlink()
        before = file_bytes(self.gold)
        with self.assertRaisesRegex(publication.PublicationFailure, 'INCOMPLETE_OUTPUT'):
            self.replay()
        self.assertEqual(before, file_bytes(self.gold))

    def test_strict_marker_validation(self):
        path = self.directory / 'complete.json'
        original = path.read_bytes()
        inputs = load_inputs(self.spark, self.silver)
        for raw in (b'bad', b'[]', b'{"publication_version":1,"publication_version":1}',
                    original.replace(b'"publication_version":1', b'"publication_version":true'),
                    original.replace(b'"publication_version":1', b'"publication_version":1.0'),
                    original.replace(b'"processor_revision":1', b'"processor_revision":2'),
                    original[:-1] + b',"extra":0}',
                    original.replace(b'"gold_articles":3', b'"gold_articles":99')):
            with self.subTest(raw=raw[:40]):
                path.write_bytes(raw)
                with self.assertRaises(publication.PublicationFailure):
                    publication.publish(inputs, self.gold)
                self.assertEqual(path.read_bytes(), raw)

    def test_manifest_tampering(self):
        path = self.directory / 'inputs.json'
        path.write_bytes(path.read_bytes() + b' ')
        with self.assertRaisesRegex(publication.PublicationFailure, 'OUTPUT_MANIFEST_MISMATCH'):
            self.replay()

    def test_corrupt_parquet_is_not_skipped(self):
        path = next((self.directory / 'gold_articles').glob('*.parquet'))
        path.write_bytes(b'corrupt')
        before = file_bytes(self.gold)
        with self.assertRaisesRegex(publication.PublicationFailure, 'OUTPUT_METADATA_MISMATCH'):
            self.replay()
        self.assertEqual(before, file_bytes(self.gold))

    def test_physical_schema_of_every_file(self):
        wrong = Path(self.tmp.name) / 'wrong'
        self.spark.range(1).write.parquet(str(wrong))
        shutil.copy(next(wrong.glob('*.parquet')), self.directory / 'gold_articles' / 'extra.parquet')
        self.update_marker(lambda m: m.update(files=publication.output_inventory(self.directory)))
        with self.assertRaisesRegex(publication.PublicationFailure, 'OUTPUT_SCHEMA_MISMATCH'):
            self.replay()

    def test_required_values_and_warning_elements(self):
        original = file_bytes(self.directory / 'gold_articles')
        for field, value in [('title', None), ('quality_warnings', [None])]:
            with self.subTest(field=field):
                directory = self.directory / 'gold_articles'
                shutil.rmtree(directory)
                directory.mkdir()
                for name, raw in original.items():
                    (directory / name).write_bytes(raw)
                self.rewrite_gold('gold_articles', lambda rows: [dict(r, **{field: value}) for r in rows])
                with self.assertRaisesRegex(publication.PublicationFailure, 'OUTPUT_INVALID_VALUES'):
                    self.replay()

    def test_duplicate_keys_rejected(self):
        for name in publication.SCHEMAS:
            with self.subTest(name=name):
                shutil.rmtree(self.directory)
                shutil.copytree(self.gold_base / self.relative, self.directory)
                self.rewrite_gold(name, lambda rows: rows + [rows[0]])
                with self.assertRaisesRegex(publication.PublicationFailure, 'OUTPUT_DUPLICATE_KEY'):
                    self.replay()

    def test_selected_provenance_rejected(self):
        self.rewrite_gold('gold_articles', lambda rows: [dict(r, selected_entry_index=999) for r in rows])
        with self.assertRaisesRegex(publication.PublicationFailure, 'OUTPUT_PROVENANCE_MISMATCH'):
            self.replay()

    def test_daily_rows_compared_even_when_totals_match(self):
        self.rewrite_gold('gold_source_daily', lambda rows: [dict(r, distinct_ingestion_count=1) for r in rows])
        with self.assertRaisesRegex(publication.PublicationFailure, 'OUTPUT_ROWS_MISMATCH'):
            self.replay()

    def test_observation_and_warning_reconciliation(self):
        for field in ('observation_count', 'quality_warning_count'):
            with self.subTest(field=field):
                shutil.rmtree(self.directory)
                shutil.copytree(self.gold_base / self.relative, self.directory)
                self.rewrite_gold('gold_source_daily', lambda rows: [dict(r, **{field: 99}) for r in rows])
                with self.assertRaisesRegex(publication.PublicationFailure, 'OUTPUT_RECONCILIATION_FAILED'):
                    self.replay()

    def test_changed_input_creates_new_namespace(self):
        before = file_bytes(self.directory)
        marker = next(self.silver.rglob('complete.json'))
        marker.write_bytes(marker.read_bytes() + b' ')
        result = self.replay()
        self.assertEqual(result['status'], 'PROCESSED')
        self.assertNotEqual(result['publication'], self.relative)
        self.assertEqual(before, file_bytes(self.directory))
        self.assertTrue((self.gold / result['publication'] / 'complete.json').is_file())

    def test_input_changed_after_processing_never_completes(self):
        fresh = Path(self.tmp.name) / 'fresh'
        original = publication.verify_frames
        def mutate(*args):
            result = original(*args)
            marker = next(self.silver.rglob('complete.json'))
            marker.write_bytes(marker.read_bytes() + b' ')
            return result
        with patch.object(publication, 'verify_frames', side_effect=mutate):
            with self.assertRaisesRegex(InputFailure, 'INPUT_CHANGED'):
                publication.run(self.spark, self.silver, fresh)
        self.assertFalse(list(fresh.rglob('complete.json')))
        self.assertTrue(list(fresh.rglob('inputs.json')))

    def test_write_failure_leaves_incomplete_output(self):
        fresh = Path(self.tmp.name) / 'fresh'
        original = DataFrameWriter.parquet
        def fail_second(writer, path, *args, **kwargs):
            if path.endswith('gold_source_daily'):
                raise OSError('synthetic failure with private content')
            return original(writer, path, *args, **kwargs)
        inputs = load_inputs(self.spark, self.silver)
        with patch.object(DataFrameWriter, 'parquet', fail_second):
            with self.assertRaisesRegex(publication.PublicationFailure, 'OUTPUT_WRITE_FAILED') as caught:
                publication.publish(inputs, fresh)
        self.assertNotIn('private content', str(caught.exception))
        self.assertTrue(list(fresh.rglob('gold_articles')))
        self.assertFalse(list(fresh.rglob('complete.json')))
        with self.assertRaisesRegex(publication.PublicationFailure, 'INCOMPLETE_OUTPUT'):
            publication.publish(inputs, fresh)

    def test_readback_and_marker_failure_never_complete(self):
        inputs = load_inputs(self.spark, self.silver)
        for name, target in [('read', '_read_frames'), ('marker', None)]:
            fresh = Path(self.tmp.name) / name
            context = (patch.object(publication, target, side_effect=OSError('failure')) if target
                       else patch.object(Path, 'rename', side_effect=OSError('failure')))
            with context, self.assertRaises(publication.PublicationFailure):
                publication.publish(inputs, fresh)
            self.assertFalse(list(fresh.rglob('complete.json')))
            with self.assertRaisesRegex(publication.PublicationFailure, 'INCOMPLETE_OUTPUT'):
                publication.publish(inputs, fresh)

    def test_empty_accepted_publication_and_replay(self):
        for marker in self.silver.rglob('complete.json'):
            self.spark.createDataFrame([], SILVER_SCHEMA).write.mode('overwrite').parquet(str(marker.parent / 'accepted'))
            value = json.loads(marker.read_text())
            value['counts']['accepted'] = 0
            marker.write_text(json.dumps(value))
        result = self.replay()
        self.assertEqual(result['output_counts'], dict(gold_articles=0, gold_source_daily=0))
        self.assertEqual(result['input_counts']['rejected'], 1)
        directory = self.gold / result['publication']
        for name, schema in [('gold_articles', GOLD_ARTICLES_SCHEMA), ('gold_source_daily', GOLD_SOURCE_DAILY_SCHEMA)]:
            frame = self.spark.read.parquet(str(directory / name))
            self.assertEqual(frame.schema, schema)
            self.assertEqual(frame.count(), 0)
        self.assertEqual(self.replay()['status'], 'SKIPPED')

    def test_missing_inputs_and_output_symlink(self):
        empty = Path(self.tmp.name) / 'empty'
        empty.mkdir()
        with self.assertRaisesRegex(InputFailure, 'NO_COMPLETED_INPUTS'):
            publication.run(self.spark, empty, Path(self.tmp.name) / 'not-created')
        self.assertFalse((Path(self.tmp.name) / 'not-created').exists())
        path = next((self.directory / 'gold_articles').glob('*.parquet'))
        outside = Path(self.tmp.name) / 'outside.parquet'
        path.rename(outside)
        path.symlink_to(outside)
        with self.assertRaisesRegex(publication.PublicationFailure, 'OUTPUT_LAYOUT_INVALID'):
            self.replay()


class PublicationCliTest(unittest.TestCase):
    def test_shutdown_failure_has_safe_diagnostic(self):
        spark = Mock()
        spark.stop.side_effect = RuntimeError('private JVM details')
        out = io.StringIO()
        with patch.object(publication, 'session', return_value=spark), \
                patch.object(publication, 'run', return_value={'status': 'PROCESSED'}), redirect_stdout(out):
            self.assertEqual(publication.main([]), 1)
        self.assertEqual(json.loads(out.getvalue()), dict(status='FAILED', code='SPARK_SHUTDOWN_FAILED'))

    def test_cli_options_exit_codes_and_cleanup(self):
        spark = Mock()
        out = io.StringIO()
        with patch.object(publication, 'session', return_value=spark), \
                patch.object(publication, 'run', return_value={'status': 'SKIPPED'}) as run, redirect_stdout(out):
            self.assertEqual(publication.main(['--silver-root', '/tmp/silver', '--gold-root', '/tmp/gold',
                                               '--marker', 'a', '--marker', 'b']), 0)
        run.assert_called_once_with(spark, Path('/tmp/silver'), Path('/tmp/gold'), sources=[], markers=['a', 'b'])
        self.assertEqual(json.loads(out.getvalue()), {'status': 'SKIPPED'})
        spark.stop.assert_called_once()
        out = io.StringIO()
        with patch.object(publication, 'session', return_value=spark), \
                patch.object(publication, 'run', side_effect=publication.PublicationFailure('INCOMPLETE_OUTPUT')), \
                redirect_stdout(out):
            self.assertEqual(publication.main([]), 1)
        self.assertEqual(json.loads(out.getvalue())['code'], 'INCOMPLETE_OUTPUT')
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as caught:
            publication.main(['--source', 'a', '--marker', 'b'])
        self.assertEqual(caught.exception.code, 2)


if __name__ == '__main__':
    unittest.main()
