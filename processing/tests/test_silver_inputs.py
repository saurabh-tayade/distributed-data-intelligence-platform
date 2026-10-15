"""Synthetic publications only; no network, services or retained publisher data."""
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from pyspark.sql import functions as F
from bronze_to_silver import process_one
from silver import SILVER_SCHEMA, REJECTION_SCHEMA, session
from silver_inputs import (InputFailure, canonical_bytes, discover, fingerprint,
                           inventory, load_inputs, parse_marker)
from test_bronze import Store, snapshot


class ManifestTest(unittest.TestCase):
    def test_fixed_vector(self):
        value = {'z': [1, 'é'], 'a': {'b': False}}
        expected = b'{"a":{"b":false},"z":[1,"\xc3\xa9"]}'
        self.assertEqual(canonical_bytes(value), expected)
        self.assertEqual(fingerprint(value), '73d069d0f3964dd58881492a708b60441e9e119d805019f1f4b4906e57f938b0')
        self.assertEqual(fingerprint({'a': {'b': False}, 'z': [1, 'é']}), fingerprint(value))
        with self.assertRaises(ValueError):
            canonical_bytes({'x': float('nan')})


class SilverInputsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = session()
        cls.spark.sparkContext.setLogLevel('ERROR')
        cls.base = tempfile.TemporaryDirectory()
        store = Store()
        key, _ = snapshot(store)
        process_one(cls.spark, store, 'local', key, Path(cls.base.name))
        cls.relative = next(Path(cls.base.name).rglob('complete.json')).relative_to(cls.base.name)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.base.cleanup()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / 'silver'
        shutil.copytree(self.base.name, self.root)
        self.marker = self.root / self.relative

    def marker_change(self, change):
        value = json.loads(self.marker.read_text())
        change(value)
        self.marker.write_text(json.dumps(value))

    def rewrite(self, name, rows=None, change=None):
        schema = {'accepted': SILVER_SCHEMA, 'rejected': REJECTION_SCHEMA}[name]
        directory = self.marker.parent / name
        if rows is None:
            rows = [r.asDict() for r in self.spark.read.parquet(str(directory)).collect()]
        if change:
            rows = change(rows)
        shutil.rmtree(directory)
        self.spark.createDataFrame(rows, schema).write.parquet(str(directory))
        self.marker_change(lambda m: m['counts'].update({name: len(rows)}))
        return rows

    def test_normal_selection_manifest_and_relocation(self):
        result = load_inputs(self.spark, self.root)
        self.assertEqual(result.counts, {'accepted': 1, 'accepted_before_replay_collapse': 1, 'rejected': 1})
        self.assertEqual(result.accepted.count(), 1)
        self.assertEqual(result.accepted.schema, SILVER_SCHEMA)
        selected = discover(self.root, sources=['fictional', 'fictional'])
        self.assertEqual(inventory(selected), result.manifest)
        repeated = discover(self.root, markers=[self.relative, self.relative])
        self.assertEqual(repeated.duplicate_selections, 1)
        self.assertEqual(fingerprint(inventory(repeated)), result.input_set_fingerprint)
        other = Path(self.tmp.name) / 'moved'
        shutil.copytree(self.root, other)
        self.assertEqual(fingerprint(inventory(discover(other))), result.input_set_fingerprint)
        result.verify_unchanged()

    def test_selection_errors_and_incomplete_reporting(self):
        for kwargs in ({'sources': ['absent']}, {'sources': ['fictional'], 'markers': [self.relative]},
                       {'markers': ['../outside/complete.json']}, {'markers': ['absent']},
                       {'markers': [self.marker.parent / 'accepted']}):
            with self.subTest(kwargs=kwargs), self.assertRaises(InputFailure):
                discover(self.root, **kwargs)
        other = self.marker.parent.with_name('ingestion_id=00000000-0000-0000-0000-000000000002')
        other.mkdir()
        self.assertEqual(len(discover(self.root).incomplete), 1)
        self.marker.unlink()
        with self.assertRaisesRegex(InputFailure, 'NO_COMPLETED_INPUTS'):
            discover(self.root)
        with self.assertRaisesRegex(InputFailure, 'INVALID_SELECTION'):
            discover(self.root, markers=[self.relative])

    def test_strict_marker_validation(self):
        original = self.marker.read_bytes()
        for raw in (b'bad', b'{}', b'{"identity":{},"identity":{},"counts":{}}', b'[]', b'x' * 65537):
            self.marker.write_bytes(raw)
            with self.subTest(raw=raw[:20]), self.assertRaisesRegex(InputFailure, 'INVALID_MARKER'):
                parse_marker(self.root, self.relative)
        changes = [lambda m: m['counts'].update(accepted=True),
                   lambda m: m['counts'].update(rejected=-1),
                   lambda m: m['identity'].update(contract_version=2),
                   lambda m: m['identity'].update(processor_revision=2),
                   lambda m: m['identity'].update(processor_revision=True),
                   lambda m: m['identity'].update(bucket='other'),
                   lambda m: m['identity'].update(metadata_key='bad'),
                   lambda m: m['identity'].update(payload_sha256='bad')]
        for change in changes:
            self.marker.write_bytes(original)
            self.marker_change(change)
            with self.assertRaises(InputFailure):
                parse_marker(self.root, self.relative)

    def test_missing_data_count_and_schema(self):
        self.marker_change(lambda m: m['counts'].update(accepted=10))
        with self.assertRaisesRegex(InputFailure, 'COUNT_MISMATCH'):
            load_inputs(self.spark, self.root)
        self.marker_change(lambda m: m['counts'].update(accepted=1))
        directory = self.marker.parent / 'accepted'
        shutil.rmtree(directory)
        with self.assertRaisesRegex(InputFailure, 'MISSING_DATASET'):
            load_inputs(self.spark, self.root)
        self.spark.range(1).write.parquet(str(directory))
        with self.assertRaisesRegex(InputFailure, 'SCHEMA_MISMATCH'):
            load_inputs(self.spark, self.root)

    def test_each_file_schema_checked(self):
        directory = Path(self.tmp.name) / 'bad'
        self.spark.range(1).write.parquet(str(directory))
        shutil.copy(next(directory.glob('*.parquet')), self.marker.parent / 'accepted' / 'extra.parquet')
        with self.assertRaisesRegex(InputFailure, 'SCHEMA_MISMATCH'):
            load_inputs(self.spark, self.root)

    def test_provenance_and_utc(self):
        self.spark.conf.set('spark.sql.session.timeZone', 'America/New_York')
        instant = datetime.fromisoformat('2026-10-11T20:01:00-04:00')
        for name in ('accepted', 'rejected'):
            self.rewrite(name, change=lambda rows: [dict(r, ingested_at=instant) for r in rows])
        result = load_inputs(self.spark, self.root)
        self.assertEqual(self.spark.conf.get('spark.sql.session.timeZone'), 'UTC')
        self.assertEqual(result.accepted.select(F.to_date('ingested_at')).first()[0].isoformat(), '2026-10-12')
        self.rewrite('accepted', change=lambda rows: [dict(r, ingested_at=datetime(2026, 10, 11, tzinfo=timezone.utc)) for r in rows])
        with self.assertRaisesRegex(InputFailure, 'PROVENANCE_MISMATCH'):
            load_inputs(self.spark, self.root)

    def test_ingestion_provenance_conflict(self):
        self.rewrite('accepted', change=lambda rows: [dict(r, source_uri='https://example.com/other') for r in rows])
        with self.assertRaisesRegex(InputFailure, 'INGESTION_CONFLICT'):
            load_inputs(self.spark, self.root)

    def test_replay_collapse_and_conflict(self):
        rows = self.rewrite('accepted', change=lambda rows: rows + rows + [dict(rows[0], entry_index=4)])
        result = load_inputs(self.spark, self.root)
        self.assertEqual(result.counts['accepted'], 2)
        self.assertEqual(result.diagnostics['duplicate_accepted_rows'], 1)
        self.rewrite('accepted', rows=rows + [dict(rows[0], title='Changed')])
        with self.assertRaisesRegex(InputFailure, 'REPLAY_CONFLICT'):
            load_inputs(self.spark, self.root)

    def test_accepted_rejected_conflict(self):
        row = self.spark.read.parquet(str(self.marker.parent / 'accepted')).first().asDict()
        self.rewrite('rejected', rows=[dict(row, rejection_reasons=['XML_MAPPING_ERROR'])])
        with self.assertRaisesRegex(InputFailure, 'ACCEPTED_REJECTED_CONFLICT'):
            load_inputs(self.spark, self.root)

    def test_distinct_ingestions_and_two_warnings(self):
        store = Store()
        key, _ = snapshot(store, 'atom-science.xml', 2)
        process_one(self.spark, store, 'local', key, self.root)
        result = load_inputs(self.spark, self.root)
        self.assertEqual(result.counts['accepted'], 3)
        self.assertEqual(result.accepted.select('ingestion_id').distinct().count(), 2)
        atom_marker = next(p for p in self.root.rglob('complete.json') if p != self.marker)
        saved = self.marker
        self.marker = atom_marker
        self.rewrite('accepted', change=lambda rows: [dict(r, published_at=None, updated_at=None,
                     quality_warnings=['INVALID_PUBLISHED_AT', 'INVALID_UPDATED_AT']) for r in rows])
        self.marker = saved
        result = load_inputs(self.spark, self.root)
        self.assertEqual(result.accepted.select(F.sum(F.size('quality_warnings'))).first()[0], 4)

    def test_rejected_only(self):
        self.rewrite('accepted', rows=[])
        result = load_inputs(self.spark, self.root)
        self.assertEqual(result.counts['accepted'], 0)
        self.assertEqual(result.counts['rejected'], 1)
        self.assertEqual(result.accepted.schema, SILVER_SCHEMA)

    def test_required_identity_and_warning_values(self):
        original = self.spark.read.parquet(str(self.marker.parent / 'accepted')).first().asDict()
        changes = [dict(title=None), dict(article_id='0' * 64), dict(entry_index=-1),
                   dict(quality_warnings=None), dict(quality_warnings=[None]),
                   dict(quality_warnings=['INVALID_UPDATED_AT', 'INVALID_PUBLISHED_AT']),
                   dict(quality_warnings=['UNKNOWN']), dict(source_id='other')]
        for change in changes:
            self.rewrite('accepted', rows=[dict(original, **change)])
            with self.subTest(change=change), self.assertRaises(InputFailure):
                load_inputs(self.spark, self.root)

    def test_rejection_arrays(self):
        original = self.spark.read.parquet(str(self.marker.parent / 'rejected')).first().asDict()
        for reasons in (None, [], [None], ['UNKNOWN'], ['MISSING_TITLE', 'MISSING_TITLE'], ['MISSING_TITLE']):
            self.rewrite('rejected', rows=[dict(original, rejection_reasons=reasons)])
            with self.subTest(reasons=reasons), self.assertRaisesRegex(InputFailure, 'INVALID_ROW'):
                load_inputs(self.spark, self.root)

    def test_alias_publications_and_symlink_escape(self):
        alias = self.root / 'alias'
        alias.symlink_to(self.marker.parent, target_is_directory=True)
        result = load_inputs(self.spark, self.root, markers=[self.relative, 'alias/complete.json'])
        self.assertEqual(result.counts['accepted'], 1)
        self.assertEqual(result.diagnostics['duplicate_publications'], 1)
        self.assertEqual(result.manifest['inputs'][1]['representative_path'], 'alias/complete.json')
        outside = Path(self.tmp.name) / 'outside'
        shutil.copytree(self.marker.parent, outside)
        escape = self.root / 'escape'
        escape.symlink_to(outside, target_is_directory=True)
        with self.assertRaisesRegex(InputFailure, 'INVALID_SELECTION'):
            discover(self.root, markers=['escape/complete.json'])
        file = next((self.marker.parent / 'accepted').glob('*.parquet'))
        original = outside / file.name
        shutil.copy(file, original)
        file.unlink()
        file.symlink_to(original)
        with self.assertRaisesRegex(InputFailure, 'INVALID_SELECTION'):
            load_inputs(self.spark, self.root)

    def test_conflicting_publication_copy(self):
        copied = self.root / 'alias-copy'
        shutil.copytree(self.marker.parent, copied)
        (copied / 'complete.json').unlink()
        (copied / 'complete.json').symlink_to(self.marker)
        result = load_inputs(self.spark, self.root, markers=[self.relative, 'alias-copy/complete.json'])
        self.assertEqual(result.counts['accepted'], 1)
        self.assertEqual(result.diagnostics['duplicate_publications'], 1)
        directory = copied / 'accepted'
        rows = [r.asDict() for r in self.spark.read.parquet(str(directory)).collect()]
        shutil.rmtree(directory)
        self.spark.createDataFrame([dict(r, title='Conflicting copy') for r in rows], SILVER_SCHEMA).write.parquet(str(directory))
        with self.assertRaisesRegex(InputFailure, 'PUBLICATION_CONFLICT'):
            load_inputs(self.spark, self.root, markers=[self.relative, 'alias-copy/complete.json'])

    def test_noncanonical_publication_copy(self):
        copied = self.root / 'copy'
        shutil.copytree(self.marker.parent, copied)
        with self.assertRaisesRegex(InputFailure, 'INVALID_PUBLICATION_PATH'):
            discover(self.root, markers=['copy/complete.json'])

    def test_changed_inputs(self):
        result = load_inputs(self.spark, self.root)
        self.marker.write_text(self.marker.read_text() + '\n')
        with self.assertRaisesRegex(InputFailure, 'INPUT_CHANGED'):
            result.verify_unchanged()
        changed = load_inputs(self.spark, self.root)
        self.assertNotEqual(result.input_set_fingerprint, changed.input_set_fingerprint)
        file = next((self.marker.parent / 'accepted').glob('*.parquet'))
        with file.open('ab') as stream:
            stream.write(b'corruption')
        with self.assertRaisesRegex(InputFailure, 'INPUT_CHANGED'):
            changed.verify_unchanged()
        with self.assertRaisesRegex(InputFailure, 'PARQUET_VALIDATION_FAILED'):
            load_inputs(self.spark, self.root)

    def test_change_during_validation(self):
        import silver_inputs
        original = silver_inputs.read_dataset
        def changing(*args):
            frame = original(*args)
            self.marker.write_text(self.marker.read_text() + '\n')
            return frame
        with patch('silver_inputs.read_dataset', side_effect=changing):
            with self.assertRaisesRegex(InputFailure, 'INPUT_CHANGED'):
                load_inputs(self.spark, self.root)
