"""Completion-edge tests with explicit Spark doubles and real file/SQL evidence.

These tests exercise lifecycle faults locally; test_narrow_content_spark covers
the unchanged actual Spark producer and read-back on the supported CI runtime.
"""
import ast
from pathlib import Path
import shutil
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import content_analytics as analytics
import content_dependencies as dependencies
import content_metadata as m
import narrow_content_publication as publisher
import narrow_content_reports as reports
from narrow_content_test_fixtures import content, inputs, snapshot
from publication_metadata import canonical_bytes


def lifecycle_double(template, events):
    """Only Spark transforms are doubled; native filesystem helpers are unchanged."""
    source = Path(publisher.__file__).with_name('content_publication.py')
    names = {'_write', '_check_guards', '_unchanged_before_marker'}
    body = [node for node in ast.parse(source.read_text()).body
            if isinstance(node, ast.FunctionDef) and node.name in names]
    namespace = {'m': m}
    exec(compile(ast.Module(body=body, type_ignores=[]), str(source), 'exec'), namespace)
    schema = SimpleNamespace(simpleString=lambda: 'explicit Spark schema double')
    class Frame:
        def __init__(self, table):
            self.table, self.schema, self.write = table, schema, self
        def mode(self, value):
            assert value == 'errorifexists'
            return self
        def parquet(self, path):
            shutil.copytree(template / self.table, path)
    def build(spark, dependency):
        events.append('build')
        return None, SimpleNamespace(catalog=Frame(m.CATALOG), daily=Frame(m.DAILY))
    top = analytics.TOP - {'complete.json'}
    def verify(spark, dependency, gold, expected, directory, root, existing):
        events.append('native-readback-double')
        evidence = m.capture(directory, m.TABLES, top | ({'complete.json'} if existing else set()),
                             'PUBLICATION_INVALID' if existing else 'PUBLICATION_FAILED',
                             'PUBLICATION_CHANGED', guards=m.namespace_guards(root, directory))
        m.need(evidence.raw('inputs.json') == dependency.manifest_bytes)
        files = evidence.inventory()
        with analytics.connection() as conn:
            analytics.register(conn, 'base_articles', dependency.paths('gold_articles'), m.GOLD_SCHEMA)
            for table in m.TABLES:
                analytics.register(conn, table, [str(directory / file['path']) for file in files[table]], m.SCHEMAS[table])
            counts = {table: conn.execute('SELECT count(*) FROM ' + table).fetchone()[0] for table in m.TABLES}
            analytics.validate_tables(conn, dependency, counts)
        if existing:
            marker = m.validate_completion(evidence.raw('complete.json'), dependency.fingerprint,
                                           len(dependency.manifest_bytes))
            m.need(marker['files'] == files and marker['output_counts'] == counts)
        evidence.check()
        dependency.check()
        return evidence, counts, files
    return SimpleNamespace(**{name: namespace[name] for name in names},
        verify_dependencies=dependencies.verify_dependencies, build_content=build,
        verify_frames=lambda *args: None, SCHEMAS=dict.fromkeys(m.TABLES, schema), _verify=verify,
        DATA_TOP=top, _now=lambda: '2026-11-04T00:00:00.000000Z')


class LifecycleTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = tempfile.TemporaryDirectory()
        cls.fixture_root = Path(cls.fixture.name)
        cls.fixture_args = inputs(cls.fixture_root)
        cls.dependency, cls.fixture_directory, cls.marker = content(cls.fixture_args)

    @classmethod
    def tearDownClass(cls):
        cls.fixture.cleanup()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name) / 'case'
        shutil.copytree(self.fixture_root, self.base)
        self.args = {name: self.base / value.relative_to(self.fixture_root) if isinstance(value, Path) else value
                     for name, value in self.fixture_args.items()}
        self.directory = m.publication_directory(self.args['output_root'], self.dependency.fingerprint)
        self.template = self.base / 'template'
        self.directory.rename(self.template)
        self.events = []
        self.native = lifecycle_double(self.template, self.events)
        self.patcher = patch.object(publisher, '_native', return_value=self.native)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)

    def publish(self):
        return publisher.publish_v1(None, **self.args)

    def verify(self):
        return reports.verify_publication_v1(self.args['output_root'], self.dependency.fingerprint,
            gold_root=self.args['gold_root'], reference_root=self.args['reference_root'])

    def failure(self, code, function):
        with self.assertRaises(m.ContentFailure) as caught:
            function()
        self.assertEqual(caught.exception.diagnostic, dict(code=code, message=m.MESSAGES[code]))

    def test_native_readback_precedes_every_row_scan_and_marker_creation(self):
        original = publisher.validate_precompletion_v1
        def scan(dependency, evidence, counts):
            self.assertEqual(self.events[-1], 'native-readback-double')
            self.assertFalse((self.directory / 'complete.tmp').exists())
            self.assertFalse((self.directory / 'complete.json').exists())
            result = original(dependency, evidence, counts)
            self.events.append('all-rows-checked')
            return result
        write = self.native._write
        def write_marker(path, raw):
            if path.name == 'complete.tmp':
                self.assertEqual(self.events[-1], 'all-rows-checked')
            write(path, raw)
        with patch.object(publisher, 'validate_precompletion_v1', side_effect=scan), \
                patch.object(self.native, '_write', side_effect=write_marker):
            self.assertEqual(self.publish()['status'], 'published')
        self.assertEqual((self.directory / 'complete.json').read_bytes(), canonical_bytes(self.marker))
        before = snapshot(self.base)
        self.assertEqual(self.publish()['status'], 'replayed')
        self.assertEqual(self.verify()['status'], 'VERIFIED')
        self.assertEqual(snapshot(self.base), before)

    def test_scan_rejection_retains_incomplete_no_temporary_marker(self):
        with patch.object(publisher, 'validate_precompletion_v1', side_effect=m.ContentFailure('RESOURCE_LIMIT')):
            self.failure('RESOURCE_LIMIT', self.publish)
        self.assertTrue(self.directory.is_dir())
        self.assertFalse((self.directory / 'complete.tmp').exists())
        self.assertFalse((self.directory / 'complete.json').exists())
        before = snapshot(self.base)
        self.failure('PUBLICATION_INCOMPLETE', self.publish)
        self.failure('PUBLICATION_INCOMPLETE', self.verify)
        self.assertEqual(snapshot(self.base), before)

    def _mutation_after_scan(self, target):
        original = publisher.validate_precompletion_v1
        def changed(dependency, evidence, counts):
            result = original(dependency, evidence, counts)
            if target == 'ancestor':
                parent = self.directory.parent
                moved = parent.with_name('moved')
                parent.rename(moved)
                parent.mkdir()
                for child in moved.iterdir(): child.rename(parent / child.name)
            else:
                file = (self.directory / m.CATALOG / 'part.parquet' if target == 'output'
                        else dependency.snapshot.directory / 'reference.csv')
                file.write_bytes(file.read_bytes() + b'x')
            return result
        with patch.object(publisher, 'validate_precompletion_v1', side_effect=changed):
            self.failure('DEPENDENCY_CHANGED' if target == 'dependency' else 'PUBLICATION_CHANGED', self.publish)
        self.assertFalse((self.directory / 'complete.tmp').exists())
        self.assertFalse((self.directory / 'complete.json').exists())

    def test_output_rechecked_after_scan_before_temporary(self):
        self._mutation_after_scan('output')

    def test_dependency_rechecked_after_scan_before_temporary(self):
        self._mutation_after_scan('dependency')

    def test_ancestors_rechecked_after_scan_before_temporary(self):
        self._mutation_after_scan('ancestor')

    def _write_failure(self, after):
        write = self.native._write
        def fail(path, raw):
            if path.name == 'complete.tmp':
                if after: write(path, raw)
                raise OSError('private write failure')
            write(path, raw)
        with patch.object(self.native, '_write', side_effect=fail):
            self.failure('PUBLICATION_FAILED', self.publish)
        self.assertEqual((self.directory / 'complete.tmp').exists(), after)
        self.assertFalse((self.directory / 'complete.json').exists())
        before = snapshot(self.base)
        self.failure('PUBLICATION_INCOMPLETE', self.verify)
        self.failure('PUBLICATION_INCOMPLETE', self.publish)
        self.assertEqual(snapshot(self.base), before)

    def test_failure_before_temporary_write(self):
        self._write_failure(False)

    def test_failure_after_temporary_write(self):
        self._write_failure(True)

    def _rename_failure(self, after):
        rename = Path.rename
        def fail(path, target):
            if path.name == 'complete.tmp':
                if after: rename(path, target)
                raise OSError('rename acknowledgement unavailable')
            return rename(path, target)
        with patch.object(Path, 'rename', fail):
            self.failure('PUBLICATION_FAILED', self.publish)
        self.assertEqual((self.directory / 'complete.json').exists(), after)
        self.assertEqual((self.directory / 'complete.tmp').exists(), not after)
        before = snapshot(self.base)
        with patch.object(publisher, 'publish_v1', side_effect=AssertionError('read-only reconciliation')):
            if after:
                self.assertEqual(self.verify()['status'], 'VERIFIED')
            else:
                self.failure('PUBLICATION_INCOMPLETE', self.verify)
        self.assertEqual(snapshot(self.base), before)

    def test_failure_before_rename_retains_temporary(self):
        self._rename_failure(False)

    def test_acknowledgement_loss_after_rename_resolved_readonly(self):
        self._rename_failure(True)

    def test_dependency_mutation_after_temporary_blocks_rename(self):
        write = self.native._write
        file = self.base / 'reference' / 'reference_id=fixture-publishers' / 'revision=r1' / 'reference.csv'
        def changed(path, raw):
            write(path, raw)
            if path.name == 'complete.tmp': file.write_bytes(file.read_bytes() + b'x')
        with patch.object(self.native, '_write', side_effect=changed):
            self.failure('DEPENDENCY_CHANGED', self.publish)
        self.assertTrue((self.directory / 'complete.tmp').exists())
        self.assertFalse((self.directory / 'complete.json').exists())

    def test_permission_failure_is_not_absence_or_authorization_to_create(self):
        original = Path.lstat
        def denied(path):
            if path == self.directory:
                raise PermissionError('private directory')
            return original(path)
        before = snapshot(self.base)
        with patch.object(Path, 'lstat', denied):
            self.failure('PUBLICATION_FAILED', self.publish)
        self.assertEqual(snapshot(self.base), before)


if __name__ == '__main__':
    unittest.main()
