"""One real native relation supplies all report bounds and public coverage."""
from dataclasses import FrozenInstanceError
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import duckdb

sys.path[:0] = [str(Path(__file__).resolve().parent), str(Path(__file__).resolve().parents[1])]
import content_analytics as native
import content_metadata as m
import narrow_content_profile as profile
import narrow_content_reports_v2 as reports
from narrow_content_test_fixtures import content, inputs, snapshot


class PrecompletionV2Test(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def case(self, **options):
        args = inputs(self.root, **options)
        dependency, directory, marker = content(args, complete=False)
        evidence = m.capture(directory, m.TABLES, native.TOP - {'complete.json'},
            'PUBLICATION_FAILED', 'PUBLICATION_CHANGED', guards=m.namespace_guards(args['output_root'], directory))
        return dependency, evidence, marker['output_counts']

    def test_actual_coverage_uses_same_native_relation_and_every_public_row(self):
        dependency, evidence, counts = self.case()
        before = snapshot(self.root)
        observed = []
        validate, coverage = native.validate_tables, native.coverage_totals
        def validation(conn, *args):
            observed.append(conn)
            return validate(conn, *args)
        def totals(conn, *args):
            observed.append(conn)
            return coverage(conn, *args)
        with (patch.object(native, 'validate_tables', side_effect=validation),
              patch.object(native, 'coverage_totals', side_effect=totals),
              patch.object(native, 'open_publication', side_effect=AssertionError('precompletion')),
              patch.object(profile, 'validate_report_row', wraps=profile.validate_report_row) as rows):
            result = reports.validate_precompletion_v2(dependency, evidence, counts)
        self.assertEqual(dict(result.report_counts), dict(catalog=4, reference_coverage=4, source_discovery_daily=3))
        self.assertEqual(rows.call_count, 11)
        self.assertTrue(all(conn is observed[0] for conn in observed))
        self.assertEqual(result.coverage['article_count'], '4')
        self.assertTrue(all(type(value) is str for value in result.coverage.values()))
        with self.assertRaises(TypeError):
            result.coverage['article_count'] = '99'
        with self.assertRaises(TypeError):
            result.report_counts['catalog'] = 99
        with self.assertRaises(FrozenInstanceError):
            result.coverage = {}
        self.assertEqual(snapshot(self.root), before)

    def test_reference_only_rows_are_scanned_and_included_in_coverage(self):
        dependency, evidence, counts = self.case(empty=True)
        with patch.object(profile, 'validate_report_row', wraps=profile.validate_report_row) as rows:
            result = reports.validate_precompletion_v2(dependency, evidence, counts)
        self.assertEqual(dict(result.report_counts), dict(catalog=0, reference_coverage=3, source_discovery_daily=0))
        self.assertEqual(rows.call_count, 3)
        self.assertEqual(result.coverage['article_count'], '0')

    def test_scan_failure_still_rechecks_both_evidence_sources(self):
        dependency, evidence, counts = self.case()
        with (patch.object(type(evidence), 'check', autospec=True, wraps=type(evidence).check) as output_check,
              patch.object(type(dependency), 'check', autospec=True, wraps=type(dependency).check) as dependency_check,
              patch.object(reports, '_scan_reports', side_effect=duckdb.OutOfMemoryException('private detail'))):
            with self.assertRaises(m.ContentFailure) as caught:
                reports.validate_precompletion_v2(dependency, evidence, counts)
        self.assertEqual(caught.exception.diagnostic['code'], 'RESOURCE_LIMIT')
        self.assertGreaterEqual(output_check.call_count, 2)
        self.assertGreaterEqual(dependency_check.call_count, 2)

    def test_output_check_failure_does_not_skip_final_dependency_check(self):
        dependency, evidence, counts = self.case()
        with (patch.object(type(evidence), 'check', side_effect=m.ContentFailure('PUBLICATION_CHANGED')),
              patch.object(type(dependency), 'check') as dependency_check):
            with self.assertRaises(m.ContentFailure) as caught:
                reports.validate_precompletion_v2(dependency, evidence, counts)
        self.assertEqual(caught.exception.diagnostic['code'], 'PUBLICATION_CHANGED')
        dependency_check.assert_called_once()

    def test_coverage_failure_closes_connection_and_rechecks_evidence(self):
        dependency, evidence, counts = self.case()
        conn = native.connection()
        class Connection:
            closed = False
            def __getattr__(self, name):
                return getattr(conn, name)
            def close(self):
                self.closed = True
                conn.close()
        wrapped = Connection()
        scanned = False
        scan, totals = reports._scan_reports, native.coverage_totals
        def scanned_rows(connection):
            nonlocal scanned
            result = scan(connection)
            scanned = True
            return result
        def fail_coverage(connection, *args):
            if scanned:
                raise RuntimeError('private detail')
            return totals(connection, *args)
        with (patch.object(native, 'connection', return_value=wrapped),
              patch.object(reports, '_scan_reports', side_effect=scanned_rows),
              patch.object(native, 'coverage_totals', side_effect=fail_coverage)):
            with self.assertRaises(m.ContentFailure) as caught:
                reports.validate_precompletion_v2(dependency, evidence, counts)
        self.assertEqual(caught.exception.diagnostic['code'], 'PUBLICATION_FAILED')
        self.assertTrue(wrapped.closed)
        evidence.check()
        dependency.check()

    def test_mutation_during_coverage_is_vetoed(self):
        dependency, evidence, counts = self.case()
        original = native.public
        scanned = False
        scan = reports._scan_reports
        def scanned_rows(connection):
            nonlocal scanned
            result = scan(connection)
            scanned = True
            return result
        def mutate(value):
            if not scanned:
                return original(value)
            path = evidence.directory / m.CATALOG / 'part.parquet'
            path.write_bytes(path.read_bytes() + b'x')
            return original(value)
        with (patch.object(native, 'public', side_effect=mutate),
              patch.object(reports, '_scan_reports', side_effect=scanned_rows)):
            with self.assertRaises(m.ContentFailure) as caught:
                reports.validate_precompletion_v2(dependency, evidence, counts)
        self.assertEqual(caught.exception.diagnostic['code'], 'PUBLICATION_CHANGED')


if __name__ == '__main__':
    unittest.main()
