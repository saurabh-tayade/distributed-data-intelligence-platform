"""Real DuckDB/native-consumer evidence for complete narrow Content report rows."""
from datetime import datetime
from decimal import Decimal
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import content_analytics as native
import content_metadata as m
import narrow_content_profile as profile
import narrow_content_reports as reports
from content_fixtures import article_rows
from narrow_content_test_fixtures import catalog_row, content, inputs, snapshot
from publication_metadata import canonical_bytes


class Assertions(unittest.TestCase):
    def failure(self, code, function, *args, **kwargs):
        with self.assertRaises(m.ContentFailure) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.diagnostic, dict(code=code, message=m.MESSAGES[code]))


class ScanTest(Assertions):
    def setUp(self):
        self.conn = native.connection()
        self.addCleanup(self.conn.close)
        for table, schema in m.SCHEMAS.items():
            self.conn.execute('CREATE TABLE ' + table + ' (' + ','.join(n + ' ' + t for n,t in schema) + ')')
        self.conn.execute('CREATE TABLE coverage (source_id VARCHAR, reference_status VARCHAR, reference_usage VARCHAR, '
            'publisher_name VARCHAR, publisher_category VARCHAR, declared_language VARCHAR, country_code VARCHAR, '
            'article_count BIGINT, articles_missing_reference_count BIGINT, missing_optional_attributes VARCHAR[])')

    def test_bounded_connection_configuration(self):
        values = self.conn.execute("SELECT current_setting('memory_limit'), current_setting('threads'), "
            "current_setting('temp_directory'), current_setting('TimeZone'), "
            "current_setting('autoinstall_known_extensions'), current_setting('autoload_known_extensions')").fetchone()
        self.assertEqual(values, ('512.0 MiB', 2, '', 'UTC', False, False))

    def test_empty_relations_are_all_scanned(self):
        self.assertEqual(dict(reports._scan_reports(self.conn)), dict.fromkeys(native.REPORTS, 0))

    def test_raw_strings_array_strings_and_empty_arrays_prevent_materialization(self):
        for title, warnings in (('a' * (profile.REPORT_ROW_LIMIT + 1), []),
                                ('a', ['b' * 31000, 'c' * 31000]), ('a', [''] * 30000)):
            with self.subTest(array_length=len(warnings)):
                self.conn.execute(f'DELETE FROM {m.CATALOG}')
                self.conn.execute(f'INSERT INTO {m.CATALOG} (title,quality_warnings) VALUES (?,?)', [title, warnings])
                with patch.object(profile, 'validate_report_row', side_effect=AssertionError('materialized')):
                    self.failure('RESOURCE_LIMIT', reports._scan_reports, self.conn)

    def test_escaping_is_charged_by_full_serializer_not_lower_bound(self):
        self.conn.execute(f'INSERT INTO {m.CATALOG} (title,quality_warnings) VALUES (?,[])', ['\x00' * 11000])
        with patch.object(profile, 'validate_report_row', wraps=profile.validate_report_row) as checked:
            self.failure('RESOURCE_LIMIT', reports._scan_reports, self.conn)
        self.assertEqual(checked.call_count, 1)

    def test_public_conversion_exact_keys_unicode_timestamps_null_and_counts(self):
        row = catalog_row(article_rows()[0])
        row.update(title='é🙂"\\\n\x00', summary=None, observation_count=Decimal(2**53 + 1),
                   distinct_ingestion_count=2**53 + 1, updated_at=datetime(2026, 11, 4, 1, 2, 3, 456789))
        raw = profile.validate_report_row('catalog', row)
        self.assertEqual(raw, canonical_bytes(native.public(row)))
        value = json.loads(raw)
        self.assertEqual(value['updated_at'], '2026-11-04T01:02:03.456789Z')
        self.assertEqual(value['observation_count'], '9007199254740993')
        self.assertIsNone(value['summary'])
        self.assertEqual(set(value), set(native.CATALOG_COLUMNS))
        for changed in (row | {'extra': None}, {k:v for k,v in row.items() if k != 'summary'}):
            with self.assertRaises(profile.ProfileError):
                profile.validate_report_row('catalog', changed)

    def test_late_row_and_all_three_reports(self):
        # This tiny materialized table has controlled insertion order; production
        # scanning promises full coverage, never a particular traversal order.
        self.conn.execute(f'INSERT INTO {m.CATALOG} (title,quality_warnings) SELECT \'short\',[] FROM range(80)')
        self.conn.execute(f'INSERT INTO {m.CATALOG} (title,quality_warnings) VALUES (?,[])', ['\x00' * 11000])
        with patch.object(profile, 'validate_report_row', wraps=profile.validate_report_row) as checked:
            self.failure('RESOURCE_LIMIT', reports._scan_reports, self.conn)
        self.assertEqual(checked.call_count, 81)
        self.conn.execute(f'DELETE FROM {m.CATALOG}')
        for report, table, field in (('reference_coverage', 'coverage', 'publisher_name'),
                                     ('source_discovery_daily', m.DAILY, 'source_id')):
            # Resource-guard units only: native semantics forbid these oversized
            # reference identifiers/attributes. Real native boundaries use titles.
            self.conn.execute(f'INSERT INTO {table} ({field}) VALUES (?)', ['x' * 61441])
            with patch.object(profile, 'validate_report_row', side_effect=AssertionError('materialized')):
                self.failure('RESOURCE_LIMIT', reports._scan_reports, self.conn)
            self.conn.execute(f'DELETE FROM {table}')

    def test_row_count_ceiling_before_materialization(self):
        self.conn.execute(f'INSERT INTO {m.CATALOG} (title,quality_warnings) SELECT \'short\',[] FROM range(2)')
        with patch.object(m, 'MAX_ROWS', 1), patch.object(profile, 'validate_report_row', side_effect=AssertionError()):
            self.failure('RESOURCE_LIMIT', reports._scan_reports, self.conn)

    def test_sql_uses_no_sort_or_unbounded_fetch_and_guards_late_growth(self):
        self.conn.execute(f'INSERT INTO {m.CATALOG} (title,quality_warnings) VALUES (\'short\',[])')
        original = self.conn
        statements = []
        class Connection:
            def execute(self, sql):
                statements.append(sql)
                if sql.startswith('SELECT _row_bytes'):
                    original.execute(f'UPDATE {m.CATALOG} SET title=?', ['x' * 1000000])
                return original.execute(sql)
        with patch.object(profile, 'validate_report_row', side_effect=AssertionError('materialized')):
            self.failure('RESOURCE_LIMIT', reports._scan_reports, Connection())
        self.assertFalse(any('ORDER BY' in sql or 'OFFSET' in sql for sql in statements))
        self.assertEqual(sum(sql.startswith('SELECT count(*)') for sql in statements), 3)


class PublicationReportsTest(Assertions):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)

    def verify(self, args, dependency):
        return reports.verify_publication_v1(args['output_root'], dependency.fingerprint,
            gold_root=args['gold_root'], reference_root=args['reference_root'])

    def test_real_native_valid_exact_and_plus_one_title_and_summary(self):
        for field in ('title', 'summary'):
            for extra in (0, 1):
                with self.subTest(field=field, extra=extra):
                    root = self.base / f'{field}-{extra}'
                    root.mkdir()
                    row = catalog_row(article_rows()[0])
                    row[field] = '\n\t"\\é🙂'
                    row[field] += 'a' * (profile.REPORT_ROW_LIMIT - len(canonical_bytes(native.public(row))) + extra)
                    args = inputs(root, change=lambda rows: rows[0].update({field: row[field]}))
                    dependency, _, _ = content(args)
                    before = snapshot(root)
                    with native.open_publication(args['output_root'], dependency.fingerprint,
                            gold_root=args['gold_root'], reference_root=args['reference_root']) as opened:
                        actual = next(r for r in opened.report('catalog')['rows'] if r['article_id'] == row['article_id'])
                        self.assertEqual(len(canonical_bytes(actual)), 61440 + extra)
                    if extra:
                        self.failure('RESOURCE_LIMIT', self.verify, args, dependency)
                    else:
                        result = self.verify(args, dependency)
                        self.assertEqual(dict(result['report_counts']), dict(catalog=4, reference_coverage=4,
                                                                            source_discovery_daily=3))
                    self.assertEqual(snapshot(root), before)

    def test_unused_reference_rows_and_empty_catalog_are_not_exempt(self):
        args = inputs(self.base, empty=True)
        dependency, _, _ = content(args)
        seen = []
        original = profile.validate_report_row
        def record(report, row):
            seen.append((report, row))
            return original(report, row)
        before = snapshot(self.base)
        with patch.object(profile, 'validate_report_row', side_effect=record):
            result = self.verify(args, dependency)
        self.assertEqual(dict(result['report_counts']), dict(catalog=0, reference_coverage=3, source_discovery_daily=0))
        self.assertEqual(sorted(row['source_id'] for _,row in seen), ['alpha', 'beta', 'delta'])
        self.assertTrue(all(report == 'reference_coverage' and row['reference_usage'] == 'UNUSED'
                            for report,row in seen))
        self.assertEqual(snapshot(self.base), before)

    def test_maximal_legal_unused_reference_row_is_visited(self):
        from content_fixtures import CSV
        source, publisher = 'z' * 128, '🙂' * 200
        csv = CSV.split(b'\n')[0] + b'\n' + f'{source},{publisher},,,\n'.encode()
        args = inputs(self.base, empty=True, csv=csv)
        dependency, _, _ = content(args)
        with patch.object(profile, 'validate_report_row', wraps=profile.validate_report_row) as checked:
            self.verify(args, dependency)
        checked.assert_called_once()
        report, row = checked.call_args.args
        self.assertEqual(report, 'reference_coverage')
        self.assertEqual((row['source_id'], row['publisher_name'], row['reference_usage']),
                         (source, publisher, 'UNUSED'))
        self.assertEqual(row['missing_optional_attributes'], list(m.ATTRIBUTES[1:]))
        self.assertLess(len(profile.validate_report_row(report, row)), profile.REPORT_ROW_LIMIT)

    def test_native_semantics_reject_before_profile_scan(self):
        args = inputs(self.base)
        dependency, directory, _ = content(args)
        path = directory / 'complete.json'
        marker = json.loads(path.read_bytes())
        marker['output_counts'][m.CATALOG] += 1
        path.write_bytes(canonical_bytes(marker))
        with patch.object(reports, '_scan_reports', side_effect=AssertionError('not native-valid')):
            self.failure('PUBLICATION_INVALID', self.verify, args, dependency)

    def test_large_valid_counts_preserve_public_decimal_strings(self):
        args = inputs(self.base, large_counts=True)
        dependency, _, _ = content(args)
        seen = []
        original = profile.validate_report_row
        def record(report, row):
            raw = original(report, row)
            if report == 'catalog': seen.append(json.loads(raw)['observation_count'])
            return raw
        with patch.object(profile, 'validate_report_row', side_effect=record):
            self.verify(args, dependency)
        self.assertIn('9007199254740993', seen)

    def test_real_precompletion_exact_paths_and_no_open_publication(self):
        args = inputs(self.base)
        dependency, directory, marker = content(args, complete=False)
        evidence = m.capture(directory, m.TABLES, native.TOP - {'complete.json'},
                             'PUBLICATION_FAILED', 'PUBLICATION_CHANGED',
                             guards=m.namespace_guards(args['output_root'], directory))
        before = snapshot(self.base)
        with (patch.object(native, 'open_publication', side_effect=AssertionError('requires marker')),
              patch.object(native, 'register', wraps=native.register) as register):
            result = reports.validate_precompletion_v1(dependency, evidence, marker['output_counts'])
        self.assertEqual(result['catalog'], 4)
        for call in register.call_args_list[1:]:
            table = call.args[1]
            self.assertEqual(call.args[2], (str(directory / table / 'part.parquet'),))
        self.assertEqual(snapshot(self.base), before)

    def test_precompletion_database_failures_close_and_keep_fixed_diagnostics(self):
        args = inputs(self.base)
        dependency, directory, marker = content(args, complete=False)
        evidence = m.capture(directory, m.TABLES, native.TOP - {'complete.json'},
                             'PUBLICATION_FAILED', 'PUBLICATION_CHANGED',
                             guards=m.namespace_guards(args['output_root'], directory))
        before = snapshot(self.base)
        cases = [(duckdb.OutOfMemoryException('private engine detail'), False, 'RESOURCE_LIMIT'),
                 (RuntimeError('private query detail'), False, 'PUBLICATION_FAILED'),
                 (None, True, 'PUBLICATION_FAILED'),
                 (m.ContentFailure('RESOURCE_LIMIT'), True, 'RESOURCE_LIMIT')]
        for error, close_error, code in cases:
            with self.subTest(code=code, close_error=close_error):
                original = native.connection()
                class Connection:
                    closed = False
                    def __getattr__(self, name):
                        return getattr(original, name)
                    def close(self):
                        self.closed = True
                        original.close()
                        if close_error: raise OSError('private close detail')
                conn = Connection()
                with (patch.object(native, 'connection', return_value=conn),
                      patch.object(reports, '_scan_reports', side_effect=error,
                                   return_value=dict.fromkeys(native.REPORTS, 0))):
                    self.failure(code, reports.validate_precompletion_v1, dependency, evidence,
                                 marker['output_counts'])
                self.assertTrue(conn.closed)
        self.assertEqual(snapshot(self.base), before)

    def test_missing_incomplete_stale_temporary_and_io_never_publish_or_repair(self):
        args = inputs(self.base)
        dependency, directory, _ = content(args)
        marker = directory / 'complete.json'
        raw = marker.read_bytes()
        cases = [('complete', None), ('stale', 'PUBLICATION_INVALID'), ('incomplete', 'PUBLICATION_INCOMPLETE')]
        for state, code in cases:
            if state == 'stale':
                (directory / 'complete.tmp').write_bytes(raw)
            if state == 'incomplete':
                marker.unlink()
            before = snapshot(self.base)
            with patch('narrow_content_publication.publish_v1', side_effect=AssertionError('must stay read-only')):
                if code:
                    self.failure(code, self.verify, args, dependency)
                else:
                    self.verify(args, dependency)
            self.assertEqual(snapshot(self.base), before)
        before = snapshot(self.base)
        real = Path.lstat
        def denied(path):
            if path == marker:
                raise PermissionError('private path')
            return real(path)
        with patch.object(Path, 'lstat', denied):
            self.failure('PUBLICATION_INVALID', self.verify, args, dependency)
        self.assertEqual(snapshot(self.base), before)
        self.failure('PUBLICATION_UNAVAILABLE', reports.verify_publication_v1, args['output_root'], '0' * 64,
                     gold_root=args['gold_root'], reference_root=args['reference_root'])
        self.assertEqual(snapshot(self.base), before)

    def test_mutation_during_scan_rechecks_output_dependency_and_ancestor(self):
        for target in ('output', 'dependency', 'ancestor'):
            with self.subTest(target=target):
                base = self.base / target
                base.mkdir()
                args = inputs(base)
                dependency, directory, _ = content(args)
                original = reports._scan_reports
                def changed(conn):
                    result = original(conn)
                    if target == 'ancestor':
                        parent = directory.parent
                        moved = parent.with_name('moved')
                        parent.rename(moved)
                        parent.mkdir()
                        for child in moved.iterdir():
                            child.rename(parent / child.name)
                    else:
                        file = (directory / m.CATALOG / 'part.parquet' if target == 'output'
                                else dependency.snapshot.directory / 'reference.csv')
                        file.write_bytes(file.read_bytes() + b'x')
                    return result
                with patch.object(reports, '_scan_reports', side_effect=changed):
                    self.failure('DEPENDENCY_CHANGED' if target == 'dependency' else 'PUBLICATION_CHANGED',
                                 self.verify, args, dependency)

    def test_readonly_entrypoint_imports_no_spark_or_producer(self):
        args = inputs(self.base)
        dependency, _, _ = content(args)
        script = '''
import builtins, sys
original = builtins.__import__
def guarded(name, *args, **kwargs):
    if name.split('.')[0] in {'pyspark','boto3','silver','content_processing','content_publication','narrow_content_publication'}:
        raise AssertionError('producer import')
    return original(name, *args, **kwargs)
builtins.__import__ = guarded
from narrow_content_reports import verify_publication_v1
assert verify_publication_v1(sys.argv[1], sys.argv[2], gold_root=sys.argv[3], reference_root=sys.argv[4])['report_counts']['catalog'] == 4
'''
        run = subprocess.run([sys.executable, '-c', script, str(args['output_root']), dependency.fingerprint,
                              str(args['gold_root']), str(args['reference_root'])],
                             capture_output=True, text=True, timeout=30)
        self.assertEqual(run.returncode, 0, run.stderr)


if __name__ == '__main__':
    unittest.main()
