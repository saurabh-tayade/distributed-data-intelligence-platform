"""B01–B16: synthetic Content publication and independent consumer acceptance."""
from contextlib import redirect_stderr, redirect_stdout
from collections import Counter
from datetime import date
from hashlib import sha256
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import duckdb
from pyspark.sql import functions as F
from pyspark.sql.readwriter import DataFrameWriter

import content_analytics as ca
import content_dependencies as cd
import content_metadata as m
import content_processing as cp
import content_publication as pub
import gold_analytics as ga
import publisher_reference as pr
from publication_metadata import canonical_bytes
from silver import session
from content_fixtures import CSV, IDS, article_rows, gold_fixture

REPO = Path(__file__).resolve().parents[2]


def state(root):
    return {p.relative_to(root).as_posix(): (m.stamp(p.lstat()), p.read_bytes() if p.is_file() else None)
            for p in [root, *root.rglob('*')]}


class ContentTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = session()
        cls.spark.sparkContext.setLogLevel('ERROR')
        cls.fixture = tempfile.TemporaryDirectory()
        cls.base_fixture = Path(cls.fixture.name)
        cls.gold_id = gold_fixture(cls.base_fixture / 'gold', spark=cls.spark)
        reference = cls.base_fixture / 'reference'
        reference.mkdir()
        csv = cls.base_fixture / 'source.csv'
        csv.write_bytes(CSV)
        vocabulary = cls.base_fixture / 'vocabulary.json'
        vocabulary.write_bytes(canonical_bytes(pr.VOCABULARY))
        cls.reference_id = pr.publish(reference, csv, vocabulary, 'fixture-publishers', 'r1', 'example-team')['snapshot_fingerprint']
        output = cls.base_fixture / 'content'
        output.mkdir()
        started = time.perf_counter()
        cls.published = pub.publish(cls.spark, output_root=output, gold_root=cls.base_fixture / 'gold',
            gold_fingerprint=cls.gold_id, reference_root=reference, reference_id='fixture-publishers',
            revision='r1', reference_fingerprint=cls.reference_id)
        cls.publish_seconds = time.perf_counter() - started
        print(f'Content synthetic initial publication: {cls.publish_seconds:.3f}s', file=sys.stderr)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.fixture.cleanup()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        for name in ('gold', 'reference', 'content'):
            shutil.copytree(self.base_fixture / name, self.base / name)
        self.gold, self.reference, self.root = (self.base / n for n in ('gold', 'reference', 'content'))
        self.args = dict(output_root=self.root, gold_root=self.gold, gold_fingerprint=self.gold_id,
            reference_root=self.reference, reference_id='fixture-publishers', revision='r1',
            reference_fingerprint=self.reference_id)
        self.identity = self.published['product_fingerprint']
        self.dest = m.publication_directory(self.root, self.identity)

    def publish(self, **kwargs):
        return pub.publish(self.spark, **(self.args | kwargs))

    def open(self, identity=None):
        return ca.open_publication(self.root, identity or self.identity, gold_root=self.gold,
                                   reference_root=self.reference)

    def failure(self, code, function, *args, **kwargs):
        with self.assertRaises(m.ContentFailure) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.diagnostic, dict(code=code, message=m.MESSAGES[code]))

    def marker(self, transform):
        path = self.dest / 'complete.json'
        value = json.loads(path.read_bytes())
        transform(value)
        path.write_bytes(canonical_bytes(value))

    def rewrite(self, table, sql):
        directory = self.dest / table
        files = [str(p) for p in directory.iterdir() if p.suffix == '.parquet']
        with duckdb.connect() as connection:
            connection.read_parquet(files, hive_partitioning=False).create_view('original')
            rows = connection.execute(sql).fetchall()  # Four-row test fixtures only.
        shutil.rmtree(directory)
        self.spark.createDataFrame(rows, cp.SCHEMAS[table]).coalesce(1).write.mode('errorifexists').parquet(str(directory))
        # Refresh the physical hashes so semantic rejection cannot rely on them.
        self.marker(lambda value: value['files'].update({table: [dict(
            path=p.relative_to(self.dest).as_posix(), byte_length=p.stat().st_size,
            sha256=sha256(p.read_bytes()).hexdigest()) for p in directory.iterdir() if p.suffix == '.parquet']}))

    def new_reference(self, revision, csv):
        source = self.base / 'input.csv'
        vocabulary = self.base / 'vocabulary.json'
        source.write_bytes(csv)
        vocabulary.write_bytes(canonical_bytes(pr.VOCABULARY))
        return pr.publish(self.reference, source, vocabulary, 'fixture-publishers', revision,
                          'example-team')['snapshot_fingerprint']

    def test_b02_b03_b04_literal_reports_and_preservation(self):
        before_gold, before_ref, before_content = state(self.gold), state(self.reference), state(self.root)
        started = time.perf_counter()
        with self.open() as reader:
            opened = time.perf_counter() - started
            started = time.perf_counter()
            catalog = reader.report('catalog')
            report_seconds = time.perf_counter() - started
            self.assertEqual(list(catalog['rows'][0]), list(ca.CATALOG_COLUMNS))
            self.assertEqual([r['article_id'] for r in catalog['rows']], [IDS[3], IDS[0], IDS[2], IDS[1]])
            by_id = {r['article_id']: r for r in catalog['rows']}
            for row in article_rows():
                for name in ca.CATALOG_COLUMNS:
                    if name in row:
                        self.assertEqual(by_id[row['article_id']][name], ca.public(row[name]))
            self.assertIsNone(by_id[IDS[0]]['published_at'])
            self.assertEqual(by_id[IDS[0]]['quality_warnings'], ['INVALID_PUBLISHED_AT'])
            self.assertEqual(by_id[IDS[0]]['discovered_at'], '2026-10-12T23:59:59.999999Z')
            self.assertEqual(by_id[IDS[2]]['discovered_at'], '2026-10-13T00:00:00.000000Z')
            self.assertEqual(by_id[IDS[2]]['reference_status'], 'MATCHED')
            self.assertIsNone(by_id[IDS[2]]['publisher_category'])
            self.assertEqual(by_id[IDS[3]]['reference_status'], 'MISSING')
            daily = reader.report('source_discovery_daily')['rows']
            self.assertEqual(daily, [dict(source_id=s, discovery_date=d, discovered_article_count=c,
                articles_missing_reference_count=n) for s,d,c,n in (
                    ('alpha','2026-10-12','2','0'), ('beta','2026-10-13','1','0'),
                    ('gamma','2026-10-13','1','1'))])
            coverage = reader.report('reference_coverage', limit=2)
            self.assertTrue(coverage['has_more'])
            self.assertEqual(list(coverage['totals'].values()), ['4','3','1','3','2','1','1','3','1'])
            self.assertEqual(coverage['rows'][1]['missing_optional_attributes'],
                             ['publisher_category','declared_language','country_code'])
            unused = reader.report('reference_coverage', source='delta')
            self.assertEqual(unused['rows'][0]['reference_usage'], 'UNUSED')
            self.assertEqual(list(unused['totals'].values()), ['0','0','0','0','0','0','0','1','1'])
            self.assertEqual(reader.report('reference_coverage', source='unknown')['rows'], [])
        print(f'Content synthetic open: {opened:.3f}s; catalog report: {report_seconds:.3f}s', file=sys.stderr)
        self.assertEqual(state(self.gold), before_gold)
        self.assertEqual(state(self.reference), before_ref)
        self.assertEqual(state(self.root), before_content)

    def test_b08_identical_replay(self):
        before = state(self.root)
        self.spark.conf.set('spark.sql.session.timeZone','America/New_York')
        self.assertEqual(self.publish()['status'], 'replayed')
        self.assertEqual(self.spark.conf.get('spark.sql.session.timeZone'),'UTC')
        self.assertEqual(state(self.root), before)

    def test_b14_filters_pagination_and_public_integer_encoding(self):
        with self.open() as reader:
            first = reader.report('catalog', limit=2)
            last = reader.report('catalog', limit=2, offset=2)
            self.assertTrue(first['has_more'])
            self.assertFalse(last['has_more'])
            self.assertEqual(first['rows'] + last['rows'], reader.report('catalog')['rows'])
            self.assertEqual(reader.report('catalog', offset=4)['rows'], [])
            self.assertEqual(reader.report('catalog', offset=1_000_000, limit=1000)['rows'], [])
            self.assertEqual(reader.report('catalog', publisher_category='science')['rows'][0]['source_id'], 'alpha')
            self.assertEqual(len(reader.report('catalog', reference_status='MISSING')['rows']), 1)
            self.assertEqual(len(reader.report('catalog', start_date='2026-10-13', end_date='2026-10-13')['rows']), 2)
            self.assertEqual(reader.report('source_discovery_daily', end_date='2026-10-12')['rows'][0]['discovered_article_count'], '2')
            for kwargs in ({'limit':0}, {'limit':1001}, {'limit':True}, {'offset':-1}, {'offset':1_000_001},
                           {'offset':1.0}, {'source':"alpha' OR 1=1"}, {'start_date':'2026-02-30'},
                           {'start_date':'2026-10-14','end_date':'2026-10-12'}, {'country_code':'ZZ'}):
                self.failure('ARGUMENT_INVALID', reader.report, 'catalog', **kwargs)
            self.failure('ARGUMENT_INVALID', reader.report, 'reference_coverage', start_date='2026-10-12')
            self.failure('ARGUMENT_INVALID', reader.report, 'source_discovery_daily', reference_status='MISSING')
        self.assertEqual(ca.public({'count': 2**53+1})['count'], '9007199254740993')

    def test_b13_separate_spark_free_process(self):
        script = '''
import builtins, sys
original = builtins.__import__
def guarded(name, *a, **k):
    if name.split('.')[0] in {'pyspark','silver','silver_inputs','gold','gold_publication','content_processing','content_publication','boto3'}:
        raise AssertionError('Spark/producer import')
    return original(name,*a,**k)
builtins.__import__=guarded
from content_analytics import open_publication
with open_publication(sys.argv[1],sys.argv[2],gold_root=sys.argv[3],reference_root=sys.argv[4]) as r:
    assert len(r.report('catalog')['rows'])==4
    assert len(r.report('source_discovery_daily')['rows'])==3
    assert r.report('reference_coverage')['totals']['missing_article_count']=='1'
print('standalone verified')
'''
        result = subprocess.run([sys.executable, '-c', script, str(self.root), self.identity,
            str(self.gold), str(self.reference)], cwd=REPO, env=os.environ | {'PYTHONPATH':'processing',
            'JAVA_HOME':'/nonexistent'}, capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('standalone verified', result.stdout)

    def test_b03_header_only_reference(self):
        fingerprint = self.new_reference('empty', CSV.split(b'\n')[0] + b'\n')
        result = self.publish(revision='empty', reference_fingerprint=fingerprint)
        with self.open(result['product_fingerprint']) as reader:
            rows = reader.report('catalog')['rows']
            self.assertEqual({r['article_id'] for r in rows}, set(IDS))
            self.assertEqual({r['reference_status'] for r in rows}, {'MISSING'})
            self.assertTrue(all(r[n] is None for r in rows for n in m.ATTRIBUTES))
            self.assertEqual([r['articles_missing_reference_count'] for r in reader.report('source_discovery_daily')['rows']], ['2','1','1'])
            self.assertEqual(list(reader.report('reference_coverage')['totals'].values()), ['4','0','4','3','0','3','0','0','0'])

    def test_b05_empty_and_rejected_only_gold(self):
        for rejected in (0,1):
            identity = gold_fixture(self.gold, spark=self.spark, empty=True, rejected=rejected)
            result = self.publish(gold_fingerprint=identity)
            with self.open(result['product_fingerprint']) as reader:
                self.assertEqual(reader.report('catalog')['rows'], [])
                self.assertEqual(reader.report('source_discovery_daily')['rows'], [])
                self.assertEqual(list(reader.report('reference_coverage')['totals'].values()), ['0','0','0','0','0','0','0','3','3'])
            evidence = m.publication_directory(self.root, result['product_fingerprint']) / 'gold-complete.json'
            self.assertEqual(json.loads(evidence.read_bytes())['input_counts']['rejected'], rejected)

    def test_b08_reference_correction_and_older_history(self):
        old = state(self.dest)
        fingerprint = self.new_reference('r2', CSV.replace(b'science', b'technology'))
        self.assertEqual(fingerprint, '487620b9287188e8d69b6ae67f027edeff259e6b0301315a32d3bee9b39c8da4')
        result = self.publish(revision='r2', reference_fingerprint=fingerprint)
        self.assertNotEqual(result['product_fingerprint'], self.identity)
        with self.open(result['product_fingerprint']) as reader:
            alpha = reader.report('catalog', source='alpha')['rows']
            self.assertEqual({r['publisher_category'] for r in alpha}, {'technology'})
            self.assertEqual({r['article_id'] for r in alpha}, set(IDS[:2]))
        identity = gold_fixture(self.gold, spark=self.spark, older=True)
        older = self.publish(gold_fingerprint=identity)
        with self.open(older['product_fingerprint']) as reader:
            rows = reader.report('source_discovery_daily', source='alpha')['rows']
            self.assertEqual([(r['discovery_date'],r['discovered_article_count']) for r in rows],
                             [('2026-10-11','1'),('2026-10-12','1')])
            article = next(r for r in reader.report('catalog')['rows'] if r['article_id']==IDS[0])
            self.assertEqual((article['title'],article['observation_count']), ('Alpha latest','3'))
        self.assertEqual(state(self.dest), old)

    def test_b06_semantic_corruption_with_refreshed_hashes(self):
        cases = ((m.CATALOG, "SELECT * REPLACE ('wrong' AS title) FROM original"),
                 (m.CATALOG, "SELECT * REPLACE ('MISSING' AS reference_status) FROM original"),
                 (m.CATALOG, "SELECT * REPLACE (NULL::VARCHAR AS publisher_name) FROM original"),
                 (m.CATALOG, "SELECT * REPLACE (CAST('2026-10-14' AS DATE) AS discovery_date) FROM original"),
                 (m.DAILY, 'SELECT * REPLACE (discovered_article_count+1 AS discovered_article_count) FROM original'))
        for table,sql in cases:
            with self.subTest(table=table, sql=sql):
                shutil.rmtree(self.dest)
                shutil.copytree(m.publication_directory(self.base_fixture / 'content', self.identity), self.dest)
                self.rewrite(table, sql)
                self.failure('PUBLICATION_INVALID', self.open)

    def test_b05_duplicate_and_required_values(self):
        for table,sql in ((m.CATALOG, 'SELECT * FROM original UNION ALL SELECT * FROM original LIMIT 5'),
                          (m.CATALOG, 'SELECT * REPLACE (NULL::VARCHAR AS title) FROM original'),
                          (m.CATALOG, 'SELECT * REPLACE (2 AS content_contract_version) FROM original'),
                          (m.DAILY, 'SELECT * REPLACE (-1::BIGINT AS articles_missing_reference_count) FROM original')):
            with self.subTest(sql=sql):
                shutil.rmtree(self.dest)
                shutil.copytree(m.publication_directory(self.base_fixture / 'content', self.identity), self.dest)
                self.rewrite(table, sql)
                self.failure('PUBLICATION_INVALID', self.open)

    def test_b09_dependency_corruption_and_retention_loss(self):
        gold_dir = self.gold / 'v1' / 'processor_revision=1' / f'input_set={self.gold_id}'
        ref_dir = self.reference / 'reference_id=fixture-publishers' / 'revision=r1'
        for path in (gold_dir / 'inputs.json', gold_dir / 'complete.json',
                     next((gold_dir / 'gold_articles').glob('*.parquet')),
                     ref_dir / 'reference.csv', ref_dir / 'vocabulary.json', ref_dir / 'manifest.json'):
            original = path.read_bytes()
            with self.subTest(name=path.name):
                path.write_bytes(b'bad secret credentials')
                self.failure('DEPENDENCY_INVALID', self.open)
                path.write_bytes(original)
        shutil.rmtree(ref_dir)
        self.failure('DEPENDENCY_UNAVAILABLE', self.open)

    def test_b10_output_metadata_layout_and_schema_corruption(self):
        marker = (self.dest / 'complete.json').read_bytes()
        for raw in (b'bad', marker + b'\n', marker.replace(b'"completion_version":1', b'"completion_version":true'),
                    marker[:-1]+b',"extra":1}'):
            (self.dest / 'complete.json').write_bytes(raw)
            self.failure('PUBLICATION_INVALID' if b'true' not in raw else 'UNSUPPORTED_VERSION', self.open)
        (self.dest / 'complete.json').write_bytes(marker)
        for name in ('extra', 'complete.tmp'):
            path = self.dest / name
            path.write_bytes(b'extra')
            self.failure('PUBLICATION_INVALID', self.open)
            path.unlink()
        path = self.dest / m.CATALOG / '_hidden.parquet'
        path.write_bytes(b'extra')
        self.failure('PUBLICATION_INVALID', self.open)
        path.unlink()
        # One unexpected physical schema must not be hidden by another correct file.
        path = self.dest / m.CATALOG / 'wrong.parquet'
        with duckdb.connect() as conn:
            conn.sql('SELECT 1 AS wrong').write_parquet(str(path))
        self.marker(lambda value: value['files'][m.CATALOG].append(dict(
            path=f'{m.CATALOG}/wrong.parquet', byte_length=path.stat().st_size,
            sha256=sha256(path.read_bytes()).hexdigest())))
        self.marker(lambda value: value['files'][m.CATALOG].sort(key=lambda f:f['path']))
        self.failure('PUBLICATION_INVALID', self.open)

    def test_b12_session_mutations_and_ordinary_io(self):
        for target in ('output', 'gold', 'reference'):
            with self.subTest(target=target):
                with self.open() as reader:
                    path = (self.dest / 'inputs.json' if target=='output' else
                            reader._dependency.gold.directory / 'inputs.json' if target=='gold' else
                            reader._dependency.snapshot.directory / 'manifest.json')
                    original = path.read_bytes()
                    before = path.stat()
                    path.write_bytes(original.replace(b'1', b'2', 1))
                    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
                    self.failure('PUBLICATION_CHANGED' if target=='output' else 'DEPENDENCY_CHANGED',
                                 reader.report, 'catalog')
                    self.assertIsNone(reader._conn)
                    path.write_bytes(original)
        with self.open() as reader:
            with patch.object(os, 'open', side_effect=OSError('secret path')):
                self.failure('PUBLICATION_INVALID', reader.report, 'catalog')
            self.assertIsNone(reader._conn)

    def test_b15_resources_busy_and_response_limits(self):
        for name,value in (('MAX_ROWS',3), ('MAX_FILES',1), ('MAX_DATASET_FILES',0),
                           ('MAX_BYTES',1), ('MANIFEST_LIMIT',1), ('GOLD_MANIFEST_LIMIT',1),
                           ('COMPLETION_LIMIT',1)):
            with self.subTest(name=name), patch.object(m, name, value):
                self.failure('RESOURCE_LIMIT', self.open)
        for constant in ('PAGE_BYTES', 'RESPONSE_LIMIT'):
            with self.open() as reader:
                with patch.object(m, constant, 1):
                    self.failure('RESPONSE_LIMIT', reader.report, 'catalog')
                self.assertIsNone(reader._conn)
        with self.open() as reader:
            self.assertEqual(reader._conn.execute("SELECT current_setting('threads'),current_setting('memory_limit'),current_setting('temp_directory'),current_setting('autoload_known_extensions'),current_setting('autoinstall_known_extensions')").fetchone(),
                             (2,'512.0 MiB','',False,False))
            reader._lock.acquire()
            try:
                self.failure('BUSY', reader.report, 'catalog')
            finally:
                reader._lock.release()
            self.assertEqual(len(reader.report('catalog')['rows']), 4)
        self.failure('COUNT_OVERFLOW', ca.public, m.MAX_COUNT+1)

    def test_b11_write_readback_and_finalization_faults(self):
        before = state(self.dest)
        for phase in ('write', 'exclusive', 'close', 'readback', 'marker', 'rename'):
            root = self.base / phase
            root.mkdir()
            write = pub._write
            def fail_write(path, raw):
                if path.name == ('complete.tmp' if phase=='marker' else 'gold-complete.json'):
                    if phase=='exclusive':
                        path.write_bytes(b'preexisting evidence')
                        return write(path,raw)
                    raise OSError('secret write')
                return write(path,raw)
            if phase in ('write','marker','exclusive'):
                context = patch.object(pub, '_write', side_effect=fail_write)
            elif phase=='close':
                original_open=Path.open
                class CloseFailure:
                    def __init__(self,stream):
                        self.stream=stream
                    def __enter__(self):
                        return self.stream
                    def __exit__(self,*args):
                        self.stream.close()
                        raise OSError('secret write close')
                def fail_close(path,*args,**kwargs):
                    stream=original_open(path,*args,**kwargs)
                    return CloseFailure(stream) if args==('xb',) else stream
                context=patch.object(Path,'open',fail_close)
            elif phase == 'readback':
                context = patch.object(pub, '_verify', side_effect=OSError('secret readback'))
            else:
                context = patch.object(Path, 'rename', side_effect=OSError('secret finalization'))
            with self.subTest(phase=phase), context:
                self.failure('PUBLICATION_FAILED', self.publish, output_root=root)
            self.assertEqual(list(root.rglob('complete.json')), [])
            self.assertTrue(list(root.rglob('inputs.json')))
            if phase=='exclusive':
                self.assertEqual(next(root.rglob('gold-complete.json')).read_bytes(),b'preexisting evidence')
            self.failure('PUBLICATION_INCOMPLETE', self.publish, output_root=root)
        self.assertEqual(state(self.dest), before)

    def test_b11_partial_spark_write_and_interruption_after_rename(self):
        root = self.base / 'partial'
        root.mkdir()
        parquet = DataFrameWriter.parquet
        def fail(writer, path, *args, **kwargs):
            if path.endswith(m.DAILY):
                raise OSError('secret Spark write')
            return parquet(writer,path,*args,**kwargs)
        with patch.object(DataFrameWriter, 'parquet', fail):
            self.failure('PUBLICATION_FAILED', self.publish, output_root=root)
        self.assertEqual(list(root.rglob('complete.json')), [])
        other = self.base / 'lost-response'
        other.mkdir()
        rename = Path.rename
        def lost(path, target):
            rename(path,target)
            raise OSError('lost response')
        with patch.object(Path, 'rename', lost):
            self.failure('PUBLICATION_FAILED', self.publish, output_root=other)
        self.assertEqual(self.publish(output_root=other)['status'], 'replayed')

    def test_b12_dependency_changes_during_spark_actions(self):
        root = self.base / 'changed'
        root.mkdir()
        verify = pub.verify_frames
        changed = False
        def mutate(*args):
            nonlocal changed
            result = verify(*args)
            if not changed:
                changed = True
                path = self.gold / 'v1' / 'processor_revision=1' / f'input_set={self.gold_id}' / 'inputs.json'
                path.write_bytes(path.read_bytes()+b'\n')
            return result
        with patch.object(pub, 'verify_frames', side_effect=mutate):
            self.failure('DEPENDENCY_CHANGED', self.publish, output_root=root)
        self.assertEqual(list(root.iterdir()), [])

    def test_b12_root_replacement_during_output_writes(self):
        root = self.base / 'replaced-root'
        root.mkdir()
        write = pub._write
        moved = self.base / 'old-root'
        def replace(path,raw):
            write(path,raw)
            if path.name == 'inputs.json':
                root.rename(moved)
                root.mkdir()
        with patch.object(pub, '_write', side_effect=replace):
            self.failure('PUBLICATION_CHANGED', self.publish, output_root=root)
        self.assertEqual(list(root.iterdir()), [])
        self.assertEqual(list(moved.rglob('complete.json')), [])

    def test_b16_standard_library_discovery_oracle(self):
        observations = [
            ('alpha','shared','2026-10-12'),('alpha','second','2026-10-12'),
            ('alpha','shared','2026-10-13'),('beta','shared','2026-10-13'),
            ('gamma','third','2026-10-13')]
        earliest = {}
        for source,guid,day in observations:
            earliest[source,guid] = min(earliest.get((source,guid),day),day)
        expected = Counter((source,day) for (source,guid),day in earliest.items())
        with self.open() as reader:
            actual = reader.report('source_discovery_daily')['rows']
        self.assertEqual({(r['source_id'],r['discovery_date']):int(r['discovered_article_count']) for r in actual},
                         dict(expected))

    def test_b16_producer_backed_gold_and_silver_independence(self):
        from bronze_to_silver import process_one
        from gold_publication import run
        from test_bronze import Store, snapshot
        store = Store()
        key,_ = snapshot(store)
        silver = self.base / 'synthetic-silver'
        process_one(self.spark, store, 'local', key, silver)
        gold_root = self.base / 'producer-gold'
        original = run(self.spark, silver, gold_root)
        # The second RSS item lacks both GUID and fallback link; Silver rejects it.
        self.assertEqual(original['input_counts']['accepted'],1)
        self.assertEqual(original['input_counts']['rejected'],1)
        root = self.base / 'producer-content'
        root.mkdir()
        before = state(gold_root)
        result = self.publish(output_root=root, gold_root=gold_root,
                              gold_fingerprint=original['input_set_fingerprint'])
        shutil.rmtree(silver)
        with ca.open_publication(root,result['product_fingerprint'],gold_root=gold_root,
                                 reference_root=self.reference) as reader:
            self.assertEqual(reader.report('reference_coverage')['totals']['missing_article_count'],'1')
            self.assertEqual(sum(int(r['discovered_article_count']) for r in reader.report('source_discovery_daily')['rows']),1)
        self.assertEqual(state(gold_root),before)

    def test_b16_schema_parity_and_existing_gold_consumer(self):
        self.assertEqual(tuple(ga.ARTICLE_SCHEMA), m.GOLD_SCHEMA)
        self.assertEqual(len(cp.SCHEMAS[m.CATALOG]), 38)
        self.assertEqual(len(cp.SCHEMAS[m.DAILY]), 5)
        before = state(self.gold)
        with ga.open_publication(self.gold, self.gold_id) as reader:
            self.assertEqual(len(reader.report('articles')), 4)
            self.assertEqual(sum(r['observation_count'] for r in reader.report('publishers')), 5)
        self.assertEqual(state(self.gold), before)

    def test_b15_large_counts_and_oversized_lookahead(self):
        for kind in ('counts', 'text'):
            root = self.base / f'large-{kind}'
            identity = gold_fixture(root, spark=self.spark, large_counts=kind=='counts',
                                    large_text=kind=='text')
            result = self.publish(gold_root=root, gold_fingerprint=identity)
            with ca.open_publication(self.root,result['product_fingerprint'],gold_root=root,
                                     reference_root=self.reference) as reader:
                if kind=='counts':
                    article = next(r for r in reader.report('catalog')['rows'] if r['article_id']==IDS[0])
                    self.assertEqual(article['observation_count'], '9007199254740993')
                    self.assertEqual(reader.report('reference_coverage')['totals']['article_count'], '4')
                else:
                    self.assertEqual(len(reader.report('catalog',source='beta')['rows']), 1)
                    # The large title is in the extra lookahead row, not the returned first row.
                    self.failure('RESPONSE_LIMIT', reader.report, 'catalog', limit=1)
                    self.assertIsNone(reader._conn)

    def test_b15_decimal_overflow_before_group_cast(self):
        frame = self.spark.createDataFrame([('a',date(2026,10,12),'MISSING')]*2,
            'source_id string, discovery_date date, reference_status string')
        with patch.object(m, 'MAX_COUNT', 1):
            self.failure('COUNT_OVERFLOW', cp.discovery, frame)

    def test_b02_warning_array_order_is_preserved_and_verified(self):
        root=self.base/'warnings-gold'
        identity=gold_fixture(root,spark=self.spark,two_warnings=True)
        result=self.publish(gold_root=root,gold_fingerprint=identity)
        self.gold=root
        self.identity=result['product_fingerprint']
        self.dest=m.publication_directory(self.root,self.identity)
        with self.open() as reader:
            row=next(r for r in reader.report('catalog')['rows'] if r['article_id']==IDS[0])
            self.assertEqual(row['quality_warnings'],['INVALID_PUBLISHED_AT','INVALID_UPDATED_AT'])
        self.rewrite(m.CATALOG,'SELECT * REPLACE (list_reverse(quality_warnings) AS quality_warnings) FROM original')
        self.failure('PUBLICATION_INVALID',self.open)

    def test_b14_cli_safe_diagnostics_and_exit_status(self):
        options = ['--root',str(self.root),'--publication',self.identity,'--gold-root',str(self.gold),
                   '--reference-root',str(self.reference),'--report','catalog']
        for extra,code,expected in (([],0,None), (['--limit','0'],2,'ARGUMENT_INVALID'),
                                    (['--source','secret/path'],2,'ARGUMENT_INVALID'),
                                    (['--unknown','secret'],2,'ARGUMENT_INVALID'),
                                    (['--report','catalog'],2,'ARGUMENT_INVALID')):
            out,err = io.StringIO(),io.StringIO()
            with redirect_stdout(out),redirect_stderr(err):
                self.assertEqual(ca.main(options+extra), code)
            if expected:
                self.assertEqual(out.getvalue(), '')
                self.assertEqual(json.loads(err.getvalue()), dict(code=expected,message=m.MESSAGES[expected]))
            else:
                self.assertEqual(err.getvalue(), '')
                self.assertEqual(len(json.loads(out.getvalue())['rows']),4)
        out,err=io.StringIO(),io.StringIO()
        with redirect_stdout(out),redirect_stderr(err),patch.object(pub, 'publish', side_effect=m.ContentFailure('PUBLICATION_FAILED')):
            # Argument rejection precedes any Spark initialization.
            self.assertEqual(pub.main(['--secret','credential']),2)
        self.assertEqual(out.getvalue(),'')
        self.assertNotIn('credential',err.getvalue())


if __name__ == '__main__':
    unittest.main()
