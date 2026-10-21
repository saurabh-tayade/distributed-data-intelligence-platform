"""Independent literal evidence, forged hashes, reader safety and raw correspondence."""
from pathlib import Path
from hashlib import sha256
import json
import os
import tempfile
import sqlite3
import unittest
from unittest.mock import patch
import duckdb
from publication_metadata import canonical_bytes
import reliability_format_metadata as m
import reliability_format_analytics as analytics
import reliability_benchmark_oracle as oracle
from reliability_format_fixtures import completed


def forge(output,fp,table,query):
    directory=m.publication_directory(output,fp);target=directory/table/'part.parquet'
    temporary=output/'replacement.parquet'
    with oracle.codec() as db:
        db.read_parquet(str(target),hive_partitioning=False).create_view('prior')
        db.execute('COPY ('+query+') TO ? (FORMAT PARQUET,COMPRESSION SNAPPY)',[str(temporary)])
    target.write_bytes(temporary.read_bytes());temporary.unlink()
    evidence=m.capture(directory,{'inputs.json':65536,'complete.json':1048576},m.TABLES,'PUBLICATION_INVALID','PUBLICATION_CHANGED')
    marker=json.loads((directory/'complete.json').read_bytes());marker['files']=evidence.inventory()
    (directory/'complete.json').write_bytes(canonical_bytes(marker))


class VerificationTests(unittest.TestCase):
    def setUp(self):self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
    def tearDown(self):self.temp.cleanup()

    def assert_code(self,code,operation,*args,**kwargs):
        with self.assertRaises(m.ReliabilityFailure) as caught:operation(*args,**kwargs)
        self.assertEqual(code,caught.exception.code)
        self.assertNotIn(str(self.root),str(caught.exception.diagnostic));self.assertNotIn('secret',str(caught.exception.diagnostic))

    def oracle(self,output,fp,batches,refs):
        scratch=self.root/('scratch-'+str(len(list(self.root.iterdir()))));scratch.mkdir()
        return oracle.verify_publication(output,fp,batch_root=batches,reference_root=refs,scratch_root=scratch)

    def test_literal_completed_outputs_and_three_logical_vectors(self):
        vectors=[]
        for fmt in ('JSONL','PARQUET'):
            root=self.root/fmt;root.mkdir();o,fp,b,r=completed(root,fmt)
            checked=self.oracle(o,fp,b,r)
            self.assertEqual((10,5,4,1,5,3),tuple(checked[k] for k in ('physical_occurrences','valid_occurrences','unique_valid_events','identical_duplicate_occurrences','rejected_occurrences','groups_checked')))
            self.assertLessEqual(checked['max_output_chunk_bytes'],2097152)
            vectors.append(tuple(checked[k+'_multiset_sha256'] for k in ('valid','unique','rejected')))
            with analytics.open_publication(o,fp,batch_root=b,reference_root=r) as s:
                coverage=s.report('reference_coverage')
                self.assertEqual(dict(event_count='4',matched_event_count='3',missing_event_count='1',event_service_count='3',matched_service_count='2',missing_service_count='1',matched_services_missing_optional_attributes_count='1',reference_service_count='3',unused_reference_service_count='1'),coverage['totals'])
                incoming=s.report('input_coverage')['rows'][0]
                self.assertEqual('1969-12-31T23:59:59.999999Z',incoming['first_event_time'])
                self.assertEqual('2026-10-01T00:05:00.000000Z',incoming['last_event_time'])
                self.assertEqual('1790899500000001',incoming['max_receipt_delay_us'])
        self.assertEqual(*vectors)

    def test_forged_values_rejected_by_both_independent_verifiers(self):
        cases=[(m.EVENTS,'SELECT * REPLACE(duration_ms+1 AS duration_ms) FROM prior'),
            (m.EVENTS,"SELECT * REPLACE('example-service' AS owner_team) FROM prior"),
            (m.EVENTS,'SELECT * REPLACE(first_received_at+INTERVAL 1 MICROSECOND AS first_received_at) FROM prior'),
            (m.OCCURRENCES,'SELECT * REPLACE(record_ordinal+1 AS record_ordinal) FROM prior'),
            (m.OCCURRENCES,"SELECT * REPLACE('DUPLICATE' AS disposition) FROM prior"),
            (m.OCCURRENCES,"SELECT * REPLACE(['FIELD_VALUE'] AS reason_codes) FROM prior"),
            (m.METRICS,'SELECT * REPLACE(request_count+1 AS request_count) FROM prior'),
            (m.METRICS,'SELECT * REPLACE((duration_sum_ms+1)::DECIMAL(38,0) AS duration_sum_ms) FROM prior'),
            (m.METRICS,'SELECT * REPLACE(0.125::DOUBLE AS server_error_rate) FROM prior'),
            (m.METRICS,'SELECT * REPLACE(99.0::DOUBLE AS mean_duration_ms) FROM prior'),
            (m.METRICS,'SELECT * REPLACE(999999::BIGINT AS p95_duration_ms_approx) FROM prior'),
            (m.EVENTS,"SELECT * FROM prior WHERE service_id<>'svc09'"),
            (m.OCCURRENCES,'SELECT * FROM prior UNION ALL SELECT * FROM prior LIMIT 11')]
        for i,(table,query) in enumerate(cases):
            with self.subTest(table=table,case=i):
                root=self.root/str(i);root.mkdir();o,fp,b,r=completed(root,'PARQUET')
                forge(o,fp,table,query)
                self.assert_code('PUBLICATION_INVALID',analytics.open_publication,o,fp,batch_root=b,reference_root=r)
                self.assert_code('PUBLICATION_INVALID',self.oracle,o,fp,b,r)

    def test_qualified_unequal_percentiles_are_accepted(self):
        o,fp,b,r=completed(self.root)
        forge(o,fp,m.METRICS,"SELECT * REPLACE(CASE WHEN service_id='svc00' THEN 10 ELSE p95_duration_ms_approx END AS p95_duration_ms_approx) FROM prior")
        result=self.oracle(o,fp,b,r);self.assertEqual(1,result['max_rank_displacement']);self.assertEqual(10,result['max_absolute_p95_error_ms'])
        with analytics.open_publication(o,fp,batch_root=b,reference_root=r):pass

    def test_every_physical_schema_is_checked_with_refreshed_hashes(self):
        for i,query in enumerate(('SELECT * REPLACE(duration_ms::INTEGER AS duration_ms) FROM prior',
                                 'SELECT * REPLACE(event_time::TIMESTAMP AS event_time) FROM prior',
                                 'SELECT *,1 AS unapproved FROM prior')):
            with self.subTest(case=i):
                root=self.root/str(i);root.mkdir();o,fp,b,r=completed(root)
                forge(o,fp,m.EVENTS,query)
                self.assert_code('PUBLICATION_INVALID',analytics.open_publication,o,fp,batch_root=b,reference_root=r)
                self.assert_code('PUBLICATION_INVALID',self.oracle,o,fp,b,r)

    def test_missing_completion_and_corrupt_output_never_promoted(self):
        o,fp,b,r=completed(self.root);directory=m.publication_directory(o,fp)
        marker=directory/'complete.json';marker.rename(directory/'complete.tmp')
        self.assert_code('PUBLICATION_INCOMPLETE',analytics.open_publication,o,fp,batch_root=b,reference_root=r)
        self.assert_code('PUBLICATION_INCOMPLETE',self.oracle,o,fp,b,r)
        self.assertFalse(marker.exists());(directory/'complete.tmp').rename(marker)
        (directory/m.EVENTS/'part.parquet').write_bytes(b'corrupt')
        self.assert_code('PUBLICATION_INVALID',analytics.open_publication,o,fp,batch_root=b,reference_root=r)
        self.assert_code('PUBLICATION_INVALID',self.oracle,o,fp,b,r)

    def test_dependency_missing_or_corrupted(self):
        for name in ('batch','reference'):
            with self.subTest(name=name):
                root=self.root/name;root.mkdir();o,fp,b,r=completed(root)
                target=next(b.rglob('events.jsonl')) if name=='batch' else next(r.rglob('reference.csv'))
                target.write_bytes(target.read_bytes()+b'\n')
                self.assert_code('DEPENDENCY_INVALID',analytics.open_publication,o,fp,batch_root=b,reference_root=r)
                self.assert_code('DEPENDENCY_INVALID',self.oracle,o,fp,b,r)

    def test_missing_selected_dependency_namespaces(self):
        for name in ('batch','reference'):
            with self.subTest(name=name):
                root=self.root/name;root.mkdir();o,fp,b,r=completed(root)
                directory=next(b.rglob('batch_id=b00')) if name=='batch' else next(r.glob('reference_id=*'))/'revision=r1'
                directory.rename(root/'retained-away')
                self.assert_code('DEPENDENCY_UNAVAILABLE',analytics.open_publication,o,fp,batch_root=b,reference_root=r)
                self.assert_code('DEPENDENCY_UNAVAILABLE',self.oracle,o,fp,b,r)

    def test_oracle_scratch_failure_restores_environment(self):
        o,fp,b,r=completed(self.root);prior=os.environ.get('SQLITE_TMPDIR')
        with patch.object(sqlite3,'connect',side_effect=sqlite3.OperationalError('secret')):
            self.assert_code('PUBLICATION_INVALID',self.oracle,o,fp,b,r)
        self.assertEqual(prior,os.environ.get('SQLITE_TMPDIR'))

    def test_replay_does_not_require_spark_or_modify_evidence(self):
        import reliability_format_publication as publisher
        o,fp,b,r=completed(self.root);directory=m.publication_directory(o,fp);manifest=json.loads((directory/'inputs.json').read_bytes())
        prior={p:(p.stat().st_mtime_ns,sha256(p.read_bytes()).hexdigest()) for p in self.root.rglob('*') if p.is_file()}
        staging=self.root/'stage';staging.mkdir();ref=manifest['reference']
        result=publisher.publish(None,batch_root=b,input_format='JSONL',batches=[(x['source_id'],x['batch_id'],x['batch_fingerprint']) for x in manifest['batches']],
            reference_root=r,reference_id=ref['reference_id'],revision=ref['revision'],reference_fingerprint=ref['snapshot_fingerprint'],
            staging_root=staging,output_root=o,execution_config=None)
        self.assertEqual('replayed',result.status);self.assertEqual([],list(staging.iterdir()))
        self.assertEqual(prior,{p:(p.stat().st_mtime_ns,sha256(p.read_bytes()).hexdigest()) for p in prior})

    def test_oracle_rechecks_raw_dependencies_after_output_comparison(self):
        o,fp,b,r=completed(self.root);target=next(b.rglob('events.jsonl'));original=oracle.Oracle.compare
        def changed(instance,*args):
            result=original(instance,*args);target.write_bytes(target.read_bytes()+b'\n');return result
        with patch.object(oracle.Oracle,'compare',changed):self.assert_code('DEPENDENCY_CHANGED',self.oracle,o,fp,b,r)

    def test_reports_parameters_ordering_pagination_and_filter_totals(self):
        o,fp,b,r=completed(self.root)
        with analytics.open_publication(o,fp,batch_root=b,reference_root=r) as s:
            first=s.report('reference_coverage',limit=1);second=s.report('reference_coverage',limit=1,offset=1)
            self.assertTrue(first['has_more']);self.assertEqual(['svc-unused','svc00'],[first['rows'][0]['service_id'],second['rows'][0]['service_id']])
            self.assertEqual('4',first['totals']['event_count'])
            self.assertFalse(s.report('reference_coverage',offset=1000000)['has_more'])
            unused=s.report('reference_coverage',service_id='svc-unused');self.assertEqual('0',unused['totals']['event_count'])
            self.assertEqual('UNUSED',unused['rows'][0]['reference_usage'])
            self.assertEqual([],s.report('input_coverage',source_id='absent')['rows'])
            bounds=dict(start='2026-10-01T00:00:00.000000Z',end='2026-10-01T00:05:00.000000Z')
            self.assertEqual(1,len(s.report('requests_5m',**bounds)['rows']))
            self.assertEqual([],s.report('requests_5m',**bounds,environment='production')['rows'])
            self.assertEqual(m.LABEL,first['label']);self.assertEqual(fp,first['product_fingerprint'])
            settings=s._db.execute("SELECT current_setting('threads'),current_setting('memory_limit'),current_setting('temp_directory'),current_setting('autoload_known_extensions'),current_setting('autoinstall_known_extensions')").fetchone()
            self.assertEqual((2,'512.0 MiB','',False,False),settings)
            for kwargs in ({'limit':0},{'limit':1001},{'limit':True},{'offset':-1},{'offset':1000001},{'service_id':"svc00' OR 1=1"},{'source_id':'bad'}):
                self.assert_code('ARGUMENT_INVALID',s.report,'reference_coverage',**kwargs)
            self.assertEqual('OK',s.report('reference_coverage')['status'])

    def test_year_10000_exclusive_end_and_negative_roles(self):
        o,fp,b,r=completed(self.root)
        with analytics.open_publication(o,fp,batch_root=b,reference_root=r) as s:
            self.assertEqual([],s.report('requests_5m',start='9999-12-31T23:55:00.000000Z',end=m.END_SENTINEL)['rows'])
            for start,end in ((m.END_SENTINEL,m.END_SENTINEL),('9999-12-31T23:55:00.000000Z','10001-01-01T00:00:00.000000Z'),('9999-11-30T23:55:00.000000Z',m.END_SENTINEL),('2026-10-01T00:00:00.000001Z','2026-10-01T00:05:00.000000Z')):
                self.assert_code('ARGUMENT_INVALID',s.report,'requests_5m',start=start,end=end)

    def test_response_admission_and_no_partial_result_after_failure(self):
        o,fp,b,r=completed(self.root)
        for name,value in (('PAGE_BYTES',0),('RESPONSE_LIMIT',100)):
            s=analytics.open_publication(o,fp,batch_root=b,reference_root=r)
            with patch.object(m,name,value):self.assert_code('RESPONSE_LIMIT',s.report,'reference_coverage')
            self.assertIsNone(s._db);s.close()
        s=analytics.open_publication(o,fp,batch_root=b,reference_root=r)
        with patch.object(analytics,'canonical_bytes',return_value=b'x'*(1048576+1)):
            self.assert_code('RESPONSE_LIMIT',s.report,'reference_coverage')
        self.assertIsNone(s._db)

    def test_query_memory_failure_busy_and_closed_session(self):
        o,fp,b,r=completed(self.root);s=analytics.open_publication(o,fp,batch_root=b,reference_root=r)
        s._lock.acquire()
        try:self.assert_code('BUSY',s.report,'reference_coverage')
        finally:s._lock.release()
        original=s._db
        class Failed:
            def execute(self,*a,**k):raise duckdb.OutOfMemoryException('secret')
            def close(self):original.close()
        s._db=Failed();self.assert_code('RESOURCE_LIMIT',s.report,'reference_coverage')
        self.assertIsNone(s._db);self.assert_code('QUERY_FAILED',s.report,'reference_coverage');s.close()

    def test_post_query_mutation_prevents_success(self):
        o,fp,b,r=completed(self.root);s=analytics.open_publication(o,fp,batch_root=b,reference_root=r)
        check=s._check;calls=0
        def changed():
            nonlocal calls
            calls+=1
            if calls==2:
                target=next(b.rglob('events.jsonl'));replacement=self.root/'same-bytes';replacement.write_bytes(target.read_bytes());replacement.replace(target)
            check()
        with patch.object(s,'_check',changed):self.assert_code('DEPENDENCY_CHANGED',s.report,'reference_coverage')
        self.assertIsNone(s._db)


if __name__=='__main__':unittest.main()
