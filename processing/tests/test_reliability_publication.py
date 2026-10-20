from contextlib import redirect_stderr,redirect_stdout
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
from pyspark.sql import SparkSession
from publication_metadata import canonical_bytes
import reliability_metadata as m
import reliability_inputs as inputs
import reliability_processing as processing
import reliability_publication as publication
import reliability_analytics as analytics
from reliability_fixtures import event,profile,f1,f2,register_fixture,reference_fixture,RECEIVED


class PublicationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory();cls.root=Path(cls.temp.name)
        cls.spark=SparkSession.builder.master('local[2]').appName('e2-publication-tests').config('spark.ui.enabled','false').config('spark.driver.memory','1g').getOrCreate()
        cls.spark.sparkContext.setLogLevel('ERROR')
        started=time.perf_counter();cls.batchroot,summary=register_fixture(cls.root);cls.refroot,ref=reference_fixture(cls.root)
        cls.registration_seconds=time.perf_counter()-started
        cls.stageroot=cls.root/'staging';cls.stageroot.mkdir();cls.output=cls.root/'output';cls.output.mkdir()
        cls.args=dict(batch_root=cls.batchroot,batches=[('fixture-source','b1',summary['batch_fingerprint'])],reference_root=cls.refroot,
            reference_id='fixture-services',revision='r1',reference_fingerprint=ref['snapshot_fingerprint'],staging_root=cls.stageroot,output_root=cls.output)
        started=time.perf_counter();cls.result=publication.publish(cls.spark,**cls.args);cls.publish_seconds=time.perf_counter()-started
        cls.identity=cls.result.product_fingerprint;cls.directory=m.publication_directory(cls.output,cls.identity)
        print(json.dumps({'e2_f2_registration_seconds':round(cls.registration_seconds,4),'e2_f2_publication_seconds':round(cls.publish_seconds,4),'physical_occurrences':32,'events':23}))

    @classmethod
    def tearDownClass(cls):cls.spark.stop();cls.temp.cleanup()

    def open(self,root=None,**kwargs):
        return analytics.open_publication(self.output if root is None else root,self.identity,batch_root=self.batchroot,reference_root=self.refroot,**kwargs)

    def copied(self,name):
        root=self.root/name;shutil.copytree(self.output,root)
        return root,m.publication_directory(root,self.identity)

    def assert_code(self,code,fn,*args,**kwargs):
        with self.assertRaises(m.ReliabilityFailure) as raised:fn(*args,**kwargs)
        self.assertEqual(code,raised.exception.code)
        text=canonical_bytes(raised.exception.diagnostic).decode()
        self.assertNotIn(str(self.root),text);self.assertNotIn('secret',text)
        self.assertEqual(m.MESSAGES[code],raised.exception.diagnostic['message'])
        return raised.exception

    def state(self,directory):
        return {p.relative_to(directory).as_posix():(p.read_bytes(),p.stat().st_mtime_ns) for p in directory.rglob('*') if p.is_file()}

    def mutate_table(self,directory,table,sql):
        marker=json.loads((directory/'complete.json').read_bytes());paths=[str(directory/f['path']) for f in marker['files'][table]]
        with analytics.connection() as db:
            db.read_parquet(paths,hive_partitioning=False,union_by_name=False).create_view('original')
            db.execute('CREATE TABLE changed AS '+sql)
            temp=directory/'changed.parquet';db.table('changed').write_parquet(str(temp))
        target=Path(paths[0]);temp.replace(target)
        for name in paths[1:]:
            path=Path(name);path.unlink();crc=path.parent/('.'+path.name+'.crc')
            if crc.exists():crc.unlink()
        evidence=m.capture(directory,{'inputs.json':m.MANIFEST_LIMIT,'complete.json':m.COMPLETION_LIMIT},m.TABLES,'PUBLICATION_INVALID','PUBLICATION_CHANGED')
        marker['files']=evidence.inventory();(directory/'complete.json').write_bytes(canonical_bytes(marker))

    def test_r05_cross_batch_redelivery_conflicts_and_scoped_coverage(self):
        before=self.state(self.directory)
        _,summary=register_fixture(self.root,[canonical_bytes(event(1)),b''],batch_id='redelivery')
        selected=self.args['batches']+[('fixture-source','redelivery',summary['batch_fingerprint'])]
        result=publication.publish(self.spark,**(self.args|dict(batches=selected)))
        self.assertEqual(dict(physical_occurrences=34,valid_occurrences=25,unique_valid_events=23,identical_duplicate_occurrences=2,rejected_occurrences=9),dict(result.occurrence_counts))
        with analytics.open_publication(self.output,result.product_fingerprint,batch_root=self.batchroot,reference_root=self.refroot) as session:
            page=session.report('input_coverage',batch_id='redelivery')
            self.assertEqual('0',page['totals']['accepted_representative_count']);self.assertEqual('1',page['totals']['duplicate_occurrence_count'])
            self.assertEqual('1',page['totals']['distinct_valid_identity_count']);self.assertEqual('0',page['totals']['duplicate_excess_in_scope'])
            self.assertEqual('23',session.report('reference_coverage')['totals']['event_count'])
        for index,record in enumerate((event(1,duration_ms=99),event(1,event_time=None))):
            batch_id=f'cross-conflict-{index}'
            _,summary=register_fixture(self.root,[canonical_bytes(record)],batch_id=batch_id)
            root=self.root/batch_id;root.mkdir()
            self.assert_code('EVENT_IDENTITY_CONFLICT',publication.publish,self.spark,**(self.args|dict(output_root=root,batches=self.args['batches']+[('fixture-source',batch_id,summary['batch_fingerprint'])])))
            self.assertEqual([],list(root.iterdir()))
        replay=publication.publish(self.spark,**(self.args|dict(batches=self.args['batches']*2)))
        self.assertEqual(self.identity,replay.product_fingerprint);self.assertEqual(1,replay.duplicate_batch_selections)
        self.assertEqual(before,self.state(self.directory))

    def test_r14_dependency_recheck_io_and_disappearance(self):
        batch=inputs.open_batch(self.batchroot,*self.args['batches'][0]);target=batch.evidence.directory/'data/events.jsonl'
        before=(target.read_bytes(),m.stamp(target.stat()))
        actual_open=os.open;actual_fdopen=os.fdopen
        for fault in ('read','close'):
            session=self.open();descriptors={}
            def record(path,*a,**kw):
                fd=actual_open(path,*a,**kw);descriptors[fd]=Path(path);return fd
            class Broken:
                def __init__(self,s):self.s=s
                def __enter__(self):return self
                def __exit__(self,*a):
                    self.s.close()
                    if fault=='close':raise OSError('secret-close')
                def fileno(self):return self.s.fileno()
                def read(self,*a):
                    if fault=='read':raise OSError('secret-read')
                    return self.s.read(*a)
            def wrap(fd,*a):
                stream=actual_fdopen(fd,*a)
                return Broken(stream) if descriptors.get(fd)==target else stream
            with patch.object(os,'open',side_effect=record),patch.object(os,'fdopen',side_effect=wrap):
                self.assert_code('DEPENDENCY_INVALID',session.report,'reference_coverage')
            self.assertIsNone(session._db);self.assertEqual(before,(target.read_bytes(),m.stamp(target.stat())))
        session=self.open();moved=target.with_suffix('.held');target.rename(moved)
        try:self.assert_code('DEPENDENCY_CHANGED',session.report,'reference_coverage')
        finally:moved.rename(target)

    def test_r16_output_caps_and_decimal_overflow_guards(self):
        for name in ('MAX_FILES','MAX_OUTPUT_BYTES','MAX_DATASET_FILES','MAX_ROWS'):
            with patch.object(m,name,0):
                with self.assertRaises(m.ReliabilityFailure) as caught:self.open()
                self.assertIn(caught.exception.code,('RESOURCE_LIMIT','PUBLICATION_INVALID'))
        with patch.object(m,'MAX_COUNT',19):
            with self.assertRaises(m.ReliabilityFailure) as caught:processing.aggregate(self.spark.read.parquet(*[str(p) for p in (self.directory/m.EVENTS).glob('*.parquet')]))
            self.assertEqual('COUNT_OVERFLOW',caught.exception.code)
        root,directory=self.copied('negative-epsilon')
        self.mutate_table(directory,m.METRICS,"SELECT * REPLACE(CASE WHEN server_error_count=0 THEN -0.0000000000001 ELSE server_error_rate END AS server_error_rate) FROM original")
        self.assert_code('PUBLICATION_INVALID',self.open,root)

    def test_r17_successful_cli_reports(self):
        for name in ('requests_5m','input_coverage','reference_coverage'):
            command=[sys.executable,'-m','reliability_analytics','--root',str(self.output),'--publication',self.identity,'--batch-root',str(self.batchroot),'--reference-root',str(self.refroot),'--report',name]
            if name=='requests_5m':command+=['--start','2026-10-20T12:00:00.000000Z','--end','2026-10-20T12:10:00.000000Z']
            proc=subprocess.run(command,capture_output=True,text=True)
            self.assertEqual(0,proc.returncode,proc.stderr);self.assertEqual('',proc.stderr)
            self.assertEqual('OK',json.loads(proc.stdout)['status'])

    def test_r11_replay_preserves_bytes_mtimes_and_skips_approximation(self):
        before=self.state(self.directory);started=time.perf_counter()
        with patch.object(processing,'aggregate',side_effect=AssertionError('replay must not approximate')):
            result=publication.publish(self.spark,**self.args)
        self.assertEqual('replayed',result.status);self.assertEqual(before,self.state(self.directory))
        print(json.dumps({'e2_f2_replay_seconds':round(time.perf_counter()-started,4)}))

    def test_r15_exact_reports_coverage_and_pagination(self):
        start=time.perf_counter()
        with self.open() as session:
            opened=time.perf_counter()-start;start=time.perf_counter()
            page=session.report('requests_5m',start='2026-10-20T12:00:00.000000Z',end='2026-10-20T12:10:00.000000Z')
            report_seconds=time.perf_counter()-start
            self.assertEqual(m.LABEL,page['label']);self.assertEqual('synthetic',page['origin'])
            self.assertEqual(['alpha','gamma','beta'],[r['service_id'] for r in page['rows']])
            self.assertEqual(['20','1','1'],[r['request_count'] for r in page['rows']])
            self.assertEqual(['2','1','0'],[r['server_error_count'] for r in page['rows']])
            self.assertEqual(['210','0','40'],[r['duration_sum_ms'] for r in page['rows']])
            self.assertEqual(10.5,page['rows'][0]['mean_duration_ms']);self.assertEqual(0.1,page['rows'][0]['server_error_rate'])
            self.assertIn(page['rows'][0]['p95_duration_ms_approx'],('18','19','20'))
            pre=session.report('requests_5m',start='1969-12-31T23:55:00.000000Z',end='1970-01-01T00:00:00.000000Z')
            self.assertEqual('3600000',pre['rows'][0]['duration_sum_ms'])
            first=session.report('requests_5m',start='2026-10-20T12:00:00.000000Z',end='2026-10-20T12:05:00.000000Z',limit=1)
            second=session.report('requests_5m',start='2026-10-20T12:00:00.000000Z',end='2026-10-20T12:05:00.000000Z',limit=1,offset=1)
            self.assertTrue(first['has_more']);self.assertFalse(second['has_more']);self.assertEqual('gamma',second['rows'][0]['service_id'])
            coverage=session.report('reference_coverage')
            self.assertEqual(['alpha','beta','delta','gamma'],[r['service_id'] for r in coverage['rows']])
            self.assertEqual(['21','1','0','1'],[r['event_count'] for r in coverage['rows']])
            self.assertEqual(['USED','USED','UNUSED','ABSENT'],[r['reference_usage'] for r in coverage['rows']])
            self.assertEqual(['owner_team','service_tier'],coverage['rows'][1]['missing_optional_attributes'])
            self.assertEqual(['23','22','1','3','2','1','1','3','1'],list(coverage['totals'].values()))
            input_page=session.report('input_coverage')
            self.assertEqual(['32','24','23','1','8'],[input_page['rows'][0][n] for n in ('physical_occurrence_count','valid_occurrence_count','accepted_representative_count','duplicate_occurrence_count','rejected_occurrence_count')])
            reasons=input_page['rows'][0]['rejected_reason_counts'];self.assertEqual('2',reasons['JSON_SYNTAX']);self.assertEqual('2',reasons['FIELD_VALUE']);self.assertEqual(8,sum(map(int,reasons.values())))
            self.assertEqual('1969-12-31T23:59:59.999999Z',input_page['rows'][0]['first_event_time'])
            self.assertEqual('2026-10-20T12:05:00.000000Z',input_page['rows'][0]['last_event_time'])
            self.assertEqual('0',input_page['rows'][0]['future_occurrence_count'])
            self.assertEqual('300000000',input_page['rows'][0]['min_receipt_delay_us'])
            self.assertEqual(str(m.time_us(RECEIVED)+1),input_page['rows'][0]['max_receipt_delay_us'])
            self.assertEqual('23',input_page['totals']['distinct_valid_identity_count'])
            self.assertEqual([],session.report('reference_coverage',service_id='absent')['rows'])
            for report in (page,coverage,input_page):
                self.assertEqual(self.identity,report['product_fingerprint']);self.assertNotIn('event_id',json.dumps(report));self.assertNotIn(str(self.root),json.dumps(report))
        print(json.dumps({'e2_f2_open_seconds':round(opened,4),'e2_f2_metrics_report_seconds':round(report_seconds,4)}))

    def test_r14_known_change_survives_later_io_and_cleanup(self):
        opened=self.open()
        with patch.object(opened,'_check',side_effect=[m.ReliabilityFailure('PUBLICATION_CHANGED'),m.ReliabilityFailure('DEPENDENCY_INVALID')]) as checked:
            self.assert_code('PUBLICATION_CHANGED',opened.report,'reference_coverage')
            self.assertEqual(1,checked.call_count)
        self.assertIsNone(opened._db)

    def test_r15_parameters_bounds_and_page_edges(self):
        with self.open() as session:
            for kwargs in [dict(limit=True),dict(limit=0),dict(limit=1001),dict(offset=-1),dict(offset=1000001),dict(offset=1.0),dict(source_id='alpha'),dict(service_id="x' OR 1=1"),dict(start=m.END_SENTINEL,end=m.END_SENTINEL),dict(start='9999-11-30T00:00:00.000000Z',end=m.END_SENTINEL)]:
                args=dict(start='2026-10-20T12:00:00.000000Z',end='2026-10-20T12:05:00.000000Z')|kwargs
                self.assert_code('ARGUMENT_INVALID',session.report,'requests_5m',**args)
            for name,kwargs in [('input_coverage',dict(service_id='alpha')),('reference_coverage',dict(environment='test')),('bad',{})]:
                self.assert_code('ARGUMENT_INVALID',session.report,name,**kwargs)
            page=session.report('reference_coverage',limit=1000,offset=1000000)
            self.assertEqual([],page['rows']);self.assertFalse(page['has_more'])
            self.assertEqual('0',session.report('input_coverage',source_id='absent')['totals']['physical_occurrence_count'])
            self.assertEqual('1',session.report('reference_coverage',service_id='delta')['totals']['unused_reference_service_count'])

    def test_r16_resource_response_busy_and_closed_session(self):
        with self.open() as session:
            self.assertEqual((2,'512.0 MiB','',False,False),session._db.execute("SELECT current_setting('threads'),current_setting('memory_limit'),current_setting('temp_directory'),current_setting('autoinstall_known_extensions'),current_setting('autoload_known_extensions')").fetchone())
            session._lock.acquire()
            try:self.assert_code('BUSY',session.report,'reference_coverage')
            finally:session._lock.release()
            self.assertEqual('OK',session.report('reference_coverage')['status'])
        self.assert_code('QUERY_FAILED',session.report,'reference_coverage')
        for setting in ('PAGE_BYTES','RESPONSE_LIMIT'):
            opened=self.open()
            with patch.object(m,setting,1):self.assert_code('RESPONSE_LIMIT',opened.report,'reference_coverage')
            self.assertIsNone(opened._db)
        original=analytics.connection
        def low_memory():
            return duckdb.connect(':memory:',config={'threads':2,'memory_limit':'1KiB','temp_directory':'','autoinstall_known_extensions':False,'autoload_known_extensions':False})
        with patch.object(analytics,'connection',side_effect=low_memory):self.assert_code('RESOURCE_LIMIT',self.open)
        self.assertEqual('9007199254740993',m.public(2**53+1))

    def test_r13_metadata_inventory_and_schema_corruption(self):
        for index,mutation in enumerate(('complete','manifest','extra','missing','temporary','schema')):
            root,directory=self.copied('corrupt-'+mutation)
            if mutation=='complete':(directory/'complete.json').write_bytes(b'{"secret":1}')
            elif mutation=='manifest':(directory/'inputs.json').write_bytes((directory/'inputs.json').read_bytes()+b' ')
            elif mutation=='extra':(directory/m.EVENTS/'.hidden').write_bytes(b'secret')
            elif mutation=='missing':next((directory/m.EVENTS).glob('*.parquet')).unlink()
            elif mutation=='temporary':(directory/'complete.tmp').write_bytes(b'{}')
            else:self.mutate_table(directory,m.EVENTS,'SELECT * REPLACE(duration_ms::VARCHAR AS duration_ms) FROM original')
            self.assert_code('PUBLICATION_INVALID',self.open,root)
        root,directory=self.copied('incomplete');(directory/'complete.json').unlink()
        self.assert_code('PUBLICATION_INCOMPLETE',self.open,root)
        self.assert_code('PUBLICATION_UNAVAILABLE',self.open,self.root/'absent')

    def test_r10_independent_semantics_with_refreshed_hashes(self):
        for name,table,sql in [('event',m.EVENTS,"SELECT * REPLACE(CASE WHEN service_id='beta' THEN 41 ELSE duration_ms END AS duration_ms) FROM original"),
            ('occurrence',m.OCCURRENCES,"SELECT * REPLACE(CASE WHEN line_number=21 THEN 'ACCEPTED' ELSE disposition END AS disposition) FROM original"),
            ('p95',m.METRICS,"SELECT * REPLACE(CASE WHEN service_id='alpha' AND request_count=20 THEN 1 ELSE p95_duration_ms_approx END AS p95_duration_ms_approx) FROM original"),
            ('rate',m.METRICS,"SELECT * REPLACE(0.5::DOUBLE AS server_error_rate) FROM original"),
            ('reference',m.EVENTS,"SELECT * REPLACE('Other Service' AS service_name) FROM original")]:
            root,directory=self.copied('semantic-'+name);self.mutate_table(directory,table,sql)
            self.assert_code('PUBLICATION_INVALID',self.open,root)
        root,directory=self.copied('qualified-alternative')
        self.mutate_table(directory,m.METRICS,"SELECT * REPLACE(CASE WHEN request_count=20 THEN 18 ELSE p95_duration_ms_approx END AS p95_duration_ms_approx) FROM original")
        with self.open(root) as session:
            self.assertEqual('18',session.report('requests_5m',service_id='alpha',start='2026-10-20T12:00:00.000000Z',end='2026-10-20T12:05:00.000000Z')['rows'][0]['p95_duration_ms_approx'])
        result=publication.publish(self.spark,**(self.args|dict(output_root=root)))
        self.assertEqual('replayed',result.status)

    def test_r13_missing_corrupt_dependencies_and_r14_mutation(self):
        batch=inputs.open_batch(self.batchroot,*self.args['batches'][0]);raw=batch.evidence.directory/'data/events.jsonl'
        saved=raw.read_bytes();stat=raw.stat()
        try:
            with self.open() as session:
                raw.write_bytes(b'corrupt')
                self.assert_code('DEPENDENCY_CHANGED',session.report,'reference_coverage')
                self.assertIsNone(session._db)
            self.assert_code('DEPENDENCY_INVALID',self.open)
        finally:raw.write_bytes(saved);os.utime(raw,ns=(stat.st_atime_ns,stat.st_mtime_ns))
        root,directory=self.copied('observed-output-change')
        with self.open(root) as session:
            target=next((directory/m.EVENTS).glob('*.parquet'));saved=target.read_bytes();saved_stat=target.stat()
            clone=target.with_suffix('.copy');clone.write_bytes(saved);os.utime(clone,ns=(saved_stat.st_atime_ns,saved_stat.st_mtime_ns));clone.replace(target)
            self.assert_code('PUBLICATION_CHANGED',session.report,'reference_coverage')

    def test_r14_ordinary_read_close_failures_without_mutation(self):
        target=(self.directory/'inputs.json').resolve();before=(target.read_bytes(),m.stamp(target.stat()))
        original_open=os.open;original_fdopen=os.fdopen
        for fault in ('read','close'):
            descriptors={}
            def record(path,*args,**kwargs):
                fd=original_open(path,*args,**kwargs);descriptors[fd]=Path(path);return fd
            class Broken:
                def __init__(self,handle):self.h=handle
                def __enter__(self):return self
                def __exit__(self,*args):
                    self.h.close()
                    if fault=='close':raise OSError('secret-close')
                def fileno(self):return self.h.fileno()
                def read(self,*args):
                    if fault=='read':raise OSError('secret-read')
                    return self.h.read(*args)
            def wrap(fd,*args):
                handle=original_fdopen(fd,*args)
                return Broken(handle) if descriptors.get(fd)==target else handle
            with patch.object(os,'open',side_effect=record),patch.object(os,'fdopen',side_effect=wrap):
                self.assert_code('PUBLICATION_INVALID',self.open)
            self.assertEqual(before,(target.read_bytes(),m.stamp(target.stat())))

    def test_r12_precompletion_failures_and_lost_response(self):
        from pyspark.sql.readwriter import DataFrameWriter
        original_write=m.write;original_parquet=DataFrameWriter.parquet;original_rename=Path.rename
        for fault in ('stage','metadata','spark','readback','temporary','rename','after-rename'):
            root=self.root/('failure-'+fault);root.mkdir()
            args=self.args|dict(output_root=root)
            def write(path,raw):
                if fault=='stage' and path.name=='stage.json':raise OSError('secret-stage-close')
                if fault=='metadata' and path.name=='inputs.json':raise OSError('secret-write')
                if fault=='temporary' and path.name=='complete.tmp':raise OSError('secret-close')
                return original_write(path,raw)
            def parquet(writer,path,*a,**kw):
                if fault=='spark':
                    Path(path).mkdir();(Path(path)/'partial').write_bytes(b'partial');raise OSError('secret-spark')
                return original_parquet(writer,path,*a,**kw)
            def rename(path,target):
                if path.name=='complete.tmp':
                    if fault=='rename':raise OSError('secret-rename')
                    if fault=='after-rename':original_rename(path,target);raise OSError('secret-response')
                return original_rename(path,target)
            context=patch.object(analytics,'verify_core',side_effect=OSError('secret-readback')) if fault=='readback' else patch.object(m,'now',wraps=m.now)
            with patch.object(m,'write',side_effect=write),patch.object(DataFrameWriter,'parquet',new=parquet),patch.object(Path,'rename',new=rename),context:
                self.assert_code('PUBLICATION_FAILED',publication.publish,self.spark,**args)
            directory=m.publication_directory(root,self.identity)
            if fault=='stage':
                self.assertFalse(directory.exists());self.assertEqual([],list(root.iterdir()))
                continue
            self.assertEqual(fault=='after-rename',(directory/'complete.json').exists())
            if fault=='after-rename':self.assertEqual('replayed',publication.publish(self.spark,**args).status)
            else:self.assert_code('PUBLICATION_INCOMPLETE',publication.publish,self.spark,**args)

    def test_r11_empty_rejected_header_reference_and_year9999(self):
        for name,rows,emptyref in [('empty',[],False),('rejected',[b'{}',b''],False),('header',f1(),True),('last',[canonical_bytes(event(99,event_time='9999-12-31T23:59:59.999999Z'))],False),('first',[canonical_bytes(event(99,event_time='0001-01-01T00:00:00.000000Z'))],False)]:
            root=self.root/('variant-'+name);root.mkdir();batchroot,summary=register_fixture(root,rows);refs,ref=reference_fixture(root,empty=emptyref)
            output=root/'output';output.mkdir();staging=root/'staging';staging.mkdir()
            args=dict(batch_root=batchroot,batches=[('fixture-source','b1',summary['batch_fingerprint'])],reference_root=refs,reference_id='fixture-services',revision='r1',reference_fingerprint=ref['snapshot_fingerprint'],staging_root=staging,output_root=output)
            result=publication.publish(self.spark,**args)
            with analytics.open_publication(output,result.product_fingerprint,batch_root=batchroot,reference_root=refs) as session:
                coverage=session.report('reference_coverage')
                if name in ('empty','rejected'):
                    self.assertEqual('0',coverage['totals']['event_count']);self.assertEqual('3',coverage['totals']['unused_reference_service_count'])
                    self.assertEqual('2' if name=='rejected' else '0',session.report('input_coverage')['totals']['rejected_occurrence_count'])
                elif name=='header':self.assertEqual('20',coverage['totals']['missing_event_count'])
                elif name=='last':
                    page=session.report('requests_5m',start='9999-12-31T23:55:00.000000Z',end=m.END_SENTINEL)
                    self.assertEqual('9999-12-31T23:55:00.000000Z',page['rows'][0]['window_start']);self.assertEqual('1',page['rows'][0]['request_count'])
                    input_row=session.report('input_coverage')['rows'][0]
                    self.assertEqual('1',input_row['future_occurrence_count'])
                    self.assertEqual(str(m.time_us(RECEIVED)-(m.END_US-1)),input_row['min_receipt_delay_us'])
                else:self.assertEqual('0001-01-01T00:00:00.000000Z',session.report('requests_5m',start='0001-01-01T00:00:00.000000Z',end='0001-01-01T00:05:00.000000Z')['rows'][0]['window_start'])

    def test_r11_reference_correction_and_historical_batch(self):
        before=self.state(self.directory)
        refs,ref=reference_fixture(self.root,revision='r2',correction=True)
        changed=publication.publish(self.spark,**(self.args|dict(revision='r2',reference_fingerprint=ref['snapshot_fingerprint'])))
        self.assertNotEqual(self.identity,changed.product_fingerprint)
        with analytics.open_publication(self.output,changed.product_fingerprint,batch_root=self.batchroot,reference_root=self.refroot) as session:
            self.assertEqual('example-service',session.report('reference_coverage',service_id='alpha')['rows'][0]['owner_team'])
        _,summary=register_fixture(self.root,[canonical_bytes(event(29,event_time='2020-01-01T00:00:00.000000Z',duration_ms=7))],batch_id='b2')
        history=self.args['batches']+[('fixture-source','b2',summary['batch_fingerprint'])]
        result=publication.publish(self.spark,**(self.args|dict(batches=history)))
        self.assertEqual(24,result.occurrence_counts['unique_valid_events']);self.assertNotEqual(self.identity,result.product_fingerprint)
        self.assertEqual(before,self.state(self.directory))

    def test_r17_separate_spark_free_consumer_and_cli_safety(self):
        script="""import importlib.abc,sys,json
class Block(importlib.abc.MetaPathFinder):
 def find_spec(self,fullname,path=None,target=None):
  if fullname.split('.')[0] in ('pyspark','silver','silver_inputs','gold','gold_publication','content_publication','reliability_processing','reliability_publication','pytz'):raise RuntimeError('forbidden import')
sys.meta_path.insert(0,Block())
import reliability_analytics as a
with a.open_publication(sys.argv[1],sys.argv[2],batch_root=sys.argv[3],reference_root=sys.argv[4]) as s:
 results=[s.report('reference_coverage'),s.report('input_coverage'),s.report('requests_5m',start='2026-10-20T12:00:00.000000Z',end='2026-10-20T12:10:00.000000Z')]
 print(json.dumps([r['status'] for r in results]))
"""
        proc=subprocess.run([sys.executable,'-c',script,str(self.output),self.identity,str(self.batchroot),str(self.refroot)],env=os.environ|{'JAVA_HOME':'/nonexistent','TZ':'Pacific/Honolulu'},capture_output=True,text=True)
        self.assertEqual(0,proc.returncode,proc.stderr);self.assertEqual(['OK']*3,json.loads(proc.stdout))
        for module in ('reliability_publication','reliability_analytics'):
            for args,code in [(['--help'],0),(['--secret','private'],2)]:
                proc=subprocess.run([sys.executable,'-m',module,*args],capture_output=True,text=True)
                self.assertEqual(code,proc.returncode,proc.stderr)
                if code:self.assertEqual('',proc.stdout);self.assertNotIn('private',proc.stderr);self.assertEqual('ARGUMENT_INVALID',json.loads(proc.stderr)['code'])


if __name__=='__main__':unittest.main()
