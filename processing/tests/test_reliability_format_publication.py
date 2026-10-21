"""Literal G1 v2 publication, independent reports and immutable replay."""
from pathlib import Path
from hashlib import sha256
import tempfile
import unittest
import subprocess
import sys
import json
from unittest.mock import patch
import reliability_format_metadata as m
import reliability_format_inputs as inputs
import reliability_format_publication as publication
import reliability_format_analytics as analytics
from reliability_format_fixtures import register,reference,g1,benchmark_reference,completed

EXECUTION=dict(master='local[2]',shuffle_partitions=8,default_parallelism=8,
    driver_memory_mib=1024,executor_memory_mib=1024,output_partitions=4,event_log_enabled=False)


class FormatPublicationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.logs=tempfile.TemporaryDirectory()
        cls.spark=SparkSession.builder.master('local[2]').appName('ddip-v2-tests').config(
            'spark.ui.enabled','false').config('spark.default.parallelism','8').config(
            'spark.driver.memory','1g').config('spark.executor.memory','1g').config(
            'spark.eventLog.enabled','false').getOrCreate()

    @classmethod
    def tearDownClass(cls):cls.spark.stop();cls.logs.cleanup()

    def test_g1_complete_parity_reports_and_replay(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);refs,ref=reference(root);results=[]
            for fmt in ('JSONL','PARQUET'):
                batches,batch=register(root,fmt,rows=list(reversed(g1())) if fmt=='PARQUET' else None)
                output=root/('output-'+fmt);stage=root/('stage-'+fmt);output.mkdir();stage.mkdir()
                kwargs=dict(batch_root=batches,input_format=fmt,batches=[('bench-source','b00',batch['batch_fingerprint'])],
                    reference_root=refs,reference_id='bench-services',revision='r1',reference_fingerprint=ref['snapshot_fingerprint'],
                    staging_root=stage,output_root=output,execution_config=EXECUTION)
                # Test the concrete state machine directly too, so unexpected engine errors
                # have an internal traceback in test output, never a public diagnostic.
                result=publication._publish(self.spark,**kwargs,ledger=publication.Ledger())
                self.assertEqual('published',result.status)
                self.assertEqual(dict(physical_occurrences=10,valid_occurrences=5,unique_valid_events=4,
                    identical_duplicate_occurrences=1,rejected_occurrences=5),dict(result.occurrence_counts))
                directory=m.publication_directory(output,result.product_fingerprint)
                before={p.relative_to(directory).as_posix():(p.stat().st_mtime_ns,sha256(p.read_bytes()).hexdigest()) for p in directory.rglob('*') if p.is_file()}
                with analytics.open_publication(output,result.product_fingerprint,batch_root=batches,reference_root=refs) as session:
                    report=session.report('requests_5m',start='2026-10-01T00:00:00.000000Z',end='2026-10-01T00:10:00.000000Z')
                    self.assertEqual(2,len(report['rows']))
                    a,b=report['rows'];self.assertEqual(('2','1','30',15.0),tuple(a[n] for n in ('request_count','server_error_count','duration_sum_ms','mean_duration_ms')))
                    self.assertEqual(('1','0','7'),tuple(b[n] for n in ('request_count','server_error_count','duration_sum_ms')))
                    self.assertIn(a['p95_duration_ms_approx'],('10','20'))
                    previous=session.report('requests_5m',start='1969-12-31T23:55:00.000000Z',end='1970-01-01T00:00:00.000000Z')
                    self.assertEqual('MISSING',previous['rows'][0]['reference_status'])
                    self.assertEqual('1',previous['rows'][0]['server_error_count'])
                    coverage=session.report('reference_coverage');self.assertEqual(4,len(coverage['rows']))
                    incoming=session.report('input_coverage');self.assertEqual('10',incoming['rows'][0]['physical_occurrence_count'])
                    self.assertEqual('5',incoming['rows'][0]['rejected_occurrence_count'])
                    self.assertEqual(2,report['product_contract_version']);self.assertEqual(fmt,report['input_format'])
                    self.assertEqual(1,session._verification_stats['ordered_query_executions'])
                    results.append((a['request_count'],a['duration_sum_ms'],coverage['totals']))
                timing=[];replay=publication.publish(self.spark,**kwargs,observer=timing.append)
                self.assertEqual('replayed',replay.status)
                self.assertEqual(timing[0]['pipeline_ns'],sum(timing[0]['exclusive_phases_ns'].values()))
                after={p.relative_to(directory).as_posix():(p.stat().st_mtime_ns,sha256(p.read_bytes()).hexdigest()) for p in directory.rglob('*') if p.is_file()}
                self.assertEqual(before,after)
                from reliability_benchmark_oracle import verify_publication
                scratch=root/('oracle-'+fmt);scratch.mkdir()
                checked=verify_publication(output,result.product_fingerprint,batch_root=batches,reference_root=refs,scratch_root=scratch)
                self.assertEqual(('PASSED',4,3),(checked['status'],checked['unique_valid_events'],checked['groups_checked']))
                self.assertEqual('84a1b3d7acfcc43ee055a4369e8681e6f018a577a6e90468fed0cff8d34094d7',checked['valid_multiset_sha256'])
                child='''import importlib.abc,json,sys
class Block(importlib.abc.MetaPathFinder):
 def find_spec(self,name,path=None,target=None):
  if name.startswith(('pyspark','py4j','reliability_processing','reliability_format_processing','reliability_publication','reliability_format_publication','gold','content_')):raise AssertionError('Producer import forbidden')
sys.meta_path.insert(0,Block())
import reliability_format_analytics as a
with a.open_publication(sys.argv[1],sys.argv[2],batch_root=sys.argv[3],reference_root=sys.argv[4]) as s:
 out=[s.report('requests_5m',start='2026-10-01T00:00:00.000000Z',end='2026-10-01T00:10:00.000000Z'),s.report('input_coverage'),s.report('reference_coverage')]
print(json.dumps(out))
'''
                child_result=subprocess.run([sys.executable,'-c',child,str(output),result.product_fingerprint,str(batches),str(refs)],capture_output=True,text=True)
                self.assertEqual(0,child_result.returncode,child_result.stderr)
                self.assertEqual([2,1,4],[len(r['rows']) for r in json.loads(child_result.stdout)])
            self.assertEqual(*results)

    def publish_fixture(self,root,fmt,rows,*,empty_reference=False):
        batches,b=register(root,fmt,rows);refs,r=reference(root,empty=empty_reference)
        stage=root/'stage';output=root/'output';stage.mkdir();output.mkdir()
        kwargs=dict(batch_root=batches,input_format=fmt,batches=[('bench-source','b00',b['batch_fingerprint'])],
            reference_root=refs,reference_id='bench-services',revision='r1',reference_fingerprint=r['snapshot_fingerprint'],
            staging_root=stage,output_root=output,execution_config=EXECUTION)
        return publication.publish(self.spark,**kwargs),kwargs

    def test_empty_and_rejected_only_schema_bearing_outputs(self):
        from reliability_benchmark_oracle import verify_publication
        for fmt in ('JSONL','PARQUET'):
            for rejected in (False,True):
                with self.subTest(fmt=fmt,rejected=rejected),tempfile.TemporaryDirectory() as t:
                    root=Path(t);rows=g1()[5:] if rejected else []
                    result,kw=self.publish_fixture(root,fmt,rows,empty_reference=True)
                    self.assertEqual(0,result.occurrence_counts['unique_valid_events'])
                    self.assertEqual(len(rows),result.occurrence_counts['rejected_occurrences'])
                    with analytics.open_publication(kw['output_root'],result.product_fingerprint,batch_root=kw['batch_root'],reference_root=kw['reference_root']) as s:
                        self.assertEqual([],s.report('reference_coverage')['rows'])
                        self.assertEqual([],s.report('requests_5m',start='2026-10-01T00:00:00.000000Z',end='2026-10-02T00:00:00.000000Z')['rows'])
                    scratch=root/'oracle';scratch.mkdir()
                    checked=verify_publication(kw['output_root'],result.product_fingerprint,batch_root=kw['batch_root'],reference_root=kw['reference_root'],scratch_root=scratch)
                    self.assertEqual(0,checked['groups_checked'])

    def test_cross_batch_conflicts_and_invalid_identity_policy_precede_reservation(self):
        a=g1()[0]
        variants=[([a],[a|{'duration_ms':11}]),([a],[a|{'event_time':'2026-10-01T00:00:00.000001Z'}]),
            ([a],[a|{'event_time':None}]),([a|{'duration_ms':-1}],[a|{'environment':'invalid'}])]
        for fmt in ('JSONL','PARQUET'):
            for left,right in variants:
                with self.subTest(fmt=fmt,left=left,right=right),tempfile.TemporaryDirectory() as t:
                    root=Path(t);batches,b0=register(root,fmt,left);_,b1=register(root,fmt,right,batch_id='b01')
                    refs,r=reference(root);output=root/'output';stage=root/'stage';output.mkdir();stage.mkdir()
                    with self.assertRaises(m.ReliabilityFailure) as e:publication.publish(self.spark,batch_root=batches,input_format=fmt,
                        batches=[('bench-source','b01',b1['batch_fingerprint']),('bench-source','b00',b0['batch_fingerprint'])],
                        reference_root=refs,reference_id='bench-services',revision='r1',reference_fingerprint=r['snapshot_fingerprint'],
                        staging_root=stage,output_root=output,execution_config=EXECUTION)
                    self.assertEqual('EVENT_IDENTITY_CONFLICT',e.exception.code)
                    self.assertNotIn(a['event_id'],str(e.exception.diagnostic));self.assertEqual([],list(output.iterdir()))
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);result,kw=self.publish_fixture(root,'JSONL',[a|{'event_id':None},a|{'event_id':None}])
            self.assertEqual(2,result.occurrence_counts['rejected_occurrences'])

    def test_retained_history_receipts_reference_correction_and_immutable_prior(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);a=g1()[0];historical=g1()[3]
            batches,b0=register(root,'JSONL',[a,a]);_,b1=register(root,'JSONL',[a,historical],batch_id='b01',received_at='2026-10-03T00:05:00.000000Z')
            refs,r=reference(root);stage=root/'stage';output=root/'output';stage.mkdir();output.mkdir()
            kwargs=dict(batch_root=batches,input_format='JSONL',batches=[('bench-source','b00',b0['batch_fingerprint'])],
                reference_root=refs,reference_id='bench-services',revision='r1',reference_fingerprint=r['snapshot_fingerprint'],staging_root=stage,output_root=output,execution_config=EXECUTION)
            first=publication.publish(self.spark,**kwargs)
            directory=m.publication_directory(output,first.product_fingerprint)
            before={p:(p.stat().st_mtime_ns,sha256(p.read_bytes()).hexdigest()) for p in directory.rglob('*') if p.is_file()}
            kwargs['batches']=[('bench-source','b01',b1['batch_fingerprint']),('bench-source','b00',b0['batch_fingerprint']),('bench-source','b00',b0['batch_fingerprint'])]
            both=publication.publish(self.spark,**kwargs);self.assertNotEqual(first.product_fingerprint,both.product_fingerprint)
            self.assertEqual(1,both.duplicate_batch_selections);self.assertEqual((4,2,2),tuple(both.occurrence_counts[k] for k in ('physical_occurrences','unique_valid_events','identical_duplicate_occurrences')))
            with analytics.open_publication(output,both.product_fingerprint,batch_root=batches,reference_root=refs) as s:
                row=s._db.execute('SELECT valid_delivery_count,epoch_us(first_received_at),epoch_us(last_received_at) FROM '+m.EVENTS+" WHERE service_id='svc00'").fetchone()
                self.assertEqual((3,1790899500000000,1790985900000000),row)
            _,r2=reference(root,revision='r2',correction=True);kwargs.update(revision='r2',reference_fingerprint=r2['snapshot_fingerprint'])
            corrected=publication.publish(self.spark,**kwargs);self.assertNotEqual(both.product_fingerprint,corrected.product_fingerprint)
            self.assertEqual(before,{p:(p.stat().st_mtime_ns,sha256(p.read_bytes()).hexdigest()) for p in before})

    def test_year_limits_midnight_and_exact_final_window(self):
        from reliability_benchmark_oracle import verify_publication
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);a=g1()[0]
            times=['0001-01-01T00:00:00.000000Z','1969-12-31T23:59:59.999999Z',
                '1970-01-01T00:00:00.000000Z','2026-10-01T00:04:59.999999Z',
                '2026-10-01T00:05:00.000000Z','9999-12-31T23:59:59.999999Z']
            rows=[a|dict(event_id=f'00000000-0000-0000-0000-{i+1:012d}',event_time=time) for i,time in enumerate(times)]
            result,kw=self.publish_fixture(root,'PARQUET',rows)
            with analytics.open_publication(kw['output_root'],result.product_fingerprint,batch_root=kw['batch_root'],reference_root=kw['reference_root']) as s:
                last=s.report('requests_5m',start='9999-12-31T23:55:00.000000Z',end=m.END_SENTINEL)
                self.assertEqual('9999-12-31T23:55:00.000000Z',last['rows'][0]['window_start'])
                self.assertEqual('1',last['rows'][0]['request_count'])
                window=s.report('requests_5m',start='2026-10-01T00:00:00.000000Z',end='2026-10-01T00:05:00.000000Z')
                self.assertEqual('1',window['rows'][0]['request_count'])
                self.assertTrue(s.report('input_coverage')['rows'][0]['min_receipt_delay_us'].startswith('-'))
            scratch=root/'oracle';scratch.mkdir()
            self.assertEqual(6,verify_publication(kw['output_root'],result.product_fingerprint,batch_root=kw['batch_root'],reference_root=kw['reference_root'],scratch_root=scratch)['groups_checked'])

    def test_moderate_generator_two_formats_full_oracle_and_chunked_readers(self):
        import reliability_benchmark_generator as generator
        from reliability_benchmark_oracle import verify_publication
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);incoming=root/'incoming';incoming.mkdir();config=generator.config(2000)
            generator.prepare(incoming,config);refs,r=benchmark_reference(root);batches=root/'batches';batches.mkdir();digests=[]
            for fmt in ('JSONL','PARQUET'):
                selected=[]
                for bid,receipt in (('b00','2026-10-02T00:05:00.000000Z'),('b01','2026-10-03T00:05:00.000000Z')):
                    batch=inputs.register(batches,input_root=incoming/fmt.lower()/bid,source_id='bench-source',batch_id=bid,input_format=fmt,
                        files=[dict(artifact_id=f'part-{i:02d}',input_file=f'part-{i:02d}.{fmt.lower()}') for i in range(8)],
                        received_at=receipt,profile=generator.profile(fmt,config));selected.append(('bench-source',bid,batch['batch_fingerprint']))
                output=root/('output-'+fmt);stage=root/('stage-'+fmt);output.mkdir();stage.mkdir()
                result=publication.publish(self.spark,batch_root=batches,input_format=fmt,batches=selected,
                    reference_root=refs,reference_id='bench-services',revision='r1',reference_fingerprint=r['snapshot_fingerprint'],
                    staging_root=stage,output_root=output,execution_config=EXECUTION)
                self.assertEqual((2060,2040,2000,40,20),tuple(result.occurrence_counts[k] for k in ('physical_occurrences','valid_occurrences','unique_valid_events','identical_duplicate_occurrences','rejected_occurrences')))
                scratch=root/('oracle-'+fmt);scratch.mkdir();checked=verify_publication(output,result.product_fingerprint,batch_root=batches,reference_root=refs,scratch_root=scratch)
                self.assertGreater(checked['output_chunks'],1);self.assertLessEqual(checked['max_output_chunk_bytes'],2097152)
                digests.append(tuple(checked[k+'_multiset_sha256'] for k in ('valid','unique','rejected')))
                with analytics.open_publication(output,result.product_fingerprint,batch_root=batches,reference_root=refs) as s:
                    self.assertGreater(s._verification_stats['chunks'],1)
                    totals=s.report('reference_coverage')['totals'];self.assertEqual('2000',totals['event_count']);self.assertEqual('88',totals['missing_event_count'])
                    self.assertEqual('9',totals['matched_service_count']);self.assertEqual('1',totals['unused_reference_service_count'])
            self.assertEqual(*digests)

    def test_write_readback_finalization_faults_preserve_prior_publication(self):
        from pyspark.sql.readwriter import DataFrameWriter
        import reliability_format_processing as processing
        for failure in ('write','independent','readback','temporary','rename','after-rename'):
            with self.subTest(failure=failure),tempfile.TemporaryDirectory() as t:
                root=Path(t);prior,fp,batches,refs=completed(root)
                prior_dir=m.publication_directory(prior,fp)
                before={p:(p.stat().st_mtime_ns,sha256(p.read_bytes()).hexdigest()) for p in prior_dir.rglob('*') if p.is_file()}
                manifest=json.loads((prior_dir/'inputs.json').read_bytes());ref=manifest['reference']
                output=root/'attempt';stage=root/'stage';output.mkdir();stage.mkdir()
                kwargs=dict(batch_root=batches,input_format='JSONL',batches=[(b['source_id'],b['batch_id'],b['batch_fingerprint']) for b in manifest['batches']],
                    reference_root=refs,reference_id=ref['reference_id'],revision=ref['revision'],reference_fingerprint=ref['snapshot_fingerprint'],
                    staging_root=stage,output_root=output,execution_config=EXECUTION)
                write=m.write;rename=Path.rename;verify=processing.verify_frames;calls=0
                def failed_write(path,raw):
                    if path.name=='complete.tmp':raise OSError('secret')
                    return write(path,raw)
                def failed_rename(path,target):
                    if path.name=='complete.tmp':
                        if failure=='after-rename':rename(path,target)
                        raise OSError('secret')
                    return rename(path,target)
                def failed_readback(*args):
                    nonlocal calls
                    calls+=1
                    if calls==2:raise m.ReliabilityFailure('PUBLICATION_INVALID')
                    return verify(*args)
                injector=patch.object(DataFrameWriter,'parquet',side_effect=OSError('secret')) if failure=='write' else \
                    patch.object(analytics,'verify_core',side_effect=m.ReliabilityFailure('PUBLICATION_INVALID')) if failure=='independent' else \
                    patch.object(processing,'verify_frames',side_effect=failed_readback) if failure=='readback' else \
                    patch.object(m,'write',side_effect=failed_write) if failure=='temporary' else patch.object(Path,'rename',failed_rename)
                with injector:
                    with self.assertRaises(m.ReliabilityFailure) as e:publication.publish(self.spark,**kwargs)
                self.assertEqual('PUBLICATION_INVALID' if failure in ('independent','readback') else 'PUBLICATION_FAILED',e.exception.code)
                self.assertNotIn('secret',str(e.exception.diagnostic));self.assertNotIn(str(root),str(e.exception.diagnostic))
                directory=m.publication_directory(output,fp)
                if failure=='after-rename':
                    frozen={p:(p.stat().st_mtime_ns,sha256(p.read_bytes()).hexdigest()) for p in directory.rglob('*') if p.is_file()}
                    self.assertEqual('replayed',publication.publish(None,**kwargs).status)
                    self.assertEqual(frozen,{p:(p.stat().st_mtime_ns,sha256(p.read_bytes()).hexdigest()) for p in frozen})
                else:
                    self.assertFalse((directory/'complete.json').exists())
                    with self.assertRaises(m.ReliabilityFailure) as e:publication.publish(None,**kwargs)
                    self.assertEqual('PUBLICATION_INCOMPLETE',e.exception.code)
                self.assertEqual(before,{p:(p.stat().st_mtime_ns,sha256(p.read_bytes()).hexdigest()) for p in before})
                with analytics.open_publication(prior,fp,batch_root=batches,reference_root=refs):pass

    def test_p95_heavy_tail_and_equal_groups_across_layouts(self):
        from pyspark.sql import functions as F
        import reliability_format_processing as processing
        events=processing.session(self.spark).range(10001).selectExpr('1 AS contract_version','1 AS schema_version',
            "printf('00000000-0000-0000-0000-%012d',id) AS event_id","'svc00' AS service_id","'test' AS environment",
            'timestamp_micros(1790812800000000L) AS event_time',"'GET' AS method","'svc00-r00' AS route_id",
            'CASE WHEN id=0 THEN 500 WHEN id=10000 THEN 599 ELSE 404 END AS status_code',
            'CASE WHEN id=10000 THEN 3600000L ELSE id END AS duration_ms','true AS is_synthetic','1L AS valid_delivery_count',
            'timestamp_micros(1790899500000000L) AS first_received_at','timestamp_micros(1790899500000000L) AS last_received_at',
            "'Service 00' AS service_name",'CAST(NULL AS STRING) AS owner_team',"'standard' AS service_tier","'MATCHED' AS reference_status",
            "'bench-services' AS reference_id","'r1' AS reference_revision","repeat('0',64) AS reference_fingerprint")
        with patch.object(m,'MAX_COUNT',1):
            with self.assertRaises(m.ReliabilityFailure) as error:processing.aggregate(events)
            self.assertEqual('COUNT_OVERFLOW',error.exception.code)
        for partitions in (1,3):
            frame=events.orderBy(F.xxhash64('event_id')).repartition(partitions)
            metrics=processing.aggregate(frame);row=metrics.first();v=row.p95_duration_ms_approx
            # Hand-defined integer distribution: 0..9999 plus 3,600,000.
            present=0<=v<=9999 or v==3600000
            below=v if 0<=v<=9999 else 10000 if v==3600000 else 0
            upto=below+1 if present else 0;r=9501;b=2
            self.assertTrue(present);self.assertLessEqual(max(below+1-r,r-upto,0),b)
            self.assertEqual((10001,2,53595000),(row.request_count,row.server_error_count,int(row.duration_sum_ms)))
            self.assertAlmostEqual(2/10001,row.server_error_rate,12)
            self.assertAlmostEqual(53595000/10001,row.mean_duration_ms,9)
            processing.verify_ranks(frame,metrics)
            for count,duration in ((1,0),(10001,17)):
                equal=frame.limit(count).withColumn('duration_ms',F.lit(duration).cast('long'))
                self.assertEqual(duration,processing.aggregate(equal).first().p95_duration_ms_approx)


if __name__=='__main__':unittest.main()
