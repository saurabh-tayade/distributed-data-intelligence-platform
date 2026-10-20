from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import json
import os
import tempfile
import time
import unittest

import duckdb
from pyspark.sql import SparkSession, functions as F
import reliability_metadata as m
import reliability_inputs as inputs
import reliability_processing as processing
from reliability_fixtures import event,profile,f1,f2,register_fixture,reference_fixture


def rank_oracle(values,candidate):
    n=len(values); rank=(95*n+99)//100; budget=max(1,(n+9999)//10000)
    ordered=sorted(values)
    lower=sum(x<candidate for x in values)+1;upper=sum(x<=candidate for x in values)
    displacement=max(lower-rank,rank-upper,0)
    return candidate in values and displacement<=budget,ordered[rank-1],lower,upper,displacement,budget


class ProcessingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory();cls.root=Path(cls.temp.name)
        cls.base=SparkSession.builder.master('local[2]').appName('e2-focused-tests').config('spark.ui.enabled','false').config('spark.driver.memory','1g').getOrCreate()
        cls.base.sparkContext.setLogLevel('ERROR');cls.original=(cls.base.conf.get('spark.sql.session.timeZone'),cls.base.conf.get('spark.sql.parquet.outputTimestampType'))
        cls.spark=processing.session(cls.base)
        batchroot,summary=register_fixture(cls.root)
        refroot,reference=reference_fixture(cls.root)
        cls.stageroot=cls.root/'staging';cls.stageroot.mkdir()
        cls.deps=inputs.verify_dependencies(batch_root=batchroot,batches=[('fixture-source','b1',summary['batch_fingerprint'])],
            reference_root=refroot,reference_id='fixture-services',revision='r1',reference_fingerprint=reference['snapshot_fingerprint'])
        cls.stage=inputs.stage(cls.deps.batches,cls.stageroot,cls.deps.fingerprint)
        started=time.perf_counter()
        cls.frames,cls.counts=processing.normalize(cls.spark,cls.deps,cls.stage)
        cls.events=cls.frames.events.cache();cls.events.count()
        cls.occurrences=cls.frames.occurrences.cache();cls.occurrences.count()
        cls.metrics=processing.aggregate(cls.events).cache();cls.metrics.count()
        cls.elapsed=time.perf_counter()-started
        print(json.dumps({'e2_fixture_normalize_aggregate_seconds':round(cls.elapsed,3),'physical_rows':32,'unique_events':23}))

    @classmethod
    def tearDownClass(cls):
        for frame in (cls.events,cls.occurrences,cls.metrics):frame.unpersist()
        cls.base.stop();cls.temp.cleanup()

    def test_r05_f2_exact_dispositions_counts_and_events(self):
        self.assertEqual(dict(physical_occurrences=32,valid_occurrences=24,unique_valid_events=23,identical_duplicate_occurrences=1,rejected_occurrences=8),self.counts)
        rows=self.occurrences.select('line_number','byte_offset','physical_byte_length','disposition','reason_codes').orderBy('line_number').collect()
        self.assertEqual(Counter(ACCEPTED=23,DUPLICATE=1,REJECTED=8),Counter(r.disposition for r in rows))
        self.assertEqual('DUPLICATE',rows[20].disposition)
        self.assertEqual([['JSON_SYNTAX'],['JSON_SYNTAX'],['JSON_DUPLICATE_KEY'],['JSON_NONFINITE'],['FIELD_VALUE'],['TIMESTAMP_INVALID'],['FIELD_NULL'],['FIELD_VALUE']],[r.reason_codes for r in rows[24:]])
        self.assertEqual(list(range(1,33)),[r.line_number for r in rows])
        for a,b in zip(rows,rows[1:]):self.assertEqual(a.byte_offset+a.physical_byte_length,b.byte_offset)
        parsed=self.events.select('service_id','event_id','valid_delivery_count','reference_status','owner_team','service_tier',F.unix_micros('first_received_at').alias('first'),F.unix_micros('last_received_at').alias('last')).collect()
        alpha=next(r for r in parsed if r.service_id=='alpha' and r.event_id==event(1)['event_id'])
        self.assertEqual(2,alpha.valid_delivery_count)
        beta=next(r for r in parsed if r.service_id=='beta');self.assertEqual('MATCHED',beta.reference_status);self.assertIsNone(beta.owner_team)
        self.assertEqual('MISSING',next(r for r in parsed if r.service_id=='gamma').reference_status)
        self.assertTrue(all(r.first==r.last for r in parsed))

    def test_r07_exact_schemas_and_preserved_business_fields(self):
        # Literal contract oracle, independent of the producer/schema constants.
        expected={
            m.EVENTS:'struct<contract_version:int,schema_version:int,event_id:string,service_id:string,environment:string,event_time:timestamp,method:string,route_id:string,status_code:int,duration_ms:bigint,is_synthetic:boolean,valid_delivery_count:bigint,first_received_at:timestamp,last_received_at:timestamp,service_name:string,owner_team:string,service_tier:string,reference_status:string,reference_id:string,reference_revision:string,reference_fingerprint:string>',
            m.OCCURRENCES:'struct<contract_version:int,batch_fingerprint:string,source_id:string,batch_id:string,ingestion_id:string,artifact_id:string,input_file_sha256:string,line_number:bigint,byte_offset:bigint,physical_byte_length:int,received_at:timestamp,disposition:string,service_id:string,event_id:string,semantic_sha256:string,reason_codes:array<string>>',
            m.METRICS:'struct<contract_version:int,service_id:string,environment:string,window_start:timestamp,request_count:bigint,server_error_count:bigint,server_error_rate:double,duration_sum_ms:decimal(38,0),mean_duration_ms:double,p95_duration_ms_approx:bigint,service_name:string,owner_team:string,service_tier:string,reference_status:string,reference_id:string,reference_revision:string,reference_fingerprint:string>'}
        for name,frame in ((m.EVENTS,self.events),(m.OCCURRENCES,self.occurrences),(m.METRICS,self.metrics)):
            self.assertEqual(expected[name],frame.schema.simpleString())
        columns=[F.unix_micros(n).alias(n) if n=='event_time' else F.col(n) for n in m.EVENT_FIELDS]
        actual=[tuple(row) for row in self.events.select(*columns).collect()]
        expected=[]
        for raw in f2()[:24]:
            obj=json.loads(raw);obj['event_time']=m.time_us(obj['event_time'])
            value=tuple(obj[n] for n in m.EVENT_FIELDS)
            if value not in expected:expected.append(value)
        self.assertCountEqual(expected,actual)

    def test_r08_literal_utc_groups_and_exact_arithmetic(self):
        cols=['service_id','environment',F.unix_micros('window_start').alias('window_us'),'request_count','server_error_count','server_error_rate','duration_sum_ms','mean_duration_ms','p95_duration_ms_approx']
        rows=self.metrics.select(*cols).orderBy('window_us','service_id').collect()
        expected=[('alpha',-300000000,1,0,0.0,3600000,3600000.0),('alpha',m.time_us('2026-10-20T12:00:00.000000Z'),20,2,0.1,210,10.5),('gamma',m.time_us('2026-10-20T12:00:00.000000Z'),1,1,1.0,0,0.0),('beta',m.time_us('2026-10-20T12:05:00.000000Z'),1,0,0.0,40,40.0)]
        self.assertEqual(expected,[(r.service_id,r.window_us,r.request_count,r.server_error_count,r.server_error_rate,int(r.duration_sum_ms),r.mean_duration_ms) for r in rows])
        self.assertEqual(3600250,sum(r.duration_sum_ms for r in rows));self.assertEqual(3,sum(r.server_error_count for r in rows))
        self.assertIn(rows[1].p95_duration_ms_approx,[18,19,20])
        self.assertEqual((self.original),(self.base.conf.get('spark.sql.session.timeZone'),self.base.conf.get('spark.sql.parquet.outputTimestampType')))

    def test_r08_physical_timestamp_interoperability(self):
        instants=['2026-10-20T12:00:00.123456Z','1969-12-31T23:59:59.999999Z','1970-01-01T00:00:00.000000Z','0001-01-01T00:00:00.000000Z','9999-12-31T23:59:59.999999Z','2026-10-20T00:00:00.000000Z','2026-10-20T12:04:59.999999Z','2026-10-20T12:05:00.000000Z','9999-12-31T23:55:00.000000Z']
        path=self.root/'timestamp-roundtrip'
        self.spark.createDataFrame([(i,m.time_us(v)) for i,v in enumerate(instants)],'id integer, us long').selectExpr('id','us','timestamp_micros(us) as instant').write.mode('errorifexists').parquet(str(path))
        files=[str(p) for p in sorted(path.glob('*.parquet'))]
        with duckdb.connect(':memory:') as db:
            for file in files:
                row=db.execute("SELECT type,logical_type FROM parquet_schema(?) WHERE name='instant'",[file]).fetchone()
                self.assertEqual('INT64',row[0]);self.assertIn('isAdjustedToUTC=1',row[1]);self.assertIn('MICROS=MicroSeconds()',row[1])
            view=db.read_parquet(files,hive_partitioning=False,union_by_name=False)
            self.assertEqual('TIMESTAMP WITH TIME ZONE',str(view.types[-1]));view.create_view('times')
            for zone in ('UTC','America/New_York','Asia/Kolkata'):
                db.execute('SET TimeZone=?',[zone])
                self.assertEqual([(i,m.time_us(v)) for i,v in enumerate(instants)],db.execute('SELECT id,epoch_us(instant) FROM times ORDER BY id').fetchall())
            db.execute("SET TimeZone='UTC'")
            self.assertEqual(instants,[row[0] for row in db.execute("SELECT strftime(instant,'%Y-%m-%dT%H:%M:%S.%fZ') FROM times ORDER BY id").fetchall()])

    def test_r08_amended_query_bounds(self):
        self.assertEqual((253402300500000000,m.END_US),m.metric_bounds('9999-12-31T23:55:00.000000Z',m.END_SENTINEL))
        self.assertEqual(m.END_US,m.metric_bounds('9999-12-01T00:00:00.000000Z',m.END_SENTINEL)[1])
        for a,b in [(m.END_SENTINEL,m.END_SENTINEL),('9999-11-30T00:00:00.000000Z',m.END_SENTINEL),('9999-12-31T23:55:00.000000Z','10001-01-01T00:00:00.000000Z'),('2026-10-20T12:00:00.000001Z','2026-10-20T12:05:00.000000Z')]:
            with self.assertRaises(m.ReliabilityFailure):m.metric_bounds(a,b)

    def test_r05_valid_and_recognizable_invalid_conflicts(self):
        for index,lines in enumerate([f2()+[canonical(event(1,duration_ms=99))],f1()+[canonical(event(1,event_time=None))],[canonical(event(40,duration_ms=-1)),canonical(event(40,event_time=None))]]):
            root=self.root/f'conflict-{index}';root.mkdir()
            batchroot,summary=register_fixture(root,lines);refroot,ref=reference_fixture(root)
            dependencies=inputs.verify_dependencies(batch_root=batchroot,batches=[('fixture-source','b1',summary['batch_fingerprint'])],reference_root=refroot,reference_id='fixture-services',revision='r1',reference_fingerprint=ref['snapshot_fingerprint'])
            stage=inputs.stage(dependencies.batches,self.stageroot,dependencies.fingerprint)
            with self.assertRaises(m.ReliabilityFailure) as caught:processing.normalize(self.spark,dependencies,stage)
            self.assertEqual('EVENT_IDENTITY_CONFLICT',caught.exception.code)
            self.assertEqual('1',caught.exception.diagnostic['conflict_summary']['conflicting_identity_count'])
            self.assertNotIn(event(1)['event_id'],json.dumps(caught.exception.diagnostic))

    def test_r09_candidate_independent_rank_across_layouts(self):
        groups={'singleton':[0],'equal':[7]*20,'ties':[1,1,1,7,7,7,7,7,7,7],'sparse':[0,3600000],
                'tail':[1]*19+[3600000],'literal':list(range(1,21)),'large':list(range(12005))}
        rows=[(name,value) for name,values in groups.items() for value in values]
        observations=[]
        for partitions,reverse in ((1,False),(2,True),(3,False)):
            frame=self.spark.createDataFrame(list(reversed(rows)) if reverse else rows,'key string, duration_ms long').repartition(partitions)
            results=frame.groupBy('key').agg(F.percentile_approx('duration_ms',0.95,10000).alias('value')).collect()
            for row in results:
                oracle=rank_oracle(groups[row.key],row.value);self.assertTrue(oracle[0],(row,oracle))
                observations.append(dict(group=row.key,partitions=partitions,candidate=row.value,exact=oracle[1],displacement=oracle[4],budget=oracle[5],absolute_ms_error=abs(row.value-oracle[1])))
        print(json.dumps({'e2_p95_observations':observations},sort_keys=True))
        self.assertFalse(rank_oracle(list(range(1,21)),17)[0]);self.assertFalse(rank_oracle([1,1,7],3)[0])

    def test_r10_complete_multiset_comparison(self):
        expected=processing.Frames(self.events,self.occurrences,self.metrics)
        processing.verify_frames(expected,expected)
        bad=self.events.withColumn('duration_ms',F.when(F.col('service_id')=='beta',F.lit(41)).otherwise(F.col('duration_ms')))
        with self.assertRaises(m.ReliabilityFailure):processing.verify_frames(expected,processing.Frames(bad,self.occurrences,self.metrics))
        bad_daily=self.metrics.withColumn('server_error_count',F.when(F.col('service_id')=='beta',F.lit(1)).otherwise(F.col('server_error_count')))
        with self.assertRaises(m.ReliabilityFailure):processing.verify_frames(expected,processing.Frames(self.events,self.occurrences,bad_daily))


def canonical(value):
    from publication_metadata import canonical_bytes
    return canonical_bytes(value)


if __name__=='__main__':unittest.main()
