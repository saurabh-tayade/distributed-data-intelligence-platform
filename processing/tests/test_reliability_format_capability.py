"""ADR 0009 checkpoint A: installed-runtime proof, never a scale benchmark."""
from bisect import bisect_right
from pathlib import Path
import json
import tempfile
import time
import unittest

import duckdb
from publication_metadata import canonical_bytes


INSTANTS = (1790812800000001, -1, 0, -62135596800000000,
            253402300799999999, 1790813100000000)
FIELDS = ('schema_version', 'event_id', 'service_id', 'environment', 'event_time',
          'method', 'route_id', 'status_code', 'duration_ms', 'is_synthetic')


def connection(threads=2):
    db = duckdb.connect(':memory:', config=dict(
        threads=threads, memory_limit='512MiB', temp_directory='',
        autoinstall_known_extensions=False, autoload_known_extensions=False,
        allow_unsigned_extensions=False))
    db.execute("SET TimeZone='UTC'")
    return db


def seed_file(db, path, count, reverse=False):
    # Integer constructor, OPTIONAL typed fields, explicit physical order.
    instants = ','.join(str(t) for t in INSTANTS)
    db.execute(f"""CREATE OR REPLACE TEMP TABLE seed AS SELECT 1::INTEGER schema_version,
        printf('00000000-0000-0000-0000-%012d', i%7) event_id,
        'svc00' service_id, 'test' environment,
        CASE WHEN i=7 THEN NULL ELSE make_timestamptz(
          list_extract([{instants}]::BIGINT[],(i%6)+1)) END event_time,
        'GET' AS method, 'svc00-r00' route_id,
        CASE WHEN i%17=0 THEN NULL ELSE 200 END::INTEGER status_code,
        (i%7)::BIGINT duration_ms, true is_synthetic
        FROM range(?) t(i) ORDER BY i {'DESC' if reverse else 'ASC'}""", [count])
    db.execute('COPY seed TO ? (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 2048)',[str(path)])


def schema(db, path):
    cur = db.execute('SELECT * FROM parquet_schema(?)', [str(path)])
    names = [d[0] for d in cur.description]
    return [dict(zip(names, row, strict=True)) for row in cur.fetchall()]


def assert_schema(case, db, path):
    rows = schema(db, path)
    case.assertEqual(FIELDS, tuple(r['name'] for r in rows[1:]))
    case.assertEqual(11, len(rows))
    expected = ('INT32','BYTE_ARRAY','BYTE_ARRAY','BYTE_ARRAY','INT64',
                'BYTE_ARRAY','BYTE_ARRAY','INT32','INT64','BOOLEAN')
    case.assertEqual(expected, tuple(r['type'] for r in rows[1:]))
    case.assertTrue(all(r['repetition_type']=='OPTIONAL' for r in rows[1:]))
    stamp = rows[5]
    case.assertEqual('TIMESTAMP_MICROS', stamp['converted_type'])
    case.assertIn('isAdjustedToUTC=1', stamp['logical_type'])
    case.assertIn('MICROS=MicroSeconds()', stamp['logical_type'])
    case.assertNotIn('INT96', expected)
    return stamp['logical_type']


class ParquetCapabilityTests(unittest.TestCase):
    def test_physical_positions_bounded_ordered_decode(self):
        self.assertEqual('1.4.5', duckdb.__version__)
        started = time.perf_counter()
        observations = []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with connection() as writer:
                for name, count, reverse in (('forward',6149,False),
                                              ('reverse',4101,True),('empty',0,False)):
                    seed_file(writer, root/(name+'.parquet'), count, reverse)
            for threads in (1,2):
                with connection(threads) as db:
                    for name, count, reverse in (('forward',6149,False),
                                                  ('reverse',4101,True),('empty',0,False)):
                        path = root/(name+'.parquet')
                        annotation = assert_schema(self,db,path)
                        groups = db.execute('''SELECT row_group_id,
                            min(row_group_num_rows),sum(total_compressed_size),
                            sum(total_uncompressed_size),count(DISTINCT compression)
                            FROM parquet_metadata(?) GROUP BY row_group_id
                            ORDER BY row_group_id''',[str(path)]).fetchall()
                        if count:
                            self.assertGreaterEqual(len(groups),3)
                        self.assertEqual(count,sum(g[1] for g in groups))
                        starts=[];total=0
                        for ordinal, n, compressed, uncompressed, codecs in groups:
                            self.assertEqual(len(starts),ordinal)
                            starts.append(total);total+=n
                            self.assertLessEqual(n,32768)
                            self.assertLessEqual(uncompressed,32*1024*1024)
                            self.assertEqual(1,codecs)
                        for filtered in (False,True):
                            sql='''SELECT file_row_number, event_id,
                                epoch_us(event_time),status_code,duration_ms
                                FROM read_parquet(?,file_row_number=true)
                                '''+('WHERE duration_ms=3 ' if filtered else '')+'ORDER BY file_row_number'
                            plan=db.execute('EXPLAIN (FORMAT JSON) '+sql,[str(path)]).fetchone()[1]
                            self.assertIn('ORDER_BY',plan)
                            self.assertIn('READ_PARQUET',plan)
                            db.execute(sql,[str(path)])
                            seen=chunks=max_bytes=0;last=-1;examples=[]
                            expected_positions=(p for p in range(count)
                                if not filtered or (count-1-p if reverse else p)%7==3)
                            while rows:=db.fetchmany(1024):
                                size=len(canonical_bytes(rows))
                                self.assertLessEqual(size,2*1024*1024)
                                chunks+=1;max_bytes=max(max_bytes,size)
                                for p,event_id,instant,status,duration in rows:
                                    self.assertEqual(next(expected_positions),p)
                                    self.assertGreater(p,last);last=p
                                    i=count-1-p if reverse else p
                                    group=bisect_right(starts,p)-1
                                    within=p-starts[group]
                                    self.assertTrue(0<=within<groups[group][1])
                                    self.assertEqual(f'00000000-0000-0000-0000-{i%7:012d}',event_id)
                                    self.assertEqual(None if i==7 else INSTANTS[i%6],instant)
                                    self.assertEqual(None if i%17==0 else 200,status)
                                    self.assertEqual(i%7,duration)
                                    if p<2 or p in starts or within==groups[group][1]-1:
                                        examples.append([p,group,within,duration,instant])
                                    seen+=1
                            self.assertIsNone(next(expected_positions,None))
                            observations.append(dict(file=name,threads=threads,
                                filtered=filtered,rows=seen,chunks=chunks,
                                max_chunk_bytes=max_bytes,groups=[g[1] for g in groups],
                                position_examples=examples,timestamp_annotation=annotation,
                                settings=list(db.execute("SELECT current_setting('memory_limit'),current_setting('temp_directory')").fetchone())))
                    # No cast/schema override makes an incompatible file admissible.
                    wrong=root/f'unsupported-{threads}.parquet'
                    db.execute('COPY (SELECT 1::INTEGER extra) TO ? (FORMAT PARQUET)',[str(wrong)])
                    self.assertNotEqual(FIELDS,tuple(r['name'] for r in schema(db,wrong)[1:]))
                    corrupt=root/f'corrupt-{threads}.parquet';corrupt.write_bytes(b'PAR1brokenPAR1')
                    with self.assertRaises(duckdb.Error):schema(db,corrupt)
            print(json.dumps(dict(parquet_capability=observations,
                seconds=time.perf_counter()-started,scale_capacity_proven=False),sort_keys=True))

    def test_sort_memory_profile(self):
        # Bounded Python chunks do not bound the SQL sort; record its actual state.
        with tempfile.TemporaryDirectory() as directory, connection() as db:
            root=Path(directory);path=root/'source.parquet';profile=root/'profile.json'
            seed_file(db,path,6149)
            db.execute("SET enable_profiling='json'")
            db.execute('SET profiling_output=?',[str(profile)])
            db.execute('SELECT file_row_number,epoch_us(event_time),event_id FROM read_parquet(?,file_row_number=true) ORDER BY file_row_number',[str(path)])
            n=0
            while rows:=db.fetchmany(1024):n+=len(rows)
            self.assertEqual(6149,n)
            data=json.loads(profile.read_text())
            self.assertEqual(0,data['system_peak_temp_dir_size'])
            self.assertLess(data['system_peak_buffer_memory'],512*1024*1024)
            print(json.dumps(dict(parquet_sort_profile={k:data[k] for k in
                ('system_peak_buffer_memory','system_peak_temp_dir_size','rows_returned','latency')}),sort_keys=True))


class SparkTimestampCapabilityTests(unittest.TestCase):
    def test_spark_parquet_duckdb_microseconds(self):
        from pyspark.sql import SparkSession,functions as F
        spark=SparkSession.builder.master('local[2]').appName('ddip-format-capability').config('spark.ui.enabled','false').getOrCreate()
        try:
            self.assertEqual('4.0.2',spark.version)
            self.assertEqual('21',spark.sparkContext._jvm.java.lang.System.getProperty('java.specification.version'))
            isolated=spark.newSession()
            for k,v in {'spark.sql.session.timeZone':'UTC',
                'spark.sql.parquet.outputTimestampType':'TIMESTAMP_MICROS',
                'spark.sql.parquet.datetimeRebaseModeInRead':'CORRECTED',
                'spark.sql.parquet.datetimeRebaseModeInWrite':'CORRECTED'}.items():isolated.conf.set(k,v)
            with tempfile.TemporaryDirectory() as directory:
                path=Path(directory)/'parquet'
                isolated.createDataFrame(list(enumerate(INSTANTS)),'position long,us long').select(
                    'position',F.expr('timestamp_micros(us)').alias('event_time')).coalesce(1).write.mode('errorifexists').parquet(str(path))
                files=list(path.glob('*.parquet'));self.assertEqual(1,len(files))
                with connection() as db:
                    rows=schema(db,files[0]);stamp=rows[2]
                    self.assertEqual('INT64',stamp['type'])
                    self.assertIn('isAdjustedToUTC=1',stamp['logical_type'])
                    self.assertIn('MICROS=MicroSeconds()',stamp['logical_type'])
                    actual=db.execute('SELECT position,epoch_us(event_time) FROM read_parquet(?) ORDER BY position',[str(files[0])]).fetchall()
                    self.assertEqual(list(enumerate(INSTANTS)),actual)
                    print(json.dumps(dict(spark_timestamp_gate=dict(instants=actual,
                        physical_type=stamp['type'],logical_type=stamp['logical_type'],
                        timezone='UTC',spark=spark.version)),sort_keys=True))
                    raw=Path(directory)/'raw.parquet';seed_file(db,raw,12)
                # Decoder-independent ten-field value check. Spark supplies no positions.
                expected=[]
                for i in range(12):
                    expected.append((1,f'00000000-0000-0000-0000-{i%7:012d}',
                        'svc00','test',None if i==7 else INSTANTS[i%6],
                        'GET','svc00-r00',None if i%17==0 else 200,i%7,True))
                for partitions in (1,2):
                    frame=isolated.read.parquet(str(raw)).repartition(partitions)
                    self.assertEqual(FIELDS,tuple(frame.columns))
                    got=frame.select(*[F.expr('unix_micros(event_time)').alias(n)
                        if n=='event_time' else F.col(n) for n in FIELDS]).collect()
                    self.assertCountEqual(expected,[tuple(r) for r in got])
                bad=Path(directory)/'int96'
                isolated.conf.set('spark.sql.parquet.outputTimestampType','INT96')
                isolated.read.parquet(str(raw)).coalesce(1).write.mode('errorifexists').parquet(str(bad))
                from reliability_format_inputs import scan_file
                from reliability_format_fixtures import policy
                import reliability_format_metadata as metadata
                with self.assertRaises(metadata.ReliabilityFailure) as caught:list(scan_file(next(bad.glob('*.parquet')),policy('PARQUET')))
                self.assertEqual('PARQUET_INVALID',caught.exception.code)
                isolated.conf.set('spark.sql.parquet.outputTimestampType','TIMESTAMP_MICROS')
        finally:spark.stop()


if __name__=='__main__':unittest.main()
