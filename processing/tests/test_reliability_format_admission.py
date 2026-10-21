"""Strict v2 format admission, framing, schema/privacy precedence and staging."""
from dataclasses import replace
from pathlib import Path
from hashlib import sha256
import json
import os
import tempfile
import unittest
from unittest.mock import patch
from publication_metadata import canonical_bytes
import reliability_format_metadata as m
import reliability_format_inputs as inputs
import reliability_format_parquet as parquet
import reliability_benchmark_oracle as oracle
from reliability_benchmark_generator import write_parquet
from reliability_format_fixtures import g1,policy,register


class AdmissionTests(unittest.TestCase):
    def setUp(self):self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
    def tearDown(self):self.temp.cleanup()
    def assert_code(self,code,operation,*args,**kwargs):
        with self.assertRaises(m.ReliabilityFailure) as caught:operation(*args,**kwargs)
        self.assertEqual(code,caught.exception.code)
        self.assertNotIn(str(self.root),str(caught.exception.diagnostic));self.assertNotIn('secret',str(caught.exception.diagnostic))

    def test_jsonl_exact_lf_crlf_blank_and_optional_final_newline(self):
        path=self.root/'events.jsonl';row=canonical_bytes(g1()[0])
        for raw,positions in ((b'',[]),(row,[(0,1,0,len(row))]),
                             (row+b'\r\n\n'+row,[(0,1,0,len(row)+2),(1,2,len(row)+2,1),(2,3,len(row)+3,len(row))])):
            with self.subTest(raw_length=len(raw)):
                path.write_bytes(raw);detail={};values=list(inputs.scan_file(path,policy('JSONL'),result=detail))
                self.assertEqual(positions,[(r['record_ordinal'],r['line_number'],r['byte_offset'],r['physical_byte_length']) for r in values])
                if len(values)==3:self.assertEqual(['BLANK_LINE'],values[1]['reason_codes'])
                self.assertEqual(len(values),detail['record_count'])

    def test_structural_jsonl_failure_never_registers_a_readable_prefix(self):
        incoming=self.root/'incoming';batches=self.root/'batches';incoming.mkdir();batches.mkdir();p=incoming/'events.jsonl'
        for tail in (b'\xff\n',b'{}\rbroken\n',b'x'*16385+b'\n',b'\xef\xbb\xbf{}\n'):
            with self.subTest(tail_length=len(tail)):
                # BOM only has structural meaning at the beginning of a file.
                p.write_bytes(tail if tail.startswith(b'\xef') else canonical_bytes(g1()[0])+b'\n'+tail)
                self.assert_code('RESOURCE_LIMIT' if len(tail)>=16386 else 'BATCH_INVALID',inputs.register,batches,
                    input_root=incoming,source_id='bench-source',batch_id='b00',input_format='JSONL',files=[dict(artifact_id='events',input_file='events.jsonl')],
                    received_at='2026-10-02T00:05:00.000000Z',profile=policy('JSONL'))
                self.assertEqual([],list(batches.iterdir()))

    def test_parser_only_rejections_and_independent_oracle_agree(self):
        raw=canonical_bytes(g1()[0]).decode()
        cases=[('', ['BLANK_LINE']),('{',['JSON_SYNTAX']),('[]',['JSON_NOT_OBJECT']),
            (raw[:-1]+',"status_code":200}',['JSON_DUPLICATE_KEY']),
            (raw.replace('"duration_ms":10','"duration_ms":NaN'),['JSON_NONFINITE']),
            (raw.replace('"duration_ms":10','"duration_ms":"10"'),['FIELD_TYPE']),
            (raw.replace('"duration_ms":10','"duration_ms":10.5'),['FIELD_TYPE']),
            (raw.replace('"event_time":"2026-10-01T00:00:00.000000Z"','"event_time":"2026-10-01T00:00:00.0000001Z"'),['TIMESTAMP_INVALID'])]
        p=self.root/'events.jsonl'
        for text,reasons in cases:
            with self.subTest(reasons=reasons):
                p.write_bytes(text.encode()+b'\n');row=list(inputs.scan_file(p,policy('JSONL')))[0]
                self.assertEqual(reasons,row['reason_codes']);self.assertIsNone(row['event'])
                self.assertEqual(reasons,oracle.typed(text,policy('JSONL'),text=True)[3])

    def test_privacy_precedes_representable_row_errors_in_both_parsers(self):
        texts=['{"duration_ms":NaN,"cookie":"secret"}',
            '{"schema_version":2,"cookie":"secret"}',
            '[{"headers":"secret"}]','{"environment":[[{"headers":"secret"}]]}',
            '{"route_id":"secret","event_time":null}','{"is_synthetic":false,"event_time":null}']
        p=self.root/'events.jsonl'
        for text in texts:
            with self.subTest(text=text):
                p.write_bytes(text.encode()+b'\n')
                self.assert_code('INTAKE_NOT_APPROVED',lambda:list(inputs.scan_file(p,policy('JSONL'))))
                self.assert_code('INTAKE_NOT_APPROVED',oracle.typed,text,policy('JSONL'),text=True)
        for text in ('{"event_id":"a","event_id":"a"}','{"schema_version":2}','{"duration_ms":'+('9'*65)+'}'):
            p.write_bytes(text.encode()+b'\n')
            self.assert_code('BATCH_INVALID',lambda:list(inputs.scan_file(p,policy('JSONL'))))
            self.assert_code('BATCH_INVALID',oracle.typed,text,policy('JSONL'),text=True)

    def raw_parquet(self,projection='*',options='COMPRESSION SNAPPY',repeat=False):
        seed=self.root/'seed.parquet'
        if not seed.exists():
            values=[]
            for row in g1():
                obj=dict(row);time=obj.pop('event_time');obj['event_time_epoch_us']=m.time_us(time) if time else None;values.append(obj)
            write_parquet(seed,values)
        path=self.root/'candidate.parquet'
        if path.exists():path.unlink()
        with parquet.connection() as db:
            db.read_parquet(str(seed)).create_view('seed')
            if repeat and projection=='*':projection='seed.*'
            db.execute('COPY (SELECT '+projection+' FROM seed'+(' CROSS JOIN range(3300)' if repeat else '')+') TO ? (FORMAT PARQUET,'+options+')',[str(path)])
        return path

    def test_unsupported_physical_types_order_codec_and_metadata(self):
        # Each case below is a distinct actual physical file.
        variants=[('* REPLACE(status_code::UINTEGER AS status_code)','COMPRESSION SNAPPY','PARQUET_INVALID'),
            ('* REPLACE(duration_ms::DOUBLE AS duration_ms)','COMPRESSION SNAPPY','PARQUET_INVALID'),
            ('* REPLACE(event_time::TIMESTAMP AS event_time)','COMPRESSION SNAPPY','PARQUET_INVALID'),
            ('* REPLACE(event_time::TIMESTAMP::TIMESTAMP_MS AS event_time)','COMPRESSION SNAPPY','PARQUET_INVALID'),
            ('* REPLACE(event_time::TIMESTAMP::TIMESTAMP_NS AS event_time)','COMPRESSION SNAPPY','PARQUET_INVALID'),
            ('* REPLACE(encode(service_id) AS service_id)','COMPRESSION SNAPPY','PARQUET_INVALID'),
            ('*,1 AS cookie','COMPRESSION SNAPPY','INTAKE_NOT_APPROVED'),
            ('*','COMPRESSION GZIP','PARQUET_INVALID'),
            ('*',"COMPRESSION SNAPPY,KV_METADATA {cookie:'secret'}",'INTAKE_NOT_APPROVED'),
            ('*',"COMPRESSION SNAPPY,FIELD_IDS {event_id:42}",'INTAKE_NOT_APPROVED'),
            ('*',"COMPRESSION SNAPPY,KV_METADATA {'ddip.logical_schema':'wrong'}",'INTAKE_NOT_APPROVED')]
        for projection,options,code in variants:
            with self.subTest(projection=projection,options=options):
                path=self.raw_parquet(projection,options)
                self.assert_code(code,lambda:list(inputs.scan_file(path,policy('PARQUET'))))

    def test_parquet_bounds_corrupt_bytes_and_typed_invalid_nulls(self):
        path=self.raw_parquet();rows=list(inputs.scan_file(path,policy('PARQUET')))
        self.assertEqual(['FIELD_NULL'],rows[5]['reason_codes']);self.assertIsNone(rows[5]['event'])
        for value,code in ((b'','PARQUET_INVALID'),(b'PAR1broken'+(6).to_bytes(4,'little')+b'PAR1','PARQUET_INVALID'),(b'PAR1'+b'\x00'*4+(1048577).to_bytes(4,'little')+b'PAR1','RESOURCE_LIMIT')):
            path.write_bytes(value);self.assert_code(code,lambda:list(inputs.scan_file(path,policy('PARQUET'))))
        path=self.raw_parquet(options='COMPRESSION SNAPPY,ROW_GROUP_SIZE 65536',repeat=True)
        self.assert_code('RESOURCE_LIMIT',lambda:list(inputs.scan_file(path,policy('PARQUET'))))
        self.assert_code('PARQUET_INVALID',parquet.validate_groups,[{}]*65,0)

    def test_post_capture_content_directory_and_identity_changes(self):
        p=self.root/'events.jsonl';p.write_bytes(canonical_bytes(g1()[0])+b'\n');detail={}
        list(inputs.scan_file(p,policy('JSONL'),result=detail))
        p.write_bytes(canonical_bytes(g1()[1])+b'\n')
        self.assert_code('INPUT_CHANGED',lambda:list(inputs.scan_file(p,policy('JSONL'),previous=detail['evidence'])))
        d=self.root/'guarded';d.mkdir();(d/'metadata').write_bytes(b'{}')
        evidence=m.capture(d,{'metadata':32},(),'DEPENDENCY_INVALID','DEPENDENCY_CHANGED',guards=(self.root,))
        d.rename(self.root/'old');d.mkdir();(d/'metadata').write_bytes(b'{}')
        self.assert_code('DEPENDENCY_CHANGED',evidence.check)

    def test_staging_exact_schema_hashes_and_no_unapproved_payload(self):
        batches,b=register(self.root,'JSONL');batch=inputs.open_batch(batches,'JSONL','bench-source','b00',b['batch_fingerprint'])
        staging=self.root/'staging';staging.mkdir();stage=inputs.stage([batch],staging,'0'*64)
        stage.check();raw=stage.captures[0].path.read_bytes().splitlines()[0]
        obj=json.loads(raw);obj['cookie']='secret'
        self.assert_code('PUBLICATION_INVALID',inputs.validate_stage,canonical_bytes(obj))
        obj=json.loads(raw);obj['record_ordinal']=False
        self.assert_code('PUBLICATION_INVALID',inputs.validate_stage,canonical_bytes(obj))
        path=stage.captures[0].path;replacement=self.root/'replacement';replacement.write_bytes(path.read_bytes());replacement.replace(path)
        self.assert_code('DEPENDENCY_CHANGED',stage.check)

    def test_batch_versions_inventory_and_immutable_conflict(self):
        batches,b=register(self.root,'PARQUET');batch=inputs.open_batch(batches,'PARQUET','bench-source','b00',b['batch_fingerprint'])
        obj=json.loads(batch.evidence.raw('manifest.json'))
        for key,value in (('batch_contract_version',1),('registrar_revision',True),('request_schema_version',2)):
            self.assert_code('UNSUPPORTED_VERSION',inputs.validate_manifest,canonical_bytes(obj|{key:value}))
        changed=json.loads(canonical_bytes(obj));changed['files'][0]['row_groups'][0]['record_start']=1
        self.assert_code('PARQUET_INVALID',inputs.validate_manifest,canonical_bytes(changed))
        self.assert_code('BATCH_CONFLICT',inputs.select,batches,'PARQUET',[('bench-source','b00',b['batch_fingerprint']),('bench-source','b00','1'*64)])
        self.assert_code('BATCH_UNAVAILABLE',inputs.open_batch,batches,'JSONL','bench-source','b00',b['batch_fingerprint'])
        raw=batch.evidence.raw('complete.json');p=batch.evidence.directory/'complete.json';p.write_bytes(raw+b'\n')
        self.assert_code('BATCH_INVALID',inputs.open_batch,batches,'PARQUET','bench-source','b00',b['batch_fingerprint'])


if __name__=='__main__':unittest.main()
