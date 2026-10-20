import contextlib
from hashlib import sha256
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from publication_metadata import canonical_bytes
import reliability_metadata as m
import reliability_inputs as inputs
import service_reference as reference
from reliability_fixtures import event,profile,f1,f2,raw_file,register_fixture,reference_fixture,RECEIVED,VOCABULARY


class IntakeTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.root=Path(self.temp.name)
        self.incoming=self.root/'incoming'; self.batches=self.root/'batches'
        self.incoming.mkdir(); self.batches.mkdir()

    def tearDown(self): self.temp.cleanup()

    def assert_code(self,code,fn,*args,**kwargs):
        with self.assertRaises(m.ReliabilityFailure) as raised: fn(*args,**kwargs)
        self.assertEqual(code,raised.exception.code)
        self.assertEqual(m.MESSAGES[code],raised.exception.diagnostic['message'])
        self.assertNotIn(str(self.root),str(raised.exception.diagnostic))
        self.assertNotIn('secret',str(raised.exception.diagnostic))

    def register(self,rows=None,**kwargs):
        raw_file(self.incoming,f1() if rows is None else rows)
        return inputs.register(self.batches,input_root=self.incoming,source_id='fixture-source',batch_id='b1',
            files=[dict(artifact_id='events',input_file='events.jsonl')],received_at=RECEIVED,profile=profile(),**kwargs)

    def test_r01_identity_replay_and_selection(self):
        summary=self.register(); directory=self.batches/'source_id=fixture-source'/'batch_id=b1'
        before={p:(p.read_bytes(),p.stat().st_mtime_ns) for p in directory.rglob('*') if p.is_file()}
        self.assertEqual('replayed',self.register()['status'])
        self.assertEqual(before,{p:(p.read_bytes(),p.stat().st_mtime_ns) for p in before})
        selection=('fixture-source','b1',summary['batch_fingerprint'])
        selected,duplicates=inputs.select(self.batches,[selection,selection])
        self.assertEqual((1,1),(len(selected),duplicates))
        self.assert_code('BATCH_CONFLICT',self.register,[canonical_bytes(event(1,duration_ms=99))])
        self.assertEqual(before,{p:(p.read_bytes(),p.stat().st_mtime_ns) for p in before})

    def test_r01_literal_batch_vector(self):
        raw_file(self.incoming)
        policy=profile(); policy['allowed_service_ids']=['alpha']
        result=inputs.register(self.batches,input_root=self.incoming,source_id='fixture-source',batch_id='empty-1',
            files=[dict(artifact_id='events',input_file='events.jsonl')],received_at=RECEIVED,profile=policy)
        self.assertEqual('ad1ffe5f7a244dd8c27b832db8b728dc5e07e2e66858cb436d7f8e639c29b38d',result['batch_fingerprint'])
        self.assertEqual('93aa01d3-76be-5c9a-be35-66f68d660c8e',result['ingestion_id'])

    def test_r01_route_amendment_and_metadata_bounds(self):
        policy=profile(); policy['allowed_route_ids']=['r'+f'{i:03x}'+'a'*60 for i in range(256)]
        policy['allowed_service_ids']=['s'+f'{i:03x}'+'a'*124 for i in range(128)]
        inputs.profile(policy,'fixture-source')
        self.assertLess(len(canonical_bytes(policy)),m.PROFILE_LIMIT)
        policy['allowed_route_ids'].append('zz')
        self.assert_code('INTAKE_NOT_APPROVED',inputs.profile,policy,'fixture-source')
        policy=profile();policy['allowed_route_ids']=['health','health']
        self.assert_code('INTAKE_NOT_APPROVED',inputs.profile,policy,'fixture-source')
        with patch.object(m,'PROFILE_LIMIT',10): self.assert_code('RESOURCE_LIMIT',inputs.profile,profile(),'fixture-source')

    def test_r02_exact_physical_positions(self):
        path=self.incoming/'events.jsonl'; path.write_bytes(b'{}\r\n\n{}')
        result={}; rows=list(inputs.scan_file(path,profile(),result=result))
        self.assertEqual([(1,0,4),(2,4,1),(3,5,2)],[(r['line_number'],r['byte_offset'],r['physical_byte_length']) for r in rows])
        self.assertEqual([['FIELD_MISSING'],['BLANK_LINE'],['FIELD_MISSING']],[r['reason_codes'] for r in rows])
        path.write_bytes(b''); self.assertEqual([],list(inputs.scan_file(path,profile())))
        path.write_bytes(b'{}\n');self.assertEqual(1,len(list(inputs.scan_file(path,profile()))))

    def test_r03_structural_framing_and_resource_failures(self):
        for raw,code in [(b'\xff','BATCH_INVALID'),(b'\xef\xbb\xbf{}','BATCH_INVALID'),(b'{}\r','BATCH_INVALID'),
                         (b' '*16385,'RESOURCE_LIMIT'),(canonical_bytes(event(1,schema_version=2)),'BATCH_INVALID'),
                         (b'{"event_id":"a","event_id":"a"}','BATCH_INVALID'),(b'[[[[[]]]]]','BATCH_INVALID'),
                         (b'{"duration_ms":'+b'1'*65+b'}','BATCH_INVALID')]:
            with self.subTest(raw=raw[:30]):
                path=self.incoming/'events.jsonl';path.write_bytes(raw)
                self.assert_code(code,lambda:list(inputs.scan_file(path,profile())))
        with patch.object(m,'FILE_LIMIT',1): self.assert_code('RESOURCE_LIMIT',self.register)
        with patch.object(m,'MAX_ROWS',1): self.assert_code('RESOURCE_LIMIT',self.register)
        self.assertEqual([],list(self.batches.iterdir()))

    def test_r04_literal_safe_rejections(self):
        parsed=[inputs.parse_event(raw.decode(),profile()) for raw in f2()]
        self.assertEqual(24,sum(p['state']=='VALID' for p in parsed))
        self.assertEqual([['JSON_SYNTAX'],['JSON_SYNTAX'],['JSON_DUPLICATE_KEY'],['JSON_NONFINITE'],['FIELD_VALUE'],['TIMESTAMP_INVALID'],['FIELD_NULL'],['FIELD_VALUE']], [p['reason_codes'] for p in parsed[24:]])
        self.assertIsNone(parsed[24]['recognized_event_id'])
        self.assertEqual(event(23)['event_id'],parsed[26]['recognized_event_id'])
        self.assertNotIn('NaN',canonical_bytes(parsed[27]).decode())
        for name,value,code in [('status_code',True,'FIELD_TYPE'),('duration_ms',1.0,'FIELD_TYPE'),('duration_ms','1','FIELD_TYPE'),('event_time','10000-01-01T00:00:00.000000Z','TIMESTAMP_INVALID')]:
            p=inputs.parse_event(canonical_bytes(event(1,**{name:value})).decode(),profile())
            self.assertEqual([code],p['reason_codes'])

    def test_r04_privacy_precedes_invalid_rows(self):
        for row in [event(1,event_time=None,ip_address='secret'),event(1,route_id='secret-url'),event(1,is_synthetic=False),
                    [dict(ip_address='secret')],dict(duration_ms=dict(headers='secret'))]:
            self.assert_code('INTAKE_NOT_APPROVED',inputs.parse_event,canonical_bytes(row).decode(),profile())
        self.assert_code('INTAKE_NOT_APPROVED',self.register,[canonical_bytes(event(1)),canonical_bytes(event(2,headers='secret'))])
        self.assertEqual([],list(self.batches.iterdir()))

    def test_r02_staging_has_only_safe_records(self):
        result=self.register(f2());batch=inputs.open_batch(self.batches,'fixture-source','b1',result['batch_fingerprint'])
        staging=self.root/'staging';staging.mkdir()
        path,files=inputs.stage([batch],staging,'0'*64)
        lines=(path/files[0]['file']).read_bytes().splitlines()
        self.assertEqual(32,len(lines));rows=[json.loads(r) for r in lines]
        self.assertTrue(all(set(r)==set(inputs.STAGE_KEYS) for r in rows))
        self.assertIsNone(rows[24]['event']);self.assertNotIn('raw',rows[24])
        self.assertEqual(32,json.loads((path/'stage.json').read_bytes())['files'][0]['row_count'])
        with patch.object(inputs,'scan_file',side_effect=m.ReliabilityFailure('DEPENDENCY_INVALID')):
            self.assert_code('DEPENDENCY_INVALID',inputs.stage,[batch],staging,'0'*64)
        self.assertEqual(1,sum((p/'stage.json').exists() for p in staging.iterdir()))

    def test_r03_read_close_errors_and_mutations(self):
        path=raw_file(self.incoming,[canonical_bytes(event(1))]);result={}
        list(inputs.scan_file(path,profile(),result=result)); original=os.fdopen
        class Broken:
            def __init__(self,handle,read=False):self.h=handle;self.read=read
            def __enter__(self):return self
            def __exit__(self,*args): self.h.close();raise OSError('secret-close')
            def fileno(self): return self.h.fileno()
            def readline(self,*args):
                if self.read: raise OSError('secret-read')
                return self.h.readline(*args)
        for read in (True,False):
            before=(path.read_bytes(),m.stamp(path.stat()))
            with patch.object(os,'fdopen',side_effect=lambda *a:Broken(original(*a),read)):
                self.assert_code('INPUT_UNAVAILABLE',lambda:list(inputs.scan_file(path,profile(),previous=result['evidence'])))
            self.assertEqual(before,(path.read_bytes(),m.stamp(path.stat())))
        old=result['evidence']; saved=path.stat(); path.write_bytes(canonical_bytes(event(1,duration_ms=2))+b'\n')
        os.utime(path,ns=(saved.st_atime_ns,saved.st_mtime_ns))
        self.assert_code('INPUT_CHANGED',lambda:list(inputs.scan_file(path,profile(),previous=old)))
        path.unlink();self.assert_code('INPUT_CHANGED',lambda:list(inputs.scan_file(path,profile(),previous=old)))

    def test_r12_partial_copy_failure_is_publication_io(self):
        raw_file(self.incoming,f1());original=Path.open
        class Broken:
            def __init__(self,handle):self.handle=handle
            def __enter__(self):return self
            def __exit__(self,*args):self.handle.close()
            def write(self,raw):
                self.handle.write(raw[:3]);raise OSError('secret-copy-write')
        def opened(path,*args,**kwargs):
            handle=original(path,*args,**kwargs)
            return Broken(handle) if path.parent.name=='data' and args==('xb',) else handle
        before=(self.incoming/'events.jsonl').read_bytes()
        with patch.object(Path,'open',new=opened):self.assert_code('PUBLICATION_FAILED',self.register)
        directory=self.batches/'source_id=fixture-source'/'batch_id=b1'
        self.assertEqual(before,(self.incoming/'events.jsonl').read_bytes())
        self.assertEqual(before[:3],(directory/'data/events.jsonl').read_bytes())
        self.assertFalse((directory/'complete.json').exists())
        self.assert_code('BATCH_INCOMPLETE',self.register)

    def test_r01_corrupt_incomplete_and_unsafe_batches(self):
        result=self.register(); directory=self.batches/'source_id=fixture-source'/'batch_id=b1'
        (directory/'complete.json').unlink()
        self.assert_code('BATCH_INCOMPLETE',inputs.open_batch,self.batches,'fixture-source','b1',result['batch_fingerprint'])
        self.assert_code('BATCH_INCOMPLETE',self.register)
        self.assert_code('PATH_UNSAFE',inputs.register,self.batches,input_root=self.batches,source_id='fixture-source',batch_id='x',files=[],received_at=RECEIVED,profile=profile())
        self.assert_code('ARGUMENT_INVALID',inputs.select,self.batches, [('fixture-source','../x','0'*64)])

    def test_r06_service_reference_independence_and_replay(self):
        root,summary=reference_fixture(self.root)
        opened=reference.open_snapshot(root,'fixture-services','r1',summary['snapshot_fingerprint'])
        self.assertEqual(('alpha','beta','delta'),tuple(r.service_id for r in opened.rows))
        self.assertIsNone(opened.rows[1].owner_team)
        before={p:(p.read_bytes(),p.stat().st_mtime_ns) for p in root.rglob('*') if p.is_file()}
        self.assertEqual('replayed',reference_fixture(self.root)[1]['status'])
        self.assertEqual(before,{p:(p.read_bytes(),p.stat().st_mtime_ns) for p in before})
        (self.root/'service.csv').unlink();(self.root/'vocabulary.json').unlink()
        self.assertEqual(opened,reference.open_snapshot(root,'fixture-services','r1',summary['snapshot_fingerprint']))
        with self.assertRaises(TypeError): opened.manifest['revision']='x'

    def test_r06_header_vector_and_invalid_whole_file(self):
        root,summary=reference_fixture(self.root,empty=True)
        self.assertEqual('040ae62fb022e957480e9c67fbe4a01cced2dde84a9a32e16aa5b5a5b753119a',summary['snapshot_fingerprint'])
        self.assertEqual((),reference.open_snapshot(root,'fixture-services','r1',summary['snapshot_fingerprint']).rows)
        for rows in [b'alpha,A,,,\n',b'alpha,A,,\nalpha,A,,\n',b'alpha,A,secret,critical\n',b'alpha, A,,\n',b'alpha,A,,NULL\n']:
            (self.root/'service.csv').write_bytes(b'service_id,service_name,owner_team,service_tier\n'+rows)
            self.assert_code('CSV_INVALID',reference.validate,self.root/'service.csv',self.root/'vocabulary.json','fixture-services','r2','example-team')
        (self.root/'service.csv').write_bytes(b'service_id,service_name,owner_team,service_tier\n')
        self.assertEqual('replayed',reference.publish(root,self.root/'service.csv',self.root/'vocabulary.json','fixture-services','r1','example-team')['status'])

    def test_r06_reference_conflict_corruption_and_io(self):
        root,summary=reference_fixture(self.root)
        csv=self.root/'service.csv';vocab=self.root/'vocabulary.json'
        csv.write_bytes(csv.read_bytes().replace(b'Alpha Service',b'Other Service'))
        self.assert_code('REVISION_CONFLICT',reference.publish,root,csv,vocab,'fixture-services','r1','example-team')
        with patch.object(reference,'_read_stream',side_effect=OSError('secret-read')):
            self.assert_code('INPUT_UNAVAILABLE',reference.validate,csv,vocab,'fixture-services','r2','example-team')
            self.assert_code('SNAPSHOT_INVALID',reference.open_snapshot,root,'fixture-services','r1',summary['snapshot_fingerprint'])
        selected=root/'reference_id=fixture-services'/'revision=r1'/'reference.csv'
        selected.write_bytes(b'corrupt')
        self.assert_code('SNAPSHOT_INVALID',reference.open_snapshot,root,'fixture-services','r1',summary['snapshot_fingerprint'])

    def test_r06_reference_precompletion_failure(self):
        refs=self.root/'references';refs.mkdir()
        csv=self.root/'service.csv';csv.write_bytes(b'service_id,service_name,owner_team,service_tier\n')
        vocab=self.root/'vocab.json';vocab.write_bytes(VOCABULARY)
        with patch.object(reference,'_write',side_effect=OSError('secret-write')):
            self.assert_code('PUBLICATION_FAILED',reference.publish,refs,csv,vocab,'fixture-services','r1','example-team')
        self.assertFalse((refs/'reference_id=fixture-services'/'revision=r1'/'complete.json').exists())

    def test_r01_cli_safe_arguments_and_help(self):
        for module in ('reliability_inputs','service_reference'):
            for args,exitcode in [(['--help'],0),(['--secret','sensitive'],2)]:
                proc=subprocess.run([sys.executable,'-m',module,*args],capture_output=True,text=True)
                self.assertEqual(exitcode,proc.returncode,proc.stderr)
                if exitcode:
                    self.assertEqual('',proc.stdout);self.assertNotIn('sensitive',proc.stderr)
                    self.assertEqual('ARGUMENT_INVALID',json.loads(proc.stderr)['code'])

    def test_amended_gregorian_end_sentinel(self):
        self.assertEqual(253402300800000000,m.time_us(m.END_SENTINEL,end=True))
        self.assertEqual(253402300799999999,m.time_us('9999-12-31T23:59:59.999999Z'))
        self.assertEqual(-1,m.time_us('1969-12-31T23:59:59.999999Z'))
        self.assertEqual('0001-01-01T00:00:00.000000Z',m.timestamp(m.MIN_US))
        for value in [m.END_SENTINEL,'10001-01-01T00:00:00.000000Z','10000-01-01T00:00:00.000001Z']:
            self.assert_code('ARGUMENT_INVALID',m.time_us,value)
        self.assert_code('ARGUMENT_INVALID',m.time_us,'10001-01-01T00:00:00.000000Z',end=True)


if __name__=='__main__': unittest.main()
