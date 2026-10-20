"""Deterministic evidence, admission and classification edge cases."""
from datetime import datetime,timezone
from pathlib import Path
from types import SimpleNamespace
import json
import os
import tempfile
import unittest
from unittest.mock import patch

from publication_metadata import canonical_bytes
import reliability_metadata as m
import reliability_inputs as inputs
import service_reference as reference
from reliability_fixtures import profile,event,register_fixture,reference_fixture,VOCABULARY,RECEIVED


class IntegrityTests(unittest.TestCase):
    def setUp(self):self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
    def tearDown(self):self.temp.cleanup()
    def assert_code(self,code,fn,*args,**kwargs):
        with self.assertRaises(m.ReliabilityFailure) as raised:fn(*args,**kwargs)
        self.assertEqual(code,raised.exception.code)
        self.assertNotIn('secret',str(raised.exception.diagnostic))
        self.assertNotIn(str(self.root),str(raised.exception.diagnostic))

    def test_r14_directory_change_survives_read_failure(self):
        directory=self.root/'selected';directory.mkdir();(directory/'metadata.json').write_bytes(b'{}')
        evidence=m.capture(directory,{'metadata.json':16},(),'DEPENDENCY_INVALID','DEPENDENCY_CHANGED',guards=(self.root,))
        original=m.observe
        def failed(path,*args,**kwargs):
            directory.rename(self.root/'old');directory.mkdir();(directory/'metadata.json').write_bytes(b'{}')
            raise m.ReliabilityFailure('DEPENDENCY_INVALID')
        with patch.object(m,'observe',side_effect=failed):self.assert_code('DEPENDENCY_CHANGED',evidence.check)

    def test_r14_partial_inventory_io_and_observed_new_entry(self):
        (self.root/'known').write_bytes(b'')
        old=m.names(self.root,'DEPENDENCY_INVALID','DEPENDENCY_CHANGED')
        class Listing:
            def __init__(self,new=False,complete=False):self.new=new;self.done=False;self.complete=complete
            def __enter__(self):return self
            def __exit__(self,*args):
                if self.complete:raise OSError('secret-close')
            def __iter__(self):return self
            def __next__(self):
                if self.done:
                    if self.complete:raise StopIteration
                    raise OSError('secret-read')
                self.done=True
                if self.new:(selfroot/'new').write_bytes(b'')
                return SimpleNamespace(name='new' if self.new else 'known')
        selfroot=self.root
        with patch.object(os,'scandir',return_value=Listing()):self.assert_code('DEPENDENCY_INVALID',m.names,self.root,'DEPENDENCY_INVALID','DEPENDENCY_CHANGED',old)
        with patch.object(os,'scandir',return_value=Listing(new=True)):self.assert_code('DEPENDENCY_CHANGED',m.names,self.root,'DEPENDENCY_INVALID','DEPENDENCY_CHANGED',old)
        (self.root/'new').unlink();(self.root/'gone').write_bytes(b'');old=m.names(self.root,'DEPENDENCY_INVALID','DEPENDENCY_CHANGED');(self.root/'gone').unlink()
        with patch.object(os,'scandir',return_value=Listing(complete=True)):self.assert_code('DEPENDENCY_CHANGED',m.names,self.root,'DEPENDENCY_INVALID','DEPENDENCY_CHANGED',old)

    def test_r14_observed_byte_change_survives_close_error(self):
        path=self.root/'data';path.write_bytes(b'old');original=m.observe(path,16,'DEPENDENCY_INVALID','DEPENDENCY_CHANGED',retain=True)
        actual_fdopen=os.fdopen
        class Broken:
            def __init__(self,stream):self.s=stream
            def __enter__(self):return self
            def __exit__(self,*a):self.s.close();raise OSError('secret-close')
            def fileno(self):return self.s.fileno()
            def read(self,*args):return b'new' if self.s.tell()==0 and self.s.seek(3) else b''
        with patch.object(os,'fdopen',side_effect=lambda *a:Broken(actual_fdopen(*a))):
            self.assert_code('DEPENDENCY_CHANGED',m.observe,path,16,'DEPENDENCY_INVALID','DEPENDENCY_CHANGED',original,retain=True)
        self.assertEqual(b'old',path.read_bytes())

    def test_r13_staging_replacement_not_hidden_by_final_capture(self):
        batches,summary=register_fixture(self.root,[canonical_bytes(event(1))]);batch=inputs.open_batch(batches,'fixture-source','b1',summary['batch_fingerprint'])
        staging=self.root/'staging';staging.mkdir();original=m.write
        def changed(path,raw):
            if path.name=='stage.json':
                target=next(path.parent.glob('stage-*.jsonl'));copy=target.with_suffix('.copy');copy.write_bytes(target.read_bytes());copy.replace(target)
            return original(path,raw)
        with patch.object(m,'write',side_effect=changed):self.assert_code('DEPENDENCY_CHANGED',inputs.stage,[batch],staging,'0'*64)

    def test_r01_all_metadata_boundaries_and_unknown_versions(self):
        raw=canonical_bytes(dict(x=1))
        self.assert_code('RESOURCE_LIMIT',m.parse,raw,len(raw)-1,'BATCH_INVALID')
        for raw in (b'{"x":1,"x":2}',b'{ "x":1}',b'{"x":1.0}',b'{"x":-0}',b'{"x":NaN}'):
            self.assert_code('BATCH_INVALID',m.parse,raw,128,'BATCH_INVALID')
        batches,summary=register_fixture(self.root,[])
        directory=batches/'source_id=fixture-source'/'batch_id=b1';manifest=json.loads((directory/'manifest.json').read_bytes())
        for key,value in (('registrar_revision',2),('request_schema_version',True)):
            changed=manifest|{key:value}
            self.assert_code('UNSUPPORTED_VERSION',inputs.validate_manifest,canonical_bytes(changed))
        with patch.object(m,'MAX_INPUT_FILES',0):self.assert_code('RESOURCE_LIMIT',inputs.select,batches,[('fixture-source','b1',summary['batch_fingerprint'])])
        with patch.object(m,'MAX_BATCHES',0):self.assert_code('ARGUMENT_INVALID',inputs.select,batches,[('fixture-source','b1',summary['batch_fingerprint'])])

    def test_r01_exact_route_cap_full_profile_fits(self):
        p=profile();source='s'*128;p['synthetic_source_id']=source;p['logical_dataset_id']='l'*64
        p['allowed_route_ids']=['r'+f'{i:03x}'+'a'*60 for i in range(256)]
        p['allowed_service_ids']=['s'+f'{i:03x}'+'a'*124 for i in range(128)]
        inputs.profile(p,source);size=len(canonical_bytes(p));self.assertLessEqual(size,65536)
        print(json.dumps({'e2_maximum_profile_bytes':size,'approved_profile_limit':65536}))

    def test_r01_root_aliases_and_unsafe_spellings(self):
        actual=self.root/'actual';actual.mkdir();selected=actual/'selected';selected.mkdir()
        alias=self.root/'alias';alias.symlink_to(actual,target_is_directory=True)
        a=m.root_path(selected);b=m.root_path(alias/'selected')
        self.assertEqual(a,b)
        self.assert_code('PATH_UNSAFE',m.separate,a,b)
        self.assert_code('PATH_UNSAFE',m.root_path,alias)
        for name in ('back\\slash','star*','question?','bracket['):
            path=self.root/name;path.mkdir()
            self.assert_code('PATH_UNSAFE',m.root_path,path)
            self.assert_code('PATH_UNSAFE',reference._path,path/'reference.csv')
        refs,summary=reference_fixture(self.root)
        link=refs/'reference_id=linked';link.symlink_to(refs/'reference_id=fixture-services',target_is_directory=True)
        self.assert_code('PATH_UNSAFE',reference.open_snapshot,refs,'linked','r1',summary['snapshot_fingerprint'])

    def test_r03_exact_line_cap_and_no_prefix_registration(self):
        incoming=self.root/'incoming';incoming.mkdir();batches=self.root/'batches';batches.mkdir()
        path=incoming/'events.jsonl';path.write_bytes(b'{}'+b' '*(m.LINE_LIMIT-2)+b'\r\n')
        self.assertEqual(m.LINE_LIMIT+2,next(iter(inputs.scan_file(path,profile())))['physical_byte_length'])
        path.write_bytes(canonical_bytes(event(1))+b'\n'+b'\xff\n')
        self.assert_code('BATCH_INVALID',inputs.register,batches,input_root=incoming,source_id='fixture-source',batch_id='x',files=[dict(artifact_id='events',input_file='events.jsonl')],received_at=RECEIVED,profile=profile())
        self.assertEqual([],list(batches.iterdir()))

    def test_r04_duplicate_privacy_schema_and_type_precedence(self):
        for text in ('{"event_id":"x","event_id":"x","headers":"secret"}',
                     '{"schema_version":1,"schema_version":2,"headers":"secret"}',
                     '{"duration_ms":NaN,"headers":"secret"}',
                     '{"route_id":"health","route_id":"secret"}'):
            self.assert_code('INTAKE_NOT_APPROVED',inputs.parse_event,text,profile())
        self.assert_code('BATCH_INVALID',inputs.parse_event,'{"schema_version":1,"schema_version":2}',profile())
        for text in ('null','true','[]','42'):
            self.assertEqual(['JSON_NOT_OBJECT'],inputs.parse_event(text,profile())['reason_codes'])
        text=canonical_bytes(event(1,duration_ms=1)).decode().replace('"duration_ms":1','"duration_ms":1e300')
        self.assertEqual(['FIELD_TYPE'],inputs.parse_event(text,profile())['reason_codes'])

    def test_r06_reference_unsupported_vocab_symlinks_and_limits(self):
        refs,summary=reference_fixture(self.root)
        csv=self.root/'service.csv';vocab=self.root/'vocabulary.json'
        vocab.write_bytes(VOCABULARY.replace(b'"version":1',b'"version":2'))
        self.assert_code('VOCABULARY_INVALID',reference.validate,csv,vocab,'fixture-services','r2','example-team')
        vocab.write_bytes(VOCABULARY)
        with patch.object(reference,'ROW_LIMIT',1):self.assert_code('RESOURCE_LIMIT',reference.validate,csv,vocab,'fixture-services','r2','example-team')
        link=self.root/'link.csv';link.symlink_to(csv)
        self.assert_code('PATH_UNSAFE',reference.validate,link,vocab,'fixture-services','r2','example-team')
        selected=refs/'reference_id=fixture-services'/'revision=r1';(selected/'complete.tmp').write_bytes(b'{}')
        self.assert_code('SNAPSHOT_INVALID',reference.open_snapshot,refs,'fixture-services','r1',summary['snapshot_fingerprint'])

    def test_r04_semantic_json_spelling_equality(self):
        data=event(1)
        alternate=json.dumps(dict(reversed(list(data.items()))),ensure_ascii=True).replace('alpha',chr(92)+'u0061lpha')
        parsed=inputs.parse_event(canonical_bytes(data).decode(),profile())
        actual=inputs.parse_event(alternate,profile())
        self.assertEqual(parsed['event'],actual['event']);self.assertEqual(parsed['semantic_sha256'],actual['semantic_sha256'])

    def test_r08_calendar_oracle_and_invalid_end_roles(self):
        epoch=datetime(1970,1,1,tzinfo=timezone.utc)
        for year in (1,4,100,400,1582,1900,1970,2000,2026,9999):
            for month,day in ((1,1),(3,1),(12,31)):
                value=f'{year:04d}-{month:02d}-{day:02d}T23:59:59.999999Z'
                d=datetime(year,month,day,23,59,59,999999,tzinfo=timezone.utc)-epoch
                expected=(d.days*86400+d.seconds)*1000000+d.microseconds
                self.assertEqual(expected,m.time_us(value));self.assertEqual(value,m.timestamp(expected))
        for value in ('0000-01-01T00:00:00.000000Z','1900-02-29T00:00:00.000000Z','2026-01-01T00:00:60.000000Z','2026-01-01T00:00:00Z','2026-01-01T00:00:00.000000+00:00'):
            self.assert_code('ARGUMENT_INVALID',m.time_us,value,end=True)


if __name__=='__main__':unittest.main()
