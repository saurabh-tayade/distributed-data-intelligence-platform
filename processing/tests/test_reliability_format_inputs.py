"""Small v2 intake/vector/privacy tests; no benchmark workload in discovery."""
from hashlib import sha256
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from publication_metadata import canonical_bytes
import reliability_format_metadata as m
import reliability_format_inputs as inputs
import reliability_benchmark_generator as generator
from reliability_format_fixtures import g1,policy,register


class FormatInputTests(unittest.TestCase):
    def test_generator_literal_vectors(self):
        self.assertEqual('5ac37e3b8e4609b93f5c2c37a79c29d22fdfeb08762a1a884d286136ada63545',generator.digest(generator.config()))
        self.assertEqual('f3869488-a723-8658-870d-1e99b0647066',generator.event_id(0))
        row=generator.event(0)
        expected=[1,'f3869488-a723-8658-870d-1e99b0647066','svc00','production',1790849532459480,'PUT','svc00-r11',204,68,True]
        self.assertEqual(expected,[row[n] for n in m.TRANSPORT_FIELDS])
        self.assertEqual('06d4654f9c14ee059e9f0f5e0de0a146c9d111d492b1049955f489e4266f5820',sha256(canonical_bytes(expected)).hexdigest())
        self.assertEqual(7761429314360470490,generator.h(0,'environment'))
        self.assertEqual(200,len(generator.profile('JSONL',generator.config())['allowed_route_ids']))
        self.assertEqual('4dcb761e-71ed-526f-8a63-1e9c303a02d4',inputs.ingestion_id('JSONL','bench-source','b00'))
        self.assertEqual('a581aa05-df68-566e-8ba4-f17b0e22c76e',inputs.ingestion_id('PARQUET','bench-source','b00'))

    def test_g1_registration_physical_positions_and_hashes(self):
        comparisons=[]
        with tempfile.TemporaryDirectory() as root:
            for fmt in ('JSONL','PARQUET'):
                batches,summary=register(root,fmt)
                b=inputs.open_batch(batches,fmt,'bench-source','b00',summary['batch_fingerprint'])
                f=b.manifest['files'][0];result={}
                rows=list(inputs.scan_file(b.evidence.directory/f['file'],b.manifest['profile'],result=result))
                self.assertEqual(10,len(rows));self.assertEqual(list(range(10)),[r['record_ordinal'] for r in rows])
                self.assertEqual([['FIELD_NULL'],['FIELD_VALUE'],['FIELD_VALUE'],['FIELD_VALUE'],['FIELD_NULL']],[r['reason_codes'] for r in rows[5:]])
                if fmt=='JSONL':
                    self.assertEqual(list(range(1,11)),[r['line_number'] for r in rows]);self.assertTrue(all(r['row_ordinal'] is None for r in rows))
                else:
                    self.assertEqual(list(range(10)),[r['row_ordinal'] for r in rows]);self.assertTrue(all(r['line_number'] is None for r in rows))
                valid=[canonical_bytes([r['event'][n] for n in m.TRANSPORT_FIELDS]) for r in rows if r['state']=='VALID']
                rejected=[canonical_bytes([r['recognized_service_id'],r['recognized_event_id'],r['reason_codes']]) for r in rows if r['state']=='REJECTED']
                digests=[]
                for scope,tuples in (('valid',valid),('unique',set(valid)),('rejected',rejected)):
                    digests.append(sha256(('ddip-reliability-'+scope+'-multiset-v1\n').encode()+b''.join(t+b'\n' for t in sorted(tuples))).hexdigest())
                self.assertEqual(['84a1b3d7acfcc43ee055a4369e8681e6f018a577a6e90468fed0cff8d34094d7','c0606b06d05921f88116595d95f3ce944842048d7bdc8e5dd4dfe3e32f93d909','d399e551363954f697c6a020c2cef84aeeb829dee2e5fa4412a3cf0638d9c210'],digests)
                comparisons.append(digests)
            self.assertEqual(*comparisons)

    def test_jsonl_io_classification_and_observed_disappearance(self):
        with tempfile.TemporaryDirectory() as root:
            p=Path(root)/'events.jsonl';p.write_bytes(canonical_bytes(g1()[0])+b'\n')
            detail={};list(inputs.scan_file(p,policy('JSONL'),result=detail));before=p.stat()
            with patch('reliability_inputs.os.open',side_effect=OSError('sensitive-value')):
                with self.assertRaises(m.ReliabilityFailure) as e:list(inputs.scan_file(p,policy('JSONL'),previous=detail['evidence']))
            self.assertEqual('INPUT_UNAVAILABLE',e.exception.code);self.assertNotIn('sensitive',str(e.exception))
            self.assertEqual(before,p.stat());p.unlink()
            with self.assertRaises(m.ReliabilityFailure) as e:list(inputs.scan_file(p,policy('JSONL'),previous=detail['evidence']))
            self.assertEqual('INPUT_CHANGED',e.exception.code)

    def test_sensitive_field_fails_before_batch_reservation(self):
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaises(m.ReliabilityFailure) as e:register(root,'JSONL',[g1()[0]|{'cookie':'secret'}])
            self.assertEqual('INTAKE_NOT_APPROVED',e.exception.code)
            self.assertFalse((Path(root)/'batches'/'v2').exists())

    def test_target_generation_gate(self):
        with tempfile.TemporaryDirectory() as root:
            for n in (100000,1000000):
                with self.assertRaises(m.ReliabilityFailure) as e:generator.prepare(root,generator.config(n))
                self.assertEqual('PREFLIGHT_BLOCKED',e.exception.code);self.assertEqual([],list(Path(root).iterdir()))

    def test_parquet_read_close_and_replacement_classification(self):
        with tempfile.TemporaryDirectory() as root:
            batches,batch=register(root,'PARQUET')
            b=inputs.open_batch(batches,'PARQUET','bench-source','b00',batch['batch_fingerprint'])
            f=b.manifest['files'][0];path=b.evidence.directory/f['file'];detail={}
            list(inputs.scan_file(path,b.manifest['profile'],result=detail));before=path.stat()
            with patch.object(Path,'open',side_effect=OSError('sensitive-path-value')):
                with self.assertRaises(m.ReliabilityFailure) as e:list(inputs.scan_file(path,b.manifest['profile'],previous=detail['evidence']))
            self.assertEqual('INPUT_UNAVAILABLE',e.exception.code);self.assertEqual(before,path.stat())
            original=Path.open
            class CloseFailure:
                def __init__(self,stream):self.stream=stream
                def __enter__(self):return self.stream
                def __exit__(self,*args):self.stream.close();raise OSError('sensitive-close-value')
            with patch.object(Path,'open',lambda p,*a,**k:CloseFailure(original(p,*a,**k))):
                with self.assertRaises(m.ReliabilityFailure) as e:list(inputs.scan_file(path,b.manifest['profile'],previous=detail['evidence']))
            self.assertEqual('INPUT_UNAVAILABLE',e.exception.code);self.assertEqual(before,path.stat())
            self.assertNotIn('sensitive',str(e.exception))
            replacement=Path(root)/'replacement';replacement.write_bytes(path.read_bytes());replacement.replace(path)
            with self.assertRaises(m.ReliabilityFailure) as e:list(inputs.scan_file(path,b.manifest['profile'],previous=detail['evidence']))
            self.assertEqual('INPUT_CHANGED',e.exception.code)

    def test_v2_intake_replay_and_legacy_schema_unchanged(self):
        import reliability_metadata as legacy
        self.assertEqual((21,16,17),tuple(len(legacy.SCHEMAS[t]) for t in legacy.TABLES))
        self.assertEqual((21,20,17),tuple(len(m.SCHEMAS[t]) for t in m.TABLES))
        self.assertEqual(legacy.SCHEMAS[legacy.EVENTS],m.SCHEMAS[m.EVENTS])
        self.assertEqual(legacy.SCHEMAS[legacy.METRICS],m.SCHEMAS[m.METRICS])
        with tempfile.TemporaryDirectory() as root:
            batches,batch=register(root,'JSONL');b=inputs.open_batch(batches,'JSONL','bench-source','b00',batch['batch_fingerprint'])
            before={f.path:(f.path.stat().st_mtime_ns,f.digest) for f in b.evidence.files}
            replay=inputs.register(batches,input_root=Path(root)/'incoming-JSONL',source_id='bench-source',batch_id='b00',input_format='JSONL',
                files=[dict(artifact_id='events',input_file='events.jsonl')],received_at='2026-10-02T00:05:00.000000Z',profile=policy('JSONL'))
            self.assertEqual('replayed',replay['status']);b.check()
            self.assertEqual(before,{f.path:(f.path.stat().st_mtime_ns,f.digest) for f in b.evidence.files})

    def test_explicit_v1_reregistration_has_distinct_identity_without_rewrites(self):
        import reliability_inputs as v1
        with tempfile.TemporaryDirectory() as root:
            batches,b=register(root,'JSONL');incoming=Path(root)/'incoming-JSONL';old_root=Path(root)/'legacy';old_root.mkdir()
            old_policy={k:v for k,v in policy('JSONL').items() if k!='decoder'}
            old_policy.update(profile_id='request-jsonl-v1',policy_revision=1)
            old=v1.register(old_root,input_root=incoming,source_id='bench-source',batch_id='b00',files=[dict(artifact_id='events',input_file='events.jsonl')],
                received_at='2026-10-02T00:05:00.000000Z',profile=old_policy)
            self.assertNotEqual(old['batch_fingerprint'],b['batch_fingerprint']);self.assertNotEqual(old['ingestion_id'],b['ingestion_id'])
            legacy=v1.open_batch(old_root,'bench-source','b00',old['batch_fingerprint'])
            before={f.path:(f.path.stat().st_mtime_ns,f.digest) for f in legacy.evidence.files}
            inputs.open_batch(batches,'JSONL','bench-source','b00',b['batch_fingerprint']).check();legacy.check()
            self.assertEqual(before,{f.path:(f.path.stat().st_mtime_ns,f.digest) for f in legacy.evidence.files})

    def test_sqlite_oracle_g1_raw_accounting_and_vectors(self):
        from reliability_benchmark_oracle import Oracle
        from reliability_format_fixtures import reference
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);refs,ref=reference(root)
            for fmt in ('JSONL','PARQUET'):
                batches,batch=register(root,fmt)
                deps=inputs.verify_dependencies(batch_root=batches,input_format=fmt,
                    batches=[('bench-source','b00',batch['batch_fingerprint'])],reference_root=refs,
                    reference_id='bench-services',revision='r1',reference_fingerprint=ref['snapshot_fingerprint'])
                m.validate_manifest(deps.manifest_bytes)
                bad=deps.manifest();bad['dataset_contract_versions'][m.EVENTS]=True
                with self.assertRaises(m.ReliabilityFailure) as e:m.validate_manifest(canonical_bytes(bad))
                self.assertEqual('UNSUPPORTED_VERSION',e.exception.code)
                scratch=root/('oracle-'+fmt);scratch.mkdir();oracle=Oracle(scratch)
                try:
                    oracle.ingest(deps.manifest(),batches)
                    self.assertEqual((10,5,5,1),(oracle.p,oracle.v,oracle.r,oracle.d))
                    self.assertEqual(4,oracle.sql.execute('SELECT count(*) FROM events').fetchone()[0])
                    self.assertEqual((2,37),oracle.sql.execute('SELECT sum(status BETWEEN 500 AND 599),sum(duration) FROM events').fetchone())
                    self.assertEqual('84a1b3d7acfcc43ee055a4369e8681e6f018a577a6e90468fed0cff8d34094d7',oracle.digests()['valid_multiset_sha256'])
                    self.assertEqual('c0606b06d05921f88116595d95f3ce944842048d7bdc8e5dd4dfe3e32f93d909',oracle.digests()['unique_multiset_sha256'])
                    self.assertEqual('d399e551363954f697c6a020c2cef84aeeb829dee2e5fa4412a3cf0638d9c210',oracle.digests()['rejected_multiset_sha256'])
                finally:oracle.close()

    def test_oracle_calendar_rank_and_import_independence(self):
        import ast
        import reliability_benchmark_oracle as oracle
        self.assertEqual(-1,oracle.instant('1969-12-31T23:59:59.999999Z'))
        self.assertEqual(-62135596800000000,oracle.instant('0001-01-01T00:00:00.000000Z'))
        self.assertEqual(253402300799999999,oracle.instant('9999-12-31T23:59:59.999999Z'))
        for candidate in (18,19,20):self.assertTrue(oracle.qualified(candidate,20,candidate-1,candidate,1)[0])
        self.assertFalse(oracle.qualified(17,20,16,17,1)[0])
        self.assertFalse(oracle.qualified(19,20,18,19,0)[0])
        self.assertEqual((True,1,2),oracle.qualified(9500,10001,9499,9500,1))
        self.assertEqual((False,3,2),oracle.qualified(9498,10001,9497,9498,1))
        tree=ast.parse(Path(oracle.__file__).read_text())
        imported={n.module for n in ast.walk(tree) if isinstance(n,ast.ImportFrom)}
        imported|={a.name for n in ast.walk(tree) if isinstance(n,ast.Import) for a in n.names}
        self.assertFalse(imported&{'pyspark','reliability_processing','reliability_format_processing',
            'reliability_benchmark_generator','reliability_analytics','reliability_format_analytics','reliability_inputs','reliability_format_inputs'})


if __name__=='__main__':unittest.main()
