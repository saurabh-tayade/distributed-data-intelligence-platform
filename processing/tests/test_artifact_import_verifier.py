"""Actual authoritative readers, read-only Spark-free import protocol, small synthetic artifacts."""
from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid
import artifact_import_verifier as v
from publication_metadata import canonical_bytes
from artifact_import_fixtures import create


class ImportVerifierTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.fixture = create(Path(self.tmp.name))

    def request(self, index=0):
        a = deepcopy(self.fixture['artifacts'][index])
        root = Path(self.fixture['roots'][a['storageBindingId']])
        s = root.stat()
        return dict(protocolVersion=1,operationId=str(uuid.uuid4()),selectionSha256=sha256(canonical_bytes(a)).hexdigest(),mode='VERIFY',
                    binding=dict(root=str(root),rootDevice=str(s.st_dev),rootInode=str(s.st_ino)),artifact=a,expectedCapture=None)

    def directory(self,r):
        return Path(r['binding']['root'])/r['artifact']['relativeAddress']

    def failure(self,r,code=None):
        try:
            v.execute(r)
        except Exception as e:
            if code:self.assertEqual(e.code,code)
            return
        self.fail('unverified artifact accepted')

    def test_all_four_readers_and_stable_readonly_evidence(self):
        for i in range(4):
            with self.subTest(kind=i):
                r=self.request(i);d=self.directory(r)
                before={p:(p.read_bytes(),p.stat().st_mtime_ns) for p in d.rglob('*') if p.is_file()}
                status,e=v.execute(r)
                self.assertEqual(status,'VERIFIED')
                self.assertEqual(e['native']['fingerprint'],r['artifact']['fingerprint'])
                self.assertTrue(e['native']['dependencies'])
                r.update(mode='RECHECK',expectedCapture=e['capture'])
                self.assertEqual(v.execute(r),('UNCHANGED',None))
                self.assertEqual(before,{p:(p.read_bytes(),p.stat().st_mtime_ns) for p in before})
        self.assertNotIn('pyspark',sys.modules)

    def test_physical_inventory_preserves_format_positions(self):
        for i in (0,1):
            r=self.request(i);_,e=v.execute(r);f=e['native']['inventory'][0]
            self.assertEqual(f['record_count'],10)
            if i==0:self.assertEqual(f['physical_line_count'],10);self.assertIsNone(f['row_groups'])
            else:self.assertIsNone(f['physical_line_count']);self.assertEqual(f['row_groups'][0]['record_start'],0)

    def test_fixed_subprocess_protocol(self):
        r=self.request(0)
        p=subprocess.run([sys.executable,'-I','-B',str(Path(v.__file__))],input=canonical_bytes(r),capture_output=True,timeout=10)
        self.assertEqual(p.returncode,0,p.stderr)
        result=json.loads(p.stdout)
        self.assertEqual(result['status'],'VERIFIED');self.assertEqual(result['operationId'],r['operationId']);self.assertEqual(p.stderr,b'')
        self.assertNotIn(b'event_id',p.stdout)

    def test_wrong_or_mixed_format(self):
        r=self.request();f=self.directory(r)/'data/events.jsonl';f.rename(f.with_suffix('.parquet'));self.failure(r)

    def test_unsupported_contract(self):
        r=self.request();r['artifact']['contractVersion']=1;r['selectionSha256']=sha256(canonical_bytes(r['artifact'])).hexdigest();self.failure(r,'ADAPTER_INVALID')

    def test_unsupported_product(self):
        r=self.request();r['artifact']['productId']='content-intelligence'
        r['selectionSha256']=sha256(canonical_bytes(r['artifact'])).hexdigest();self.failure(r,'ADAPTER_INVALID')

    def test_wrong_registered_profile_digest(self):
        r=self.request();r['artifact']['identity']['registeredProfileSha256']='0'*64
        r['selectionSha256']=sha256(canonical_bytes(r['artifact'])).hexdigest();self.failure(r,'INTAKE_NOT_APPROVED')

    def test_wrong_source_and_batch(self):
        for name in ('sourceId','batchId'):
            r=self.request();r['artifact']['identity'][name]='other';r['selectionSha256']=sha256(canonical_bytes(r['artifact'])).hexdigest();self.failure(r,'PATH_UNSAFE')

    def test_missing_completion(self):
        r=self.request();(self.directory(r)/'complete.json').unlink();self.failure(r,'EVIDENCE_INVALID')

    def test_corrupt_raw(self):
        r=self.request();p=self.directory(r)/'data/events.jsonl';p.write_bytes(b'!'+p.read_bytes()[1:]);self.failure(r,'BATCH_INVALID')

    def test_wrong_raw_length(self):
        r=self.request();p=self.directory(r)/'data/events.jsonl';p.write_bytes(p.read_bytes()+b'\n');self.failure(r,'BATCH_INVALID')

    def test_changed_manifest_hash_count(self):
        r=self.request();p=self.directory(r)/'manifest.json';m=json.loads(p.read_bytes());m['files'][0]['record_count']+=1;p.write_bytes(canonical_bytes(m));self.failure(r)

    def test_unexpected_input(self):
        r=self.request();(self.directory(r)/'data/extra.jsonl').write_bytes(b'{}\n');self.failure(r,'BATCH_INVALID')

    def test_reference_corruption(self):
        for i in (2,3):
            r=self.request(i);(self.directory(r)/'complete.json').write_bytes(b'{}');self.failure(r,'SNAPSHOT_INVALID')

    def test_missing_reference_dependency(self):
        for i in (2,3):
            r=self.request(i);(self.directory(r)/'vocabulary.json').unlink();self.failure(r,'EVIDENCE_INVALID')

    def test_wrong_reference_identity(self):
        r=self.request(2);r['artifact']['identity']['revision']='r2';r['selectionSha256']=sha256(canonical_bytes(r['artifact'])).hexdigest();self.failure(r,'PATH_UNSAFE')

    def test_symlink_rejected(self):
        r=self.request();p=self.directory(r)/'data/events.jsonl';target=p.with_name('retained');p.rename(target);p.symlink_to(target);self.failure(r)

    def test_mutation_during_native_qualification(self):
        r=self.request();original=v.qualify
        def changed(a,b):
            e=original(a,b);p=self.directory(r)/'complete.json';p.write_bytes(p.read_bytes());return e
        with patch.object(v,'qualify',changed):self.failure(r,'EVIDENCE_CHANGED')

    def test_observed_disappearance_is_changed(self):
        r=self.request();original=v.qualify
        def changed(a,b):
            e=original(a,b);(self.directory(r)/'complete.json').unlink();return e
        with patch.object(v,'qualify',changed):self.failure(r,'EVIDENCE_CHANGED')

    def test_missing_evidence_during_recheck_is_changed(self):
        r=self.request();_,e=v.execute(r);r.update(mode='RECHECK',expectedCapture=e['capture'])
        (self.directory(r)/'data/events.jsonl').unlink();self.failure(r,'EVIDENCE_CHANGED')

    def test_changed_after_qualification(self):
        r=self.request();_,e=v.execute(r);r.update(mode='RECHECK',expectedCapture=e['capture']);p=self.directory(r)/'data/events.jsonl';p.write_bytes(p.read_bytes());self.failure(r,'EVIDENCE_CHANGED')

    def test_unavailable_not_invalid(self):
        r=self.request();r['binding']['root']+='/missing';self.failure(r,'EVIDENCE_UNAVAILABLE')

    def test_duplicate_fields_and_oversized_protocol(self):
        for raw in (b'{"a":1,"a":2}',b'x'*(v.PROTOCOL_LIMIT+1),b'{"a":1.0}'):
            with self.assertRaises(v.Failure):v.parse(raw)

    def test_failure_protocol_has_no_private_diagnostics(self):
        r=self.request();(self.directory(r)/'manifest.json').write_bytes(b'secret-private-payload')
        p=subprocess.run([sys.executable,'-I','-B',str(Path(v.__file__))],input=canonical_bytes(r),capture_output=True,timeout=10)
        self.assertEqual(p.returncode,1);n=json.loads(p.stdout);self.assertEqual(n['status'],'FAILED');self.assertIsNone(n['evidence']);self.assertNotIn(b'secret',p.stdout);self.assertEqual(p.stderr,b'')


if __name__=='__main__':unittest.main()
