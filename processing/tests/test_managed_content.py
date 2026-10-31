"""D5a adapter contracts. One native fixture, independent real DuckDB/ref readers per case.

The preceding D4 receipt recheck is stubbed here; RssContentIT exercises it against SeaweedFS.
"""
from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path
import unittest
from unittest.mock import patch
import artifact_import_verifier as p
import content_metadata as m
import managed_content as d5
from publication_metadata import canonical_bytes
from test_content_publication import ContentTest as NativeFixture

# Do not expose/import NativeFixture as a TestCase to unittest discovery in this module.
_native_setup=NativeFixture.setUpClass.__func__
_native_teardown=NativeFixture.tearDownClass.__func__
_native_case=NativeFixture.setUp
del NativeFixture


class ManagedContentTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):_native_setup(cls)
    @classmethod
    def tearDownClass(cls):_native_teardown(cls)
    def setUp(self):
        _native_case(self)
        self.gold,self.reference,self.root=(p.resolve() for p in (self.gold,self.reference,self.root))
        def binding(root):
            s=root.stat();return dict(root=str(root),rootDevice=str(s.st_dev),rootInode=str(s.st_ino))
        a=dict(kind='PUBLISHER_REFERENCE',productId='content-intelligence',storageBindingId='reference',relativeAddress='reference_id=fixture-publishers/revision=r1',contractVersion=1,processorRevision=1,identity=dict(referenceId='fixture-publishers',revision='r1'),fingerprint=self.reference_id)
        b=binding(self.reference)
        q=dict(selection=a,verification=dict(capture=p.capture(a,b),native=p.qualify(a,b)))
        self.c=dict(protocolVersion=1,mode='PREPARE',runId='run',attemptId='attempt',ownerEpoch=1,checkpointId='checkpoint',runtimeSha256='a'*64,inputSha256='b'*64,goldProof=dict(command=dict(gold=binding(self.gold)),artifact=dict(nativeFingerprint=self.gold_id,verification=dict(prepared={},capture={}))),reference=dict(qualification=q),referenceBindingId='reference',referenceBinding=b,content=binding(self.root),runtime={},prepared=None,verified=None)
        stub=patch.object(d5,'recheck_gold',return_value=dict(status='UNCHANGED'));self.d4=stub.start();self.addCleanup(stub.stop)
    def prepare(self):self.c['prepared']=d5.execute(self.c)['prepared'];self.c['mode']='VERIFY'
    def snapshot(self):return {str(p.relative_to(self.root)):(p.stat().st_mtime_ns,p.read_bytes()) for p in self.root.rglob('*') if p.is_file()}
    def recover(self):
        import content_recovery_verifier as recovery
        return recovery.execute(dict(kind='CONTENT',original=self.c))
    def test_recovery_completed_publication_without_original_verify_result(self):
        self.prepare();before=self.snapshot();self.c['mode']='PUBLISH'
        with patch('content_publication.publish',side_effect=AssertionError('writer called')):
            result=self.recover()
        self.assertEqual(result['status'],'VERIFIED');self.assertEqual(result['prepared']['fingerprint'],self.identity);self.assertEqual(self.snapshot(),before)
    def test_recovery_prepare_is_read_only(self):
        before=self.snapshot();result=self.recover();self.assertEqual(result['status'],'PREPARED');self.assertEqual(self.snapshot(),before)
    def test_recovery_absent_namespace_is_positive_observation(self):
        import shutil
        self.prepare();shutil.rmtree(self.dest);before=self.snapshot();self.assertEqual(self.recover(),dict(recoveryOutcome='PUBLICATION_ABSENT'));self.assertEqual(self.snapshot(),before)
    def test_recovery_incomplete_namespace_is_retained(self):
        self.prepare();(self.dest/'complete.json').unlink();before=self.snapshot();self.assertEqual(self.recover(),dict(recoveryOutcome='INCOMPLETE_OUTPUT'));self.assertEqual(self.snapshot(),before)
    def test_recovery_missing_reference_blocks_even_absent_output(self):
        import shutil
        self.prepare();shutil.rmtree(self.dest);(self.reference/'reference_id=fixture-publishers/revision=r1/complete.json').unlink()
        with self.assertRaises(Exception):self.recover()
    def test_recovery_changed_gold_blocks(self):
        self.prepare();next(self.gold.rglob('*.parquet')).write_bytes(b'corrupt')
        with self.assertRaises(Exception):self.recover()
    def test_recovery_corrupt_completion_blocks(self):
        self.prepare();(self.dest/'complete.json').write_bytes(b'{}');before=self.snapshot()
        with self.assertRaises(Exception):self.recover()
        self.assertEqual(self.snapshot(),before)
    def test_recovery_substituted_native_identity_blocks(self):
        self.prepare();self.c['prepared']['fingerprint']='0'*64
        with self.assertRaises(Exception):self.recover()
    def test_recovery_ordinary_stat_io_is_not_absence(self):
        import content_recovery_verifier as recovery
        self.prepare()
        with patch.object(recovery.p,'info',side_effect=PermissionError('private')):
            with self.assertRaises(PermissionError):self.recover()
    def test_recovery_safe_failure_codes_distinguish_change_and_io(self):
        import content_recovery_verifier as recovery
        self.assertEqual(recovery.safe_code(p.Failure('INPUT_CHANGED')),'INPUT_CHANGED')
        self.assertEqual(recovery.safe_code(OSError('private')),'EVIDENCE_UNAVAILABLE')
    def test_recovery_unsupported_kind_never_dispatches(self):
        import content_recovery_verifier as recovery
        with self.assertRaises(Exception):recovery.execute(dict(kind='PUBLISH',original=self.c))
    def test_exact_identity_full_native_verifier_and_named_queries(self):
        self.prepare();before=self.snapshot();value=d5.execute(self.c);self.assertEqual(value['prepared']['fingerprint'],self.identity);self.assertEqual(value['verifier'],'content_analytics.open_publication');self.assertEqual([q['name'] for q in value['smoke']],['catalog','reference_coverage','source_discovery_daily']);self.assertEqual(self.snapshot(),before)
        self.c.update(mode='RECHECK',verified=value);self.assertEqual(d5.execute(self.c),value);self.assertEqual(self.snapshot(),before)
    def test_frozen_reference_cannot_change_or_disappear(self):
        self.prepare();path=self.reference/'reference_id=fixture-publishers/revision=r1/reference.csv'
        path.write_bytes(path.read_bytes()+b' ')
        with self.assertRaises(Exception):d5.execute(self.c)
    def test_missing_reference_rejected(self):
        self.prepare();(self.reference/'reference_id=fixture-publishers/revision=r1/complete.json').unlink()
        with self.assertRaises(Exception):d5.execute(self.c)
    def test_changed_gold_rejected(self):
        self.prepare();path=next(self.gold.rglob('*.parquet'));path.write_bytes(b'corrupt')
        with self.assertRaises(Exception):d5.execute(self.c)
    def test_missing_gold_rejected(self):
        self.prepare();next(self.gold.rglob('complete.json')).unlink()
        with self.assertRaises(Exception):d5.execute(self.c)
    def test_substituted_reference_identity_rejected(self):
        self.prepare();self.c['reference']['qualification']['selection']['identity']['revision']='r2'
        with self.assertRaises(Exception):d5.execute(self.c)
    def test_incomplete_content_is_never_repaired(self):
        self.prepare();(self.dest/'complete.json').unlink();before=self.snapshot()
        with self.assertRaises(Exception):d5.execute(self.c)
        self.assertEqual(self.snapshot(),before)
    def test_corrupt_content_parquet_rejected(self):
        self.prepare();next(self.dest.rglob('*.parquet')).write_bytes(b'corrupt');before=self.snapshot()
        with self.assertRaises(Exception):d5.execute(self.c)
        self.assertEqual(self.snapshot(),before)
    def test_corrupt_completion_rejected(self):
        self.prepare();(self.dest/'complete.json').write_bytes(b'{}')
        with self.assertRaises(Exception):d5.execute(self.c)
    def test_substituted_fingerprint_rejected(self):
        self.prepare();self.c['prepared']['fingerprint']='0'*64
        with self.assertRaises(Exception):d5.execute(self.c)
    def test_substituted_verified_capture_rejected(self):
        self.prepare();self.c['verified']=d5.execute(self.c);self.c['verified']['completionSha256']='0'*64;self.c['mode']='RECHECK'
        with self.assertRaises(Exception):d5.execute(self.c)
    def test_output_root_replacement_rejected(self):
        self.prepare();self.c['content']['rootInode']='0'
        with self.assertRaises(Exception):d5.execute(self.c)
    def test_no_publication_call_during_verification(self):
        import content_publication
        self.prepare()
        with patch.object(content_publication,'publish',side_effect=AssertionError('must not write')):d5.execute(self.c)

class ProtocolTest(unittest.TestCase):
    def invoke(self,value=None,error=None):
        import tempfile
        from contextlib import ExitStack
        with tempfile.TemporaryDirectory() as temporary,ExitStack() as stack:
            path=Path(temporary)/'command.json';raw=canonical_bytes(dict(protocolVersion=1));path.write_bytes(raw)
            stack.enter_context(patch.object(d5.sys,'argv',['managed_content.py',str(path)]))
            stack.enter_context(patch.object(d5,'execute',side_effect=error,return_value=value))
            status=d5.main();result=json.loads(path.with_name('result.json').read_bytes())
            self.assertEqual(result['commandSha256'],sha256(raw).hexdigest())
            self.assertEqual(set(result),{'commandSha256','evidenceSha256','status','cleanup','code'})
            evidence=path.with_name('evidence.json')
            if status==0:self.assertEqual(result['evidenceSha256'],sha256(evidence.read_bytes()).hexdigest())
            else:self.assertFalse(evidence.exists())
            return status,result
    def test_canonical_success_envelope(self):
        status,result=self.invoke(dict(status='VERIFIED',counts=dict(rows=2)))
        self.assertEqual(status,0);self.assertEqual(result['status'],'VERIFIED');self.assertEqual(result['cleanup'],'CONFIRMED');self.assertIsNone(result['code'])
    def test_arbitrary_exception_details_are_not_machine_evidence(self):
        status,result=self.invoke(error=RuntimeError('PRIVATE payload credentials /private/location'))
        self.assertEqual(status,1);self.assertEqual(result['code'],'INPUT_INVALID');self.assertNotIn('PRIVATE',str(result));self.assertNotIn('credentials',str(result))
    def test_reviewed_native_codes_and_cleanup_uncertainty_remain_distinct(self):
        for error in (m.ContentFailure('PUBLICATION_INCOMPLETE'),p.Failure('EVIDENCE_CHANGED'),p.Failure('SPARK_CLEANUP_UNCERTAIN')):
            with self.subTest(code=error.code):
                status,result=self.invoke(error=error);self.assertEqual(status,1);self.assertEqual(result['code'],error.code)
                self.assertEqual(result['cleanup'],'UNCONFIRMED' if error.code=='SPARK_CLEANUP_UNCERTAIN' else 'CONFIRMED')


if __name__=='__main__':unittest.main()
