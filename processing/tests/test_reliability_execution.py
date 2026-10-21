"""C2b Gate A: real, bounded Spark writers and distinct independent-reader processes."""
import copy
from hashlib import sha256
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import uuid
from publication_metadata import canonical_bytes
from reliability_preflight_fixtures import create, load


def prepare(f):
    prior=f['command']; evidence=load(prior['runtime']).qualify(prior,f['bundle'])
    result=dict(protocolVersion=1,runId=prior['runId'],attemptId=prior['attemptId'],stage='CHECK_INPUTS',itemOrdinal=0,status='OK',code=None,evidenceId=prior['checkpointId'],counts=evidence['counts'])
    anchor=dict(protocolVersion=1,checkpointId=prior['checkpointId'],commandSha256=sha256(canonical_bytes(prior)).hexdigest(),
        resultSha256=sha256(canonical_bytes(result)).hexdigest(),evidenceSha256=sha256(canonical_bytes(evidence)).hexdigest(),
        selectionSha256=prior['capturedConfigSha256'],nativeDependencyFingerprint=evidence['nativeDependencyFingerprint'],cleanup='OBSERVED_CLEAN_EXIT')
    return dict(command=prior,bundle=f['bundle'],result=result,evidence=evidence,anchor=anchor)


def child(pack, stage):
    prior=pack['command'];runtime=prior['runtime']['configuration'];root=Path(pack['bundle']['bindings']['control']['root'])
    directory=root/str(uuid.uuid4());directory.mkdir(mode=0o700)
    c={key:prior[key] for key in ('protocolVersion','runId','attemptId','ownerEpoch','itemOrdinal','capturedConfigSha256','runtime')}
    c.update(stage=stage,checkpointId=str(uuid.uuid4()),retainedSha256=sha256(canonical_bytes(pack)).hexdigest(),nativeFingerprint=pack['evidence']['nativeDependencyFingerprint'])
    for name,obj in (('command.json',c),('retained.json',pack)):
        path=directory/name;path.write_bytes(canonical_bytes(obj));path.chmod(0o600)
    env=dict(PATH='/usr/bin:/bin',TZ='UTC',LANG='C.UTF-8',JAVA_HOME=runtime['javaHome'],TMPDIR=pack['bundle']['bindings']['temp']['root'],OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1')
    proc=subprocess.run([runtime['python'],'-I','-B','-X','utf8',str(Path(runtime['processingRoot'])/'reliability_execution.py'),str(directory/'command.json')],env=env,cwd=env['TMPDIR'],capture_output=True,timeout=180)
    result=json.loads((directory/'result.json').read_bytes())
    evidence=json.loads((directory/'evidence.json').read_bytes()) if (directory/'evidence.json').exists() else None
    return proc,result,evidence


class ReliabilityExecutionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary=tempfile.TemporaryDirectory();cls.root=Path(cls.temporary.name).resolve();cls.fixtures={}
        for fmt in ('JSONL','PARQUET'):
            fixture=create(cls.root/fmt,fmt,multi=True,publication=True);pack=prepare(fixture)
            proc,result,evidence=child(pack,'PUBLISH_RELIABILITY')
            if proc.returncode:raise AssertionError((fmt,result,proc.stderr[-2000:].decode(errors='replace')))
            cls.fixtures[fmt]=(fixture,pack,evidence)
    @classmethod
    def tearDownClass(cls):cls.temporary.cleanup()
    def test_both_formats_real_spark_and_separate_independent_verification(self):
        for fmt,(f,pack,published) in self.fixtures.items():
            with self.subTest(fmt=fmt):
                self.assertEqual(published['publicationStatus'],'published')
                self.assertEqual(published['cleanup']['kind'],'PYSPARK_GATEWAY_REAPED')
                proc,result,evidence=child(pack,'VERIFY_RESULT');self.assertEqual(proc.returncode,0,result)
                output=evidence['publication'];self.assertEqual(output['inputFormat'],fmt)
                self.assertEqual(output['nativeFingerprint'],pack['evidence']['nativeDependencyFingerprint'])
                self.assertEqual(output['counts'],dict(occurrences='15',accepted='10',rejected='5',events='4',windows='3'))
                self.assertEqual(evidence['cleanup'],dict(kind='READ_ONLY'))
                import base64
                marker=json.loads(base64.b64decode(output['completionBase64']))
                self.assertEqual(marker['occurrence_counts']['identical_duplicate_occurrences'],6)
    def test_completed_native_replay_never_changes_bytes_or_mtimes(self):
        for f,pack,_ in self.fixtures.values():
            root=Path(pack['bundle']['bindings']['output']['root'])
            before={str(p):(p.read_bytes() if p.is_file() else None,p.stat().st_mtime_ns) for p in (root,*root.rglob('*'))}
            proc,result,evidence=child(pack,'PUBLISH_RELIABILITY');self.assertEqual(proc.returncode,0,result)
            self.assertEqual(evidence['publicationStatus'],'replayed');self.assertEqual(evidence['cleanup']['kind'],'READ_ONLY_REPLAY')
            self.assertEqual(before,{str(p):(p.read_bytes() if p.is_file() else None,p.stat().st_mtime_ns) for p in (root,*root.rglob('*'))})
    def test_z_changed_dependency_refuses_completed_replay(self):
        _,pack,_=self.fixtures['JSONL'];item=next(i for i in pack['bundle']['capturedSelection']['inputs'] if i['role']=='RELIABILITY_BATCH')
        path=next((Path(pack['bundle']['bindings']['batch']['root'])/item['relativeAddress']/'data').iterdir());old=path.read_bytes();st=path.stat()
        try:
            path.write_bytes(old+b'\n');proc,result,_=child(pack,'PUBLISH_RELIABILITY');self.assertEqual(proc.returncode,1);self.assertTrue(result['code'].endswith('CHANGED'))
        finally:path.write_bytes(old);os.utime(path,ns=(st.st_atime_ns,st.st_mtime_ns))
    def test_corrupted_complete_or_parquet_refused(self):
        _,pack,_=self.fixtures['PARQUET'];root=Path(pack['bundle']['bindings']['output']['root'])
        for path in [next(root.rglob('complete.json')),next(root.rglob('*.parquet'))]:
            old=path.read_bytes();st=path.stat()
            try:
                path.write_bytes(b'corrupt');proc,result,_=child(pack,'VERIFY_RESULT');self.assertEqual(proc.returncode,1);self.assertNotEqual(result['status'],'OK')
            finally:path.write_bytes(old);os.utime(path,ns=(st.st_atime_ns,st.st_mtime_ns))
    def test_known_identity_conflict_has_quiescent_absent_output_evidence(self):
        fixture=create(self.root/'conflict','JSONL',multi=True);pack=prepare(fixture)
        proc,result,evidence=child(pack,'PUBLISH_RELIABILITY')
        self.assertEqual(proc.returncode,1,result);self.assertEqual(result['code'],'EVENT_IDENTITY_CONFLICT')
        self.assertEqual(evidence['outputState'],'ABSENT');self.assertEqual(evidence['cleanup']['kind'],'PYSPARK_GATEWAY_REAPED')
        self.assertFalse(list(Path(pack['bundle']['bindings']['output']['root']).rglob('complete.json')))
    def test_retained_digest_mutation_refused(self):
        _,pack,_=self.fixtures['JSONL'];changed=copy.deepcopy(pack);changed['anchor']['evidenceSha256']='0'*64
        proc,result,_=child(changed,'VERIFY_RESULT');self.assertEqual(proc.returncode,1);self.assertEqual(result['code'],'EVIDENCE_CHANGED')
    def test_changed_deployment_refused(self):
        _,pack,_=self.fixtures['JSONL'];path=Path(pack['command']['runtime']['configuration']['processingRoot'])/'service_reference.py';old=path.read_bytes()
        try:
            path.write_bytes(old+b'\n');proc,result,_=child(pack,'VERIFY_RESULT');self.assertEqual(proc.returncode,1);self.assertEqual(result['code'],'RUNTIME_MISMATCH')
        finally:path.write_bytes(old)
    def test_missing_completion_and_incomplete_namespace_not_repaired(self):
        _,pack,_=self.fixtures['PARQUET'];path=next(Path(pack['bundle']['bindings']['output']['root']).rglob('complete.json'));old=path.read_bytes();st=path.stat()
        try:
            path.unlink();proc,result,_=child(pack,'PUBLISH_RELIABILITY');self.assertEqual(proc.returncode,1);self.assertEqual(result['code'],'PUBLICATION_INCOMPLETE');self.assertFalse(path.exists())
        finally:path.write_bytes(old);os.utime(path,ns=(st.st_atime_ns,st.st_mtime_ns))


if __name__=='__main__':unittest.main()
