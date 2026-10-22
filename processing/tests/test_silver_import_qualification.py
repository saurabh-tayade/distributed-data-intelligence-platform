"""Gate A: real native readers/Spark, exact Bronze GETs, synthetic inputs only."""
from copy import deepcopy
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid

from pyspark.sql import functions as F, types as T
import silver_inputs
from silver import session, SILVER_SCHEMA, REJECTION_SCHEMA
from publication_metadata import canonical_bytes
import artifact_import_verifier as protocol
import silver_import_qualification as q
from silver_import_fixtures import create, Store, serve


class SilverQualificationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark=session();cls.spark.sparkContext.setLogLevel('ERROR')
        cls.base=tempfile.TemporaryDirectory();cls.original,cls.original_store,cls.selections=create(cls.spark,Path(cls.base.name))
    @classmethod
    def tearDownClass(cls):cls.spark.stop();cls.base.cleanup()
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.base=Path(self.tmp.name).resolve();self.root=self.base/'silver';shutil.copytree(self.original,self.root)
        self.store=Store();self.store.objects=deepcopy(self.original_store.objects);self.a=deepcopy(self.selections[0])
        s=self.root.stat();self.binding=dict(root=str(self.root),rootDevice=str(s.st_dev),rootInode=str(s.st_ino))
    def marker(self):return self.root/self.a['relativeAddress']
    def prepare(self):
        q.selection(self.a);local=q.capture(self.a,self.binding)
        marker,_,_=silver_inputs.parse_marker(self.root,self.a['relativeAddress'])
        bronze,expected=q.bronze_read(self.a,None,marker,self.store)
        return local,marker,bronze,expected
    def qualify(self):
        local,marker,bronze,expected=self.prepare();result=q.qualify(self.spark,self.a,self.binding,local,marker,expected)
        self.assertEqual(local,q.capture(self.a,self.binding,local));self.assertEqual(bronze,q.bronze_read(self.a,None,marker,self.store)[0]);return result
    def failure(self,code=None):
        with self.assertRaises(Exception) as c:self.qualify()
        self.assertTrue(hasattr(c.exception,'code'),type(c.exception))
        if code:self.assertEqual(c.exception.code,code)
    def rewrite(self,name,change=None,schema=None,counts=False):
        path=self.marker().parent/name;rows=[r.asDict() for r in self.spark.read.parquet(str(path)).collect()]
        if change:rows=change(rows)
        shutil.rmtree(path);self.spark.createDataFrame(rows,schema or {'accepted':SILVER_SCHEMA,'rejected':REJECTION_SCHEMA}[name]).write.parquet(str(path))
        if counts:
            m=json.loads(self.marker().read_bytes());m['counts'][name]=len(rows);self.marker().write_bytes(canonical_bytes(m))
    def test_valid_rss_atom_rejected_only_and_readonly(self):
        for a,counts in zip(self.selections,({'accepted':1,'rejected':1},{'accepted':1,'rejected':0},{'accepted':0,'rejected':1})):
            self.a=deepcopy(a);before={p:(p.read_bytes(),p.stat().st_mtime_ns) for p in self.marker().parent.rglob('*') if p.is_file()}
            result=self.qualify();self.assertEqual(result['counts'],counts);self.assertFalse(result['semanticReconstruction'])
            self.assertEqual(before,{p:(p.read_bytes(),p.stat().st_mtime_ns) for p in before})
    def test_empty_datasets(self):
        for name in ('accepted','rejected'):self.rewrite(name,lambda rows:[],counts=True)
        self.assertEqual(self.qualify()['counts'],{'accepted':0,'rejected':0})
    def test_wrong_source_and_ingestion(self):
        for field,value in [('sourceId','other'),('ingestionId',str(uuid.uuid4()))]:
            a=deepcopy(self.a);a['identity'][field]=value
            with self.assertRaises(protocol.Failure):q.selection(a)
    def test_missing_marker(self):self.marker().unlink();self.failure()
    def test_forged_marker(self):self.marker().write_bytes(b'{}');self.failure('INVALID_MARKER')
    def test_valid_marker_alone_is_insufficient(self):
        shutil.rmtree(self.marker().parent/'accepted');shutil.rmtree(self.marker().parent/'rejected')
        self.failure('SILVER_INVALID')
    def test_marker_changed_between_capture_and_native_parse(self):
        local,_,_,_=self.prepare();old=silver_inputs.parse_marker
        def changed(*args):
            self.marker().write_bytes(b'{}');return old(*args)
        request=dict(artifact=self.a,binding=self.binding,expectedCapture=None,bronze={},runtime={},mode='VERIFY')
        with patch.object(silver_inputs,'parse_marker',changed):
            with self.assertRaises(protocol.Failure) as e:q.execute(request)
            self.assertEqual(e.exception.code,'EVIDENCE_CHANGED')
    def test_unknown_marker_field(self):
        m=json.loads(self.marker().read_bytes());m['extra']=1;self.marker().write_bytes(canonical_bytes(m));self.failure('INVALID_MARKER')
    def test_changed_completion_bytes_changes_operational_identity(self):
        local,m,b,_=self.prepare();_,before=q.identity(self.a,local,b);self.marker().write_bytes(self.marker().read_bytes()+b' ')
        new=q.capture(self.a,self.binding);_,after=q.identity(self.a,new,b);self.assertNotEqual(before,after)
        with self.assertRaises(protocol.Failure):q.capture(self.a,self.binding,local)
    def test_missing_parquet(self):
        for path in (self.marker().parent/'accepted').glob('*.parquet'):path.unlink()
        self.failure('SILVER_INVALID')
    def test_extra_visible_file(self):
        (self.marker().parent/'accepted'/'extra.txt').write_bytes(b'bad');self.failure('SILVER_INVALID')
    def test_replaced_parquet_is_changed(self):
        local,_,_,_=self.prepare();path=next((self.marker().parent/'accepted').glob('*.parquet'));data=path.read_bytes();path.unlink();path.write_bytes(data)
        with self.assertRaises(protocol.Failure) as e:q.capture(self.a,self.binding,local)
        self.assertEqual(e.exception.code,'EVIDENCE_CHANGED')
    def test_bookkeeping_is_inventoried_and_native_policy_preserved(self):
        path=self.marker().parent/'accepted'/'_private-bookkeeping';path.mkdir();(path/'.extra').write_bytes(b'fixture')
        local,_,_,_=self.prepare();self.assertIn('accepted/_private-bookkeeping/.extra',[f['path'] for f in local['files']]);self.qualify()
    def test_hidden_bookkeeping_subtree_keeps_native_policy(self):
        path=self.marker().parent/'accepted'/'_temporary'/'0'/'attempt';path.mkdir(parents=True);(path/'part.parquet').write_bytes(b'native-ignored-bookkeeping')
        local,_,_,_=self.prepare();self.assertIn('accepted/_temporary/0/attempt/part.parquet',[f['path'] for f in local['files']]);self.qualify()
    def test_glob_characters_cannot_select_analytical_files(self):
        source=next((self.marker().parent/'accepted').glob('*.parquet'));source.rename(source.with_name('part[0].parquet'));self.failure('PATH_UNSAFE')
    def test_symlink_escape(self):
        path=next((self.marker().parent/'accepted').glob('*.parquet'));target=self.base/'outside';path.rename(target);path.symlink_to(target);self.failure('PATH_UNSAFE')
    def test_schema_mismatch(self):
        schema=T.StructType([T.StructField(f.name,T.StringType() if f.name=='payload_byte_length' else f.dataType) for f in SILVER_SCHEMA])
        self.rewrite('accepted',lambda rows:[dict(r,payload_byte_length=str(r['payload_byte_length'])) for r in rows],schema);self.failure('SCHEMA_MISMATCH')
    def test_required_null_value(self):self.rewrite('accepted',lambda rows:[dict(r,title=None) for r in rows]);self.failure('INVALID_ROW')
    def test_nullable_array_element(self):self.rewrite('accepted',lambda rows:[dict(r,quality_warnings=[None]) for r in rows]);self.failure('INVALID_ROW')
    def test_provenance_source_uri_timestamp_length_feed(self):
        for field,value in [('source_uri','https://different.invalid'),('payload_byte_length',1)]:
            for dataset in ('accepted','rejected'):self.rewrite(dataset,lambda rows:[dict(r,**{field:value}) for r in rows])
            self.failure('PROVENANCE_MISMATCH')
            shutil.rmtree(self.root);shutil.copytree(self.original,self.root);s=self.root.stat();self.binding.update(rootDevice=str(s.st_dev),rootInode=str(s.st_ino))
    def test_count_mismatch(self):
        m=json.loads(self.marker().read_bytes());m['counts']['accepted']+=1;self.marker().write_bytes(canonical_bytes(m));self.failure('COUNT_MISMATCH')
    def test_wrong_bronze_hash(self):
        key=('local',self.a['identity']['bronzePayloadKey']);self.store.objects[key]+=b' ';self.failure('BRONZE_INVALID')
    def test_wrong_bronze_metadata(self):
        key=('local',self.a['identity']['bronzeMetadataKey']);self.store.objects[key]=b'{}';self.failure('BRONZE_INVALID')
    def test_missing_bronze_payload(self):del self.store.objects[('local',self.a['identity']['bronzePayloadKey'])];self.failure('BRONZE_UNAVAILABLE')
    def test_real_sdk_get_and_access_denied_are_unavailable(self):
        local,m,_,_=self.prepare()
        with serve(self.store) as c:
            self.assertTrue(q.bronze_read(self.a,c,m)[0]);self.assertTrue(all('Credential=synthetic-access/' in r[2] for r in self.store.requests));self.assertEqual(self.store.writes,[])
            key=('local',self.a['identity']['bronzeMetadataKey'])
            for status in (403,404,500):
                self.store.status[key]=status
                with self.assertRaises(protocol.Failure) as e:q.bronze_read(self.a,c,m)
                self.assertEqual(e.exception.code,'BRONZE_UNAVAILABLE')
    def test_observed_mutation_during_native_verification(self):
        local,m,_,expected=self.prepare();old=silver_inputs.load_inputs
        def changed(*args,**kwargs):
            result=old(*args,**kwargs);self.marker().write_bytes(self.marker().read_bytes()+b' ');return result
        with patch.object(silver_inputs,'load_inputs',changed):
            with self.assertRaises(silver_inputs.InputFailure) as e:q.qualify(self.spark,self.a,self.binding,local,m,expected)
            self.assertEqual(e.exception.code,'INPUT_CHANGED')
            with self.assertRaises(protocol.Failure) as observed:q.capture(self.a,self.binding,local)
            self.assertEqual(observed.exception.code,'EVIDENCE_CHANGED')
    def test_digest_excludes_machine_and_operational_stamps(self):
        local,m,b,_=self.prepare();value,digest=q.identity(self.a,local,b)
        changed=deepcopy(local)
        for f in changed['files']:f['stamp']=['other']*7
        changed['guards']=[];changed['directories']=[]
        self.assertEqual(q.identity(self.a,changed,b),(value,digest));changed['files'][0]['sha256']='0'*64;self.assertNotEqual(q.identity(self.a,changed,b)[1],digest)
    def test_missing_file_after_capture_is_changed(self):
        local,_,_,_=self.prepare();next((self.marker().parent/'accepted').glob('*.parquet')).unlink()
        with self.assertRaises(protocol.Failure) as e:q.capture(self.a,self.binding,local)
        self.assertEqual(e.exception.code,'EVIDENCE_CHANGED')
    def test_bronze_mutation_after_capture_is_changed(self):
        _,m,baseline,_=self.prepare();key=('local',self.a['identity']['bronzePayloadKey']);self.store.objects[key]+=b' '
        with self.assertRaises(protocol.Failure) as e:q.bronze_read(self.a,None,m,self.store,baseline)
        self.assertEqual(e.exception.code,'BRONZE_CHANGED')
    def test_exact_original_timestamp(self):
        from datetime import datetime,timezone
        for dataset in ('accepted','rejected'):self.rewrite(dataset,lambda rows:[dict(r,ingested_at=datetime(2026,10,12,14,tzinfo=timezone.utc)) for r in rows])
        self.failure('PROVENANCE_MISMATCH')
    def test_extra_parquet_nonempty_rejects_count(self):
        source=next(p for p in (self.marker().parent/'accepted').glob('*.parquet') if self.spark.read.parquet(str(p)).count())
        shutil.copyfile(source,source.with_name('extra.parquet'));self.failure('COUNT_MISMATCH')
    def test_fixed_machine_protocol_stops_spark_and_rechecks(self):
        scratch=self.base/'scratch';scratch.mkdir(mode=0o700);s=scratch.stat()
        java=Path(os.environ['JAVA_HOME']).resolve()
        with serve(self.store) as c:
            request=dict(protocolVersion=2,operationId=str(uuid.uuid4()),selectionSha256=sha256(canonical_bytes(self.a)).hexdigest(),mode='VERIFY',binding=self.binding,artifact=self.a,expectedCapture=None,bronze=c,
                         runtime=dict(javaHome=str(java),scratchRoot=str(scratch),scratchDevice=str(s.st_dev),scratchInode=str(s.st_ino)))
            env=dict(PATH='/usr/bin:/bin',LANG='C.UTF-8',TZ='UTC')
            command=[sys.executable,'-I','-B','-X','utf8',str(Path(protocol.__file__).resolve())]
            result=subprocess.run(command,input=canonical_bytes(request),capture_output=True,env=env,timeout=120)
            self.assertEqual(result.returncode,0,result.stdout.decode()+result.stderr.decode()[-3000:]);n=json.loads(result.stdout)
            self.assertEqual(n['evidence']['native']['sparkShutdown'],'CONFIRMED');self.assertNotIn(b'https://example.invalid',result.stdout)
            request.update(mode='RECHECK',expectedCapture=n['evidence']['capture'])
            recheck=subprocess.run(command,input=canonical_bytes(request),capture_output=True,env=env,timeout=30)
            self.assertEqual(recheck.returncode,0,recheck.stdout);self.assertEqual(json.loads(recheck.stdout)['status'],'UNCHANGED');self.assertEqual(self.store.writes,[])


class SilverInventoryVectorTest(unittest.TestCase):
    """Literal metadata vectors, not fabricated native acceptance fixtures."""
    def vector(self):
        key='bronze/rss/source_id=ZGVtbw/ingestion_date=2026-10-12/ingestion_id=50000000-0000-4000-8000-000000000001/metadata.json'
        address='v1/25bf8e1a2393f1108d37029b3df5593236c755742ec93465bbafa9b290bddcf6/'+key[len('bronze/rss/'):].replace('metadata.json','complete.json')
        a=dict(productId='content-intelligence',contractVersion=1,processorRevision=1,relativeAddress=address,identity=dict(sourceId='demo',ingestionId='50000000-0000-4000-8000-000000000001'))
        local=dict(files=[dict(path='complete.json',dataset='bookkeeping',byteLength=2,sha256='44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a'),dict(path='accepted/a.parquet',dataset='accepted',byteLength=1,sha256='a'*64),dict(path='rejected/r.parquet',dataset='rejected',byteLength=2,sha256='b'*64)])
        bronze=dict(bucket='local',metadataKey=key,metadataByteLength=2,metadataSha256='c'*64,payloadKey=key.replace('metadata.json','payload.xml'),payloadByteLength=3,payloadSha256='d'*64)
        return a,local,bronze
    def test_literal_base_vector(self):
        a,local,bronze=self.vector()
        value,digest=q.identity(a,local,bronze)
        self.assertEqual(digest,'e7d22d80f101997d6d9e010f09dd3d13b92630688c6f6ccf8dfa9fd61123f411')
        local['files'].reverse();self.assertEqual(q.identity(a,local,bronze),(value,digest))
        a['operationId']=str(uuid.uuid4());a['absoluteRoot']='/not/part/of/identity'
        self.assertEqual(q.identity(a,local,bronze),(value,digest))
    def test_literal_bookkeeping_vector(self):
        a,local,bronze=self.vector()
        local['files'].append(dict(path='accepted/_SUCCESS',dataset='bookkeeping',byteLength=0,sha256='e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'))
        self.assertEqual(q.identity(a,local,bronze)[1],'4f17688a978d6e5ad6dee7463797a7a3d99696e80e751a22a858de05a92f8f32')


if __name__=='__main__':unittest.main()
