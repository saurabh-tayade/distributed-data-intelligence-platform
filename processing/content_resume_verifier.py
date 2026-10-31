"""Explicit resume qualification only. No native publisher or HTTP retrieval can be dispatched."""
from hashlib import sha256
import json
import os
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).absolute().parent))
import artifact_import_verifier as p
import silver_import_qualification as q
from publication_metadata import canonical_bytes


def absent(binding, fingerprint):
    root=Path(binding['root']);s=p.info(root,True)
    p.need([str(s.st_dev),str(s.st_ino)]==[binding['rootDevice'],binding['rootInode']],'INPUT_CHANGED')
    current=root
    for part in ('v1','processor_revision=1','publication='+fingerprint):
        current/=part
        try:current.lstat()
        except FileNotFoundError:return
        p.info(current,True)
    raise p.Failure('RESUME_NAMESPACE_UNSAFE')


def execute(c):
    p.fields(c,('boundary','reference','referenceBinding','content','contentCommand'))
    ref=c['reference'];selection=ref['qualification']['selection'];verification=ref['qualification']['verification']
    p.capture(selection,c['referenceBinding'],verification['capture'])
    p.need(p.qualify(selection,c['referenceBinding'])==verification['native'],'INPUT_CHANGED')
    if c['boundary']=='BEFORE_FETCH':return dict(boundary='BEFORE_FETCH',reference=verification)
    p.need(c['boundary'] in ('GOLD','CONTENT_INPUTS'),'INPUT_INVALID')
    command=c['contentCommand'];gold=command['goldProof']
    import managed_gold
    value=managed_gold.execute(dict(gold['command'],mode='VERIFY',prepared=gold['artifact']['verification']['prepared'],verified=None))
    p.need(value==gold['artifact']['verification'],'INPUT_CHANGED')
    import managed_content
    prepared=managed_content.execute(dict(command,mode='PREPARE',prepared=None,verified=None))['prepared']
    if command['prepared'] is not None:p.need(prepared==command['prepared'],'INPUT_CHANGED')
    absent(c['content'],prepared['fingerprint'])
    return dict(boundary=c['boundary'],gold=value,prepared=prepared,reference=verification)


def safe_code(error):
    code=getattr(error,'code',None)
    if code=='RESUME_NAMESPACE_UNSAFE':return code
    # Only fixed native evidence/validation codes establish a mismatch. I/O remains unavailable.
    if code in {'BRONZE_CHANGED','INPUT_CHANGED','EVIDENCE_CHANGED','SNAPSHOT_CHANGED','PUBLICATION_CHANGED','INPUT_INVALID','PUBLICATION_INVALID','SNAPSHOT_INVALID','DEPENDENCY_CHANGED','DEPENDENCY_INVALID','IDENTITY_MISMATCH','PATH_UNSAFE','COUNT_MISMATCH','FILE_MISMATCH','INCOMPLETE_OUTPUT','INVALID_INVENTORY','INVALID_LAYOUT','INVALID_METADATA','INVALID_SELECTION','INVENTORY_MISMATCH','MANIFEST_MISMATCH','PUBLICATION_INCOMPLETE','OUTPUT_ROWS_MISMATCH','OUTPUT_SCHEMA_MISMATCH','OUTPUT_METADATA_MISMATCH','OUTPUT_PROVENANCE_MISMATCH','OUTPUT_INVALID_VALUES','OUTPUT_DUPLICATE_KEY','INPUT_IDENTITY_MISMATCH','OUTPUT_CHANGED','OUTPUT_MANIFEST_MISMATCH','OUTPUT_LAYOUT_INVALID','INVALID_OUTPUT_MARKER'}:return 'RESUME_EVIDENCE_INVALID'
    return 'RESUME_EVIDENCE_UNAVAILABLE'


def main():
    os.umask(0o077);path=Path(sys.argv[1]);raw=path.read_bytes();p.need(len(raw)<=16*1024**2,'RESOURCE_LIMIT')
    def pairs(items):
        result={}
        for k,v in items:p.need(k not in result);result[k]=v
        return result
    c=json.loads(raw,object_pairs_hook=pairs);p.need(canonical_bytes(c)==raw)
    try:
        with q.quiet_stdout():value=execute(c)
        data=canonical_bytes(value);p.need(len(data)<=16*1024**2,'RESOURCE_LIMIT')
        with path.with_name('evidence.json').open('xb') as f:f.write(data)
        result=dict(commandSha256=sha256(raw).hexdigest(),evidenceSha256=sha256(data).hexdigest(),status="VERIFIED",cleanup="CONFIRMED",code=None);status=0
    except Exception as error:
        result=dict(commandSha256=sha256(raw).hexdigest(),evidenceSha256=None,status="FAILED",cleanup="UNCONFIRMED" if getattr(error,'code',None)=='SPARK_CLEANUP_UNCERTAIN' else "CONFIRMED",code=safe_code(error));status=1
    with path.with_name('result.json').open('xb') as f:f.write(canonical_bytes(result))
    return status

if __name__=='__main__':raise SystemExit(main())
