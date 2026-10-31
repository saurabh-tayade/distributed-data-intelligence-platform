"""D5b-1 fixed read-only dispatcher. Never calls a native publisher or discovers inputs."""
from hashlib import sha256
import os
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).absolute().parent))
import artifact_import_verifier as p
from publication_metadata import canonical_bytes


def namespace(c, kind):
    """Positive absent/incomplete observations; I/O and unsafe paths never imply absence."""
    if kind == 'CONTENT':
        root=Path(c['content']['root']);relative='v1/processor_revision=1/publication='+c['prepared']['fingerprint']
    elif kind == 'GOLD':
        root=Path(c['gold']['root']);relative='v1/processor_revision=1/input_set='+c['prepared']['inputSetFingerprint']
    else:
        root=Path(c['binding']['root']);relative=str(Path(c['artifact']['relativeAddress']).parent)
    p.info(root, True)
    current=root
    for segment in Path(relative).parts:
        p.need(segment not in ('.','..') and '/' not in segment,'IDENTITY_MISMATCH')
        current/=segment
        try:p.info(current, True)
        except p.Failure:
            # Only exact ENOENT is evidence of absence; p.info also rejects aliases and I/O.
            try:current.lstat()
            except FileNotFoundError:return 'PUBLICATION_ABSENT'
            raise
    try:(current/'complete.json').lstat()
    except FileNotFoundError:return 'INCOMPLETE_OUTPUT'
    return None


def execute(request):
    p.fields(request,('kind','original'))
    kind=request['kind'];c=dict(request['original'])
    p.need(kind in ('SILVER','GOLD','CONTENT'),'ADAPTER_INVALID')
    # Modes are selected here, never supplied as an executable operation.
    prepare=kind in ('GOLD','CONTENT') and c['prepared'] is None
    c['mode']='PREPARE' if prepare else 'VERIFY'
    if kind in ('GOLD','CONTENT'):c['verified']=None
    # Qualify dependencies even when the intended namespace is positively absent.
    if kind=='CONTENT':
        import managed_content
        managed_content.dependencies(c)
    if not prepare:
        missing=namespace(c,kind)
        if missing is not None:
            if kind=='GOLD':
                import managed_gold
                prepared=managed_gold.execute(dict(c,mode='PREPARE',prepared=None,verified=None))['prepared']
                p.need(prepared==c['prepared'],'INPUT_CHANGED')
            return dict(recoveryOutcome=missing)
    if kind=='CONTENT':
        import managed_content as native
    elif kind=='GOLD':
        import managed_gold as native
    else:
        import managed_silver as native
    return native.execute(c)


def safe_code(error):
    code=getattr(error,'code',None)
    return code if code in {'INPUT_CHANGED','EVIDENCE_CHANGED','SNAPSHOT_CHANGED','PUBLICATION_CHANGED','PUBLICATION_INVALID','SNAPSHOT_INVALID','INPUT_INVALID','BRONZE_CHANGED','BRONZE_UNAVAILABLE','PUBLICATION_UNAVAILABLE','INPUT_UNAVAILABLE','RESOURCE_LIMIT','RUNTIME_INCOMPATIBLE'} else 'EVIDENCE_UNAVAILABLE'


def main():
    import json
    import silver_import_qualification as q
    os.umask(0o077);path=Path(sys.argv[1]);raw=path.read_bytes();p.need(len(raw)<=16*1024**2,'RESOURCE_LIMIT')
    def pairs(items):
        result={}
        for k,v in items:p.need(k not in result);result[k]=v
        return result
    request=json.loads(raw,object_pairs_hook=pairs);p.need(canonical_bytes(request)==raw)
    try:
        with q.quiet_stdout():value=execute(request)
        data=canonical_bytes(value);p.need(len(data)<=16*1024**2,'RESOURCE_LIMIT')
        with path.with_name('evidence.json').open('xb') as f:f.write(data)
        result=dict(commandSha256=sha256(raw).hexdigest(),evidenceSha256=sha256(data).hexdigest(),status='VERIFIED',cleanup='CONFIRMED',code=None);status=0
    except Exception as error:
        # Failure is always unresolved. Native codes do not alone prove corruption or mutation.
        result=dict(commandSha256=sha256(raw).hexdigest(),evidenceSha256=None,status='FAILED',cleanup='UNCONFIRMED' if getattr(error,'code',None)=='SPARK_CLEANUP_UNCERTAIN' else 'CONFIRMED',code=safe_code(error));status=1
    with path.with_name('result.json').open('xb') as f:f.write(canonical_bytes(result))
    return status

if __name__=='__main__':raise SystemExit(main())
