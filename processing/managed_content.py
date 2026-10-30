"""Internal D5a fixed adapter. Native Content publication and independent read-only qualification."""
from hashlib import sha256
import os
from pathlib import Path
import sys
import tempfile
sys.path.insert(0, str(Path(__file__).absolute().parent))
import artifact_import_verifier as p
import managed_gold as d4
import content_metadata as m
from publication_metadata import canonical_bytes

LIMIT = d4.LIMIT
recheck_gold = d4.execute


def dependencies(c):
    # D4's retained receipt/Silver/Gold evidence is rechecked, never rediscovered or processed.
    gold=c['goldProof'];command=dict(gold['command'],mode='RECHECK',prepared=gold['artifact']['verification']['prepared'],verified=gold['artifact']['verification'])
    recheck_gold(command)
    ref=c['reference'];a=ref['qualification']['selection'];v=ref['qualification']['verification']
    p.need(a['kind']=='PUBLISHER_REFERENCE' and a['storageBindingId']==c['referenceBindingId'],'IDENTITY_MISMATCH')
    p.capture(a,c['referenceBinding'],v['capture'])
    p.need(p.qualify(a,c['referenceBinding'])==v['native'],'INPUT_CHANGED')
    from content_dependencies import verify_dependencies
    identity=a['identity']
    args=dict(gold_root=command['gold']['root'],gold_fingerprint=gold['artifact']['nativeFingerprint'],reference_root=c['referenceBinding']['root'],reference_id=identity['referenceId'],revision=identity['revision'],reference_fingerprint=a['fingerprint'])
    dep=verify_dependencies(**args,output_root=c['content']['root'])
    prepared=dict(manifest=p.thaw(dep.manifest()),fingerprint=dep.fingerprint)
    if c['prepared'] is not None:p.need(prepared==c['prepared'],'INPUT_CHANGED')
    dep.check()
    return args,prepared


def capture(c):
    binding=c['content'];root=Path(binding['root']);s=p.info(root,True)
    p.need(root==root.resolve(strict=True) and [str(s.st_dev),str(s.st_ino)]==[binding['rootDevice'],binding['rootInode']],'PUBLICATION_CHANGED')
    directory=m.publication_directory(root,c['prepared']['fingerprint'])
    e=m.capture(directory,m.TABLES,{'inputs.json','gold-inputs.json','gold-complete.json','complete.json',*m.TABLES},'PUBLICATION_INVALID','PUBLICATION_CHANGED',guards=m.namespace_guards(root,directory))
    value=dict(files=[dict(path=f.path.relative_to(directory).as_posix(),byteLength=f.length,sha256=f.digest,stamp=[str(v) for v in f.stamp]) for f in e.files],directories=[dict(path=path.relative_to(directory).as_posix(),identity=[str(v) for v in value[0]],names=list(value[1])) for path,value in e.directories],guards=[dict(path=path.relative_to(root).as_posix(),identity=[str(v) for v in value]) for path,value in e.guards])
    return e,value


def verify(c,args):
    import content_analytics
    e,inventory=capture(c);fp=c['prepared']['fingerprint']
    p.need(e.raw('inputs.json')==canonical_bytes(c['prepared']['manifest']),'PUBLICATION_CHANGED')
    smoke=[];coverage=None
    with content_analytics.open_publication(c['content']['root'],fp,gold_root=args['gold_root'],reference_root=args['reference_root']) as opened:
        for name in ('catalog','reference_coverage','source_discovery_daily'):
            report=opened.report(name,limit=1)
            smoke.append(dict(name=name,sha256=sha256(canonical_bytes(report)).hexdigest(),rows=len(report['rows'])))
            if name=='reference_coverage':coverage=report['totals']
    marker=p.thaw(m.validate_completion(e.raw('complete.json'),fp,len(e.raw('inputs.json'))))
    e.check();p.need(capture(c)[1]==inventory,'PUBLICATION_CHANGED')
    value=dict(status='VERIFIED',prepared=c['prepared'],completion=marker,completionSha256=sha256(e.raw('complete.json')).hexdigest(),capture=inventory,coverage=coverage,smoke=smoke,verifier='content_analytics.open_publication')
    if c['verified'] is not None:p.need(value==c['verified'],'PUBLICATION_CHANGED')
    return value


def publish(c,args,prepared):
    from pyspark import SparkContext
    from silver import session
    import content_publication
    runtime=c['runtime'];scratch=Path(runtime['scratchRoot']);s=p.info(scratch,True)
    p.need([str(s.st_dev),str(s.st_ino)]==[runtime['scratchDevice'],runtime['scratchInode']],'EVIDENCE_CHANGED')
    private=Path(tempfile.mkdtemp(prefix='managed-content-',dir=scratch));tempfile.tempdir=str(private)
    os.environ.update(JAVA_HOME=runtime['javaHome'],PYSPARK_PYTHON=sys.executable,SPARK_LOCAL_IP='127.0.0.1',SPARK_LOCAL_DIRS=str(private),TMPDIR=str(private),SPARK_SUBMIT_OPTS='-Djava.io.tmpdir='+str(private))
    spark=gateway=None
    try:
        spark=session();gateway=SparkContext._gateway
        p.need(spark.version=='4.0.2' and spark._jvm.java.lang.System.getProperty('java.specification.version')=='21','RUNTIME_INCOMPATIBLE')
        spark.sparkContext.setLogLevel('ERROR')
        result=content_publication.publish(spark,output_root=c['content']['root'],**args)
        p.need(result['product_fingerprint']==prepared['fingerprint'],'IDENTITY_MISMATCH')
        return dict(status=result['status'],prepared=prepared)
    finally:
        try:
            if spark is not None:spark.stop()
            gateway=gateway or SparkContext._gateway
            p.need(gateway is not None and gateway.proc is not None,'SPARK_CLEANUP_UNCERTAIN')
            gateway.shutdown();gateway.proc.stdin.close();p.need(gateway.proc.wait(timeout=5)==0,'SPARK_CLEANUP_UNCERTAIN')
        except Exception:raise p.Failure('SPARK_CLEANUP_UNCERTAIN') from None


def execute(c):
    import pyspark,boto3,botocore,duckdb
    p.need(sys.version_info[:3]==(3,12,15) and pyspark.__version__=='4.0.2' and boto3.__version__==botocore.__version__=='1.43.114' and duckdb.__version__=='1.4.5','RUNTIME_INCOMPATIBLE')
    p.fields(c,('protocolVersion','mode','runId','attemptId','ownerEpoch','checkpointId','runtimeSha256','inputSha256','goldProof','reference','referenceBindingId','referenceBinding','content','runtime','prepared','verified'))
    p.need(type(c['protocolVersion']) is int and c['protocolVersion']==1 and c['mode'] in ('PREPARE','PUBLISH','VERIFY','RECHECK'),'ADAPTER_INVALID')
    args,prepared=dependencies(c)
    if c['mode']=='PREPARE':value=dict(status='PREPARED',prepared=prepared)
    elif c['mode']=='PUBLISH':value=publish(c,args,prepared)
    else:value=verify(c,args)
    dependencies(c)
    return value


def main():
    # Keep D4's pinned module bytes intact: its committed proofs require that original hash.
    # Use the established private envelope, with a fixed Content dispatcher and no submitted command.
    import json
    import silver_import_qualification as q
    os.umask(0o077);path=Path(sys.argv[1]);raw=path.read_bytes();p.need(len(raw)<=LIMIT,'RESOURCE_LIMIT')
    def pairs(items):
        result={}
        for key,value in items:p.need(key not in result);result[key]=value
        return result
    c=json.loads(raw,object_pairs_hook=pairs);p.need(canonical_bytes(c)==raw)
    try:
        with q.quiet_stdout():value=execute(c)
        data=canonical_bytes(value);p.need(len(data)<=LIMIT,'RESOURCE_LIMIT')
        with path.with_name('evidence.json').open('xb') as f:f.write(data)
        result=dict(commandSha256=sha256(raw).hexdigest(),evidenceSha256=sha256(data).hexdigest(),status=value['status'],cleanup='CONFIRMED',code=None);status=0
    except Exception as error:
        from bronze_to_silver import Failure
        code=getattr(error,'code','PROCESSING_FAILED')
        if isinstance(error,Failure):code='BRONZE_CHANGED' if str(error)=='BRONZE_RECEIPT_CHANGED' else 'BRONZE_UNAVAILABLE'
        allowed={'INPUT_CHANGED','IDENTITY_MISMATCH','RESOURCE_LIMIT','RUNTIME_INCOMPATIBLE','SPARK_CLEANUP_UNCERTAIN','INCOMPLETE_OUTPUT','PUBLICATION_CHANGED','BRONZE_CHANGED','BRONZE_UNAVAILABLE'}
        # Only reviewed native codes are retained privately; arbitrary messages/stacks never are.
        allowed |= p.CODES | set(m.MESSAGES)
        if code not in allowed:code='INPUT_INVALID'
        result=dict(commandSha256=sha256(raw).hexdigest(),evidenceSha256=None,status='FAILED',cleanup='UNCONFIRMED' if code=='SPARK_CLEANUP_UNCERTAIN' else 'CONFIRMED',code=code);status=1
    with path.with_name('result.json').open('xb') as f:f.write(canonical_bytes(result))
    return status


if __name__=='__main__':raise SystemExit(main())
