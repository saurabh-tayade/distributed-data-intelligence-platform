"""Internal D4 command adapter. Exact D3 dependencies, native Gold, no discovery/activation."""
from hashlib import sha256
import os
from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).absolute().parent))
import artifact_import_verifier as p
import silver_import_qualification as q
import managed_silver as d3
from publication_metadata import canonical_bytes, fingerprint

LIMIT = 16*1024**2


def selected_markers(c):
    p.need(type(c['inputs']) is list and 1 <= len(c['inputs']) <= 128,'INPUT_INVALID')
    seen=set();ids=set()
    for item in c['inputs']:
        a=item['qualification']['selection']
        p.need(item['artifactId'] not in ids and a['relativeAddress'] not in seen and a['storageBindingId']==c['silverBindingId'],'INPUT_INVALID')
        p.need(d3.selection(item['receipt'],c['silverBindingId'])==a,'IDENTITY_MISMATCH')
        seen.add(a['relativeAddress']);ids.add(item['artifactId'])
    return sorted(seen,key=lambda s:s.encode())


def silver_check(c, spark=None):
    from bronze_to_silver import local_client
    from silver_inputs import parse_marker
    markers=selected_markers(c);total=0
    for item in c['inputs']:
        a=item['qualification']['selection'];v=item['qualification']['verification'];r=item['receipt']
        local=q.capture(a,c['silver'],v['capture']['local']);total+=sum(f['byteLength'] for f in local['files'])
        p.need(total <= 1024**3,'RESOURCE_LIMIT')
        config=item['bronze'];raw=Path(config['credentialsFile']).read_bytes();p.need(len(raw)<=8192 and sha256(raw).hexdigest()==item['credentialsSha256'],'INPUT_CHANGED');credentials=p.parse(raw)
        env={'BRONZE_S3_'+k:v for k,v in dict(ENDPOINT=config['endpoint'],REGION=config['region'],BUCKET=config['bucket'],ACCESS_KEY=credentials['accessKey'],SECRET_KEY=credentials['secretKey']).items()}
        client,bucket=local_client(env)
        try:
            d3.bronze(client,r)
            marker,_,_=parse_marker(c['silver']['root'],a['relativeAddress'])
            bronze,expected=q.bronze_read(a,None,marker,client,baseline=v['capture']['bronze'])
            if spark is not None:
                native=q.qualify(spark,a,c['silver'],local,marker,expected)
                p.need(native == {k:v for k,v in v['native'].items() if k != 'sparkShutdown'},'INPUT_CHANGED')
            q.capture(a,c['silver'],local);d3.bronze(client,r)
        finally:client.close()
    return markers


def gold_capture(c, identity):
    import content_metadata as m
    root=Path(c['gold']['root']);s=p.info(root,True)
    p.need(root == root.resolve(strict=True) and [str(s.st_dev),str(s.st_ino)] == [c['gold']['rootDevice'],c['gold']['rootInode']],'EVIDENCE_CHANGED')
    directory=root/'v1'/'processor_revision=1'/('input_set='+identity)
    e=m.capture(directory,('gold_articles','gold_source_daily'),{'inputs.json','complete.json','gold_articles','gold_source_daily'},'PUBLICATION_INVALID','PUBLICATION_CHANGED',guards=m.namespace_guards(root,directory))
    value=dict(files=[dict(path=f.path.relative_to(directory).as_posix(),byteLength=f.length,sha256=f.digest,stamp=[str(v) for v in f.stamp]) for f in e.files],
               directories=[dict(path=path.relative_to(directory).as_posix(),identity=[str(v) for v in value[0]],names=list(value[1])) for path,value in e.directories],
               guards=[dict(path=path.relative_to(root).as_posix(),identity=[str(v) for v in value]) for path,value in e.guards])
    return e,value


def spark_action(c):
    from pyspark import SparkContext
    from silver import session
    from silver_inputs import load_inputs
    import gold_publication as gold
    runtime=c['runtime'];scratch=Path(runtime['scratchRoot']);s=p.info(scratch,True)
    p.need([str(s.st_dev),str(s.st_ino)] == [runtime['scratchDevice'],runtime['scratchInode']],'EVIDENCE_CHANGED')
    private=Path(tempfile.mkdtemp(prefix='managed-gold-',dir=scratch));tempfile.tempdir=str(private)
    os.environ.update(JAVA_HOME=runtime['javaHome'],PYSPARK_PYTHON=sys.executable,SPARK_LOCAL_IP='127.0.0.1',SPARK_LOCAL_DIRS=str(private),TMPDIR=str(private),SPARK_SUBMIT_OPTS='-Djava.io.tmpdir='+str(private))
    spark=gateway=None
    try:
        spark=session();gateway=SparkContext._gateway
        p.need(spark.version == '4.0.2' and spark._jvm.java.lang.System.getProperty('java.specification.version') == '21','RUNTIME_INCOMPATIBLE')
        spark.sparkContext.setLogLevel('ERROR')
        markers=silver_check(c,spark if c['mode']=='PREPARE' else None)
        inputs=load_inputs(spark,c['silver']['root'],markers=markers)
        prepared=dict(manifest=inputs.manifest,inputSetFingerprint=inputs.input_set_fingerprint,counts=inputs.counts)
        if c['mode'] != 'PREPARE':p.need(prepared == c['prepared'],'INPUT_CHANGED')
        if c['mode']=='PUBLISH':
            # Native publish verifies exact existing publications without rewriting them.
            result=gold.publish(inputs,c['gold']['root'])
            p.need(result['input_set_fingerprint'] == prepared['inputSetFingerprint'],'IDENTITY_MISMATCH')
            value=dict(status=result['status'],prepared=prepared)
        elif c['mode']=='VERIFY':
            before,capture=gold_capture(c,prepared['inputSetFingerprint'])
            marker=gold.verify_publication(inputs,c['gold']['root'])
            import gold_analytics
            with gold_analytics.open_publication(c['gold']['root'],prepared['inputSetFingerprint'],memory_limit='512MiB') as opened:
                # Full opener checks schemas, keys, values and article/daily SQL accounting.
                opened.report('quality')
            before.check();p.need(gold_capture(c,prepared['inputSetFingerprint'])[1] == capture,'PUBLICATION_CHANGED')
            value=dict(status='VERIFIED',prepared=prepared,completion=marker,capture=capture,
                       completionSha256=sha256(before.raw('complete.json')).hexdigest(),verifiers=['gold_publication.verify_publication','gold_analytics.open_publication'])
        else:value=dict(status='PREPARED',prepared=prepared)
        inputs.verify_unchanged();silver_check(c)
        return value
    finally:
        try:
            if spark is not None:spark.stop()
            gateway=gateway or SparkContext._gateway
            p.need(gateway is not None and gateway.proc is not None,'SPARK_CLEANUP_UNCERTAIN')
            gateway.shutdown();gateway.proc.stdin.close();p.need(gateway.proc.wait(timeout=5)==0,'SPARK_CLEANUP_UNCERTAIN')
        except Exception:raise p.Failure('SPARK_CLEANUP_UNCERTAIN') from None


def execute(c):
    import pyspark,boto3,botocore,duckdb
    p.need(sys.version_info[:3] == (3,12,15) and pyspark.__version__ == '4.0.2' and boto3.__version__ == botocore.__version__ == '1.43.114' and duckdb.__version__ == '1.4.5','RUNTIME_INCOMPATIBLE')
    p.fields(c,('protocolVersion','mode','runId','attemptId','ownerEpoch','checkpointId','runtimeSha256','inputSha256','silverBindingId','inputs','silver','gold','runtime','prepared','verified'))
    p.need(type(c['protocolVersion']) is int and c['protocolVersion']==1 and c['mode'] in ('PREPARE','PUBLISH','VERIFY','RECHECK'),'ADAPTER_INVALID')
    if c['mode']=='RECHECK':
        silver_check(c);old=c['verified'];e,capture=gold_capture(c,c['prepared']['inputSetFingerprint'])
        p.need(capture == old['capture'],'PUBLICATION_CHANGED')
        from gold_reader import verify_metadata
        meta=verify_metadata(c['gold']['root'],c['prepared']['inputSetFingerprint'])
        p.need(meta.marker == old['completion'] and meta.manifest_bytes == canonical_bytes(c['prepared']['manifest']),'PUBLICATION_CHANGED')
        e.check();return dict(status='UNCHANGED')
    return spark_action(c)


def main():
    os.umask(0o077);path=Path(sys.argv[1]);raw=path.read_bytes();p.need(len(raw)<=LIMIT,'RESOURCE_LIMIT')
    # Same strict duplicate-key/canonical policy as the existing adapter, with the E5 large evidence bound.
    import json
    def pairs(items):
        d={}
        for k,v in items:p.need(k not in d);d[k]=v
        return d
    c=json.loads(raw,object_pairs_hook=pairs);p.need(canonical_bytes(c)==raw)
    try:
        with q.quiet_stdout():value=execute(c)
        data=canonical_bytes(value);p.need(len(data)<=LIMIT,'RESOURCE_LIMIT')
        with path.with_name('evidence.json').open('xb') as f:f.write(data)
        result=dict(commandSha256=sha256(raw).hexdigest(),evidenceSha256=sha256(data).hexdigest(),status=value['status'],cleanup='CONFIRMED',code=None)
        status=0
    except Exception as error:
        from bronze_to_silver import Failure
        code=getattr(error,'code','PROCESSING_FAILED')
        if isinstance(error,Failure):code='BRONZE_CHANGED' if str(error)=='BRONZE_RECEIPT_CHANGED' else 'BRONZE_UNAVAILABLE'
        allowed={'INPUT_CHANGED','IDENTITY_MISMATCH','RESOURCE_LIMIT','RUNTIME_INCOMPATIBLE','SPARK_CLEANUP_UNCERTAIN','INCOMPLETE_OUTPUT','PUBLICATION_CHANGED','BRONZE_CHANGED','BRONZE_UNAVAILABLE'}
        if code not in allowed:code='INPUT_INVALID'
        result=dict(commandSha256=sha256(raw).hexdigest(),evidenceSha256=None,status='FAILED',cleanup='UNCONFIRMED' if code=='SPARK_CLEANUP_UNCERTAIN' else 'CONFIRMED',code=code);status=1
    with path.with_name('result.json').open('xb') as f:f.write(canonical_bytes(result))
    return status


if __name__=='__main__':raise SystemExit(main())
