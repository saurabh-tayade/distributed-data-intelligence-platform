"""Internal D3 adapter: one exact D2 receipt, native M1 publisher, read-only qualification.

No discovery, fetch, retry, repair, Gold, or production dispatch entry point.
"""
from hashlib import sha256
import os
from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).absolute().parent))
import artifact_import_verifier as p
from publication_metadata import canonical_bytes
import silver_import_qualification as q


def selection(receipt, binding):
    r = receipt
    p.fields(r, ('receiptVersion','runId','attemptId','sourceId','revision','capturedConfigSha256',
                 'ingestionId','ingestedAt','payloadByteLength','payloadSha256','journalSha256',
                 'storageBindingId','storageBindingSha256','bucket','expected','payloadContentType','metadataContentType'))
    p.need(type(r['receiptVersion']) is int and r['receiptVersion'] == 1)
    e = r['expected']
    address = ('v1/' + sha256(r['bucket'].encode()).hexdigest() + '/' +
               e['metadataKey'][len('bronze/rss/'):].removesuffix('metadata.json') + 'complete.json')
    a = dict(kind='SILVER', productId='content-intelligence', storageBindingId=binding,
             relativeAddress=address, contractVersion=1, processorRevision=1,
             identity=dict(sourceId=r['sourceId'], ingestionId=r['ingestionId'],
                           bronzeStorageBindingId=r['storageBindingId'], bronzeBucket=r['bucket'],
                           bronzeMetadataKey=e['metadataKey'], bronzePayloadKey=e['payloadKey']))
    q.selection(a)
    return a


def bronze(client, receipt):
    """GET only these two keys and compare original evidence before native processing."""
    from bronze_to_silver import read_object, metadata, METADATA_LIMIT, MAX_BYTES, Failure
    r = receipt; e = r['expected']
    class ReceiptObjects:
        def get_object(self, Bucket, Key):
            response = client.get_object(Bucket=Bucket,Key=Key)
            expected_type,expected_length = ((r['metadataContentType'],e['metadataByteLength']) if Key == e['metadataKey']
                                             else (r['payloadContentType'],r['payloadByteLength']))
            if (response.get('ContentType') != expected_type or response.get('ContentLength') != expected_length or
                    response.get('ContentEncoding') not in (None,'')):
                response['Body'].close()
                raise Failure('BRONZE_RECEIPT_CHANGED')
            return response
    raw = read_object(ReceiptObjects(), r['bucket'], e['metadataKey'], METADATA_LIMIT)
    payload = read_object(ReceiptObjects(), r['bucket'], e['payloadKey'], MAX_BYTES)
    m, _ = metadata(raw, e['metadataKey'])
    p.need(len(raw) == e['metadataByteLength'] and sha256(raw).hexdigest() == e['metadataSha256'] and
           len(payload) == r['payloadByteLength'] and sha256(payload).hexdigest() == r['payloadSha256'] and
           m['sourceId'] == r['sourceId'] and m['ingestionId'] == r['ingestionId'] and
           m['ingestedAt'] == r['ingestedAt'] and m['payloadKey'] == e['payloadKey'] and
           m['payloadSha256'] == r['payloadSha256'] and m['payloadByteLength'] == r['payloadByteLength'], 'BRONZE_CHANGED')
    # process_one receives verified bytes, eliminating a second-read substitution window.
    class Exact:
        def get_object(self, Bucket, Key):
            import io
            p.need(Bucket == r['bucket'] and Key in (e['metadataKey'], e['payloadKey']))
            return dict(Body=io.BytesIO(raw if Key == e['metadataKey'] else payload))
    return Exact()


def publish(client, receipt, a, binding, runtime):
    from bronze_to_silver import process_one
    from pyspark import SparkContext
    from silver import session
    exact = bronze(client, receipt)
    root = Path(binding['root']); s = p.info(root, True)
    p.need(root == root.resolve(strict=True) and [str(s.st_dev),str(s.st_ino)] == [binding['rootDevice'],binding['rootInode']], 'EVIDENCE_CHANGED')
    directory = (root/a['relativeAddress']).parent
    current = root
    for part in directory.relative_to(root).parts:
        current /= part
        if os.path.lexists(current): p.info(current, True)
    if directory.exists():
        # Existing namespaces are only read by the independent qualifier. Never resume a writer.
        q.capture(a, binding)
        return dict(status='EXISTING', cleanup=None)
    scratch = Path(runtime['scratchRoot']); s = p.info(scratch, True)
    p.need([str(s.st_dev),str(s.st_ino)] == [runtime['scratchDevice'],runtime['scratchInode']], 'EVIDENCE_CHANGED')
    private = Path(tempfile.mkdtemp(prefix='managed-silver-', dir=scratch))
    os.environ.update(JAVA_HOME=runtime['javaHome'], PYSPARK_PYTHON=sys.executable, SPARK_LOCAL_IP='127.0.0.1',
                      SPARK_LOCAL_DIRS=str(private), TMPDIR=str(private), SPARK_SUBMIT_OPTS='-Djava.io.tmpdir='+str(private))
    tempfile.tempdir = str(private)
    spark = gateway = None
    try:
        p.need(SparkContext._active_spark_context is None, 'RUNTIME_INCOMPATIBLE')
        spark = session(); gateway = SparkContext._gateway
        p.need(spark.version == '4.0.2' and spark._jvm.java.lang.System.getProperty('java.specification.version') == '21', 'RUNTIME_INCOMPATIBLE')
        spark.sparkContext.setLogLevel('ERROR')
        result = process_one(spark, exact, receipt['bucket'], receipt['expected']['metadataKey'], root)
        p.need(result['status'] == 'PROCESSED')
    finally:
        try:
            if spark is not None: spark.stop()
            gateway = gateway or SparkContext._gateway
            p.need(gateway is not None and gateway.proc is not None, 'SPARK_CLEANUP_UNCERTAIN')
            pid = str(gateway.proc.pid)
            gateway.shutdown(); gateway.proc.stdin.close()
            p.need(gateway.proc.wait(timeout=5) == 0, 'SPARK_CLEANUP_UNCERTAIN')
        except Exception: raise p.Failure('SPARK_CLEANUP_UNCERTAIN') from None
    bronze(client, receipt)
    return dict(status='PROCESSED', cleanup=dict(kind='PYSPARK_GATEWAY_REAPED',pid=pid))


def execute(c):
    import boto3, botocore, pyspark
    p.need(sys.version_info[:3] == (3,12,15) and pyspark.__version__ == '4.0.2' and
           boto3.__version__ == botocore.__version__ == '1.43.114', 'RUNTIME_INCOMPATIBLE')
    from bronze_to_silver import local_client
    p.fields(c, ('protocolVersion','mode','runId','attemptId','checkpointId','ownerEpoch','runtimeSha256','receipt','artifact','binding','bronze','runtime'))
    p.need(c['protocolVersion'] == 1 and c['mode'] in ('PUBLISH','VERIFY','RECHECK'))
    r = c['receipt']; a = selection(r,c['artifact']['storageBindingId'])
    p.need(a == c['artifact'] and r['runId'] == c['runId'] and r['attemptId'] == c['attemptId'])
    config = c['bronze']; credentials = p.parse(Path(config['credentialsFile']).read_bytes())
    env = {'BRONZE_S3_'+k:v for k,v in dict(ENDPOINT=config['endpoint'],REGION=config['region'],BUCKET=config['bucket'],
           ACCESS_KEY=credentials['accessKey'],SECRET_KEY=credentials['secretKey']).items()}
    client,bucket = local_client(env)
    try:
        p.need(bucket == r['bucket'])
        bronze(client,r)
        if c['mode'] == 'PUBLISH': return publish(client,r,a,c['binding'],c['runtime'])
        bronze_config = dict(endpoint=config['endpoint'],region=config['region'],bucket=bucket,**credentials)
        request = dict(artifact=a,binding=c['binding'],bronze=bronze_config,runtime=c['runtime'],mode='VERIFY',expectedCapture=None)
        if c['mode'] == 'RECHECK':
            # Bounded hash/inventory recheck after independent Spark qualification, before DB commit.
            request.update(mode='RECHECK',expectedCapture=p.parse(Path(sys.argv[1]).with_name('evidence.json').read_bytes())['capture'])
        status,evidence = q.execute(request)
        bronze(client,r)
        return dict(status=status, evidence=evidence)
    finally: client.close()


def main():
    os.umask(0o077)
    path = Path(sys.argv[1]); raw = path.read_bytes(); c = p.parse(raw)
    try:
        with q.quiet_stdout(): result = execute(c)
        if c['mode'] == 'VERIFY':
            with path.with_name('evidence.json').open('xb') as stream: stream.write(canonical_bytes(result['evidence']))
            # Retain native dependency selection/fingerprint without altering import evidence or products.
            from silver_inputs import discover, inventory
            from publication_metadata import fingerprint
            manifest = inventory(discover(c['binding']['root'],markers=[c['artifact']['relativeAddress']]))
            with path.with_name('selection.json').open('xb') as stream:
                stream.write(canonical_bytes(dict(manifest=manifest,inputSetFingerprint=fingerprint(manifest))))
        response = dict(protocolVersion=1,commandSha256=sha256(raw).hexdigest(),status=result['status'],cleanup=result.get('cleanup'),code=None)
        with path.with_name('result.json').open('xb') as stream: stream.write(canonical_bytes(response))
        return 0
    except Exception as error:
        from bronze_to_silver import Failure as BronzeFailure
        # Only fixed native classifications cross the process boundary, never exception messages.
        code = getattr(error,'code',None)
        if isinstance(error,BronzeFailure):
            code = 'BRONZE_UNAVAILABLE' if str(error) == 'OBJECT_READ_FAILED' else 'BRONZE_CHANGED' if str(error) == 'BRONZE_RECEIPT_CHANGED' else 'BRONZE_INVALID'
        if code not in p.CODES | q.CODES: code = 'PROCESSING_FAILED'
        response = dict(protocolVersion=1,commandSha256=sha256(raw).hexdigest(),status='FAILED',cleanup=None,code=code)
        with path.with_name('result.json').open('xb') as stream: stream.write(canonical_bytes(response))
        return 1


if __name__ == '__main__': raise SystemExit(main())
