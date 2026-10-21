"""Fixed E5 Reliability writer/independent reader. No queue, discovery or publication activation."""
import base64
from hashlib import sha256
import os
from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).absolute().parent))
import reliability_execution_preflight as p
from publication_metadata import canonical_bytes

POLICY = 'E5_RELIABILITY_RESULT_V1'
CODES = p.CODES | {'EVENT_IDENTITY_CONFLICT', 'PUBLICATION_FAILED', 'PUBLICATION_INVALID', 'PUBLICATION_CHANGED',
    'PUBLICATION_UNAVAILABLE', 'PUBLICATION_INCOMPLETE', 'TERMINATION_UNCONFIRMED', 'QUERY_FAILED'}


def retained(command, pack):
    p.fields(command, ('protocolVersion','runId','attemptId','ownerEpoch','stage','itemOrdinal','capturedConfigSha256',
        'checkpointId','runtime','retainedSha256','nativeFingerprint'))
    p.need(command['stage'] in ('PUBLISH_RELIABILITY','VERIFY_RESULT'))
    prior = pack['command']
    p.validate_command(prior)
    p.fields(pack, ('command','bundle','result','evidence','anchor'))
    for key in ('protocolVersion','runId','attemptId','ownerEpoch','itemOrdinal','capturedConfigSha256','runtime'):
        p.need(command[key] == prior[key])
    p.text(command['checkpointId'], r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}')
    p.need(sha256(canonical_bytes(pack)).hexdigest() == command['retainedSha256'], 'EVIDENCE_CHANGED')
    anchor = pack['anchor']
    for field, key in (('commandSha256','command'),('resultSha256','result'),('evidenceSha256','evidence')):
        p.need(anchor[field] == sha256(canonical_bytes(pack[key])).hexdigest(), 'EVIDENCE_CHANGED')
    p.need(anchor['checkpointId'] == prior['checkpointId'] and anchor['cleanup'] == 'OBSERVED_CLEAN_EXIT' and
        anchor['selectionSha256'] == command['capturedConfigSha256'] and
        prior['bundleSha256'] == sha256(canonical_bytes(pack['bundle'])).hexdigest(), 'EVIDENCE_CHANGED')
    result = pack['result']
    p.need(result['status'] == 'OK' and result['code'] is None and result['evidenceId'] == prior['checkpointId'])
    fresh = p.recheck(prior, pack['bundle'], pack['evidence'])
    p.need(fresh['nativeDependencyFingerprint'] == anchor['nativeDependencyFingerprint'] == command['nativeFingerprint'], 'DEPENDENCY_CHANGED')
    return fresh


def arguments(pack, evidence):
    roots = {k:p.root(v) for k,v in pack['bundle']['bindings'].items()}
    manifest = evidence['nativeDependencies']; ref = manifest['reference']
    return dict(batch_root=roots['batch'], input_format=evidence['inputFormat'],
        batches=[(b['source_id'],b['batch_id'],b['batch_fingerprint']) for b in manifest['batches']],
        reference_root=roots['reference'],reference_id=ref['reference_id'],revision=ref['revision'],
        reference_fingerprint=ref['snapshot_fingerprint'],staging_root=roots['staging'],output_root=roots['output'],
        execution_config=p.SPARK.copy())


def spark_publish(kwargs, scratch):
    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    import reliability_format_publication as publisher
    private = Path(tempfile.mkdtemp(prefix='reliability-',dir=scratch))
    # Approved private scratch is retained; never clean native staging/output as recovery.
    os.environ.update(PYSPARK_PYTHON=sys.executable,SPARK_LOCAL_IP='127.0.0.1',SPARK_LOCAL_DIRS=str(private),
        TMPDIR=str(private),SPARK_SUBMIT_OPTS='-Djava.io.tmpdir='+str(private))
    tempfile.tempdir = str(private)
    spark = gateway = result = failure = None
    pid = None
    try:
        p.need(SparkContext._active_spark_context is None, 'RUNTIME_MISMATCH')
        spark = (SparkSession.builder.master('local[2]').appName('ddip-e5-reliability-publication')
            .config('spark.ui.enabled','false').config('spark.default.parallelism','8')
            .config('spark.sql.shuffle.partitions','8').config('spark.driver.memory','1g')
            .config('spark.executor.memory','1g').config('spark.eventLog.enabled','false')
            .config('spark.driver.host','127.0.0.1').config('spark.driver.bindAddress','127.0.0.1')
            .config('spark.sql.warehouse.dir',(private/'warehouse').as_uri()).getOrCreate())
        gateway = SparkContext._gateway
        p.need(gateway is not None and gateway.proc is not None, 'TERMINATION_UNCONFIRMED')
        pid = str(gateway.proc.pid)
        p.need(spark._jvm.java.lang.System.getProperty('java.specification.version') == '21','RUNTIME_MISMATCH')
        spark.sparkContext.setLogLevel('ERROR')
        result = publisher.publish(spark,**kwargs)
    except Exception as error:
        failure = error
    finally:
        try:
            if spark is not None:spark.stop()
            gateway = gateway or SparkContext._gateway
            if gateway is not None:
                p.need(gateway.proc is not None,'TERMINATION_UNCONFIRMED')
                pid = str(gateway.proc.pid)
                gateway.shutdown()
                gateway.proc.stdin.close()
                p.need(gateway.proc.wait(timeout=5) == 0,'TERMINATION_UNCONFIRMED')
        except Exception:
            raise p.Failure('TERMINATION_UNCONFIRMED') from None
    return result, dict(kind='PYSPARK_GATEWAY_REAPED',pid=pid), failure


def snapshot(opened, pack):
    import reliability_format_metadata as m
    e = opened._evidence; marker = opened._marker
    e.check(); opened._deps.check()
    counts = dict(occurrences=str(marker['occurrence_counts']['physical_occurrences']),
        accepted=str(marker['occurrence_counts']['valid_occurrences']),
        rejected=str(marker['occurrence_counts']['rejected_occurrences']),
        events=str(marker['output_counts'][m.EVENTS]),windows=str(marker['output_counts'][m.METRICS]))
    # All files include native bookkeeping evidence; no raw event values or full report rows.
    files = [dict(path=f.path.relative_to(e.directory).as_posix(),byteLength=str(f.length),sha256=f.digest) for f in e.files]
    config = pack['command']['runtime']['configuration']
    return dict(qualificationVersion=1,policy=POLICY,productId=m.PRODUCT,kind='RELIABILITY_V2',contractVersion=2,
        processorRevision=2,inputFormat=opened._manifest['input_format'],nativeFingerprint=opened.product_fingerprint,
        storageBindingId=config['bindings']['output'],relativeAddress=e.directory.relative_to(opened._root).as_posix(),
        completionBase64=base64.b64encode(e.raw('complete.json')).decode('ascii'),
        inputsBase64=base64.b64encode(e.raw('inputs.json')).decode('ascii'),
        files=sorted(files,key=lambda f:f['path']),counts=counts)


def inspect(command, pack):
    """Full native independent read, also used to qualify immediate failed-writer state."""
    fresh = retained(command, pack)
    import reliability_format_metadata as m
    import reliability_format_analytics as analytics
    kwargs = arguments(pack,fresh); fp = command['nativeFingerprint']
    with analytics.open_publication(kwargs['output_root'],fp,batch_root=kwargs['batch_root'],reference_root=kwargs['reference_root']) as opened:
        p.need(opened._deps.fingerprint == fp,'DEPENDENCY_CHANGED')
        before = snapshot(opened,pack)
        first = opened._db.execute('SELECT epoch_us(window_start) FROM reliability_requests_5m_v1 ORDER BY window_start LIMIT 1').fetchone()
        low = first[0] if first else 0
        window = dict(start=m.timestamp(low),end='10000-01-01T00:00:00.000000Z' if low+m.WINDOW_US==m.END_US else m.timestamp(low+m.WINDOW_US))
        for name in ('requests_5m','input_coverage','reference_coverage'):
            opened.report(name,limit=1,**(window if name=='requests_5m' else {}))
        opened._check()
        p.need(before == snapshot(opened,pack),'PUBLICATION_CHANGED')
        p.need(opened._verification_stats['ordered_query_executions'] == 1,'PUBLICATION_INVALID')
    return before


def execute(command, pack, state=None):
    fresh = retained(command, pack)
    if command['stage'] == 'VERIFY_RESULT':
        result = inspect(command,pack)
        return dict(policy=POLICY,runId=command['runId'],attemptId=command['attemptId'],checkpointId=command['checkpointId'],
            nativeFingerprint=command['nativeFingerprint'],cleanup=dict(kind='READ_ONLY'),publication=result)
    import reliability_format_metadata as m
    import reliability_format_publication as publisher
    kwargs = arguments(pack,fresh)
    if state is not None:state['writerAuthorized']=True
    directory = m.publication_directory(kwargs['output_root'],command['nativeFingerprint'])
    # Completed replay is native read-only; never start a gateway to rewrite an existing namespace.
    cleanup = dict(kind='READ_ONLY_REPLAY')
    result = None
    failure = None
    try:
        try:directory.lstat()
        except FileNotFoundError:
            result, cleanup, failure = spark_publish(kwargs,p.root(pack['bundle']['bindings']['temp']))
        else:result = publisher.publish(None,**kwargs)
    except Exception as error:
        if getattr(error,'code',None)=='TERMINATION_UNCONFIRMED':raise
        failure = error
    if failure is not None:
        # Only proven quiescence + freshly reopened dependencies + exact absent/incomplete state
        # can close a failed writer. Other failures remain unresolved for explicit recovery.
        p.recheck(pack['command'],pack['bundle'],pack['evidence'])
        try:directory.lstat()
        except FileNotFoundError:state='ABSENT'
        else:
            m.root_path(directory,'PUBLICATION_UNAVAILABLE')
            try:(directory/'complete.json').lstat()
            except FileNotFoundError:state='INCOMPLETE'
            else:
                # A completed result after a lost native acknowledgment is independently checked
                # again by the separate mandatory VERIFY_RESULT process.
                inspect(dict(command,stage='VERIFY_RESULT'),pack)
                failure=None
        if failure is not None:
            code=getattr(failure,'code','PUBLICATION_FAILED')
            return dict(policy=POLICY,runId=command['runId'],attemptId=command['attemptId'],checkpointId=command['checkpointId'],
                nativeFingerprint=command['nativeFingerprint'],cleanup=cleanup,outputState=state,failureCode=code if code in CODES else 'PUBLICATION_FAILED')
    marker=m.observe(directory/'complete.json',m.COMPLETION_LIMIT,'PUBLICATION_UNAVAILABLE','PUBLICATION_CHANGED',retain=True)
    m.validate_completion(marker.raw,command['nativeFingerprint'],len(canonical_bytes(fresh['nativeDependencies'])))
    return dict(policy=POLICY,runId=command['runId'],attemptId=command['attemptId'],checkpointId=command['checkpointId'],
        nativeFingerprint=command['nativeFingerprint'],cleanup=cleanup,publicationStatus=result.status if result is not None else 'recovered',
        completionBase64=base64.b64encode(marker.raw).decode('ascii'))


def main():
    command = None
    state = dict(writerAuthorized=False)
    try:
        p.need(len(sys.argv) == 2)
        path = Path(sys.argv[1]); command = p.parse(p.read(path,p.COMMAND_LIMIT,private=True),p.COMMAND_LIMIT)
        pack = p.parse(p.read(path.parent/'retained.json',p.EVIDENCE_LIMIT,private=True),p.EVIDENCE_LIMIT)
        evidence = execute(command,pack,state)
        p.write_new(path.parent/'evidence.json',canonical_bytes(evidence),p.EVIDENCE_LIMIT)
        counts = evidence['publication']['counts'] if command['stage']=='VERIFY_RESULT' else {}
        result = dict(protocolVersion=1,runId=command['runId'],attemptId=command['attemptId'],stage=command['stage'],itemOrdinal=0,
            status='FAILED' if 'failureCode' in evidence else 'OK',code=evidence.get('failureCode'),evidenceId=None if 'failureCode' in evidence else command['checkpointId'],counts=counts)
        p.write_new(path.parent/'result.json',canonical_bytes(result),p.COMMAND_LIMIT)
        return 1 if 'failureCode' in evidence else 0
    except Exception as error:
        code = getattr(error,'code','PUBLICATION_INVALID')
        if code not in CODES:code='PUBLICATION_INVALID'
        if command is not None:
            try:
                if command.get('stage')=='PUBLISH_RELIABILITY' and not state['writerAuthorized']:
                    early = dict(policy=POLICY,runId=command['runId'],attemptId=command['attemptId'],checkpointId=command['checkpointId'],nativeFingerprint=command['nativeFingerprint'],cleanup=dict(kind='READ_ONLY_FAILURE'),outputState='NOT_AUTHORIZED',failureCode=code)
                    p.write_new(path.parent/'evidence.json',canonical_bytes(early),p.EVIDENCE_LIMIT)
                result = dict(protocolVersion=1,runId=command['runId'],attemptId=command['attemptId'],stage=command['stage'],itemOrdinal=0,
                    status='FAILED',code=code,evidenceId=None,counts={})
                p.write_new(path.parent/'result.json',canonical_bytes(result),p.COMMAND_LIMIT)
            except Exception:pass
        return 1


if __name__ == '__main__':raise SystemExit(main())
