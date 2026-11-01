"""E5-only read-only qualification; native Silver readers/publishers remain unchanged."""
import base64
from contextlib import contextmanager
from hashlib import sha256
import os
from pathlib import Path
import re
import shutil
import stat
import sys
import tempfile
import unicodedata
from uuid import UUID

from publication_metadata import canonical_bytes
import artifact_import_verifier as p

POLICY = 'ddip-e5-silver-import'
MAX_FILES = 128
MAX_ENTRIES = 512
MAX_RAW = 1024**3
CODES = {'SILVER_INVALID', 'SILVER_UNAVAILABLE', 'BRONZE_INVALID', 'BRONZE_UNAVAILABLE',
         'BRONZE_CHANGED', 'SPARK_CLEANUP_UNCERTAIN', 'SCHEMA_MISMATCH', 'COUNT_MISMATCH',
         'PROVENANCE_MISMATCH', 'INVALID_ROW', 'PUBLICATION_CONFLICT', 'INGESTION_CONFLICT',
         'REPLAY_CONFLICT', 'ARTICLE_IDENTITY_CONFLICT', 'ACCEPTED_REJECTED_CONFLICT',
         'PARQUET_VALIDATION_FAILED', 'INVALID_MARKER', 'MARKER_PATH_MISMATCH',
         'INVALID_PUBLICATION_PATH', 'MISSING_DATASET', 'INVALID_DATASET', 'INPUT_READ_FAILED'}


def selection(a):
    p.fields(a, ('kind', 'productId', 'storageBindingId', 'relativeAddress', 'contractVersion', 'processorRevision', 'identity'))
    p.need(a['kind'] == 'SILVER' and a['productId'] == 'content-intelligence')
    p.need(type(a['contractVersion']) is int and a['contractVersion'] == 1 and type(a['processorRevision']) is int and a['processorRevision'] == 1)
    p.text(a['storageBindingId'], r'[A-Za-z0-9_-]{1,128}')
    i = a['identity']
    p.fields(i, ('sourceId', 'ingestionId', 'bronzeStorageBindingId', 'bronzeBucket', 'bronzeMetadataKey', 'bronzePayloadKey'))
    source = i['sourceId']
    p.need(type(source) is str and source.strip() and len(source.encode('utf-8')) <= 512 and not any(unicodedata.category(c) in ('Cc', 'Cs') for c in source))
    p.need(type(i['ingestionId']) is str and str(UUID(i['ingestionId'])) == i['ingestionId'])
    p.text(i['bronzeStorageBindingId'], r'[A-Za-z0-9_-]{1,128}')
    p.text(i['bronzeBucket'], r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]')
    encoded = base64.urlsafe_b64encode(source.encode()).decode().rstrip('=')
    pattern = r'bronze/rss/source_id=' + re.escape(encoded) + r'/ingestion_date=(\d{4}-\d{2}-\d{2})/ingestion_id=' + re.escape(i['ingestionId']) + '/metadata.json'
    match = re.fullmatch(pattern, i['bronzeMetadataKey'])
    p.need(match is not None, 'PATH_UNSAFE')
    from datetime import date
    p.need(date.fromisoformat(match[1]).isoformat() == match[1])
    p.need(i['bronzePayloadKey'] == i['bronzeMetadataKey'].removesuffix('metadata.json') + 'payload.xml', 'PATH_UNSAFE')
    expected = 'v1/' + sha256(i['bronzeBucket'].encode()).hexdigest() + '/' + i['bronzeMetadataKey'][len('bronze/rss/'):].removesuffix('metadata.json') + 'complete.json'
    p.need(a['relativeAddress'] == expected and len(expected.encode()) <= 1024, 'PATH_UNSAFE')
    return i


def capture(a, binding, previous=None):
    """Capture the exact selected namespace, including native-ignored hidden bookkeeping.

    MAX_ENTRIES counts its root and every file/directory once; ancestor guards are excluded.
    """
    root = Path(binding['root'])
    p.need(root.is_absolute() and root == root.resolve(strict=True), 'PATH_UNSAFE')
    r = p.info(root, True, previous is not None)
    p.need([str(r.st_dev), str(r.st_ino)] == [binding['rootDevice'], binding['rootInode']], 'EVIDENCE_CHANGED')
    namespace = root / str(Path(a['relativeAddress']).parent)
    guards, current = [], root
    for part in ('', *namespace.relative_to(root).parts):
        if part: current /= part
        s = p.info(current, True, previous is not None)
        guards.append(dict(path=current.relative_to(root).as_posix(), identity=[str(s.st_dev), str(s.st_ino), str(s.st_mode)]))
    files, directories = [], []
    previous_dirs = {d['path']: d for d in previous['directories']} if previous else {}
    previous_files = {f['path']: f for f in previous['files']} if previous else {}
    total, analytical = 0, {'accepted': 0, 'rejected': 0}
    completion = None
    entries = 1  # Namespace root; ancestor guards are separate identity checks.

    def visit(directory, dataset=None, depth=0, bookkeeping=False):
        nonlocal total, completion, entries
        p.need(depth <= 6, 'RESOURCE_LIMIT')
        relative = directory.relative_to(namespace).as_posix()
        before = p.info(directory, True, previous is not None)
        if relative in previous_dirs:p.need(p.stamp(before) == previous_dirs[relative]['stamp'], 'EVIDENCE_CHANGED')
        try:
            with os.scandir(directory) as stream:
                names = []
                for entry in stream:
                    # Charge every child once, including siblings still awaiting traversal.
                    p.need(entries < MAX_ENTRIES, 'RESOURCE_LIMIT')
                    entries += 1
                    names.append(entry.name)
            names.sort(key=lambda n: n.encode('utf-8'))
        except FileNotFoundError:
            raise p.Failure('EVIDENCE_CHANGED' if previous else 'SILVER_UNAVAILABLE') from None
        if relative in previous_dirs:
            p.need(names == previous_dirs[relative]['names'], 'EVIDENCE_CHANGED')
        directories.append(dict(path=relative, stamp=p.stamp(before), names=names))
        for name in names:
            path = directory / name
            rel = path.relative_to(namespace).as_posix()
            hidden = name.startswith(('.', '_'))
            if directory == namespace and name in ('accepted', 'rejected'):
                visit(path, name, depth + 1)
                continue
            if not hidden and not bookkeeping:
                p.need((directory == namespace and name == 'complete.json') or
                       (relative in analytical and name.endswith('.parquet')), 'SILVER_INVALID')
            try: s = path.lstat()
            except FileNotFoundError: raise p.Failure('EVIDENCE_CHANGED' if previous else 'SILVER_UNAVAILABLE') from None
            if rel in previous_files:p.need(p.stamp(s) == previous_files[rel]['stamp'], 'EVIDENCE_CHANGED')
            if stat.S_ISDIR(s.st_mode) and (hidden or bookkeeping):
                visit(path, None, depth + 1, True)
                continue
            s = p.info(path, False, previous is not None)
            association = dataset if not hidden and not bookkeeping and relative in analytical else 'bookkeeping'
            if association in analytical:p.need(not any(c in name for c in '*?[]{}\\'), 'PATH_UNSAFE')
            if association in analytical:
                analytical[association] += 1
                p.need(analytical[association] <= MAX_FILES, 'RESOURCE_LIMIT')
            limit = 65536 if rel == 'complete.json' else 16*1024**2 if association == 'bookkeeping' else MAX_RAW
            p.need(s.st_size <= limit and total + s.st_size <= MAX_RAW, 'RESOURCE_LIMIT')
            digest, length, raw = sha256(), 0, bytearray()
            fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
            with os.fdopen(fd, 'rb') as stream:
                p.need(p.stamp(os.fstat(stream.fileno())) == p.stamp(s), 'EVIDENCE_CHANGED')
                while block := stream.read(min(1024**2, limit + 1 - length)):
                    length += len(block);p.need(length <= limit, 'RESOURCE_LIMIT');digest.update(block)
                    if rel == 'complete.json':raw.extend(block)
                p.need(p.stamp(os.fstat(stream.fileno())) == p.stamp(s), 'EVIDENCE_CHANGED')
            p.need(p.stamp(p.info(path, False, True)) == p.stamp(s) and length == s.st_size, 'EVIDENCE_CHANGED')
            total += length
            files.append(dict(path=rel, dataset=association, byteLength=length, sha256=digest.hexdigest(), stamp=p.stamp(s)))
            if rel == 'complete.json':completion = base64.b64encode(raw).decode()
        p.need(p.stamp(p.info(directory, True, True)) == p.stamp(before), 'EVIDENCE_CHANGED')
    visit(namespace)
    p.need(completion is not None and all(analytical.values()), 'SILVER_INVALID')
    for g in guards:
        s = p.info(root / g['path'], True, True)
        p.need([str(s.st_dev), str(s.st_ino), str(s.st_mode)] == g['identity'], 'EVIDENCE_CHANGED')
    result = dict(files=sorted(files, key=lambda f: f['path'].encode()), directories=directories,
                  guards=guards, completionBase64=completion, analyticalFileCounts=analytical)
    if previous is not None:p.need(result == previous, 'EVIDENCE_CHANGED')
    return result


def bronze_read(a, configuration, marker, client=None, baseline=None):
    from bronze_to_silver import local_client, metadata, read_object, METADATA_LIMIT, MAX_BYTES, preflight
    from bronze_to_silver import Failure
    i = a['identity'];owned = client is None
    try:
        if owned:
            p.fields(configuration, ('endpoint', 'region', 'bucket', 'accessKey', 'secretKey'))
            p.need(configuration['bucket'] == i['bronzeBucket'], 'BRONZE_INVALID')
            for v in configuration.values():p.need(type(v) is str and 0 < len(v.encode()) <= 4096, 'ADAPTER_INVALID')
            env = {'BRONZE_S3_'+k:v for k,v in dict(ENDPOINT=configuration['endpoint'],REGION=configuration['region'],BUCKET=configuration['bucket'],ACCESS_KEY=configuration['accessKey'],SECRET_KEY=configuration['secretKey']).items()}
            client, _ = local_client(env)
        raw = read_object(client, i['bronzeBucket'], i['bronzeMetadataKey'], METADATA_LIMIT)
        payload = read_object(client, i['bronzeBucket'], i['bronzePayloadKey'], MAX_BYTES)
        evidence = dict(bucket=i['bronzeBucket'], metadataKey=i['bronzeMetadataKey'], metadataByteLength=len(raw), metadataSha256=sha256(raw).hexdigest(),
                        payloadKey=i['bronzePayloadKey'], payloadByteLength=len(payload), payloadSha256=sha256(payload).hexdigest())
        if baseline is not None:p.need(evidence == baseline, 'BRONZE_CHANGED')
        m, instant = metadata(raw, i['bronzeMetadataKey'])
        p.need(m['sourceId'] == i['sourceId'] and m['ingestionId'] == i['ingestionId'] and m['payloadKey'] == i['bronzePayloadKey'], 'BRONZE_INVALID')
        p.need(sha256(raw).hexdigest() == marker['identity']['metadata_sha256'] and
               sha256(payload).hexdigest() == m['payloadSha256'] == marker['identity']['payload_sha256'] and len(payload) == m['payloadByteLength'], 'BRONZE_INVALID')
        fmt = next((fmt for fmt in ('rss','atom') if preflight(payload, fmt) is None), None)
        p.need(fmt is not None, 'BRONZE_INVALID')
        evidence = dict(bucket=i['bronzeBucket'], metadataKey=i['bronzeMetadataKey'], metadataByteLength=len(raw), metadataSha256=sha256(raw).hexdigest(),
                        payloadKey=i['bronzePayloadKey'], payloadByteLength=len(payload), payloadSha256=m['payloadSha256'])
        expected = dict(source_uri=m['sourceUri'], ingested_at=instant, payload_byte_length=len(payload), feed_format=fmt)
        return evidence, expected
    except Failure as e:
        raise p.Failure('BRONZE_UNAVAILABLE' if str(e) == 'OBJECT_READ_FAILED' else 'BRONZE_INVALID') from None
    finally:
        if owned and client is not None:client.close()


def identity(a, local, bronze):
    marker = next(f for f in local['files'] if f['path'] == 'complete.json')
    value = dict(qualificationPolicy=POLICY, qualificationRevision=1, productId=a['productId'], contractVersion=a['contractVersion'], processorRevision=a['processorRevision'],
                 sourceId=a['identity']['sourceId'], ingestionId=a['identity']['ingestionId'],
                 nativeCompletion=dict(relativeAddress=a['relativeAddress'], byteLength=marker['byteLength'], sha256=marker['sha256']),
                 files=[{k:f[k] for k in ('path','dataset','byteLength','sha256')} for f in sorted(local['files'], key=lambda f:f['path'].encode()) if f['path'] != 'complete.json'], bronze=bronze)
    return value, sha256(canonical_bytes(value)).hexdigest()


def qualify(spark, a, binding, local, marker, expected):
    from pyspark.sql import functions as F
    import silver_inputs as native
    selected = native.load_inputs(spark, binding['root'], markers=[a['relativeAddress']])
    item = selected.manifest['inputs'][0]
    p.need(item['marker_sha256'] == next(f['sha256'] for f in local['files'] if f['path'] == 'complete.json'), 'EVIDENCE_CHANGED')
    for name, schema in native.SCHEMAS.items():
        # Native Spark reads declare nullable columns. Required values/array elements are enforced by native row predicates.
        frame = native.read_dataset(spark, selected.selection, item, name)
        p.need(frame.schema == schema, 'SCHEMA_MISMATCH')
        native.require(frame, native.conjunction([F.col(k).eqNullSafe(F.lit(v)) for k,v in expected.items()]), 'PROVENANCE_MISMATCH', '')
        recorded = [dict(path=f['path'],byte_length=f['byteLength'],sha256=f['sha256']) for f in local['files'] if f['dataset']==name]
        prefix = str(Path(a['relativeAddress']).parent) + '/'
        p.need([dict(f, path=prefix+f['path']) for f in recorded] == item['files'][name], 'EVIDENCE_CHANGED')
    selected.verify_unchanged()
    return dict(verifier='silver_inputs.load_inputs', counts=marker['counts'], schemaPolicy='native-nullable-schema-and-required-row-values-v1',
                bronzeProvenance='all-rows-exact-v1', semanticReconstruction=False)


@contextmanager
def quiet_stdout():
    sys.stdout.flush();saved = os.dup(1)
    try:
        os.dup2(2,1);yield
    finally:
        sys.stdout.flush();os.dup2(saved,1);os.close(saved)


def spark_qualify(a, binding, local, marker, expected, runtime):
    import pyspark
    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    p.need(pyspark.__version__ == '4.0.2', 'RUNTIME_INCOMPATIBLE')
    p.fields(runtime, ('javaHome','scratchRoot','scratchDevice','scratchInode'))
    scratch = Path(runtime['scratchRoot']);java = Path(runtime['javaHome'])
    p.need(scratch == scratch.resolve(strict=True) and java == java.resolve(strict=True) and (java/'bin/java').is_file(), 'PATH_UNSAFE')
    s = p.info(scratch,True)
    p.need([str(s.st_dev),str(s.st_ino)] == [runtime['scratchDevice'],runtime['scratchInode']], 'EVIDENCE_CHANGED')
    p.need(stat.S_IMODE(s.st_mode) == 0o700 and s.st_uid == os.getuid(), 'PATH_UNSAFE')
    p.need(re.fullmatch(r'[A-Za-z0-9/_.,=-]+',str(scratch)) is not None, 'PATH_UNSAFE')
    p.need(shutil.disk_usage(scratch).free >= 1024**3, 'RESOURCE_LIMIT')
    private = Path(tempfile.mkdtemp(prefix='silver-import-', dir=scratch))
    # No native root is an execution working/temp location. Directory is retained for operator inspection.
    for k in ('PYSPARK_SUBMIT_ARGS','SPARK_REMOTE','SPARK_CONNECT_MODE_ENABLED','JAVA_TOOL_OPTIONS','JDK_JAVA_OPTIONS','_JAVA_OPTIONS'):
        os.environ.pop(k,None)
    tempfile.tempdir = str(private)
    os.environ.update(JAVA_HOME=str(java),PYSPARK_PYTHON=sys.executable,SPARK_LOCAL_IP='127.0.0.1',SPARK_LOCAL_DIRS=str(private),TMPDIR=str(private),SPARK_SUBMIT_OPTS='-Djava.io.tmpdir='+str(private))
    spark = None;gateway = None;failure = None;result = None
    with quiet_stdout():
        try:
            p.need(SparkContext._active_spark_context is None, 'ADAPTER_INVALID')
            spark = (SparkSession.builder.master('local[2]').appName('ddip-e5-readonly-silver-qualification')
                     .config('spark.ui.enabled','false').config('spark.driver.host','127.0.0.1').config('spark.driver.bindAddress','127.0.0.1')
                     .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','2')
                     .config('spark.sql.warehouse.dir',(private/'warehouse').as_uri()).config('spark.eventLog.enabled','false')
                     .config('spark.driver.memory','512m').config('spark.driver.maxResultSize','16m').getOrCreate())
            gateway = SparkContext._gateway
            p.need(spark.version == '4.0.2' and spark._jvm.java.lang.System.getProperty('java.specification.version') == '21', 'RUNTIME_INCOMPATIBLE')
            spark.sparkContext.setLogLevel('ERROR')
            result = qualify(spark,a,binding,local,marker,expected)
        except Exception as e:failure = e
        finally:
            try:
                if spark is not None:spark.stop()
                gateway = gateway or SparkContext._gateway
                if gateway is not None:
                    gateway.shutdown()
                    if gateway.proc is not None:
                        gateway.proc.stdin.close()
                        gateway.proc.wait(timeout=5)
                if gateway is not None and gateway.proc is None:raise p.Failure('SPARK_CLEANUP_UNCERTAIN')
            except Exception:raise p.Failure('SPARK_CLEANUP_UNCERTAIN') from None
    if failure is not None:raise failure
    result['sparkShutdown'] = 'CONFIRMED'
    return result


def execute(request):
    a,binding = request['artifact'],request['binding'];selection(a)
    local = capture(a,binding,request['expectedCapture']['local'] if request['expectedCapture'] else None)
    from silver_inputs import parse_marker, InputFailure
    try:
        marker,_,namespace = parse_marker(binding['root'],a['relativeAddress'])
    except InputFailure:
        capture(a,binding,local)  # Distinguish observed mutation from an originally invalid marker.
        raise
    i = a['identity']
    p.need(namespace['source_id'] == i['sourceId'] and namespace['ingestion_id'] == i['ingestionId'] and marker['identity']['bucket'] == i['bronzeBucket'], 'IDENTITY_MISMATCH')
    bronze,expected = bronze_read(a,request['bronze'],marker, baseline=request['expectedCapture']['bronze'] if request['expectedCapture'] else None)
    captured = dict(local=local,bronze=bronze)
    if request['mode'] == 'RECHECK':
        p.need(captured == request['expectedCapture'], 'EVIDENCE_CHANGED')
        return 'UNCHANGED',None
    try:
        native = spark_qualify(a,binding,local,marker,expected,request['runtime'])
    except InputFailure as e:
        # The legacy INPUT_CHANGED also covers reread failures: only our capture proves mutation.
        p.need(capture(a,binding,local) == local, 'EVIDENCE_CHANGED')
        if e.code == 'INPUT_CHANGED':raise p.Failure('SILVER_UNAVAILABLE') from None
        raise
    p.need(capture(a,binding,local) == local, 'EVIDENCE_CHANGED')
    after,_ = bronze_read(a,request['bronze'],marker,baseline=bronze)
    p.need(after == bronze, 'BRONZE_CHANGED')
    value,digest = identity(a,local,bronze)
    return 'VERIFIED',dict(evidenceVersion=2,native=native,capture=captured,inventoryIdentity=dict(kind='E5_SILVER_INVENTORY_V1',sha256=digest,value=value))
