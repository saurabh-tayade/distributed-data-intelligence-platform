"""Fixed read-only E5 import protocol; Spark/Bronze only for explicit Silver qualification."""
import base64
from collections.abc import Mapping
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import stat
import sys

# -I excludes ambient paths; only the approved, hash-bound processing directory is added.
sys.path.insert(0, str(Path(__file__).absolute().parent))
from publication_metadata import canonical_bytes

PROTOCOL_LIMIT = 512 * 1024
CODES = {
    'ARGUMENT_INVALID', 'PATH_UNSAFE', 'UNSUPPORTED_VERSION', 'INTAKE_NOT_APPROVED',
    'INPUT_UNAVAILABLE', 'INPUT_CHANGED', 'BATCH_UNAVAILABLE', 'BATCH_INCOMPLETE',
    'BATCH_INVALID', 'BATCH_CONFLICT', 'CSV_INVALID', 'VOCABULARY_INVALID',
    'SNAPSHOT_UNAVAILABLE', 'SNAPSHOT_INVALID', 'SNAPSHOT_CHANGED', 'SNAPSHOT_INCOMPLETE',
    'IDENTITY_MISMATCH', 'DEPENDENCY_UNAVAILABLE', 'DEPENDENCY_INVALID', 'DEPENDENCY_CHANGED',
    'PARQUET_INVALID', 'PROVENANCE_UNSUPPORTED', 'RESOURCE_LIMIT', 'EVIDENCE_UNAVAILABLE',
    'EVIDENCE_INVALID', 'EVIDENCE_CHANGED', 'RUNTIME_INCOMPATIBLE', 'ADAPTER_INVALID',
}


class Failure(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def need(condition, code='ADAPTER_INVALID'):
    if not condition:
        raise Failure(code)


def classification(code):
    if code == 'SPARK_CLEANUP_UNCERTAIN':
        return 'UNRESOLVED'
    if code.endswith('_CHANGED'):
        return 'CHANGED'
    if code.endswith('_UNAVAILABLE') or code == 'INPUT_READ_FAILED':
        return 'UNAVAILABLE'
    if code == 'RESOURCE_LIMIT':
        return 'LIMIT'
    if code in ('UNSUPPORTED_VERSION', 'RUNTIME_INCOMPATIBLE'):
        return 'UNSUPPORTED'
    return 'INVALID'


def parse(raw):
    need(0 < len(raw) <= PROTOCOL_LIMIT)
    def pairs(items):
        result = {}
        for k, v in items:
            need(k not in result)
            result[k] = v
        return result
    def invalid(_):
        raise Failure('ADAPTER_INVALID')
    try:
        value = json.loads(raw.decode('utf-8'), object_pairs_hook=pairs,
                           parse_float=invalid, parse_constant=invalid)
        need(canonical_bytes(value) == raw)
        return value
    except (ValueError, UnicodeError, RecursionError):
        raise Failure('ADAPTER_INVALID') from None


def fields(value, names):
    need(type(value) is dict and set(value) == set(names))


def text(value, pattern):
    need(type(value) is str and re.fullmatch(pattern, value) is not None)
    return value


def validate(request):
    silver = type(request) is dict and request.get('protocolVersion') == 2
    fields(request, ('protocolVersion', 'operationId', 'selectionSha256', 'mode', 'binding', 'artifact', 'expectedCapture', *(('bronze','runtime') if silver else ())))
    need(type(request['protocolVersion']) is int and request['protocolVersion'] in (1,2))
    text(request['operationId'], r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}')
    text(request['selectionSha256'], r'[0-9a-f]{64}')
    need(request['mode'] in ('VERIFY', 'RECHECK'))
    need((request['expectedCapture'] is None) == (request['mode'] == 'VERIFY'))
    if request['mode'] == 'RECHECK' and not silver:
        fields(request['expectedCapture'], ('files', 'directories', 'guards', 'completionBase64'))
        need(type(request['expectedCapture']['directories']) is list and 1 <= len(request['expectedCapture']['directories']) <= 2)
    b, a = request['binding'], request['artifact']
    fields(b, ('root', 'rootDevice', 'rootInode'))
    for n in ('rootDevice', 'rootInode'):
        text(b[n], r'-?[0-9]{1,20}')
    if silver:
        from silver_import_qualification import selection
        selection(a)
        need(sha256(canonical_bytes(a)).hexdigest() == request['selectionSha256'])
        return a,b
    fields(a, ('kind', 'productId', 'storageBindingId', 'relativeAddress', 'contractVersion', 'processorRevision', 'identity', 'fingerprint'))
    need(sha256(canonical_bytes(a)).hexdigest() == request['selectionSha256'])
    text(a['fingerprint'], r'[0-9a-f]{64}')
    text(a['storageBindingId'], r'[A-Za-z0-9_-]{1,128}')
    i = a['identity']
    if a['kind'] == 'RELIABILITY_BATCH_V2':
        need(a['productId'] == 'application-reliability' and type(a['contractVersion']) is int and a['contractVersion'] == 2
             and type(a['processorRevision']) is int and a['processorRevision'] == 2)
        fields(i, ('sourceId', 'sourceRevision', 'batchId', 'inputFormat', 'registeredProfileSha256'))
        source = text(i['sourceId'], r'[a-z0-9][a-z0-9_-]{0,127}')
        batch = text(i['batchId'], r'[a-z0-9][a-z0-9_-]{0,63}')
        need(type(i['sourceRevision']) is int and 1 <= i['sourceRevision'] <= 1000)
        need(i['inputFormat'] in ('JSONL', 'PARQUET'))
        text(i['registeredProfileSha256'], r'[0-9a-f]{64}')
        expected = f"v2/format={i['inputFormat'].lower()}/source_id={source}/batch_id={batch}"
    else:
        need(a['kind'] in ('PUBLISHER_REFERENCE', 'SERVICE_REFERENCE'))
        need(a['productId'] == ('content-intelligence' if a['kind'] == 'PUBLISHER_REFERENCE' else 'application-reliability'))
        need(type(a['contractVersion']) is int and a['contractVersion'] == 1 and type(a['processorRevision']) is int and a['processorRevision'] == 1)
        fields(i, ('referenceId', 'revision'))
        ref = text(i['referenceId'], r'[a-z0-9][a-z0-9_-]{0,63}')
        rev = text(i['revision'], r'[a-z0-9][a-z0-9_-]{0,31}')
        expected = f'reference_id={ref}/revision={rev}'
    need(a['relativeAddress'] == expected, 'PATH_UNSAFE')
    return a, b


def info(path, directory, observed=False):
    try:
        value = path.lstat()
    except FileNotFoundError:
        raise Failure('EVIDENCE_CHANGED' if observed else 'EVIDENCE_UNAVAILABLE') from None
    except OSError:
        raise Failure('EVIDENCE_UNAVAILABLE') from None
    need(stat.S_ISDIR(value.st_mode) if directory else stat.S_ISREG(value.st_mode) and value.st_nlink == 1, 'PATH_UNSAFE')
    return value


def stamp(value):
    return [str(x) for x in (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns, value.st_mode, value.st_nlink)]


def names(path):
    try:
        result = []
        with os.scandir(path) as entries:
            for entry in entries:
                need(len(result) < 32, 'RESOURCE_LIMIT')
                result.append(entry.name)
        return sorted(result)
    except OSError:
        raise Failure('EVIDENCE_UNAVAILABLE') from None


def capture(a, binding, previous=None):
    root = Path(binding['root'])
    need(root.is_absolute() and root == root.absolute() and '..' not in root.parts, 'PATH_UNSAFE')
    try:
        need(root.resolve(strict=True) == root, 'PATH_UNSAFE')
    except FileNotFoundError:
        raise Failure('EVIDENCE_CHANGED' if previous is not None else 'EVIDENCE_UNAVAILABLE') from None
    except OSError:
        raise Failure('EVIDENCE_UNAVAILABLE') from None
    r = info(root, True, previous is not None)
    need((str(r.st_dev), str(r.st_ino)) == (binding['rootDevice'], binding['rootInode']), 'EVIDENCE_CHANGED')
    guards = []
    path = root
    for part in ('', *Path(a['relativeAddress']).parts):
        if part:
            path = path / part
        v = info(path, True, previous is not None)
        guards.append(dict(path=path.relative_to(root).as_posix(), identity=[str(v.st_dev), str(v.st_ino), str(v.st_mode)]))
    directory = path
    batch = a['kind'] == 'RELIABILITY_BATCH_V2'
    expected = ['complete.json', 'data', 'manifest.json'] if batch else ['complete.json', 'manifest.json', 'reference.csv', 'vocabulary.json']
    listed = names(directory)
    if previous is not None:
        need(listed == previous['directories'][0]['names'], 'EVIDENCE_CHANGED')
    need(listed == expected, 'EVIDENCE_INVALID')
    directories = [dict(path='.', stamp=stamp(info(directory, True)), names=listed)]
    paths = ['complete.json', 'manifest.json']
    limits = {'complete.json': 4096, 'manifest.json': 131072 if batch else 16384,
              'reference.csv': 10485760, 'vocabulary.json': 32768}
    if batch:
        data = directory / 'data'
        listed = names(data)
        if previous is not None:
            need(listed == previous['directories'][1]['names'], 'EVIDENCE_CHANGED')
        need(1 <= len(listed) <= 16, 'RESOURCE_LIMIT')
        need(all(re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,63}\.(?:jsonl|parquet)', n) for n in listed), 'PATH_UNSAFE')
        need(sum(info(data / n, False).st_size for n in listed) <= 1024**3, 'RESOURCE_LIMIT')
        paths += ['data/' + n for n in listed]
        directories.append(dict(path='data', stamp=stamp(info(data, True)), names=listed))
    else:
        paths += ['reference.csv', 'vocabulary.json']
    files = []
    completion = None
    for relative in sorted(paths):
        file = directory / relative
        before = info(file, False)
        limit = limits.get(relative, 256 * 1024**2)
        need(before.st_size <= limit, 'RESOURCE_LIMIT')
        digest, length, raw = sha256(), 0, bytearray()
        try:
            fd = os.open(file, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
            with os.fdopen(fd, 'rb') as stream:
                need(stamp(os.fstat(stream.fileno())) == stamp(before), 'EVIDENCE_CHANGED')
                while block := stream.read(min(1024**2, limit + 1 - length)):
                    length += len(block)
                    need(length <= limit, 'RESOURCE_LIMIT')
                    digest.update(block)
                    if relative == 'complete.json':
                        raw.extend(block)
                need(stamp(os.fstat(stream.fileno())) == stamp(before), 'EVIDENCE_CHANGED')
        except OSError:
            raise Failure('EVIDENCE_UNAVAILABLE') from None
        need(stamp(info(file, False, True)) == stamp(before) and length == before.st_size, 'EVIDENCE_CHANGED')
        files.append(dict(path=relative, byteLength=length, sha256=digest.hexdigest(), stamp=stamp(before)))
        if relative == 'complete.json':
            completion = base64.b64encode(raw).decode('ascii')
    for d in directories:
        p = directory if d['path'] == '.' else directory / d['path']
        need(stamp(info(p, True, True)) == d['stamp'] and names(p) == d['names'], 'EVIDENCE_CHANGED')
    for g in guards:
        v = info(root / g['path'], True, True)
        need([str(v.st_dev), str(v.st_ino), str(v.st_mode)] == g['identity'], 'EVIDENCE_CHANGED')
    return dict(files=files, directories=directories, guards=guards, completionBase64=completion)


def thaw(value):
    if isinstance(value, Mapping):
        return {k: thaw(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [thaw(v) for v in value]
    return value


def qualify(a, binding):
    root, i = binding['root'], a['identity']
    if a['kind'] == 'RELIABILITY_BATCH_V2':
        from reliability_format_inputs import open_batch
        selected = open_batch(root, i['inputFormat'], i['sourceId'], i['batchId'], a['fingerprint'])
        m = selected.manifest
        need(sha256(canonical_bytes(thaw(m['profile']))).hexdigest() == i['registeredProfileSha256'], 'INTAKE_NOT_APPROVED')
        selected.check()
        return dict(verifier='reliability_format_inputs.open_batch', fingerprint=selected.fingerprint,
                    ingestionId=m['ingestion_id'], receivedAt=m['received_at'], profileSha256=i['registeredProfileSha256'],
                    inventory=thaw(m['files']), dependencies=[dict(role='REGISTERED_RAW', **thaw(f)) for f in m['files']])
    if a['kind'] == 'PUBLISHER_REFERENCE':
        import publisher_reference
        selected = publisher_reference.open_snapshot(root, i['referenceId'], i['revision'], a['fingerprint'])
        verifier = 'publisher_reference.open_snapshot'
    else:
        import service_reference
        selected = service_reference.open_snapshot(root, i['referenceId'], i['revision'], a['fingerprint'])
        verifier = 'service_reference.open_snapshot'
    m = selected.manifest
    return dict(verifier=verifier, fingerprint=selected.fingerprint, rowCount=len(selected.rows),
                csvSchemaVersion=m['csv_schema_version'], validatorRevision=m['validator_revision'],
                dependencies=[dict(role='REFERENCE_CSV', **thaw(m['csv'])), dict(role='VOCABULARY', **thaw(m['vocabulary']))])


def execute(request):
    a, binding = validate(request)
    import duckdb
    need(sys.version_info[:3] == (3, 12, 15) and duckdb.__version__ == '1.4.5', 'RUNTIME_INCOMPATIBLE')
    if request['protocolVersion'] == 2:
        from silver_import_qualification import execute as silver_execute
        return silver_execute(request)
    before = capture(a, binding, request['expectedCapture'])
    if request['mode'] == 'RECHECK':
        need(before == request['expectedCapture'], 'EVIDENCE_CHANGED')
        return 'UNCHANGED', None
    native = qualify(a, binding)  # Mandatory authoritative full native reader.
    need(capture(a, binding, before) == before, 'EVIDENCE_CHANGED')
    return 'VERIFIED', dict(evidenceVersion=1, native=native, capture=before)


def main():
    request = None
    result = dict(protocolVersion=1, operationId=None, selectionSha256=None, mode=None,
                  status='FAILED', code='ADAPTER_INVALID', classification='INVALID', evidence=None)
    try:
        request = parse(sys.stdin.buffer.read(PROTOCOL_LIMIT + 1))
        a, _ = validate(request)
        result.update(protocolVersion=request['protocolVersion'], operationId=request['operationId'], selectionSha256=request['selectionSha256'], mode=request['mode'])
        status, evidence = execute(request)
        result.update(status=status, code=None, classification=None, evidence=evidence)
        raw = canonical_bytes(result)
        need(len(raw) <= PROTOCOL_LIMIT, 'RESOURCE_LIMIT')
        sys.stdout.buffer.write(raw)
        return 0
    except Exception as error:
        code = getattr(error, 'code', None)
        allowed = CODES
        if type(request) is dict and request.get('protocolVersion') == 2:
            from silver_import_qualification import CODES as SILVER_CODES
            allowed = allowed | SILVER_CODES
        if code not in allowed:
            code = 'EVIDENCE_UNAVAILABLE' if isinstance(error, OSError) else 'ADAPTER_INVALID'
        result.update(status='FAILED', code=code, classification=classification(code), evidence=None)
        sys.stdout.buffer.write(canonical_bytes(result))
        return 1


if __name__ == '__main__':
    sys.modules['artifact_import_verifier'] = sys.modules[__name__]
    raise SystemExit(main())
