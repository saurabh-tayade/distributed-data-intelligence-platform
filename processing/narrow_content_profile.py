"""Version 1 read-only bounds for the opt-in narrow managed Content profile.

These functions are NOT wired into publication or dispatch. They validate complete
serialized envelopes, not native row semantics, provenance, or pre-completion
safety. Standalone contracts and native readers remain authoritative and unchanged.
"""
import base64
from functools import wraps
from hashlib import sha256
import json
from pathlib import PurePosixPath
import re

from publication_metadata import canonical_bytes

PROFILE = 'ddip-content-narrow-v1'
# Jackson 3.1.4 StreamReadConstraints.DEFAULT_MAX_NAME_LEN, independent of
# maxStringLength. SourceJson reads a Java String, so this counts UTF-16 units.
NAME_LIMIT = 50_000
CAPTURE_LIMIT = 32 * 1024
IDENTITY_LIMIT = 16 * 1024
VERIFICATION_LIMIT = 52 * 1024
QUALIFICATION_LIMIT = 60 * 1024
SELECTION_LIMIT = 36 * 1024
REPORT_ROW_LIMIT = 60 * 1024
COMPLETION_LIMIT = 4 * 1024


class ProfileError(ValueError):
    """Fixed diagnostic: source values and parser exceptions must not escape."""
    def __init__(self):
        self.code = 'CONTENT_PROFILE_INVALID'
        super().__init__(self.code)


def _need(ok):
    if not ok:
        raise ProfileError()


def _safe(function):
    @wraps(function)
    def checked(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except Exception:
            raise ProfileError() from None
    return checked


def _fields(value, names):
    _need(type(value) is dict and set(value) == set(names.split()))


def _text(value):
    _need(type(value) is str)


def _hash(value):
    _need(type(value) is str and re.fullmatch('[0-9a-f]{64}', value) is not None)


def _uint(value):
    _need(type(value) is int and 0 <= value <= 2**63 - 1)


def _one(value):
    _need(type(value) is int and value == 1)


def _counts(value):
    _fields(value, 'accepted rejected')
    for number in value.values():
        _uint(number)


def _path(value, root=False):
    _text(value)
    _need(value == '.' and root or bool(value) and value != '.' and not value.startswith('/') and
          str(PurePosixPath(value)) == value and '..' not in value.split('/') and '\\' not in value)


def _stamp(value, size):
    _need(type(value) is list and len(value) == size)
    for part in value:
        _need(type(part) is str and re.fullmatch('-?[0-9]{1,20}', part) is not None)


def _grammar(value, string_limit, depth=0):
    # Jackson counts containers from one; SourceJson.canonical counts root as zero.
    _need(depth <= 16)
    if type(value) in (dict, list):
        _need(depth < 16)
        if type(value) is dict:
            for key, child in value.items():
                _string(key, NAME_LIMIT)
                _grammar(child, string_limit, depth + 1)
        else:
            for child in value:
                _grammar(child, string_limit, depth + 1)
    elif type(value) is str:
        _string(value, string_limit)
    elif type(value) is int:
        _need(len(str(value)) <= 20)
    else:
        _need(value is None or type(value) is bool)


def _string(value, limit):
    _text(value)
    # Strict encoding rejects lone surrogates; supplementary scalars occupy two units.
    _need(len(value.encode('utf-16-le')) // 2 <= limit)


@_safe
def canonical_envelope(value, limit, *, large=False):
    """Return exact native canonical bytes after integer-only reader grammar checks."""
    _need(type(limit) is int and 0 < limit <= 16 * 1024**2 and type(large) is bool)
    _grammar(value, 131072 if large else 65536)
    raw = canonical_bytes(value)
    _need(0 < len(raw) <= limit)
    return raw


@_safe
def parse_envelope(raw, limit, *, large=False):
    """Require complete canonical UTF-8 bytes, no BOM, duplicates or trailing data."""
    _need(type(raw) is bytes and 0 < len(raw) <= limit)
    def pairs(items):
        result = {}
        for key, value in items:
            _need(key not in result)
            result[key] = value
        return result
    def invalid(_):
        raise ProfileError()
    def integer(token):
        _need(len(token) <= 20)
        return int(token)
    value = json.loads(raw.decode('utf-8'), object_pairs_hook=pairs,
                       parse_int=integer, parse_float=invalid, parse_constant=invalid)
    _need(canonical_envelope(value, limit, large=large) == raw)
    return value


def _file(value, stamped=True, snake=False):
    length = 'byte_length' if snake else 'byteLength'
    _fields(value, 'path sha256 ' + length + (' dataset stamp' if stamped else ''))
    _path(value['path']); _uint(value[length]); _hash(value['sha256'])
    if stamped:
        _need(value['dataset'] in ('accepted', 'rejected', 'bookkeeping'))
        _stamp(value['stamp'], 7)
        _need(int(value['stamp'][2]) == value[length])


@_safe
def validate_silver_capture(local):
    """Validate exact q.capture shape, namespace counts and complete local bytes."""
    raw = canonical_envelope(local, CAPTURE_LIMIT)
    _fields(local, 'files directories guards completionBase64 analyticalFileCounts')
    _need(type(local['files']) is list and 3 <= len(local['files']) <= 24)
    _need(type(local['directories']) is list and 3 <= len(local['directories']) <= 4)
    _need(type(local['guards']) is list and 1 <= len(local['guards']) <= 6)
    paths, associations = set(), {'accepted': 0, 'rejected': 0}
    for item in local['files']:
        _file(item)
        path = item['path']; _need(path not in paths); paths.add(path)
        if item['dataset'] in associations:
            _need(PurePosixPath(path).parent.as_posix() == item['dataset'] and path.endswith('.parquet'))
            associations[item['dataset']] += 1
    _need([f['path'] for f in local['files']] == sorted(paths, key=lambda x: x.encode('utf-8')))
    _counts(local['analyticalFileCounts'])
    _need(local['analyticalFileCounts'] == associations and all(1 <= n <= 4 for n in associations.values()))
    directories = {}
    for item in local['directories']:
        _fields(item, 'path stamp names'); _path(item['path'], True); _stamp(item['stamp'], 7)
        _need(item['path'] not in directories and item['path'] not in paths)
        _need(type(item['names']) is list)
        for name in item['names']:
            _path(name); _need('/' not in name and name != '.')
        _need(item['names'] == sorted(set(item['names']), key=lambda x: x.encode('utf-8')))
        directories[item['path']] = item['names']
    _need({'.', 'accepted', 'rejected'} <= set(directories))
    all_paths = paths | (set(directories) - {'.'})
    for path in all_paths:
        _need(PurePosixPath(path).parent.as_posix() in directories)
    for directory, names in directories.items():
        _need(set(names) == {PurePosixPath(p).name for p in all_paths if PurePosixPath(p).parent.as_posix() == directory})
    guards = set()
    for item in local['guards']:
        _fields(item, 'path identity'); _path(item['path'], True); _stamp(item['identity'], 3)
        _need(item['path'] not in guards); guards.add(item['path'])
    _text(local['completionBase64'])
    completion = base64.b64decode(local['completionBase64'], validate=True)
    _need(base64.b64encode(completion).decode() == local['completionBase64'] and len(completion) <= COMPLETION_LIMIT)
    marker = next(f for f in local['files'] if f['path'] == 'complete.json')
    _need(marker['dataset'] == 'bookkeeping' and marker['byteLength'] == len(completion) and marker['sha256'] == sha256(completion).hexdigest())
    return raw


def _bronze(value):
    _fields(value, 'bucket metadataKey metadataByteLength metadataSha256 payloadKey payloadByteLength payloadSha256')
    for key in ('bucket', 'metadataKey', 'payloadKey'):
        _text(value[key])
    for key in ('metadataByteLength', 'payloadByteLength'):
        _uint(value[key])
    for key in ('metadataSha256', 'payloadSha256'):
        _hash(value[key])


@_safe
def validate_silver_verification(verification, selection):
    """Cross-check native inventory identity using the production q.identity encoder."""
    import silver_import_qualification as q
    raw = canonical_envelope(verification, VERIFICATION_LIMIT)
    q.selection(selection)
    _fields(verification, 'evidenceVersion native capture inventoryIdentity')
    _need(type(verification['evidenceVersion']) is int and verification['evidenceVersion'] == 2)
    native = verification['native']
    _fields(native, 'verifier counts schemaPolicy bronzeProvenance semanticReconstruction sparkShutdown')
    _need(native['verifier'] == 'silver_inputs.load_inputs' and
          native['schemaPolicy'] == 'native-nullable-schema-and-required-row-values-v1' and
          native['bronzeProvenance'] == 'all-rows-exact-v1' and
          native['semanticReconstruction'] is False and native['sparkShutdown'] == 'CONFIRMED')
    _counts(native['counts'])
    captured = verification['capture']; _fields(captured, 'local bronze')
    validate_silver_capture(captured['local']); _bronze(captured['bronze'])
    inventory = verification['inventoryIdentity']; _fields(inventory, 'kind sha256 value')
    _need(inventory['kind'] == 'E5_SILVER_INVENTORY_V1'); _hash(inventory['sha256'])
    canonical_envelope(inventory['value'], IDENTITY_LIMIT)
    value, digest = q.identity(selection, captured['local'], captured['bronze'])
    _need(inventory['value'] == value and inventory['sha256'] == digest)
    return raw


@_safe
def validate_silver_qualification(qualification):
    raw = canonical_envelope(qualification, QUALIFICATION_LIMIT)
    _fields(qualification, 'qualificationVersion selection verification')
    _one(qualification['qualificationVersion'])
    validate_silver_verification(qualification['verification'], qualification['selection'])
    return raw


@_safe
def validate_native_selection(selection):
    """Bound the entire native inventory wrapper, not only its manifest."""
    raw = canonical_envelope(selection, SELECTION_LIMIT)
    _fields(selection, 'manifest inputSetFingerprint')
    manifest = selection['manifest']
    _fields(manifest, 'manifest_version gold_contract_versions gold_processor_revision silver_contract_version silver_processor_revision inputs')
    for key in ('manifest_version', 'gold_processor_revision', 'silver_contract_version', 'silver_processor_revision'):
        _one(manifest[key])
    _fields(manifest['gold_contract_versions'], 'gold_articles gold_source_daily')
    for value in manifest['gold_contract_versions'].values():
        _one(value)
    _need(type(manifest['inputs']) is list and len(manifest['inputs']) == 1)
    item = manifest['inputs'][0]
    _fields(item, 'marker_path marker_sha256 identity counts files representative_path')
    _path(item['marker_path']); _hash(item['marker_sha256'])
    _need(item['representative_path'] == item['marker_path'])
    _counts(item['counts'])
    identity = item['identity']
    _fields(identity, 'metadata_sha256 payload_sha256 contract_version processor_revision bucket metadata_key')
    for key in ('metadata_sha256', 'payload_sha256'):
        _hash(identity[key])
    for key in ('contract_version', 'processor_revision'):
        _one(identity[key])
    _text(identity['bucket']); _text(identity['metadata_key'])
    _fields(item['files'], 'accepted rejected')
    for dataset, files in item['files'].items():
        _need(type(files) is list and 1 <= len(files) <= 4)
        paths = []
        for file in files:
            _file(file, stamped=False, snake=True)
            _need(PurePosixPath(file['path']).parent == PurePosixPath(item['marker_path']).parent / dataset and file['path'].endswith('.parquet'))
            paths.append(file['path'])
        _need(paths == sorted(set(paths), key=lambda x: x.encode('utf-8')))
    _hash(selection['inputSetFingerprint'])
    _need(selection['inputSetFingerprint'] == sha256(canonical_bytes(manifest)).hexdigest())
    return raw


@_safe
def validate_report_row(report, row):
    """Return full public projection bytes. No truncation or reference-only exclusion.

    Call for EVERY projected row, including reference-only coverage rows, before
    publication when integration exists. This is a size/grammar guard, not a
    replacement for native report semantic validation or a default-page proof.
    """
    from content_analytics import REPORTS, public
    _need(type(report) is str and report in REPORTS)
    _need(type(row) is dict and set(row) == set(REPORTS[report][1]))
    return canonical_envelope(public(row), REPORT_ROW_LIMIT)
