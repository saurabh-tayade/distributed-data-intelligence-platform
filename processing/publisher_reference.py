"""Bounded, standalone E1a reference snapshots (ADR 0006).

One trusted writer per existing local root. No producer, Spark or network imports.
Checks detect observed mutations; they do not provide hostile-race or crash safety.
"""
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import io
import json
import os
from pathlib import Path
import re
import stat
import sys
from threading import RLock
from types import MappingProxyType

from publication_metadata import canonical_bytes

CSV_LIMIT = 10_485_760
ROW_LIMIT = 10_000
RECORD_LIMIT = 4096
VOCABULARY_LIMIT = 32_768
MANIFEST_LIMIT = 16_384
COMPLETION_LIMIT = 4096
FIELDS = ('source_id', 'publisher_name', 'publisher_category',
          'declared_language', 'country_code')
FIELD_LIMITS = (128, 200, 64, 32, 2)
HEADER = ','.join(FIELDS).encode('ascii')
IDENTIFIER = re.compile(r'[a-z0-9][a-z0-9_-]{0,63}')
REVISION = re.compile(r'[a-z0-9][a-z0-9_-]{0,31}')
SOURCE = re.compile(r'[a-z0-9][a-z0-9_-]{0,127}')
HEX = re.compile(r'[0-9a-f]{64}')
UTC = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z')
WHITESPACE = frozenset(map(chr, [*range(9, 14), 32, 133, 160, 5760,
                                *range(8192, 8203), 8232, 8233, 8239, 8287, 12288]))
# Lexical guard only: csv.reader remains responsible for decoding fields.
CELL = r'(?:[^,"\r\n]*|"(?:[^"\r\n]|"")*")'
RECORD = re.compile(','.join(f'({CELL})' for _ in FIELDS))
CSV_LOCK = RLock()
VOCABULARY = {
    'schema_version': 1, 'version': 1,
    'publisher_category': ['business', 'culture', 'general-news', 'science', 'technology'],
    'declared_language': ['en', 'es', 'fr'], 'country_code': ['CA', 'GB', 'US'],
}
SEMANTICS = {'csv_dialect': 'publisher-csv-v1', 'null_policy': 'empty-only-v1',
             'text_policy': 'scalar-text-v1'}
MESSAGES = {
    'ARGUMENT_INVALID': 'Arguments do not match the reference interface.',
    'PATH_UNSAFE': 'A selected path violates the local path policy.',
    'INPUT_UNAVAILABLE': 'A required input is unavailable.',
    'SNAPSHOT_UNAVAILABLE': 'The selected root or snapshot is unavailable.',
    'RESOURCE_LIMIT': 'A reference resource exceeds its fixed limit.',
    'CSV_INVALID': 'Reference CSV does not match the supported contract.',
    'VOCABULARY_INVALID': 'Vocabulary does not match the supported contract.',
    'SNAPSHOT_INVALID': 'Snapshot evidence is invalid.',
    'SNAPSHOT_INCOMPLETE': 'Snapshot is incomplete; owner investigation is required.',
    'IDENTITY_MISMATCH': 'Snapshot identity does not match the selection.',
    'REVISION_CONFLICT': 'Reference revision already binds different content.',
    'INPUT_CHANGED': 'A selected input changed during the operation.',
    'SNAPSHOT_CHANGED': 'Snapshot evidence changed during the operation.',
    'PUBLICATION_FAILED': 'Snapshot publication failed; investigate retained evidence.',
}


class ReferenceFailure(Exception):
    """Only fixed messages and contract-defined field/record identifiers escape."""

    def __init__(self, code, *, field=None, record=None, first_record=None):
        self.code = code
        self.diagnostic = {'code': code, 'message': MESSAGES[code]}
        if field in (*FIELDS, 'reference_id', 'revision', 'maintainer_id', 'note'):
            self.diagnostic['field'] = field
        for name, value in (('record', record), ('first_record', first_record)):
            if type(value) is int and 1 <= value <= ROW_LIMIT + 2:
                self.diagnostic[name] = value
        super().__init__(MESSAGES[code])


def _need(condition, code, **context):
    if not condition:
        raise ReferenceFailure(code, **context)


def _freeze(value):
    if type(value) is dict:
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if type(value) is list:
        return tuple(_freeze(item) for item in value)
    return value


@dataclass(frozen=True, slots=True)
class ReferenceRow:
    source_id: str
    publisher_name: str
    publisher_category: str | None
    declared_language: str | None
    country_code: str | None


@dataclass(frozen=True, slots=True)
class Reference:
    reference_id: str
    revision: str
    fingerprint: str
    rows: tuple[ReferenceRow, ...]
    manifest: MappingProxyType
    created_at: str | None = None

    def summary(self, status):
        return {'status': status, 'reference_id': self.reference_id,
                'revision': self.revision, 'snapshot_fingerprint': self.fingerprint,
                'row_count': len(self.rows)}


@dataclass(frozen=True, slots=True)
class _Capture:
    path: Path
    parent_identity: tuple
    stamp: tuple
    data: bytes
    limit: int


@dataclass(frozen=True, slots=True)
class _Candidate:
    reference: Reference
    manifest_bytes: bytes
    csv: _Capture
    vocabulary: _Capture


def _match(value, pattern, code, **context):
    _need(type(value) is str and pattern.fullmatch(value) is not None, code, **context)


def _text(value, limit, code, **context):
    _need(type(value) is str, code, **context)
    _need(len(value) <= limit, 'RESOURCE_LIMIT', **context)
    _need(bool(value) and value[0] not in WHITESPACE and value[-1] not in WHITESPACE,
          code, **context)
    _need(not any(ord(c) < 32 or 127 <= ord(c) <= 159 or
                  ord(c) in (8232, 8233, 65279) or 0xD800 <= ord(c) <= 0xDFFF
                  for c in value), code, **context)


def _identities(reference_id, revision, maintainer_id=None, note=None):
    _match(reference_id, IDENTIFIER, 'ARGUMENT_INVALID', field='reference_id')
    _match(revision, REVISION, 'ARGUMENT_INVALID', field='revision')
    if maintainer_id is not None:
        _match(maintainer_id, IDENTIFIER, 'ARGUMENT_INVALID', field='maintainer_id')
    if note is not None:
        _text(note, 256, 'ARGUMENT_INVALID', field='note')


def _keys(value, keys, code):
    _need(type(value) is dict and value.keys() == set(keys), code)


def _integer(value, low, high, code):
    _need(type(value) is int and low <= value <= high, code)


def _json(data, code):
    def pairs(items):
        result = {}
        for key, value in items:
            # No supported object has more than nine keys. Check before growth.
            _need(key not in result and len(result) < 9, code)
            result[key] = value
        return result

    def integer(token):
        if not re.fullmatch(r'0|[1-9][0-9]{0,18}', token):
            raise ValueError()
        value = int(token)
        if value > 2**63 - 1:
            raise ValueError()
        return value

    def forbidden(_):
        raise ValueError()

    try:
        value = json.loads(data.decode('utf-8'), object_pairs_hook=pairs,
                           parse_int=integer, parse_float=forbidden, parse_constant=forbidden)
        _need(canonical_bytes(value) == data, code)
        return value
    except (ValueError, UnicodeError, RecursionError):
        raise ReferenceFailure(code) from None


def _vocabulary(data):
    _need(len(data) <= VOCABULARY_LIMIT, 'RESOURCE_LIMIT')
    code = 'VOCABULARY_INVALID'
    value = _json(data, code)
    _keys(value, VOCABULARY, code)
    for name in ('schema_version', 'version'):
        _integer(value[name], 1, 1, code)
    for name, limit, pattern in (
            ('publisher_category', 64, re.compile(r'[a-z][a-z0-9-]{0,63}')),
            ('declared_language', 32, re.compile(r'[a-z][a-z0-9-]{0,31}')),
            ('country_code', 2, re.compile(r'[A-Z]{2}'))):
        tokens = value[name]
        _need(type(tokens) is list and bool(tokens), code)
        _need(len(tokens) <= 32, 'RESOURCE_LIMIT', field=name)
        for token in tokens:
            _text(token, limit, code, field=name)
            _match(token, pattern, code, field=name)
        _need(tokens == sorted(set(tokens)), code, field=name)
    _need(value == VOCABULARY, code)
    return value


def _csv(data):
    _need(len(data) <= CSV_LIMIT, 'RESOURCE_LIMIT')
    rows, seen = [], {}
    stream = io.BytesIO(data)
    record = 0
    while True:
        raw = stream.readline(RECORD_LIMIT + 3)
        if not raw:
            break
        record += 1
        if raw.endswith(b'\r\n'):
            raw = raw[:-2]
        elif raw.endswith(b'\n'):
            raw = raw[:-1]
        _need(len(raw) <= RECORD_LIMIT, 'RESOURCE_LIMIT', record=record)
        if record == 1:
            _need(raw == HEADER, 'CSV_INVALID', record=record)
            continue
        _need(len(rows) < ROW_LIMIT, 'RESOURCE_LIMIT', record=record)
        _need(bool(raw) and b'\r' not in raw, 'CSV_INVALID', record=record)
        _need(raw != HEADER, 'CSV_INVALID', record=record)
        try:
            line = raw.decode('utf-8')
        except UnicodeError:
            raise ReferenceFailure('CSV_INVALID', record=record) from None
        lexical = RECORD.fullmatch(line)
        _need(lexical is not None, 'CSV_INVALID', record=record)
        # Count decoded scalars before handing a bounded record to csv.reader.
        for token, field, limit in zip(lexical.groups(), FIELDS, FIELD_LIMITS):
            length = len(token) - 2 - token[1:-1].count('""') if token.startswith('"') else len(token)
            _need(length <= limit, 'RESOURCE_LIMIT', record=record, field=field)
        with CSV_LOCK:
            previous = csv.field_size_limit()
            try:
                csv.field_size_limit(200)
                cells = next(csv.reader([line], delimiter=',', quotechar='"',
                                        doublequote=True, escapechar=None,
                                        skipinitialspace=False, strict=True))
            except csv.Error:
                raise ReferenceFailure('CSV_INVALID', record=record) from None
            finally:
                csv.field_size_limit(previous)
        for index, (cell, field, limit) in enumerate(zip(cells, FIELDS, FIELD_LIMITS)):
            if index >= 2 and cell == '':
                cells[index] = None
                continue
            _text(cell, limit, 'CSV_INVALID', field=field, record=record)
            if index == 0:
                _match(cell, SOURCE, 'CSV_INVALID', field=field, record=record)
            elif index >= 2:
                _need(cell in VOCABULARY[field], 'CSV_INVALID', field=field, record=record)
        _need(cells[0] not in seen, 'CSV_INVALID', field='source_id', record=record,
              first_record=seen.get(cells[0]))
        seen[cells[0]] = record
        rows.append(ReferenceRow(*cells))
    _need(record > 0, 'CSV_INVALID', record=1)
    return tuple(rows)


def _path(value):
    try:
        path = Path(value)
        _need('..' not in path.parts and '\0' not in str(path), 'PATH_UNSAFE')
        return path.absolute()
    except (TypeError, ValueError, OSError):
        raise ReferenceFailure('PATH_UNSAFE') from None


def _identity(info):
    return info.st_dev, info.st_ino


def _stamp(info):
    return (*_identity(info), info.st_size, info.st_mtime_ns, info.st_mode, info.st_nlink)


def _directory(path, missing, unavailable=None):
    try:
        info = path.lstat()
        _need(stat.S_ISDIR(info.st_mode), 'PATH_UNSAFE')
        return _identity(info)
    except FileNotFoundError:
        raise ReferenceFailure(missing) from None
    except OSError:
        raise ReferenceFailure(unavailable or missing) from None


def _file(path, missing, unavailable=None):
    try:
        info = path.lstat()
        _need(stat.S_ISREG(info.st_mode) and info.st_nlink == 1, 'PATH_UNSAFE')
        return info
    except FileNotFoundError:
        raise ReferenceFailure(missing) from None
    except OSError:
        raise ReferenceFailure(unavailable or missing) from None


def _read_stream(stream, limit):
    data = bytearray()
    while True:
        chunk = stream.read(min(65536, limit + 1 - len(data)))
        if not chunk:
            return bytes(data)
        _need(len(data) + len(chunk) <= limit, 'RESOURCE_LIMIT')
        data.extend(chunk)


def _capture(path, limit, missing='INPUT_UNAVAILABLE', changed='INPUT_CHANGED', *, previous=None):
    absent = changed if previous is not None else missing
    parent = _directory(path.parent, absent, missing)
    before = _file(path, absent, missing)
    _need(before.st_size <= limit, 'RESOURCE_LIMIT')
    if previous is not None:
        _need(_stamp(before) == previous.stamp and parent == previous.parent_identity, changed)
    data = None
    try:
        # Nonblocking/no-follow also avoids blocking if a checked file is replaced
        # by a FIFO. The contract still requires a trusted, single-writer root.
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        with os.fdopen(fd, 'rb') as stream:
            opened = os.fstat(stream.fileno())
            _need(stat.S_ISREG(opened.st_mode) and opened.st_nlink == 1, 'PATH_UNSAFE')
            _need(_stamp(opened) == _stamp(before), changed)
            data = _read_stream(stream, limit)
            if previous is not None:
                _need(data == previous.data, changed)
            _need(_stamp(os.fstat(stream.fileno())) == _stamp(before), changed)
        _need(_stamp(_file(path, changed, missing)) == _stamp(before) and
              _directory(path.parent, changed, missing) == parent, changed)
        return _Capture(path, parent, _stamp(before), data, limit)
    except OSError:
        # A read/close failure is not, by itself, evidence of a mutation.
        if previous is not None and data is not None:
            _need(data == previous.data, changed)
        _need(_stamp(_file(path, changed, missing)) == _stamp(before) and
              _directory(path.parent, changed, missing) == parent, changed)
        raise ReferenceFailure(missing) from None


def _recheck(capture, changed, unavailable):
    current = _capture(capture.path, capture.limit, missing=unavailable, changed=changed,
                       previous=capture)
    _need(current.stamp == capture.stamp and current.parent_identity == capture.parent_identity
          and current.data == capture.data, changed)


def _candidate(csv_path, vocabulary_path, reference_id, revision, maintainer_id, note):
    _identities(reference_id, revision, maintainer_id, note)
    _match(maintainer_id, IDENTIFIER, 'ARGUMENT_INVALID', field='maintainer_id')
    # Both input intake bounds precede vocabulary/CSV semantic validation.
    csv_input = _capture(_path(csv_path), CSV_LIMIT)
    vocabulary = _capture(_path(vocabulary_path), VOCABULARY_LIMIT)
    _vocabulary(vocabulary.data)
    rows = _csv(csv_input.data)
    manifest = {
        'manifest_version': 1, 'csv_schema_version': 1, 'validator_revision': 1,
        'reference_id': reference_id, 'revision': revision, 'semantic_config': dict(SEMANTICS),
        'authoring': {'maintainer_id': maintainer_id, 'note': note},
        'csv': {'file': 'reference.csv', 'byte_length': len(csv_input.data),
                'row_count': len(rows), 'sha256': sha256(csv_input.data).hexdigest()},
        'vocabulary': {'file': 'vocabulary.json', 'byte_length': len(vocabulary.data),
                       'sha256': sha256(vocabulary.data).hexdigest(), 'version': 1},
    }
    data = canonical_bytes(manifest)
    _need(len(data) <= MANIFEST_LIMIT, 'RESOURCE_LIMIT')
    reference = Reference(reference_id, revision, sha256(data).hexdigest(), rows, _freeze(manifest))
    _recheck(csv_input, 'INPUT_CHANGED', 'INPUT_UNAVAILABLE')
    _recheck(vocabulary, 'INPUT_CHANGED', 'INPUT_UNAVAILABLE')
    return _Candidate(reference, data, csv_input, vocabulary)


def validate(csv_path, vocabulary_path, reference_id, revision, maintainer_id, note=None):
    """Return detached immutable validated rows, identity and provenance; no writes."""
    return _candidate(csv_path, vocabulary_path, reference_id, revision, maintainer_id, note).reference


def _manifest(data):
    code = 'SNAPSHOT_INVALID'
    value = _json(data, code)
    _keys(value, ('manifest_version', 'csv_schema_version', 'validator_revision', 'reference_id',
                  'revision', 'semantic_config', 'authoring', 'csv', 'vocabulary'), code)
    for name in ('manifest_version', 'csv_schema_version', 'validator_revision'):
        _integer(value[name], 1, 1, code)
    _match(value['reference_id'], IDENTIFIER, code)
    _match(value['revision'], REVISION, code)
    _keys(value['semantic_config'], SEMANTICS, code)
    _need(value['semantic_config'] == SEMANTICS, code)
    _keys(value['authoring'], ('maintainer_id', 'note'), code)
    _match(value['authoring']['maintainer_id'], IDENTIFIER, code)
    if value['authoring']['note'] is not None:
        _text(value['authoring']['note'], 256, code)
    for name, low, high in (('csv', len(HEADER), CSV_LIMIT), ('vocabulary', 1, VOCABULARY_LIMIT)):
        item = value[name]
        extra = 'row_count' if name == 'csv' else 'version'
        _keys(item, ('file', 'byte_length', 'sha256', extra), code)
        _need(item['file'] == ('reference.csv' if name == 'csv' else 'vocabulary.json'), code)
        _integer(item['byte_length'], low, high, code)
        _match(item['sha256'], HEX, code)
        _integer(item[extra], 0 if name == 'csv' else 1, ROW_LIMIT if name == 'csv' else 1, code)
    return value


def _completion(data):
    code = 'SNAPSHOT_INVALID'
    value = _json(data, code)
    _keys(value, ('completion_version', 'snapshot_fingerprint', 'manifest_byte_length', 'created_at'), code)
    _integer(value['completion_version'], 1, 1, code)
    _integer(value['manifest_byte_length'], 1, MANIFEST_LIMIT, code)
    _match(value['snapshot_fingerprint'], HEX, code)
    _match(value['created_at'], UTC, code)
    try:
        datetime.strptime(value['created_at'], '%Y-%m-%dT%H:%M:%S.%fZ')
    except ValueError:
        raise ReferenceFailure(code) from None
    return value


def _exists(path):
    try:
        path.lstat()
        return True
    except FileNotFoundError:
        return False
    except OSError:
        raise ReferenceFailure('SNAPSHOT_UNAVAILABLE') from None


def _root(root):
    root = _path(root)
    _directory(root, 'SNAPSHOT_UNAVAILABLE')
    return root


def _destination(root, reference_id, revision):
    return root / f'reference_id={reference_id}' / f'revision={revision}'


def _guards(root, destination):
    return tuple((path, _directory(path, 'SNAPSHOT_UNAVAILABLE'))
                 for path in (root, destination.parent, destination))


def _check_guards(guards):
    for path, identity in guards:
        _need(_directory(path, 'SNAPSHOT_CHANGED', 'SNAPSHOT_INVALID') == identity,
              'SNAPSHOT_CHANGED')


def _directory_stamp(path):
    try:
        return _stamp(path.lstat())
    except FileNotFoundError:
        # Called only after the directory has already been observed by guards.
        raise ReferenceFailure('SNAPSHOT_CHANGED') from None
    except OSError:
        raise ReferenceFailure('SNAPSHOT_INVALID') from None


def _inventory(destination, expected, code='SNAPSHOT_INVALID'):
    names = set()
    try:
        with os.scandir(destination) as entries:
            for entry in entries:
                _need(entry.name in expected and entry.name not in names, code)
                _file(destination / entry.name, code, 'SNAPSHOT_INVALID')
                names.add(entry.name)
        _need(names == expected, code)
    except FileNotFoundError:
        raise ReferenceFailure(code) from None
    except OSError:
        raise ReferenceFailure('SNAPSHOT_INVALID') from None


def _verify(root, destination, reference_id, revision, expected=None, *, pending=False, temporary=False):
    """Private pre-completion mode is never exposed by the public opener."""
    guards = _guards(root, destination)
    directory_stamp = _directory_stamp(destination)
    names = {'reference.csv', 'vocabulary.json', 'manifest.json'}
    marker_name = 'complete.tmp' if temporary else 'complete.json'
    if not pending or temporary:
        _need(_exists(destination / marker_name), 'SNAPSHOT_INCOMPLETE')
        names.add(marker_name)
    _inventory(destination, names)
    captures = []

    def read(name, limit):
        capture = _capture(destination / name, limit, 'SNAPSHOT_INVALID', 'SNAPSHOT_CHANGED')
        captures.append(capture)
        return capture.data

    marker = _completion(read(marker_name, COMPLETION_LIMIT)) if not pending or temporary else None
    manifest_bytes = read('manifest.json', MANIFEST_LIMIT)
    manifest = _manifest(manifest_bytes)
    fingerprint = sha256(manifest_bytes).hexdigest()
    if marker:
        _need(marker['manifest_byte_length'] == len(manifest_bytes) and
              marker['snapshot_fingerprint'] == fingerprint, 'SNAPSHOT_INVALID')
    _need(expected is None or expected == fingerprint, 'IDENTITY_MISMATCH')
    _need(manifest['reference_id'] == reference_id and manifest['revision'] == revision, 'IDENTITY_MISMATCH')
    vocabulary = read('vocabulary.json', VOCABULARY_LIMIT)
    csv_bytes = read('reference.csv', CSV_LIMIT)
    for name, data in (('vocabulary', vocabulary), ('csv', csv_bytes)):
        _need(manifest[name]['byte_length'] == len(data) and
              manifest[name]['sha256'] == sha256(data).hexdigest(), 'SNAPSHOT_INVALID')
    try:
        _vocabulary(vocabulary)
        rows = _csv(csv_bytes)
    except ReferenceFailure as error:
        if error.code in ('CSV_INVALID', 'VOCABULARY_INVALID'):
            raise ReferenceFailure('SNAPSHOT_INVALID') from None
        raise
    _need(manifest['csv']['row_count'] == len(rows), 'SNAPSHOT_INVALID')
    for capture in captures:
        _recheck(capture, 'SNAPSHOT_CHANGED', 'SNAPSHOT_INVALID')
    _check_guards(guards)
    _inventory(destination, names, 'SNAPSHOT_CHANGED')
    _need(_directory_stamp(destination) == directory_stamp, 'SNAPSHOT_CHANGED')
    result = Reference(reference_id, revision, fingerprint, rows, _freeze(manifest),
                       marker['created_at'] if marker else None)
    return result, tuple(captures)


def open_snapshot(root, reference_id, revision, expected_fingerprint):
    """Independently verify the exact selection and return immutable detached data."""
    _identities(reference_id, revision)
    _match(expected_fingerprint, HEX, 'ARGUMENT_INVALID')
    root = _root(root)
    try:
        return _verify(root, _destination(root, reference_id, revision), reference_id,
                       revision, expected_fingerprint)[0]
    except OSError:
        raise ReferenceFailure('SNAPSHOT_INVALID') from None


def _write(path, data):
    with path.open('xb') as stream:
        _need(stream.write(data) == len(data), 'PUBLICATION_FAILED')


def _now():
    return datetime.now(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')


def publish(root, csv_path, vocabulary_path, reference_id, revision, maintainer_id, note=None):
    """Validate afresh, bind one revision exclusively, or verify read-only replay."""
    _identities(reference_id, revision, maintainer_id, note)
    _match(maintainer_id, IDENTIFIER, 'ARGUMENT_INVALID', field='maintainer_id')
    root = _root(root)
    root_identity = _directory(root, 'SNAPSHOT_UNAVAILABLE')
    for value in (csv_path, vocabulary_path):
        path = _path(value)
        _directory(path.parent, 'INPUT_UNAVAILABLE')
        _file(path, 'INPUT_UNAVAILABLE')
        _need(not path.resolve().is_relative_to(root.resolve()), 'PATH_UNSAFE')
    candidate = _candidate(csv_path, vocabulary_path, reference_id, revision, maintainer_id, note)
    _need(_directory(root, 'SNAPSHOT_CHANGED', 'SNAPSHOT_INVALID') == root_identity,
          'SNAPSHOT_CHANGED')
    reference = candidate.reference
    destination = _destination(root, reference_id, revision)
    if _exists(destination.parent):
        _directory(destination.parent, 'SNAPSHOT_UNAVAILABLE')
    if _exists(destination):
        # Verify the existing binding against itself, NOT the new candidate.
        existing, _ = _verify(root, destination, reference_id, revision)
        _need(existing.fingerprint == reference.fingerprint, 'REVISION_CONFLICT')
        _recheck(candidate.csv, 'INPUT_CHANGED', 'INPUT_UNAVAILABLE')
        _recheck(candidate.vocabulary, 'INPUT_CHANGED', 'INPUT_UNAVAILABLE')
        return reference.summary('replayed')
    try:
        _need(_directory(root, 'SNAPSHOT_CHANGED', 'PUBLICATION_FAILED') == root_identity,
              'SNAPSHOT_CHANGED')
        if not _exists(destination.parent):
            destination.parent.mkdir()
        _directory(destination.parent, 'SNAPSHOT_CHANGED', 'PUBLICATION_FAILED')
        destination.mkdir()
        guards = _guards(root, destination)
        for name, data in (('reference.csv', candidate.csv.data),
                           ('vocabulary.json', candidate.vocabulary.data),
                           ('manifest.json', candidate.manifest_bytes)):
            _check_guards(guards)
            _write(destination / name, data)
        _, artifacts = _verify(root, destination, reference_id, revision,
                               reference.fingerprint, pending=True)

        def recheck():
            _check_guards(guards)
            _recheck(candidate.csv, 'INPUT_CHANGED', 'INPUT_UNAVAILABLE')
            _recheck(candidate.vocabulary, 'INPUT_CHANGED', 'INPUT_UNAVAILABLE')
            for capture in artifacts:
                _recheck(capture, 'SNAPSHOT_CHANGED', 'PUBLICATION_FAILED')

        recheck()
        marker_bytes = canonical_bytes({'completion_version': 1, 'created_at': _now(),
                                         'manifest_byte_length': len(candidate.manifest_bytes),
                                         'snapshot_fingerprint': reference.fingerprint})
        _write(destination / 'complete.tmp', marker_bytes)
        temporary = _capture(destination / 'complete.tmp', COMPLETION_LIMIT,
                             'PUBLICATION_FAILED', 'SNAPSHOT_CHANGED')
        _completion(temporary.data)
        _need(temporary.data == marker_bytes, 'PUBLICATION_FAILED')
        recheck()
        _verify(root, destination, reference_id, revision, reference.fingerprint,
                pending=True, temporary=True)
        recheck()
        _recheck(temporary, 'SNAPSHOT_CHANGED', 'PUBLICATION_FAILED')
        _inventory(destination, {'reference.csv', 'vocabulary.json', 'manifest.json', 'complete.tmp'},
                   'SNAPSHOT_CHANGED')
        _need(not _exists(destination / 'complete.json'), 'SNAPSHOT_CHANGED')
        (destination / 'complete.tmp').rename(destination / 'complete.json')
        return reference.summary('published')
    except ReferenceFailure as error:
        if error.code in ('INPUT_CHANGED', 'SNAPSHOT_CHANGED', 'PATH_UNSAFE', 'RESOURCE_LIMIT'):
            raise
        raise ReferenceFailure('PUBLICATION_FAILED') from None
    except OSError:
        raise ReferenceFailure('PUBLICATION_FAILED') from None


def _arguments(argv):
    """Small fixed option grammar avoids argparse echoing untrusted arguments."""
    _need(bool(argv) and argv[0] in ('validate', 'publish', 'verify'), 'ARGUMENT_INVALID')
    command = argv[0]
    required = {'root', 'reference-id', 'revision', 'expected-fingerprint'} if command == 'verify' else {
        'csv', 'vocabulary', 'reference-id', 'revision', 'maintainer-id'}
    if command == 'publish':
        required.add('root')
    allowed = required | ({'note'} if command != 'verify' else set())
    options = {}
    _need(len(argv) % 2 == 1 and len(argv) <= 17, 'ARGUMENT_INVALID')
    for index in range(1, len(argv), 2):
        name = argv[index]
        _need(name.startswith('--') and name[2:] in allowed and name[2:] not in options,
              'ARGUMENT_INVALID')
        _need(not argv[index + 1].startswith('--'), 'ARGUMENT_INVALID')
        options[name[2:]] = argv[index + 1]
    _need(required <= options.keys(), 'ARGUMENT_INVALID')
    return command, {key.replace('-', '_'): value for key, value in options.items()}


def main(argv=None):
    try:
        command, options = _arguments(sys.argv[1:] if argv is None else argv)
        if command == 'verify':
            result = open_snapshot(**options).summary('verified')
        else:
            options['csv_path'] = options.pop('csv')
            options['vocabulary_path'] = options.pop('vocabulary')
            result = publish(**options) if command == 'publish' else validate(**options).summary('validated')
        output = canonical_bytes(result)
        _need(len(output) + 1 <= COMPLETION_LIMIT, 'RESOURCE_LIMIT')
        print(output.decode('utf-8'))
        return 0
    except ReferenceFailure as error:
        print(canonical_bytes(error.diagnostic).decode('utf-8'), file=sys.stderr)
        return 2 if error.code == 'ARGUMENT_INVALID' else 1
    except (OSError, ValueError):
        # Do not expose runtime/OS details through the CLI boundary.
        error = ReferenceFailure('PUBLICATION_FAILED' if command == 'publish' else 'INPUT_UNAVAILABLE')
        print(canonical_bytes(error.diagnostic).decode('utf-8'), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
