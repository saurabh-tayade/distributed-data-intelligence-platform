"""Inactive v1 sizing projections from captured complete.tmp, with no file I/O.

A projection is not native verification, admission, or permission to publish.
Callers must retain native evidence checks before rename, obtain real post-rename
verification, compare it, and qualify actual complete envelopes independently.
No publisher, bridge, runtime manifest or dispatcher imports this module.
"""
from dataclasses import dataclass
from functools import wraps
from hashlib import sha256
import json
from pathlib import Path
import stat
import re

import content_metadata as m
import gold_reader
from publication_metadata import canonical_bytes

GOLD_TABLES = ('gold_articles', 'gold_source_daily')
COMMON = {'protocolVersion', 'mode', 'runId', 'attemptId', 'ownerEpoch', 'checkpointId',
          'runtimeSha256', 'inputSha256', 'runtime', 'prepared', 'verified'}
FIELDS = {'GOLD': COMMON | {'silverBindingId', 'inputs', 'silver', 'gold'},
          'CONTENT': COMMON | {'goldProof', 'reference', 'referenceBindingId', 'referenceBinding', 'content'}}
REPORTS = ('catalog', 'reference_coverage', 'source_discovery_daily')


class ProjectionError(ValueError):
    def __init__(self):
        self.code = 'CONTENT_PROJECTION_INVALID'
        super().__init__(self.code)


def _need(value):
    if not value:
        raise ProjectionError()


def _safe(function):
    @wraps(function)
    def checked(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except Exception:
            raise ProjectionError() from None
    return checked


@dataclass(frozen=True, repr=False)
class ContextV1:
    """In-memory binding only, not a new serialized command/descriptor protocol."""
    kind: str
    output_binding_id: str
    command_bytes: bytes
    binding_bytes: bytes

    def __repr__(self):
        return 'PublicationProjectionContextV1[not-authority]'


@dataclass(frozen=True, repr=False)
class SizingProjectionV1:
    """Immutable snapshot. Deliberately not directly JSON-serializable as evidence."""
    context: ContextV1
    _sizing_bytes: bytes

    def sizing_value(self):
        """Fresh complete hypothetical verification shape, only for envelope sizing."""
        return json.loads(self._sizing_bytes)

    def __repr__(self):
        return 'PublicationSizingProjectionV1[not-native-verification]'


@_safe
def bind_context_v1(kind, command, *, runtime_configuration, output_binding):
    """Bind to independently loaded runtime values; never derive expected binding from command.

    This pure function cannot load/check a runtime or establish provenance for the
    supplied configuration. The command digest must match its complete canonical
    configuration, and the output binding must match the independent loaded value.
    """
    _need(kind in FIELDS)
    m.keys(command, FIELDS[kind])
    _need(type(command['protocolVersion']) is int and command['protocolVersion'] == 1
          and command['mode'] == 'PUBLISH' and command['verified'] is None
          and type(command['prepared']) is dict)
    m.keys(output_binding, ('root', 'rootDevice', 'rootInode'))
    root = Path(output_binding['root'])
    _need(root.is_absolute() and str(root) == output_binding['root'] and '..' not in root.parts)
    for name in ('rootDevice', 'rootInode'):
        value = output_binding[name]
        _need(type(value) is str and str(int(value)) == value and int(value) >= 0)
    key = 'goldBindingId' if kind == 'GOLD' else 'contentBindingId'
    binding_id = runtime_configuration[key]
    _need(type(binding_id) is str and bool(binding_id))
    _need(sha256(canonical_bytes(runtime_configuration)).hexdigest() == command['runtimeSha256'])
    binding_bytes = canonical_bytes(output_binding)
    _need(canonical_bytes(command[kind.lower()]) == binding_bytes)
    return ContextV1(kind, binding_id, canonical_bytes(command), binding_bytes)


def _identity(value, directory):
    _need(type(value) is tuple and len(value) == (3 if directory else 6)
          and all(type(part) is int for part in value))
    _need(stat.S_ISDIR(value[2]) if directory else stat.S_ISREG(value[4]) and value[5] == 1)


def _capture(context, evidence):
    """Check retained capture structure, then substitute only the final marker name."""
    _need(type(context) is ContextV1 and type(evidence) is m.Evidence)
    binding = json.loads(context.binding_bytes)
    command = json.loads(context.command_bytes)
    prepared = command['prepared']
    identity = prepared['inputSetFingerprint' if context.kind == 'GOLD' else 'fingerprint']
    m.match(identity)
    root = Path(binding['root'])
    namespace = 'input_set=' if context.kind == 'GOLD' else 'publication='
    _need(evidence.directory == root / 'v1' / 'processor_revision=1' / (namespace + identity))
    tables = GOLD_TABLES if context.kind == 'GOLD' else m.TABLES
    top = {'inputs.json', 'complete.tmp', *tables}
    if context.kind == 'CONTENT':
        top |= {'gold-inputs.json', 'gold-complete.json'}
    _need(evidence.tables == tables and evidence.top == frozenset(top))
    guards = tuple(m.namespace_guards(root, evidence.directory))
    _need(tuple(path for path, _ in evidence.guards) == guards)
    for _, identity_value in evidence.guards:
        _identity(identity_value, True)
    _need([str(value) for value in evidence.guards[0][1][:2]] == [binding['rootDevice'], binding['rootInode']])
    directories = (evidence.directory, *(evidence.directory / table for table in tables))
    _need(tuple(path for path, _ in evidence.directories) == directories)
    expected_files = []
    for path, (identity_value, names) in evidence.directories:
        _identity(identity_value, True)
        _need(type(names) is tuple and names == tuple(sorted(set(names), key=lambda name: name.encode('utf-8'))))
        _need(all(type(name) is str and name not in ('', '.', '..') and '/' not in name and
                  not any(char in name for char in '\\*?[]') for name in names))
        if path == evidence.directory:
            _need(set(names) == top)
        else:
            parquet = {name for name in names if name.endswith('.parquet') and not name.startswith(('.', '_'))}
            _need(bool(parquet) and set(names) <= parquet | {'_SUCCESS', '._SUCCESS.crc'} | {'.' + name + '.crc' for name in parquet})
        expected_files.extend(path / name for name in names if path != evidence.directory or name not in tables)
    _need(tuple(item.path for item in evidence.files) == tuple(expected_files))
    for item in evidence.files:
        _need(type(item) is m.FileEvidence)
        _identity(item.stamp, False)
        _need(type(item.length) is int and item.length >= 0 and item.stamp[2] == item.length)
        m.match(item.digest)
        _need(len(item.path.relative_to(evidence.directory).as_posix().encode()) <= 512)
        if item.path.parent == evidence.directory:
            _need(type(item.data) is bytes and len(item.data) == item.length
                  and sha256(item.data).hexdigest() == item.digest)
    raw = evidence.raw('complete.tmp')
    _need(len(raw) <= m.COMPLETION_LIMIT)
    _need(evidence.raw('inputs.json') == canonical_bytes(prepared['manifest'])
          and sha256(evidence.raw('inputs.json')).hexdigest() == identity)
    inventory = evidence.inventory()
    m.validate_inventory(inventory, tables)
    value = dict(
        files=[dict(path='complete.json' if item.path == evidence.directory / 'complete.tmp' else
                    item.path.relative_to(evidence.directory).as_posix(), byteLength=item.length,
                    sha256=item.digest, stamp=[str(part) for part in item.stamp]) for item in evidence.files],
        directories=[dict(path=path.relative_to(evidence.directory).as_posix(),
            identity=[str(part) for part in identity_value],
            names=sorted(('complete.json' if name == 'complete.tmp' and path == evidence.directory else name
                          for name in names), key=lambda name: name.encode('utf-8')))
            for path, (identity_value, names) in evidence.directories],
        guards=[dict(path=path.relative_to(root).as_posix(), identity=[str(part) for part in identity_value])
                for path, identity_value in evidence.guards])
    return command, prepared, raw, inventory, value


@_safe
def project_gold_v1(context, temporary_evidence):
    """Size-only Gold VERIFY shape, based on the actual candidate marker inode/bytes."""
    _need(type(context) is ContextV1 and context.kind == 'GOLD')
    _, prepared, raw, inventory, capture = _capture(context, temporary_evidence)
    m.keys(prepared, ('manifest', 'inputSetFingerprint', 'counts'))
    marker = m.parse(raw)
    gold_reader._validate_marker(marker, prepared['inputSetFingerprint'])
    gold_reader._validate_manifest(prepared['manifest'], marker)
    _need(canonical_bytes(marker['files']) == canonical_bytes(inventory)
          and canonical_bytes(marker['input_counts']) == canonical_bytes(prepared['counts']))
    value = dict(status='VERIFIED', prepared=prepared, completion=marker, capture=capture,
        completionSha256=sha256(raw).hexdigest(),
        verifiers=['gold_publication.verify_publication', 'gold_analytics.open_publication'])
    return SizingProjectionV1(context, canonical_bytes(value))


def _coverage(value):
    from content_analytics import TOTAL_NAMES
    m.keys(value, TOTAL_NAMES)
    for count in value.values():
        _need(type(count) is str and re.fullmatch(r'0|[1-9][0-9]{0,18}', count) is not None
              and int(count) <= m.MAX_COUNT)


def _smoke_shape():
    # SHA-256 is always 64 lowercase ASCII characters; limit=1 stores rows 0 or 1.
    # Every permitted value has the same canonical width. These are SIZE-ONLY
    # representatives, never claims that a native opener/report has run.
    return [dict(name=name, sha256='0' * 64, rows=0) for name in REPORTS]


@_safe
def project_content_v1(context, temporary_evidence, *, coverage):
    """Size-only Content shape: exact public coverage and fixed-width smoke grammar.

    Native completed-publication opening and three smoke reports happen AFTER
    completion. No pretend completed opener or fabricated smoke success is used.
    Caller obtains public coverage from the real precompletion validation relation.
    """
    _need(type(context) is ContextV1 and context.kind == 'CONTENT')
    _, prepared, raw, inventory, capture = _capture(context, temporary_evidence)
    m.keys(prepared, ('manifest', 'fingerprint'))
    manifest = m.validate_manifest(canonical_bytes(prepared['manifest']))
    marker = m.validate_completion(raw, prepared['fingerprint'], len(canonical_bytes(manifest)))
    _need(canonical_bytes(marker['files']) == canonical_bytes(inventory))
    _coverage(coverage)
    value = dict(status='VERIFIED', prepared=prepared, completion=marker,
        completionSha256=sha256(raw).hexdigest(), capture=capture, coverage=coverage,
        smoke=_smoke_shape(), verifier='content_analytics.open_publication')
    return SizingProjectionV1(context, canonical_bytes(value))


@_safe
def compare_actual_v1(projection, actual_verification):
    """Compare real verification with the projection; never return substitute evidence.

    Content smoke hashes and 0/1 row values are checked against their exact fixed
    grammar and width, not against invented precompletion results. Every other
    field must match exactly. Native qualification remains independently required.
    """
    _need(type(projection) is SizingProjectionV1)
    actual_bytes = canonical_bytes(actual_verification)
    actual = json.loads(actual_bytes)
    _need(len(actual_bytes) == len(projection._sizing_bytes))
    if projection.context.kind == 'CONTENT':
        smoke = actual['smoke']
        _need(type(smoke) is list and len(smoke) == len(REPORTS))
        for name, entry in zip(REPORTS, smoke, strict=True):
            m.keys(entry, ('name', 'sha256', 'rows'))
            m.match(entry['sha256'])
            _need(entry['name'] == name and type(entry['rows']) is int and entry['rows'] in (0, 1))
        actual['smoke'] = _smoke_shape()
    _need(canonical_bytes(actual) == projection._sizing_bytes)
