"""Inactive v2 managed projection transport, not an execution/admission protocol.

All expectations must be independently obtained and validated by a future caller.
This module checks correspondence only: it performs no SQL, filesystem, native
verification, publication, sink adoption or dispatch. No legacy command downgrade
or mixed-version nested proof is supported. Returned snapshots are size inputs,
never native evidence or permission to execute.
"""
from dataclasses import dataclass
from functools import wraps
from hashlib import sha256
from pathlib import PurePosixPath
import re
from uuid import UUID

from narrow_transport_json import canonical_envelope, parse_envelope

POLICY = 'ddip-content-narrow-v1'
# Existing ContentEvidenceBudget stage slots / RecoveryFiles.MAXIMUM, not new caps.
STAGE_LIMIT = 1024 * 1024
LARGE = 16 * 1024 * 1024
COMMON = {'protocolVersion', 'projectionContext', 'mode', 'runId', 'attemptId',
          'ownerEpoch', 'checkpointId', 'runtimeSha256', 'inputSha256', 'runtime', 'prepared', 'verified'}
FIELDS = {'GOLD': COMMON | {'silverBindingId', 'inputs', 'silver', 'gold'},
          'CONTENT': COMMON | {'goldProof', 'reference', 'referenceBindingId', 'referenceBinding', 'content'}}
RECEIPT = ('receiptVersion runId attemptId sourceId revision capturedConfigSha256 ingestionId '
           'ingestedAt payloadByteLength payloadSha256 journalSha256 storageBindingId '
           'storageBindingSha256 bucket expected payloadContentType metadataContentType')
DESTINATIONS = {'GOLD_COMMAND', 'GOLD_EVIDENCE', 'GOLD_ARTIFACT', 'CONTENT_COMMAND',
                'CONTENT_EVIDENCE', 'CONTENT_ARTIFACT', 'RECOVERY_COMMAND', 'RECOVERY_EVIDENCE',
                'RECOVERY_OBSERVATION', 'RESUME_COMMAND', 'RESUME_EVIDENCE', 'RESULT'}


class TransportError(ValueError):
    def __init__(self):
        self.code = 'CONTENT_TRANSPORT_INVALID'
        super().__init__(self.code)


def _need(value):
    if not value:
        raise TransportError()


def _safe(function):
    @wraps(function)
    def checked(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except Exception:
            raise TransportError() from None
    return checked


def _fields(value, fields):
    _need(type(value) is dict and set(value) == set(fields.split() if type(fields) is str else fields))


def _text(value):
    _need(type(value) is str and bool(value))


def _hash(value):
    _need(type(value) is str and re.fullmatch('[0-9a-f]{64}', value) is not None)


def _uuid(value):
    _need(type(value) is str and str(UUID(value)) == value)


def _uint(value, positive=False):
    _need(type(value) is int and (1 if positive else 0) <= value <= 2**63 - 1)


def _bytes(value, limit=LARGE):
    return canonical_envelope(value, limit, large=True)


def _digest(value):
    return sha256(_bytes(value)).hexdigest()


def _equal(left, right):
    # Canonical comparison preserves integer-vs-boolean distinctions.
    _need(_bytes(left) == _bytes(right))


def _binding(record, role, identifier):
    _fields(record, 'bindingId role configuration')
    _need(record['role'] == role and record['bindingId'] == identifier)
    _text(identifier)
    binding = record['configuration']
    _fields(binding, 'root rootDevice rootInode')
    path = PurePosixPath(binding['root'])
    _need(path.is_absolute() and str(path) == binding['root'] and '..' not in path.parts)
    for key in ('rootDevice', 'rootInode'):
        _need(type(binding[key]) is str and re.fullmatch('0|[1-9][0-9]*', binding[key]) is not None)
    return binding


def _runtime(record):
    _fields(record, 'id configuration sha256')
    _text(record['id']); _hash(record['sha256'])
    _need(type(record['configuration']) is dict and _digest(record['configuration']) == record['sha256'])
    return record['configuration']


def _expectations(expected):
    """Check supplied chain, never load or assert its registration/provenance."""
    _fields(expected, 'policy readyContentRuntimeSha256 contentRuntime goldRuntime silverRuntime bindings original reference goldProof')
    _need(expected['policy'] == POLICY)
    content = _runtime(expected['contentRuntime'])
    gold = _runtime(expected['goldRuntime'])
    silver = _runtime(expected['silverRuntime'])
    _need(expected['readyContentRuntimeSha256'] == expected['contentRuntime']['sha256'])
    _need(content['goldRuntimeId'] == expected['goldRuntime']['id']
          and content['goldRuntimeSha256'] == expected['goldRuntime']['sha256']
          and gold['silverRuntimeId'] == expected['silverRuntime']['id']
          and gold['silverRuntimeSha256'] == expected['silverRuntime']['sha256'])
    bindings = expected['bindings']
    _fields(bindings, 'GOLD CONTENT REFERENCE SILVER TEMP')
    for role, identifier in [('GOLD', gold['goldBindingId']), ('CONTENT', content['contentBindingId']),
            ('REFERENCE', content['referenceBindingId']), ('SILVER', silver['outputBindingId']),
            ('TEMP', silver['tempBindingId'])]:
        _binding(bindings[role], role, identifier)
    _fields(expected['original'], 'runId attemptId sourceId silverInput')
    for key in ('runId', 'attemptId'):
        _uuid(expected['original'][key])
    _text(expected['original']['sourceId'])
    return content, gold, silver


def _source(command, expected):
    """Return the FULL original receipt only after all fresh-source correspondences."""
    original = expected['original']
    _need(type(command['inputs']) is list and len(command['inputs']) == 1)
    item = command['inputs'][0]
    _fields(item, 'artifactId originRunId sourceId evidenceSha256 qualification receipt bronze credentialsSha256')
    _equal(item, original['silverInput'])
    _uuid(item['artifactId']); _hash(item['credentialsSha256'])
    _need(item['originRunId'] == original['runId'] == command['runId']
          and item['sourceId'] == original['sourceId'])
    receipt = item['receipt']
    _fields(receipt, RECEIPT)
    _need(type(receipt['receiptVersion']) is int and receipt['receiptVersion'] == 1)
    _need(receipt['runId'] == original['runId'] and receipt['attemptId'] == original['attemptId']
          and receipt['sourceId'] == original['sourceId'])
    _uuid(receipt['ingestionId']); _uint(receipt['revision'], True); _uint(receipt['payloadByteLength'])
    for key in ('capturedConfigSha256', 'payloadSha256', 'journalSha256', 'storageBindingSha256'):
        _hash(receipt[key])
    for key in ('ingestedAt', 'storageBindingId', 'bucket'):
        _text(receipt[key])
    _need(receipt['payloadContentType'] in ('application/xml', 'application/octet-stream')
          and receipt['metadataContentType'] == 'application/json')
    e = receipt['expected']
    _fields(e, 'metadataKey payloadKey metadataByteLength metadataSha256')
    _uint(e['metadataByteLength']); _hash(e['metadataSha256']); _text(e['payloadKey'])
    _need(type(e['metadataKey']) is str and e['metadataKey'].startswith('bronze/rss/')
          and e['metadataKey'].endswith('/metadata.json'))
    q = item['qualification']
    _fields(q, 'qualificationVersion selection verification')
    _need(type(q['qualificationVersion']) is int and q['qualificationVersion'] == 1)
    _hash(item['evidenceSha256']); _need(item['evidenceSha256'] == _digest(q))
    selection = dict(kind='SILVER', productId='content-intelligence', storageBindingId=command['silverBindingId'],
        contractVersion=1, processorRevision=1,
        relativeAddress='v1/' + sha256(receipt['bucket'].encode()).hexdigest() + '/' +
            e['metadataKey'][len('bronze/rss/'):].removesuffix('metadata.json') + 'complete.json',
        identity=dict(sourceId=receipt['sourceId'], ingestionId=receipt['ingestionId'],
            bronzeStorageBindingId=receipt['storageBindingId'], bronzeBucket=receipt['bucket'],
            bronzeMetadataKey=e['metadataKey'], bronzePayloadKey=e['payloadKey']))
    _equal(q['selection'], selection)
    bronze = dict(bucket=receipt['bucket'], metadataKey=e['metadataKey'], metadataByteLength=e['metadataByteLength'],
        metadataSha256=e['metadataSha256'], payloadKey=e['payloadKey'], payloadByteLength=receipt['payloadByteLength'],
        payloadSha256=receipt['payloadSha256'])
    _equal(q['verification']['capture']['bronze'], bronze)
    _need(item['bronze']['bucket'] == receipt['bucket'])
    _need(command['inputSha256'] == _digest(command['inputs']))
    return receipt


def _prepared(kind, value):
    _fields(value, 'manifest inputSetFingerprint counts' if kind == 'GOLD' else 'manifest fingerprint')
    _need(type(value['manifest']) is dict)
    key = 'inputSetFingerprint' if kind == 'GOLD' else 'fingerprint'
    _hash(value[key]); _need(value[key] == _digest(value['manifest']))
    if kind == 'GOLD':
        _fields(value['counts'], 'accepted accepted_before_replay_collapse rejected')
        for count in value['counts'].values():
            _uint(count)


def _reference(reference, binding_id):
    """Full frozen reference shape; native validity remains independently required."""
    _fields(reference, 'artifactId productId kind contractVersion processorRevision storageBindingId '
            'relativeAddress nativeIdentity evidenceSha256 verifiedAt qualification role ordinal')
    _uuid(reference['artifactId']); _hash(reference['evidenceSha256']); _text(reference['verifiedAt'])
    _need(reference['kind'] == reference['role'] == 'PUBLISHER_REFERENCE'
          and reference['productId'] == 'content-intelligence'
          and type(reference['ordinal']) is int and reference['ordinal'] == 0)
    q = reference['qualification']
    _fields(q, 'qualificationVersion selection verification')
    _need(type(q['qualificationVersion']) is int and q['qualificationVersion'] == 1)
    selection = q['selection']
    _fields(selection, 'kind productId storageBindingId relativeAddress contractVersion processorRevision identity fingerprint')
    _fields(selection['identity'], 'referenceId revision')
    for key in ('kind', 'productId', 'storageBindingId', 'relativeAddress', 'contractVersion', 'processorRevision'):
        _equal(reference[key], selection[key])
    for key in ('contractVersion', 'processorRevision'):
        _need(type(reference[key]) is int and reference[key] == 1)
    _hash(reference['nativeIdentity'])
    _need(reference['nativeIdentity'] == selection['fingerprint']
          and reference['storageBindingId'] == binding_id and reference['evidenceSha256'] == _digest(q))
    _fields(q['verification'], 'evidenceVersion native capture')
    _need(type(q['verification']['evidenceVersion']) is int and q['verification']['evidenceVersion'] == 1)
    _fields(q['verification']['native'], 'verifier fingerprint rowCount csvSchemaVersion validatorRevision dependencies')
    _fields(q['verification']['capture'], 'files directories guards completionBase64')


def _shape(kind, command):
    _need(kind in FIELDS)
    _fields(command, FIELDS[kind])
    _need(type(command['protocolVersion']) is int and command['protocolVersion'] == 2)
    context = command['projectionContext']
    _fields(context, 'projectionContextVersion policy kind outputBindingId')
    _need(type(context['projectionContextVersion']) is int and context['projectionContextVersion'] == 1
          and context['policy'] == POLICY and context['kind'] == kind)
    _text(context['outputBindingId'])
    for key in ('runId', 'attemptId', 'checkpointId'):
        _uuid(command[key])
    _uint(command['ownerEpoch'], True)
    for key in ('runtimeSha256', 'inputSha256'):
        _hash(command[key])
    mode = command['mode']
    _need(mode in ('PREPARE', 'PUBLISH', 'VERIFY', 'RECHECK'))
    if command['prepared'] is not None:
        _prepared(kind, command['prepared'])
    _need(mode == 'PREPARE' or command['prepared'] is not None)
    if mode == 'RECHECK':
        _need(type(command['verified']) is dict)
    elif not (kind == 'CONTENT' and mode == 'VERIFY'):
        _need(command['verified'] is None)
    if command['verified'] is not None:
        _verification(kind, command['verified'])
        _equal(command['prepared'], command['verified']['prepared'])


def _validate(kind, command, expected, runtimes):
    _shape(kind, command)
    content, gold, silver = runtimes
    runtime = expected['goldRuntime' if kind == 'GOLD' else 'contentRuntime']
    output_id = runtime['configuration'][kind.lower() + 'BindingId']
    _need(command['projectionContext']['outputBindingId'] == output_id
          and command['runtimeSha256'] == runtime['sha256'])
    for key in ('runId', 'attemptId'):
        _need(command[key] == expected['original'][key])
    _equal(command[kind.lower()], _binding(expected['bindings'][kind], kind, output_id))
    scratch = expected['bindings']['TEMP']['configuration']
    native = dict(javaHome=silver['javaHome'], scratchRoot=scratch['root'],
                  scratchDevice=scratch['rootDevice'], scratchInode=scratch['rootInode'])
    _equal(command['runtime'], native)
    if kind == 'GOLD':
        _need(command['silverBindingId'] == silver['outputBindingId'])
        _equal(command['silver'], expected['bindings']['SILVER']['configuration'])
        return _source(command, expected)
    proof = command['goldProof']
    _fields(proof, 'artifactId evidenceSha256 artifact command')
    # Content has an actual committed Gold identity. Never accept an ID merely
    # because the inspected command and its dependency digest agree with it.
    _equal(proof, expected['goldProof'])
    _uuid(proof['artifactId']); _hash(proof['evidenceSha256'])
    receipt = _validate('GOLD', proof['command'], expected, runtimes)
    _need(proof['command']['mode'] == 'PUBLISH')
    artifact = _artifact('GOLD', proof['command'], proof['artifact']['verification'])
    _equal(proof['artifact'], artifact)
    _need(proof['evidenceSha256'] == _digest(artifact))
    _need(command['referenceBindingId'] == content['referenceBindingId'])
    _equal(command['referenceBinding'], expected['bindings']['REFERENCE']['configuration'])
    _equal(command['reference'], expected['reference'])
    reference = command['reference']
    _reference(reference, command['referenceBindingId'])
    _need(command['inputSha256'] == _digest(_dependencies(command)))
    return receipt


@dataclass(frozen=True, repr=False)
class CommandV2:
    """Immutable correspondence snapshot; deliberately not a JSON evidence value."""
    kind: str
    _command: bytes
    _receipts: bytes

    def value(self):
        return parse_envelope(self._command, STAGE_LIMIT, large=True)

    def receipts(self):
        return parse_envelope(self._receipts, STAGE_LIMIT, large=True)

    def __repr__(self):
        return 'ProjectionCommandV2[not-authority]'


@_safe
def read_command_v2(raw, *, expectations):
    command = parse_envelope(raw, STAGE_LIMIT, large=True)
    kind = command['projectionContext']['kind']
    receipt = _validate(kind, command, expectations, _expectations(expectations))
    return CommandV2(kind, raw, _bytes([receipt], STAGE_LIMIT))


@_safe
def create_command_v2(kind, fields, *, expectations):
    """Explicit v2 construction from all fields OTHER than version and context.

    This is not a legacy upgrade: passing a v1/v2 command is rejected. Content
    requires an already explicit v2 Gold proof; historical v1 is unsupported.
    """
    _need(kind in FIELDS)
    _fields(fields, FIELDS[kind] - {'protocolVersion', 'projectionContext'})
    runtimes = _expectations(expectations)
    runtime = runtimes[1 if kind == 'GOLD' else 0]
    command = dict(fields, protocolVersion=2, projectionContext=dict(projectionContextVersion=1,
        policy=POLICY, kind=kind, outputBindingId=runtime[kind.lower() + 'BindingId']))
    return read_command_v2(_bytes(command, STAGE_LIMIT), expectations=expectations)


@_safe
def command_value_v2(bound, mode, prepared, verified=None):
    _need(type(bound) is CommandV2)
    command = bound.value() | dict(mode=mode, prepared=prepared, verified=verified)
    _shape(bound.kind, command)
    # Bound before copying complete user-supplied replacement bodies.
    return parse_envelope(_bytes(command, STAGE_LIMIT), STAGE_LIMIT, large=True)


def _dependencies(command):
    return [dict(role=role, artifactId=command[key]['artifactId'], evidenceSha256=command[key]['evidenceSha256'])
            for role, key in [('GOLD', 'goldProof'), ('PUBLISHER_REFERENCE', 'reference')]]


def _verification(kind, value):
    fields = ('status prepared completion capture completionSha256 verifiers' if kind == 'GOLD' else
              'status prepared completion capture completionSha256 coverage smoke verifier')
    _fields(value, fields); _prepared(kind, value['prepared']); _hash(value['completionSha256'])
    _need(value['status'] == 'VERIFIED')


def _artifact(kind, command, verification):
    _shape(kind, command); _verification(kind, verification)
    _need(command['mode'] == 'PUBLISH')
    _equal(command['prepared'], verification['prepared'])
    key = 'inputSetFingerprint' if kind == 'GOLD' else 'fingerprint'
    namespace = 'input_set' if kind == 'GOLD' else 'publication'
    value = {('managedGoldVersion' if kind == 'GOLD' else 'managedContentVersion'): 1,
        'kind': kind, 'productId': 'content-intelligence', 'contractVersion': 1, 'processorRevision': 1,
        'nativeFingerprint': command['prepared'][key], 'storageBindingId': command['projectionContext']['outputBindingId'],
        'relativeAddress': 'v1/processor_revision=1/' + namespace + '=' + command['prepared'][key],
        'verification': verification}
    value['inputs'] = ([dict(artifactId=item['artifactId'], evidenceSha256=item['evidenceSha256'])
                        for item in command['inputs']] if kind == 'GOLD' else _dependencies(command))
    return value


@_safe
def artifact_v2(bound, verification):
    _need(type(bound) is CommandV2)
    value = _artifact(bound.kind, bound.value(), verification)
    return parse_envelope(qualify_v2(value, bound.kind + '_ARTIFACT'), STAGE_LIMIT, large=True)


@_safe
def gold_proof_v2(bound, verification, *, artifact_id):
    """Require an actual caller-supplied identity; do not predict a future UUID.

    A proof has no independent persisted slot. The LARGE bound is the existing
    maximum reader ceiling; its actual enclosing command must still fit 1 MiB.
    """
    _need(type(bound) is CommandV2 and bound.kind == 'GOLD'); _uuid(artifact_id)
    artifact = artifact_v2(bound, verification)
    proof = dict(artifactId=artifact_id, evidenceSha256=_digest(artifact), artifact=artifact, command=bound.value())
    return parse_envelope(_bytes(proof), LARGE, large=True)


@_safe
def qualify_v2(complete, destination):
    _need(destination in DESTINATIONS)
    return canonical_envelope(complete, 65536 if destination == 'RESULT' else STAGE_LIMIT,
                              large=destination != 'RESULT')


@_safe
def recovery_command_v2(bound, *, preparing=False):
    _need(type(bound) is CommandV2 and type(preparing) is bool)
    prepared = None if preparing else bound.value()['prepared']
    original = command_value_v2(bound, 'PREPARE' if prepared is None else 'VERIFY', prepared)
    value = dict(kind=bound.kind, original=original)
    return parse_envelope(qualify_v2(value, 'RECOVERY_COMMAND'), STAGE_LIMIT, large=True)


@_safe
def qualify_publication_v2(bound, verification):
    """Full local envelopes, including complete original receipts and v2 overhead."""
    _need(type(bound) is CommandV2)
    kind, publish = bound.kind, bound.value()
    artifact = artifact_v2(bound, verification)
    prepared = dict(status='PREPARED', prepared=publish['prepared'])
    observations = [dict(state='PAUSED', code='RECOVERED_CHECKPOINT', receipts=bound.receipts(), checkpoint=prepared)]
    observations.append(dict(state='PAUSED', code='RECOVERED_CHECKPOINT', receipts=bound.receipts(), checkpoint=verification)
        if kind == 'GOLD' else dict(state='SUCCEEDED', code='RECOVERED_PUBLICATION', receipts=bound.receipts(), artifact=artifact))
    envelopes = [(publish, kind + '_COMMAND'),
        (command_value_v2(bound, 'PREPARE', None), kind + '_COMMAND'),
        (command_value_v2(bound, 'VERIFY', publish['prepared']), kind + '_COMMAND'),
        (command_value_v2(bound, 'RECHECK', publish['prepared'], verification), kind + '_COMMAND'),
        (prepared, kind + '_EVIDENCE'), (verification, kind + '_EVIDENCE'),
        (artifact, kind + '_ARTIFACT'), (recovery_command_v2(bound, preparing=True), 'RECOVERY_COMMAND'),
        (recovery_command_v2(bound), 'RECOVERY_COMMAND'), (verification, 'RECOVERY_EVIDENCE')]
    if kind == 'CONTENT':
        # RssContentExecution.inspect repeats full prior verification in VERIFY.
        envelopes.append((command_value_v2(bound, 'VERIFY', publish['prepared'], verification), 'CONTENT_COMMAND'))
    envelopes.extend((value, 'RECOVERY_OBSERVATION') for value in observations)
    return tuple(qualify_v2(value, destination) for value, destination in envelopes)


@_safe
def resume_v2(bound, boundary, *, gold_verification=None, prepared=None):
    """Size historical provenance inside a request; no current-writer identity is invented."""
    _need(type(bound) is CommandV2 and bound.kind == 'CONTENT')
    _need(boundary in ('BEFORE_FETCH', 'GOLD', 'CONTENT_INPUTS'))
    command = bound.value()
    request = dict(boundary=boundary, reference=command['reference'], referenceBinding=command['referenceBinding'],
                   content=command['content'], contentCommand=None)
    result = dict(boundary=boundary, reference=command['reference']['qualification']['verification'])
    if boundary == 'BEFORE_FETCH':
        _need(gold_verification is None and prepared is None)
    else:
        _verification('GOLD', gold_verification)
        _equal(gold_verification, command['goldProof']['artifact']['verification'])
        _prepared('CONTENT', prepared)
        if boundary == 'CONTENT_INPUTS':
            _equal(prepared, command['prepared'])
        request['contentCommand'] = command_value_v2(bound, 'PREPARE',
            None if boundary == 'GOLD' else command['prepared'])
        result.update(gold=gold_verification, prepared=prepared)
    return qualify_v2(request, 'RESUME_COMMAND'), qualify_v2(result, 'RESUME_EVIDENCE')
