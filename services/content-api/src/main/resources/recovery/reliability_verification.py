"""Private E5 recovery envelope adapter v1. Delegates only to the unchanged original inspector."""
import hashlib
import json
import os
from pathlib import Path
import stat
import sys

COMPATIBLE = {'reliability_execution.py': '11a881190b4c77fb6c1c887c0d8cb64e6cd10d5893a44c2fcb4cff759b9af982',
              'reliability_execution_preflight.py': 'c528e4ec78ced7b574ada3efc4e62fbffde4e86149abe1ffd82dddad07192927'}
POLICY = 'E5_RECOVERY_VERIFICATION_V1'


def namespace_stamp(p, root, directory):
    observed = []
    for path in (root, root/'v2', directory.parent, directory):
        try:
            info = path.lstat()
        except FileNotFoundError:
            observed.append(None)
        else:
            p.need(stat.S_ISDIR(info.st_mode) and path.resolve(strict=True) == path, 'PATH_UNSAFE')
            observed.append(p.capture.stamp(info))
    return observed


def main():
    # The owner has already checked the private command and frozen root. Verify again before imports.
    path = Path(sys.argv[1])
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as f:
        s = os.fstat(f.fileno())
        if not (stat.S_ISREG(s.st_mode) and s.st_nlink == 1 and s.st_uid == os.getuid() and s.st_mode & 0o077 == 0 and 0 < s.st_size <= 65536):
            return 1
        raw = f.read(65537)
    hint = json.loads(raw)
    root = Path(hint['runtime']['configuration']['processingRoot'])
    for name, expected in COMPATIBLE.items():
        if hashlib.sha256((root/name).read_bytes()).hexdigest() != expected:
            return 1  # No unqualified native import, output inspection or success evidence.
    sys.path.insert(0, str(root))
    import reliability_execution_preflight as p
    import reliability_execution as native
    from publication_metadata import canonical_bytes
    phase, readiness, code, publication = 'RUNTIME', 'NOT_CHECKED', None, None
    classification = 'EVIDENCE_INVALID'
    c = p.parse(p.read(path, p.COMMAND_LIMIT, private=True), p.COMMAND_LIMIT)
    p.fields(c, ('protocolVersion','recoveryVersion','policy','runId','attemptId','ownerEpoch','stage','itemOrdinal',
        'capturedConfigSha256','checkpointId','runtime','controlId','instanceId','qualificationSha256',
        'bridgeSha256','retainedSha256','nativeFingerprint','originalCommand','expectedPublication','launchReview'))
    p.need(type(c['protocolVersion']) is int and c['protocolVersion'] == 1 and type(c['recoveryVersion']) is int and c['recoveryVersion'] == 1 and c['policy'] == POLICY and c['stage'] == 'VERIFY_RESULT' and c['itemOrdinal'] == 0)
    p.need(c['bridgeSha256'] == sys.argv[2] and c['attemptId'] == c['controlId'])
    for key in ('runId','attemptId','checkpointId','instanceId'):
        p.text(c[key], r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}')
    p.text(c['ownerEpoch'], r'[1-9][0-9]{0,18}')
    p.need(all(c['launchReview'][key] is True for key in ('handlersStopped','unmanagedWritersStopped','orphanReviewComplete')))
    inner = c['originalCommand']
    for key in ('runId','checkpointId','capturedConfigSha256','runtime','retainedSha256','nativeFingerprint'):
        p.need(c[key] == inner[key])
    p.need(inner['stage'] == 'VERIFY_RESULT' and inner['attemptId'] != c['attemptId'])
    try:
        p.verify_runtime(c['runtime'])  # Actual interpreter/packages, before reading any publication/dependency.
        readiness = 'READY'
        phase = 'DEPENDENCIES'
        pack = p.parse(p.read(path.parent/'retained.json', p.EVIDENCE_LIMIT, private=True), p.EVIDENCE_LIMIT)
        fresh = native.retained(inner, pack)  # Original command-to-attempt/epoch checks are unchanged.
        phase = 'OUTPUT'
        import reliability_format_metadata as m
        output = p.root(pack['bundle']['bindings']['output'])
        directory = m.publication_directory(output, c['nativeFingerprint'])
        before_namespace = namespace_stamp(p, output, directory)
        try:
            directory_stat = directory.lstat()
        except FileNotFoundError:
            classification = 'OUTPUT_ABSENT'
        else:
            m.root_path(directory, 'PUBLICATION_UNAVAILABLE')
            try:
                (directory/'complete.json').lstat()
            except FileNotFoundError:
                classification = 'OUTPUT_INCOMPLETE'
            else:
                publication = native.inspect(inner, pack)  # Full schemas, dependencies, ranks and analytical replay.
                if c['expectedPublication'] is not None:
                    p.need(publication == c['expectedPublication'], 'PUBLICATION_CHANGED')
                classification = 'VERIFIED_PUBLICATION'
        # Absence/incompleteness also require dependencies and runtime to remain unchanged.
        phase = 'DEPENDENCIES'
        p.recheck(pack['command'], pack['bundle'], pack['evidence'])
        phase = 'RUNTIME'
        p.verify_runtime(c['runtime'])
        phase = 'OUTPUT'
        if classification in ('OUTPUT_ABSENT', 'OUTPUT_INCOMPLETE'):
            p.need(namespace_stamp(p, output, directory) == before_namespace, 'PUBLICATION_CHANGED')
        if classification == 'OUTPUT_ABSENT':
            try:
                directory.lstat()
            except FileNotFoundError:
                pass
            else:
                raise p.Failure('PUBLICATION_CHANGED')
        elif classification == 'OUTPUT_INCOMPLETE':
            p.need(p.capture.stamp(directory.lstat()) == p.capture.stamp(directory_stat), 'PUBLICATION_CHANGED')
            try:
                (directory/'complete.json').lstat()
            except FileNotFoundError:
                pass
            else:
                raise p.Failure('PUBLICATION_CHANGED')
    except Exception as error:
        code = getattr(error, 'code', ('DEPENDENCY_UNAVAILABLE' if phase == 'DEPENDENCIES' else 'PUBLICATION_UNAVAILABLE') if isinstance(error, OSError) else 'PUBLICATION_INVALID')
        if code not in native.CODES:
            code = 'PUBLICATION_INVALID'
        publication = None
        if phase == 'RUNTIME':
            readiness, classification = 'FAILED', 'RUNTIME_INCOMPATIBLE'
        elif code == 'RESOURCE_LIMIT':
            classification = 'RESOURCE_LIMIT'
        elif code in ('RUNTIME_MISMATCH', 'RUNTIME_INCOMPATIBLE', 'UNSUPPORTED_VERSION'):
            classification = 'RUNTIME_INCOMPATIBLE'
        elif phase == 'DEPENDENCIES' or code.startswith(('DEPENDENCY_', 'BATCH_', 'SNAPSHOT_', 'EVIDENCE_')):
            classification = 'DEPENDENCY_UNAVAILABLE' if code.endswith('_UNAVAILABLE') else 'DEPENDENCY_CHANGED' if code.endswith('_CHANGED') else 'DEPENDENCY_INVALID'
        else:
            classification = 'OUTPUT_UNAVAILABLE' if code.endswith('_UNAVAILABLE') else 'OUTPUT_CHANGED' if code.endswith('_CHANGED') else 'OUTPUT_INVALID'
    evidence = dict(policy=POLICY, runId=c['runId'], attemptId=c['attemptId'], checkpointId=c['checkpointId'],
        ownerEpoch=c['ownerEpoch'], controlId=c['controlId'], instanceId=c['instanceId'],
        qualificationSha256=c['qualificationSha256'], bridgeSha256=c['bridgeSha256'],
        originalAttemptId=inner['attemptId'], originalEpoch=inner['ownerEpoch'], nativeFingerprint=c['nativeFingerprint'],
        classification=classification, phase=phase, readiness=readiness, code=code,
        cleanup=dict(kind='READ_ONLY'), publication=publication)
    result = dict(protocolVersion=1, runId=c['runId'], attemptId=c['attemptId'], checkpointId=c['checkpointId'],
        classification=classification, evidenceSha256=hashlib.sha256(canonical_bytes(evidence)).hexdigest())
    p.need(p.read(path, p.COMMAND_LIMIT, private=True) == raw, 'EVIDENCE_CHANGED')
    p.write_new(path.parent/'evidence.json', canonical_bytes(evidence), p.EVIDENCE_LIMIT)
    p.write_new(path.parent/'result.json', canonical_bytes(result), p.COMMAND_LIMIT)
    return 0  # Cleanly observed classification, never a final run disposition.


try:
    raise SystemExit(main())
except Exception:
    raise SystemExit(1) from None
