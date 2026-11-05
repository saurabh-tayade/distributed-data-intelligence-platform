"""Test-only complete v2 envelope exporter; synthetic, never executable evidence."""
import base64
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import narrow_projection_transport as t
from tests.projection_transport_fixtures import fixture


def emit(name, destination, raw):
    print(name + '\t' + destination + '\t' + base64.b64encode(raw).decode('ascii'))


def main():
    _, gold, gv, content, cv = fixture()
    for kind, command, verification in [('GOLD', gold, gv), ('CONTENT', content, cv)]:
        names = ['publish', 'prepare', 'verify', 'recheck', 'prepared', 'verification',
                 'artifact', 'recovery_prepare', 'recovery_verify', 'recovery_evidence',
                 'observation_prepared', 'observation_verified']
        destinations = [kind + '_COMMAND'] * 4 + [kind + '_EVIDENCE'] * 2 + [kind + '_ARTIFACT',
            'RECOVERY_COMMAND', 'RECOVERY_COMMAND', 'RECOVERY_EVIDENCE',
            'RECOVERY_OBSERVATION', 'RECOVERY_OBSERVATION']
        if kind == 'CONTENT':
            names.insert(10, 'inspect')
            destinations.insert(10, 'CONTENT_COMMAND')
        for name, destination, raw in zip(names, destinations, t.qualify_publication_v2(command, verification), strict=True):
            emit(kind.lower() + '_' + name, destination, raw)
    proof = content.value()['goldProof']
    emit('gold_proof', 'NO_INDEPENDENT_SLOT', t.canonical_envelope(proof, t.LARGE))
    for boundary in ('BEFORE_FETCH', 'GOLD', 'CONTENT_INPUTS'):
        arguments = {} if boundary == 'BEFORE_FETCH' else dict(gold_verification=gv, prepared=cv['prepared'])
        request, evidence = t.resume_v2(content, boundary, **arguments)
        emit('resume_' + boundary + '_command', 'RESUME_COMMAND', request)
        emit('resume_' + boundary + '_evidence', 'RESUME_EVIDENCE', evidence)
    emit('result', 'RESULT', t.qualify_v2(dict(status='VERIFIED'), 'RESULT'))


if __name__ == '__main__':
    main()
