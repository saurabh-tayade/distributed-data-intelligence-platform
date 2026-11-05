"""Complete synthetic correspondence vectors, never registered or executable.

The original measurement receipt is intentionally abbreviated. These fixtures
construct a distinct full synthetic receipt; they do not relabel measurement
provenance or claim SQL delivery/native Bronze/Silver verification occurred.
"""
from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path

import narrow_projection_transport as t
from publication_metadata import canonical_bytes

RUN = '00000000-0000-4000-8000-000000000001'
ATTEMPT = '00000000-0000-4000-8000-000000000002'
CHECKPOINT = '00000000-0000-4000-8000-000000000003'
ARTIFACT = '00000000-0000-4000-8000-000000000004'
REFERENCE = '00000000-0000-4000-8000-000000000005'
SHA = 'a' * 64


def digest(value):
    return sha256(canonical_bytes(value)).hexdigest()


def binding(name):
    return dict(root='/synthetic-only/' + name, rootDevice='1', rootInode='2')


def fixture():
    repo = Path(__file__).resolve().parents[2]
    qualification = json.loads((repo / 'services/content-api/src/test/resources/narrow-content-profile-native-vectors.json').read_bytes())['qualification']
    identity = qualification['selection']['identity']
    bronze = qualification['verification']['capture']['bronze']
    receipt = dict(receiptVersion=1, runId=RUN, attemptId=ATTEMPT, sourceId=identity['sourceId'], revision=1,
        capturedConfigSha256=SHA, ingestionId=identity['ingestionId'], ingestedAt='2026-11-04T00:00:00.000000Z',
        payloadByteLength=bronze['payloadByteLength'], payloadSha256=bronze['payloadSha256'], journalSha256=SHA,
        storageBindingId='bronze', storageBindingSha256=SHA, bucket=bronze['bucket'],
        expected={key: bronze[key] for key in ('metadataKey', 'payloadKey', 'metadataByteLength', 'metadataSha256')},
        payloadContentType='application/xml', metadataContentType='application/json')
    selected = dict(artifactId=ARTIFACT, originRunId=RUN, sourceId=receipt['sourceId'],
        evidenceSha256=digest(qualification), qualification=qualification, receipt=receipt,
        bronze=dict(endpoint='http://127.0.0.1:8333', region='us-east-1', bucket=bronze['bucket'],
                    credentialsFile='/synthetic-only/not-a-credential-file'), credentialsSha256=SHA)
    silver = dict(outputBindingId='silver', tempBindingId='temp', javaHome='/synthetic-only/java')
    gold = dict(goldBindingId='gold', silverRuntimeId='silver-runtime', silverRuntimeSha256=digest(silver))
    content = dict(contentBindingId='content', referenceBindingId='reference', goldRuntimeId='gold-runtime',
                   goldRuntimeSha256=digest(gold))
    # Complete disposable captured shape, loaded without DuckDB/native readers.
    # Its old stamps are not current filesystem or registration evidence.
    reference = json.loads(Path(__file__).with_name('projection_transport_reference_vector.json').read_bytes())
    reference['artifactId'] = REFERENCE
    expected = dict(policy=t.POLICY, readyContentRuntimeSha256=digest(content),
        contentRuntime=dict(id='content-runtime', configuration=content, sha256=digest(content)),
        goldRuntime=dict(id='gold-runtime', configuration=gold, sha256=digest(gold)),
        silverRuntime=dict(id='silver-runtime', configuration=silver, sha256=digest(silver)),
        bindings={role: dict(bindingId=role.lower(), role=role, configuration=binding(role.lower()))
                  for role in ('GOLD', 'CONTENT', 'REFERENCE', 'SILVER', 'TEMP')},
        original=dict(runId=RUN, attemptId=ATTEMPT, sourceId=receipt['sourceId'], silverInput=selected),
        reference=reference, goldProof=None)
    native = dict(javaHome=silver['javaHome'], scratchRoot=binding('temp')['root'], scratchDevice='1', scratchInode='2')
    common = dict(mode='PUBLISH', runId=RUN, attemptId=ATTEMPT, ownerEpoch=3, checkpointId=CHECKPOINT,
                  runtime=native, verified=None)
    prepared_gold = dict(manifest={}, inputSetFingerprint=digest({}), counts=dict(accepted=1, accepted_before_replay_collapse=1, rejected=0))
    fields_gold = common | dict(runtimeSha256=digest(gold), inputSha256=digest([selected]),
        silverBindingId='silver', inputs=[selected], silver=binding('silver'), gold=binding('gold'), prepared=prepared_gold)
    verification_gold = dict(status='VERIFIED', prepared=prepared_gold, completion={}, capture={}, completionSha256=SHA,
        verifiers=['gold_publication.verify_publication', 'gold_analytics.open_publication'])
    bound_gold = t.create_command_v2('GOLD', fields_gold, expectations=expected)
    proof = t.gold_proof_v2(bound_gold, verification_gold, artifact_id=ARTIFACT)
    expected['goldProof'] = deepcopy(proof)
    prepared_content = dict(manifest={}, fingerprint=digest({}))
    fields_content = common | dict(runtimeSha256=digest(content), goldProof=proof, reference=reference,
        referenceBindingId='reference', referenceBinding=binding('reference'), content=binding('content'),
        prepared=prepared_content, inputSha256=digest([
            dict(role='GOLD', artifactId=proof['artifactId'], evidenceSha256=proof['evidenceSha256']),
            dict(role='PUBLISHER_REFERENCE', artifactId=reference['artifactId'], evidenceSha256=reference['evidenceSha256'])]))
    verification_content = dict(status='VERIFIED', prepared=prepared_content, completion={}, capture={},
        completionSha256=SHA, coverage={}, smoke=[], verifier='content_analytics.open_publication')
    bound_content = t.create_command_v2('CONTENT', fields_content, expectations=expected)
    return deepcopy(expected), bound_gold, verification_gold, bound_content, verification_content


def with_native_bodies(native, *, gold_only=False):
    """Reuse actual consumer bodies only; enclosing commands remain synthetic."""
    expected, gold, _, content, _ = fixture()
    for kind in ('GOLD', 'CONTENT'):
        expected['bindings'][kind]['configuration'] = json.loads(native[kind.lower()][0].binding_bytes)
    original_content = json.loads(native['content'][0].command_bytes)
    # Preserve the complete actual reference capture, native verification and
    # binding. Role/ordinal belong to the frozen-command wrapper, not native evidence.
    expected['reference'] = original_content['reference'] | dict(role='PUBLISHER_REFERENCE', ordinal=0)
    expected['bindings']['REFERENCE']['configuration'] = original_content['referenceBinding']
    fields_gold = gold.value()
    for name in ('protocolVersion', 'projectionContext'):
        del fields_gold[name]
    fields_gold.update(prepared=native['gold'][3]['prepared'], gold=expected['bindings']['GOLD']['configuration'])
    gold = t.create_command_v2('GOLD', fields_gold, expectations=expected)
    if gold_only:
        return expected, gold, native['gold'][3]
    fields_content = content.value()
    for name in ('protocolVersion', 'projectionContext'):
        del fields_content[name]
    fields_content.update(prepared=native['content'][3]['prepared'], content=expected['bindings']['CONTENT']['configuration'],
        goldProof=t.gold_proof_v2(gold, native['gold'][3], artifact_id=ARTIFACT),
        reference=expected['reference'], referenceBinding=expected['bindings']['REFERENCE']['configuration'])
    fields_content['inputSha256'] = digest(t._dependencies(fields_content))
    expected['goldProof'] = deepcopy(fields_content['goldProof'])
    content = t.create_command_v2('CONTENT', fields_content, expectations=expected)
    return expected, gold, native['gold'][3], content, native['content'][3]
