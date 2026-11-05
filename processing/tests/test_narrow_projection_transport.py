"""Inactive codec, complete synthetic correspondence and actual consumer-body tests."""
from copy import deepcopy
from dataclasses import FrozenInstanceError
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path[:0] = [str(Path(__file__).resolve().parent), str(Path(__file__).resolve().parents[1])]
import narrow_projection_transport as t
from narrow_transport_json import canonical_envelope, parse_envelope
from publication_metadata import canonical_bytes
import projection_transport_fixtures as f


class TransportTest(unittest.TestCase):
    def setUp(self):
        self.expected, self.gold, self.gv, self.content, self.cv = f.fixture()

    def bad(self, function, *args, **kwargs):
        with self.assertRaises(t.TransportError) as caught:
            function(*args, **kwargs)
        self.assertEqual(str(caught.exception), 'CONTENT_TRANSPORT_INVALID')

    def read(self, value, expectations=None):
        return t.read_command_v2(canonical_bytes(value), expectations=expectations or self.expected)

    def test_exact_versions_context_and_copy_isolation_without_io(self):
        for bound in (self.gold, self.content):
            value = bound.value()
            self.assertEqual(value['projectionContext'], dict(projectionContextVersion=1,
                policy=t.POLICY, kind=bound.kind, outputBindingId=bound.kind.lower()))
            with patch.object(Path, 'open', side_effect=AssertionError('I/O')), \
                    patch.object(Path, 'stat', side_effect=AssertionError('I/O')):
                restored = self.read(value)
            self.assertEqual(restored, bound)
            value['projectionContext']['outputBindingId'] = 'changed'
            self.assertNotEqual(value, bound.value())
            receipt = bound.receipts(); receipt[0].clear()
            self.assertEqual(bound.receipts(), [self.expected['original']['silverInput']['receipt']])
            with self.assertRaises(FrozenInstanceError):
                bound.kind = 'CHANGED'
            with self.assertRaises(TypeError):
                canonical_bytes(bound)
            for replacement in (True, 1, 3):
                changed = bound.value(); changed['protocolVersion'] = replacement
                self.bad(self.read, changed)
            for key, replacement in [('projectionContextVersion', True), ('projectionContextVersion', 2),
                    ('policy', 'other'), ('kind', 'SILVER'), ('outputBindingId', 'different')]:
                changed = bound.value(); changed['projectionContext'][key] = replacement
                self.bad(self.read, changed)
            changed = bound.value(); changed['unexpected'] = 1
            self.bad(self.read, changed)
            self.bad(t.create_command_v2, bound.kind, bound.value(), expectations=self.expected)

    def test_independent_runtime_chain_and_binding_correspondences(self):
        self.assertNotEqual(self.expected['readyContentRuntimeSha256'], self.gold.value()['runtimeSha256'])
        self.assertEqual(self.expected['readyContentRuntimeSha256'], self.content.value()['runtimeSha256'])
        variants = []
        for runtime, key in [('contentRuntime', 'goldRuntimeId'), ('contentRuntime', 'goldRuntimeSha256'),
                             ('goldRuntime', 'silverRuntimeId'), ('goldRuntime', 'silverRuntimeSha256')]:
            changed = deepcopy(self.expected); changed[runtime]['configuration'][key] = 'wrong'
            changed[runtime]['sha256'] = f.digest(changed[runtime]['configuration'])
            variants.append(changed)
        changed = deepcopy(self.expected); changed['readyContentRuntimeSha256'] = self.expected['goldRuntime']['sha256']; variants.append(changed)
        for role in ('GOLD', 'CONTENT', 'SILVER', 'REFERENCE', 'TEMP'):
            for key in ('bindingId', 'role'):
                changed = deepcopy(self.expected); changed['bindings'][role][key] = 'wrong'; variants.append(changed)
            changed = deepcopy(self.expected); changed['bindings'][role]['configuration']['rootInode'] = '999'; variants.append(changed)
        for expected in variants:
            self.bad(self.read, self.content.value(), expectations=expected)
        changed = self.gold.value(); changed['runtimeSha256'] = self.expected['readyContentRuntimeSha256']
        self.bad(self.read, changed)
        changed = self.content.value(); changed['runtimeSha256'] = self.expected['goldRuntime']['sha256']
        self.bad(self.read, changed)

    def test_full_original_receipt_only_after_single_fresh_source_checks(self):
        self.assertEqual(set(self.gold.receipts()[0]), set(t.RECEIPT.split()))
        for key in t.RECEIPT.split():
            changed = self.gold.value(); del changed['inputs'][0]['receipt'][key]
            expected = deepcopy(self.expected); expected['original']['silverInput'] = deepcopy(changed['inputs'][0])
            changed['inputSha256'] = f.digest(changed['inputs'])
            self.bad(self.read, changed, expectations=expected)
        for path in [('originRunId',), ('sourceId',), ('receipt', 'runId'), ('receipt', 'attemptId'),
                     ('receipt', 'sourceId'), ('qualification', 'selection', 'storageBindingId'),
                     ('qualification', 'verification', 'capture', 'bronze', 'payloadByteLength')]:
            changed = self.gold.value(); item = changed['inputs'][0]
            leaf = item
            for key in path[:-1]:
                leaf = leaf[key]
            leaf[path[-1]] = 'different'
            item['evidenceSha256'] = f.digest(item['qualification'])
            expected = deepcopy(self.expected); expected['original']['silverInput'] = deepcopy(item)
            changed['inputSha256'] = f.digest(changed['inputs'])
            self.bad(self.read, changed, expectations=expected)
        for items in ([], self.gold.value()['inputs'] * 2):
            changed = self.gold.value(); changed['inputs'] = items; changed['inputSha256'] = f.digest(items)
            self.bad(self.read, changed)
        changed = self.gold.value(); changed['inputs'][0]['receipt']['payloadByteLength'] = True
        expected = deepcopy(self.expected); expected['original']['silverInput'] = deepcopy(changed['inputs'][0])
        changed['inputSha256'] = f.digest(changed['inputs'])
        self.bad(self.read, changed, expectations=expected)

    def test_nested_gold_version_artifact_and_reference_cannot_be_substituted(self):
        for target in ('version', 'context', 'artifact', 'digest', 'reference', 'input'):
            changed = self.content.value()
            if target == 'version':
                changed['goldProof']['command']['protocolVersion'] = 1
                del changed['goldProof']['command']['projectionContext']
                expected = deepcopy(self.expected); expected['goldProof'] = deepcopy(changed['goldProof'])
                self.bad(self.read, changed, expectations=expected)
            elif target == 'context':
                changed['goldProof']['command']['projectionContext']['outputBindingId'] = 'other'
            elif target == 'artifact':
                changed['goldProof']['artifact']['storageBindingId'] = 'other'
                changed['goldProof']['evidenceSha256'] = f.digest(changed['goldProof']['artifact'])
            elif target == 'digest':
                changed['goldProof']['evidenceSha256'] = '0' * 64
            elif target == 'reference':
                changed['reference']['artifactId'] = f.ARTIFACT
            else:
                changed['inputSha256'] = '0' * 64
            self.bad(self.read, changed)
        changed = self.content.value()
        changed['goldProof']['artifactId'] = f.CHECKPOINT
        changed['inputSha256'] = f.digest(t._dependencies(changed))
        self.bad(self.read, changed)

    def test_reference_requires_full_frozen_wrapper_and_qualification(self):
        original = self.content.value()
        for key in original['reference']:
            changed = deepcopy(original); del changed['reference'][key]
            expected = deepcopy(self.expected); expected['reference'] = deepcopy(changed['reference'])
            self.bad(self.read, changed, expectations=expected)
        for key in ('native', 'capture'):
            changed = deepcopy(original); del changed['reference']['qualification']['verification'][key]
            changed['reference']['evidenceSha256'] = f.digest(changed['reference']['qualification'])
            changed['inputSha256'] = f.digest(t._dependencies(changed))
            expected = deepcopy(self.expected); expected['reference'] = deepcopy(changed['reference'])
            self.bad(self.read, changed, expectations=expected)

    def test_full_envelopes_include_nested_context_receipt_and_historical_identity(self):
        for bound, verification in ((self.gold, self.gv), (self.content, self.cv)):
            envelopes = [parse_envelope(raw, t.STAGE_LIMIT) for raw in t.qualify_publication_v2(bound, verification)]
            self.assertEqual(len(envelopes), 12 if bound.kind == 'GOLD' else 13)
            for observed in envelopes[-2:]:
                self.assertEqual(observed['receipts'], bound.receipts())
            recovery = t.recovery_command_v2(bound)
            self.assertEqual(recovery['original']['projectionContext'], bound.value()['projectionContext'])
            self.assertEqual(recovery['original']['attemptId'], f.ATTEMPT)
            self.assertEqual(recovery['original']['ownerEpoch'], 3)
            self.assertEqual(recovery['original']['checkpointId'], f.CHECKPOINT)
            self.assertEqual(recovery['original']['mode'], 'VERIFY')
            self.assertIsNone(recovery['original']['verified'])
        proof = self.content.value()['goldProof']
        self.assertEqual(proof['command'], self.gold.value())
        self.assertEqual(proof['evidenceSha256'], f.digest(proof['artifact']))
        self.assertEqual(proof['artifact']['inputs'][0]['artifactId'], f.ARTIFACT)

    def test_content_inspection_verify_retains_full_prior_verification(self):
        inspected = t.command_value_v2(self.content, 'VERIFY', self.cv['prepared'], self.cv)
        bound = self.read(inspected)
        self.assertEqual(bound.value()['verified'], self.cv)
        self.assertEqual(bound.value()['goldProof'], self.content.value()['goldProof'])
        self.assertEqual(t.qualify_publication_v2(self.content, self.cv)[-3], canonical_bytes(inspected))
        different = deepcopy(inspected)
        different['verified']['prepared']['manifest'] = {'different': True}
        different['verified']['prepared']['fingerprint'] = f.digest(different['verified']['prepared']['manifest'])
        self.bad(self.read, different)
        self.bad(t.command_value_v2, self.gold, 'VERIFY', self.gv['prepared'], self.gv)
        self.bad(t.command_value_v2, self.content, 'PUBLISH', self.cv['prepared'], self.cv)

    def test_all_resume_boundaries_preserve_complete_repeated_values(self):
        for boundary in ('BEFORE_FETCH', 'GOLD', 'CONTENT_INPUTS'):
            kwargs = {} if boundary == 'BEFORE_FETCH' else dict(gold_verification=self.gv, prepared=self.cv['prepared'])
            request, result = [parse_envelope(raw, t.STAGE_LIMIT) for raw in t.resume_v2(self.content, boundary, **kwargs)]
            self.assertEqual(request['reference'], self.content.value()['reference'])
            if boundary == 'BEFORE_FETCH':
                self.assertIsNone(request['contentCommand'])
                self.assertEqual(set(result), {'boundary', 'reference'})
            else:
                self.assertEqual(request['contentCommand']['goldProof']['command'], self.gold.value())
                self.assertEqual(request['contentCommand']['projectionContext'], self.content.value()['projectionContext'])
                self.assertEqual(result['gold'], self.gv)
                self.assertEqual(result['prepared'], self.cv['prepared'])
                self.assertEqual(request['contentCommand']['prepared'], None if boundary == 'GOLD' else self.cv['prepared'])
        self.bad(t.resume_v2, self.content, 'BEFORE_FETCH', gold_verification=self.gv)

    def test_exact_slot_plus_one_and_combined_overhead_not_component_estimates(self):
        def document(length):
            remaining = length - 2; result = []
            while remaining:
                size = min(100000, remaining - 2)
                if result:
                    remaining -= 1; size = min(100000, remaining - 2)
                result.append('x' * size); remaining -= size + 2
            return result
        for destination in sorted(t.DESTINATIONS):
            maximum = 65536 if destination == 'RESULT' else t.STAGE_LIMIT
            self.assertEqual(len(t.qualify_v2(document(maximum), destination)), maximum)
            self.bad(t.qualify_v2, document(maximum + 1), destination)
        verification = deepcopy(self.gv)
        verification['capture'] = document(400000)
        t.qualify_v2(verification, 'GOLD_EVIDENCE')
        # Both complete primitive bodies fit; the complete recheck does not.
        fields = self.gold.value(); del fields['protocolVersion']; del fields['projectionContext']
        fields['prepared']['manifest'] = {'padding': document(350000)}
        fields['prepared']['inputSetFingerprint'] = f.digest(fields['prepared']['manifest'])
        bound = t.create_command_v2('GOLD', fields, expectations=self.expected)
        verification['prepared'] = bound.value()['prepared']
        t.qualify_v2(verification, 'GOLD_EVIDENCE')
        t.artifact_v2(bound, verification)
        self.assertGreater(len(canonical_bytes(bound.value() | dict(mode='RECHECK', verified=verification))), t.STAGE_LIMIT)
        self.bad(t.qualify_publication_v2, bound, verification)

    def test_recovery_wrapper_depth_is_charged_and_versions_not_downgraded(self):
        fields = self.gold.value(); del fields['protocolVersion']; del fields['projectionContext']
        nested = 0
        for _ in range(13):
            nested = [nested]
        fields['prepared']['manifest'] = {'nested': nested}
        fields['prepared']['inputSetFingerprint'] = f.digest(fields['prepared']['manifest'])
        bound = t.create_command_v2('GOLD', fields, expectations=self.expected)
        self.bad(t.recovery_command_v2, bound)


class NativeBodyCorpusTest(unittest.TestCase):
    def test_four_actual_consumer_bodies_with_new_transport_overhead(self):
        import publication_projection_fixtures as native_fixtures
        with tempfile.TemporaryDirectory() as root:
            for name, options in [('small', {}), ('gold', dict(empty=True, gold_stress=True)),
                    ('content', dict(empty=True, content_stress=True)), ('both', dict(empty=True, gold_stress=True, content_stress=True))]:
                with self.subTest(case=name):
                    native = native_fixtures.case(Path(root) / name, **options)
                    # Abbreviated measurement receipts must never enter the new codec.
                    old = json.loads(native['gold'][0].command_bytes)
                    self.assertNotEqual(set(old['inputs'][0]['receipt']), set(t.RECEIPT.split()))
                    fields = old.copy(); del fields['protocolVersion']
                    with self.assertRaises(t.TransportError):
                        t.create_command_v2('GOLD', fields, expectations=f.fixture()[0])
                    # Real verification/capture bodies are reused in distinct, explicitly
                    # synthetic complete command vectors, not claimed fresh-source evidence.
                    if name in ('gold', 'both'):
                        _, gold, gv = f.with_native_bodies(native, gold_only=True)
                        self.assertGreater(len(canonical_bytes(t._artifact('GOLD', gold.value(), gv))), t.STAGE_LIMIT)
                        with self.assertRaises(t.TransportError):
                            t.qualify_publication_v2(gold, gv)
                        with self.assertRaises(t.TransportError):
                            t.artifact_v2(gold, gv)
                        with self.assertRaises(t.TransportError):
                            f.with_native_bodies(native)
                    else:
                        _, gold, gv, content, cv = f.with_native_bodies(native)
                        native_content = json.loads(native['content'][0].command_bytes)
                        expected_reference = native_content['reference'] | dict(role='PUBLISHER_REFERENCE', ordinal=0)
                        self.assertEqual(content.value()['reference'], expected_reference)
                        self.assertEqual(content.value()['referenceBinding'], native_content['referenceBinding'])
                        t.qualify_publication_v2(gold, gv)
                        if name == 'small':
                            t.qualify_publication_v2(content, cv)
                            for boundary in ('BEFORE_FETCH', 'GOLD', 'CONTENT_INPUTS'):
                                kwargs = {} if boundary == 'BEFORE_FETCH' else dict(gold_verification=gv, prepared=cv['prepared'])
                                request, result = [parse_envelope(raw, t.STAGE_LIMIT) for raw in t.resume_v2(content, boundary, **kwargs)]
                                self.assertEqual(request['reference'], expected_reference)
                                self.assertEqual(result['reference'], expected_reference['qualification']['verification'])
                        else:
                            self.assertGreater(len(canonical_bytes(content.value() | dict(mode='RECHECK', verified=cv))), t.STAGE_LIMIT)
                            with self.assertRaises(t.TransportError):
                                t.qualify_publication_v2(content, cv)


if __name__ == '__main__':
    unittest.main()
