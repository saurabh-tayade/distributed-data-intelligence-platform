"""Pure filesystem/serializer tests, not Parquet or Spark qualification claims."""
import base64
from copy import deepcopy
from hashlib import sha256
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import artifact_import_verifier as p
import narrow_content_profile as n
import narrow_silver_precompletion as guard
from publication_metadata import canonical_bytes
import silver_import_qualification as q


class UnfinishedFixture(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        source, ingestion, bucket = 'é-source', '00000000-0000-4000-8000-000000000001', 'local'
        encoded = base64.urlsafe_b64encode(source.encode()).decode().rstrip('=')
        suffix = f'source_id={encoded}/ingestion_date=2026-11-04/ingestion_id={ingestion}/'
        key = 'bronze/rss/' + suffix + 'metadata.json'
        address = 'v1/' + sha256(bucket.encode()).hexdigest() + '/' + suffix + 'complete.json'
        self.artifact = dict(kind='SILVER', productId='content-intelligence', storageBindingId='silver',
            relativeAddress=address, contractVersion=1, processorRevision=1,
            identity=dict(sourceId=source, ingestionId=ingestion, bronzeStorageBindingId='bronze',
                          bronzeBucket=bucket, bronzeMetadataKey=key,
                          bronzePayloadKey=key.removesuffix('metadata.json') + 'payload.xml'))
        self.namespace = self.root / Path(address).parent
        for dataset in ('accepted', 'rejected'):
            (self.namespace / dataset).mkdir(parents=True)
            (self.namespace / dataset / 'part-000.parquet').write_bytes(b'not a Parquet semantic test')
        self.marker = dict(identity=dict(metadata_sha256='a' * 64, payload_sha256='b' * 64,
            contract_version=1, processor_revision=1, bucket=bucket, metadata_key=key),
            counts=dict(accepted=1, rejected=0))
        value = self.root.stat()
        self.binding = dict(root=str(self.root), rootDevice=str(value.st_dev), rootInode=str(value.st_ino))
        self.bronze = dict(bucket=bucket, metadataKey=key, metadataByteLength=100, metadataSha256='a' * 64,
                          payloadKey=self.artifact['identity']['bronzePayloadKey'], payloadByteLength=100,
                          payloadSha256='b' * 64)

    def guarded(self):
        return guard.precompletion_guard_v1(self.artifact, self.binding, self.marker, self.bronze)

    def state(self):
        return {str(path.relative_to(self.root)): (p.stamp(path.lstat()),
                path.read_bytes() if path.is_file() else None)
                for path in [self.root, *self.root.rglob('*')]}

    def bad(self, function, *args, code=None, **kwargs):
        with self.assertRaises((p.Failure, n.ProfileError)) as caught:
            function(*args, **kwargs)
        if code:
            self.assertEqual(str(caught.exception), code)
        return caught.exception

    def request(self):
        return dict(protocolVersion=2, operationId='00000000-0000-4000-8000-000000000002',
                    selectionSha256=sha256(canonical_bytes(self.artifact)).hexdigest(), mode='VERIFY',
                    binding=self.binding, artifact=self.artifact, expectedCapture=None,
                    bronze={}, runtime={})


class ProjectionTest(UnfinishedFixture):
    def test_exact_native_marker_encoding_and_no_effects(self):
        before = self.state()
        result = self.guarded()
        self.assertEqual(result.marker_bytes, json.dumps(self.marker, sort_keys=True).encode())
        self.assertIn(b'": ', result.marker_bytes)
        self.assertEqual(self.state(), before)
        self.assertFalse((self.namespace / 'complete.json').exists())
        self.assertFalse((self.namespace / 'complete.tmp').exists())
        self.assertEqual(set(result.__dict__), {'snapshot_bytes', 'marker_bytes', 'bounds'})
        self.assertNotIn(b'verification', result.snapshot_bytes)
        self.assertNotIn(b'sparkShutdown', result.snapshot_bytes)

    def test_marker_byte_boundary_ascii_escaping_and_no_truncation(self):
        prefix = '"\\\n\x00é🙂'
        value = {'x': prefix}
        value['x'] += 'a' * (4096 - len(json.dumps(value, sort_keys=True).encode()))
        before = deepcopy(value)
        self.assertEqual(len(guard.marker_bytes_v1(value)), 4096)
        self.assertEqual(value, before)
        self.assertIn(b'\\ud83d\\ude42', guard.marker_bytes_v1(value))
        value['x'] += 'a'
        self.bad(guard.marker_bytes_v1, value, code='CONTENT_PROFILE_INVALID')

    def test_projected_capture_dominates_actual_completed_snapshot(self):
        result = self.guarded()
        # A completed serializer fixture, never passed to Spark qualification.
        (self.namespace / 'complete.json').write_bytes(result.marker_bytes)
        local = q.capture(self.artifact, self.binding)
        measured = guard._measure(self.artifact, self.marker, self.bronze, local)
        for name, size in measured.__dict__.items():
            self.assertLessEqual(size, getattr(result.bounds, name), name)
        self.assertEqual(measured.completion, result.bounds.completion)
        self.assertEqual(measured.selection, result.bounds.selection)
        self.assertEqual(measured.identity, result.bounds.identity)
        self.assertLess(measured.capture, result.bounds.capture)

    def test_entire_signed_stamp_grammar_is_reserved_and_size_index_is_actual(self):
        result = self.guarded()
        (self.namespace / 'complete.json').write_bytes(result.marker_bytes)
        local = q.capture(self.artifact, self.binding)
        self.assertEqual(len(guard.FUTURE_STAMP), 21)
        for entry in local['files']:
            entry['stamp'] = [guard.FUTURE_STAMP] * 7
            entry['stamp'][2] = str(entry['byteLength'])
        for directory in local['directories']:
            directory['stamp'] = [guard.FUTURE_STAMP] * 7
        for ancestor in local['guards']:
            ancestor['identity'] = [guard.FUTURE_STAMP] * 3
        self.assertEqual(guard._measure(self.artifact, self.marker, self.bronze, local), result.bounds)
        for entry in local['files']:
            entry['stamp'][2] = guard.FUTURE_STAMP
        self.bad(n.validate_silver_capture, local)

    def test_all_configured_whole_envelope_limits(self):
        result = self.guarded()
        limits = dict(completion=4096, identity=16384, capture=32768,
                      selection=36864, verification=53248, qualification=61440)
        self.assertEqual(limits, dict(completion=n.COMPLETION_LIMIT, identity=n.IDENTITY_LIMIT,
            capture=n.CAPTURE_LIMIT, selection=n.SELECTION_LIMIT,
            verification=n.VERIFICATION_LIMIT, qualification=n.QUALIFICATION_LIMIT))
        for name, limit in limits.items():
            self.assertLessEqual(getattr(result.bounds, name), limit)
            value = {'x': 'a' * (limit - 8)}
            self.assertEqual(len(n.canonical_envelope(value, limit)), limit)
            value['x'] += 'a'
            self.bad(n.canonical_envelope, value, limit)

    def test_maximum_counts_and_hidden_subtree_are_included(self):
        for dataset in ('accepted', 'rejected'):
            for index in range(1, 4):
                (self.namespace / dataset / f'part-{index}.parquet').write_bytes(b'x')
        hidden = self.namespace / '_hidden'
        hidden.mkdir()
        for index in range(15):
            (hidden / (f'.{index}-' + 'é"' * 70)).write_bytes(b'')
        result = self.guarded()
        snapshot = json.loads(result.snapshot_bytes)
        self.assertEqual(len(snapshot['files']) + 1, 24)
        self.assertEqual(len(snapshot['directories']), 4)
        self.assertEqual(snapshot['analyticalFileCounts'], dict(accepted=4, rejected=4))
        (self.namespace / '.extra').write_bytes(b'')
        before = self.state()
        self.bad(self.guarded)
        self.assertEqual(before, self.state())

    def test_fifth_analytical_file_is_rejected_before_any_completion(self):
        for index in range(1, 5):
            (self.namespace / 'accepted' / f'part-{index}.parquet').write_bytes(b'x')
        self.bad(self.guarded, code='CONTENT_PROFILE_INVALID')
        self.assertFalse((self.namespace / 'complete.tmp').exists())
        self.assertFalse((self.namespace / 'complete.json').exists())

    def test_fifth_directory_is_rejected(self):
        (self.namespace / '_first').mkdir()
        (self.namespace / '_second').mkdir()
        self.bad(self.guarded, code='CONTENT_PROFILE_INVALID')

    def test_real_legal_escaped_namespace_exceeds_configured_byte_caps(self):
        # POSIX permits these names, and each stays below NAME_MAX. JSON escapes
        # the control bytes and repeats names in complete inventory envelopes.
        for index in range(15):
            (self.namespace / (f'.{index}-' + '\x01"' * 120)).write_bytes(b'')
        snapshot = guard._capture_unfinished(self.artifact, self.binding)
        self.assertEqual(len(snapshot['files']) + 1, 18)
        self.assertEqual(len(snapshot['directories']), 3)
        self.assertEqual(snapshot['analyticalFileCounts'], dict(accepted=1, rejected=1))
        projected = deepcopy(snapshot)
        raw = guard.marker_bytes_v1(self.marker)
        projected['files'].append(dict(path='complete.json', dataset='bookkeeping',
            byteLength=len(raw), sha256=sha256(raw).hexdigest(), stamp=[]))
        identity, _ = q.identity(self.artifact, projected, self.bronze)
        self.assertGreater(len(canonical_bytes(identity)), n.IDENTITY_LIMIT)
        self.assertGreater(len(canonical_bytes(snapshot)), n.CAPTURE_LIMIT)
        before = self.state()
        self.bad(self.guarded, code='CONTENT_PROFILE_INVALID')
        self.assertEqual(before, self.state())
        self.assertFalse((self.namespace / 'complete.tmp').exists())
        self.assertFalse((self.namespace / 'complete.json').exists())

    def test_hidden_file_bytes_and_escaped_names_affect_projection(self):
        before = self.guarded().bounds
        (self.namespace / ('.' + 'é"\\' * 30)).write_bytes(b'a' * 20)
        # A backslash is forbidden by the existing envelope path grammar.
        self.bad(self.guarded, code='CONTENT_PROFILE_INVALID')
        (self.namespace / ('.' + 'é"\\' * 30)).unlink()
        (self.namespace / ('.' + 'é"' * 30)).write_bytes(b'a' * 20)
        after = self.guarded().bounds
        self.assertGreater(after.capture, before.capture)
        self.assertGreater(after.identity, before.identity)
        self.assertGreater(after.verification, before.verification)
        self.assertGreater(after.qualification, before.qualification)
        self.assertEqual(after.selection, before.selection)

    def test_wrong_marker_or_bronze_identity_is_rejected(self):
        self.marker['identity']['metadata_sha256'] = 'c' * 64
        self.bad(self.guarded, code='IDENTITY_MISMATCH')

    def test_projection_never_accepts_existing_marker_or_interrupted_tmp(self):
        for name in ('complete.tmp', 'complete.json'):
            path = self.namespace / name
            path.write_bytes(b'partial')
            before = self.state()
            self.bad(self.guarded, code='EVIDENCE_CHANGED')
            self.assertEqual(before, self.state())
            path.unlink()


class MutationAndReadOnlyTest(UnfinishedFixture):
    def test_file_mutation_after_guard(self):
        result = self.guarded()
        (self.namespace / 'accepted' / 'part-000.parquet').write_bytes(b'changed')
        self.bad(guard._recheck, self.artifact, self.binding, result, code='EVIDENCE_CHANGED')

    def test_added_hidden_file_after_guard(self):
        result = self.guarded()
        (self.namespace / '.extra').write_bytes(b'')
        self.bad(guard._recheck, self.artifact, self.binding, result, code='EVIDENCE_CHANGED')

    def test_replaced_ancestor_same_contents_after_guard(self):
        result = self.guarded()
        ancestor = self.namespace.parent
        moved = ancestor.with_name(ancestor.name + '-old')
        ancestor.rename(moved)
        ancestor.mkdir()
        (moved / self.namespace.name).rename(self.namespace)
        self.bad(guard._recheck, self.artifact, self.binding, result, code='EVIDENCE_CHANGED')

    def test_temporary_recheck_accepts_only_own_exact_bytes(self):
        result = self.guarded()
        temp = self.namespace / 'complete.tmp'
        temp.write_bytes(result.marker_bytes)
        snapshot = guard._recheck(self.artifact, self.binding, result, temporary=True)
        self.assertEqual(snapshot['temporary']['sha256'], sha256(result.marker_bytes).hexdigest())
        temp.write_bytes(result.marker_bytes + b' ')
        self.bad(guard._recheck, self.artifact, self.binding, result, temporary=True, code='EVIDENCE_CHANGED')

    def test_temporary_recheck_rejects_changed_data(self):
        result = self.guarded()
        (self.namespace / 'complete.tmp').write_bytes(result.marker_bytes)
        (self.namespace / 'rejected' / 'part-000.parquet').write_bytes(b'changed')
        self.bad(guard._recheck, self.artifact, self.binding, result, temporary=True, code='EVIDENCE_CHANGED')

    def test_symlink_file_and_ancestor_are_not_followed(self):
        path = self.namespace / 'accepted' / 'part-000.parquet'
        original = path.read_bytes()
        path.unlink()
        target = self.root / 'outside'
        target.write_bytes(original)
        path.symlink_to(target)
        self.bad(self.guarded, code='PATH_UNSAFE')
        path.unlink()
        path.write_bytes(original)
        ancestor = self.namespace.parent
        moved = ancestor.with_name(ancestor.name + '-original')
        ancestor.rename(moved)
        ancestor.symlink_to(moved, target_is_directory=True)
        self.bad(self.guarded, code='PATH_UNSAFE')

    def test_hardlinked_file_is_rejected(self):
        path = self.namespace / 'accepted' / 'part-000.parquet'
        (self.root / 'hardlink').hardlink_to(path)
        self.bad(self.guarded, code='PATH_UNSAFE')

    @unittest.skipUnless(hasattr(os, 'mkfifo'), 'POSIX FIFO refusal')
    def test_fifo_bookkeeping_is_refused_without_opening_it(self):
        # Sorted first: p.info rejects its type before os.open is reached.
        os.mkfifo(self.namespace / '.fifo')
        calls = []
        def observe(frame, event, function):
            if event == 'c_call' and function is os.open:
                calls.append(function)
        previous = sys.getprofile()
        sys.setprofile(observe)
        try:
            self.bad(self.guarded, code='PATH_UNSAFE')
        finally:
            sys.setprofile(previous)
        self.assertEqual(calls, [])

    def test_readonly_reconciliation_incomplete_never_publishes(self):
        before = self.state()
        self.bad(guard.reconcile_v1, self.request(), code='SILVER_INVALID')
        self.assertEqual(before, self.state())

    def test_readonly_reconciliation_completed_too_wide_unchanged(self):
        for index in range(1, 5):
            (self.namespace / 'accepted' / f'part-{index}.parquet').write_bytes(b'x')
        (self.namespace / 'complete.json').write_bytes(guard.marker_bytes_v1(self.marker))
        before = self.state()
        self.bad(guard.reconcile_v1, self.request(), code='CONTENT_PROFILE_INVALID')
        self.assertEqual(before, self.state())

    def test_io_denial_is_unavailable_not_absence_and_has_fixed_text(self):
        original = Path.lstat
        def denied(path, *args, **kwargs):
            if path == self.namespace:
                raise PermissionError('private source pathname must not escape')
            return original(path, *args, **kwargs)
        before = self.state()
        # Inject an OS failure only. No native helper or Spark module is replaced.
        with patch.object(Path, 'lstat', denied):
            self.bad(self.guarded, code='EVIDENCE_UNAVAILABLE')
            self.bad(guard.reconcile_v1, self.request(), code='EVIDENCE_UNAVAILABLE')
        self.assertEqual(before, self.state())


if __name__ == '__main__':
    unittest.main()
