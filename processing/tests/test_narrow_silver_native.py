"""Real native Spark equivalence/order qualification for the pinned processing CI.

Locally missing PySpark produces explicit skips, never fake-Spark validation.
One session and one small standalone publication are shared by this test class.
"""
from copy import deepcopy
from hashlib import sha256
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import artifact_import_verifier as p
import narrow_content_profile as n
import narrow_silver_precompletion as guard
from publication_metadata import canonical_bytes, fingerprint
import silver_import_qualification as q

HAS_SPARK = importlib.util.find_spec('pyspark') is not None


class Store:
    def __init__(self, objects):
        self.objects = deepcopy(objects)
        self.before_read = None

    def get_object(self, Bucket, Key):
        if self.before_read:
            self.before_read(Bucket, Key)
        return dict(Body=io.BytesIO(self.objects[(Bucket, Key)]))


@unittest.skipUnless(HAS_SPARK, 'Real PySpark 4.0.2 qualification runs in pinned processing CI')
class NarrowSilverNativeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import bronze_to_silver as native
        from silver import session
        cls.native = native
        cls.spark = session()
        cls.spark.sparkContext.setLogLevel('ERROR')
        cls.fixture = tempfile.TemporaryDirectory()
        cls.original = Path(cls.fixture.name).resolve() / 'original'
        cls.original.mkdir()
        source, ingestion = 'narrow-é', '50000000-0000-4000-8000-000000000001'
        cls.key = f'bronze/rss/source_id={native.encode(source)}/ingestion_date=2026-11-04/ingestion_id={ingestion}/metadata.json'
        payload = b'<rss version="2.0"><channel><item><guid>one</guid><title>One</title></item><item><title>No ID</title></item></channel></rss>'
        metadata = dict(schemaVersion=1, ingestionId=ingestion, sourceId=source,
            sourceUri='https://example.invalid/narrow', ingestedAt='2026-11-04T13:00:00.123456789Z',
            httpStatus=200, contentType='application/xml', declaredContentLength=None,
            payloadByteLength=len(payload), payloadSha256=sha256(payload).hexdigest(),
            payloadKey=cls.key.removesuffix('metadata.json') + 'payload.xml')
        cls.objects = {('local', cls.key): json.dumps(metadata).encode(), ('local', metadata['payloadKey']): payload}
        cls.artifact = dict(kind='SILVER', productId='content-intelligence', storageBindingId='silver',
            relativeAddress='v1/' + sha256(b'local').hexdigest() + '/' + cls.key[len('bronze/rss/'):].removesuffix('metadata.json') + 'complete.json',
            contractVersion=1, processorRevision=1,
            identity=dict(sourceId=source, ingestionId=ingestion, bronzeStorageBindingId='bronze',
                bronzeBucket='local', bronzeMetadataKey=cls.key, bronzePayloadKey=metadata['payloadKey']))
        cls.original_result = native.process_one(cls.spark, Store(cls.objects), 'local', cls.key, cls.original)
        empty = Path(cls.fixture.name).resolve() / 'empty-accepted'
        cls.spark.createDataFrame([], native.SILVER_SCHEMA).coalesce(1).write.parquet(str(empty))
        cls.empty_accepted_part = next(empty.glob('*.parquet'))

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.fixture.cleanup()

    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.base = Path(temp.name).resolve()
        self.root = self.base / 'silver'
        self.root.mkdir()
        value = self.root.stat()
        self.binding = dict(root=str(self.root), rootDevice=str(value.st_dev), rootInode=str(value.st_ino))
        self.store = Store(self.objects)
        self.namespace = (self.root / self.artifact['relativeAddress']).parent

    def publish(self):
        return guard.process_one_v1(self.spark, self.store, 'local', self.key, self.root,
                                    artifact=self.artifact, binding=self.binding)

    def trace(self, observer, function):
        previous = sys.getprofile()
        sys.setprofile(observer)
        try:
            return function()
        finally:
            sys.setprofile(previous)

    def state(self):
        return {str(path.relative_to(self.root)): (p.stamp(path.lstat()),
                    path.read_bytes() if path.is_file() else None)
                for path in [self.root, *self.root.rglob('*')]}

    def assert_failure(self, code, function):
        with self.assertRaises((p.Failure, n.ProfileError)) as caught:
            function()
        self.assertEqual(str(caught.exception), code)

    def test_native_equivalence_exact_marker_guard_order_projection_and_replay(self):
        import silver_inputs as native_inputs
        events, projected = [], []
        def observe(frame, event, result):
            if frame.f_code is guard.precompletion_guard_v1.__wrapped__.__code__:
                if event == 'call':
                    self.assertFalse((self.namespace / 'complete.tmp').exists())
                    self.assertFalse((self.namespace / 'complete.json').exists())
                    events.append('guard')
                elif event == 'return':
                    projected.append(result.bounds)
            if frame.f_code is q.qualify.__code__ and event == 'call':
                self.assertTrue((self.namespace / 'complete.json').is_file())
                events.append('native qualification')
        self.assertEqual(self.trace(observe, self.publish), self.original_result)
        self.assertEqual(events, ['guard', 'native qualification'])
        original_namespace = (self.original / self.artifact['relativeAddress']).parent
        marker = self.namespace / 'complete.json'
        self.assertEqual(marker.read_bytes(), (original_namespace / 'complete.json').read_bytes())
        for dataset in ('accepted', 'rejected'):
            left = self.spark.read.parquet(str(original_namespace / dataset))
            right = self.spark.read.parquet(str(self.namespace / dataset))
            self.assertEqual(left.schema, right.schema)
            self.assertEqual(left.exceptAll(right).count(), 0)
            self.assertEqual(right.exceptAll(left).count(), 0)
        local = q.capture(self.artifact, self.binding)
        parsed = json.loads(marker.read_bytes())
        bronze, _ = q.bronze_read(self.artifact, None, parsed, self.store)
        actual = guard._measure(self.artifact, parsed, bronze, local)
        for name, size in actual.__dict__.items():
            self.assertLessEqual(size, getattr(projected[0], name))
        selected = native_inputs.load_inputs(self.spark, self.root, markers=[self.artifact['relativeAddress']])
        self.assertEqual(guard._native_selection(self.artifact, parsed, local),
                         dict(manifest=selected.manifest, inputSetFingerprint=selected.input_set_fingerprint))
        before = self.state()
        self.assertEqual(self.publish(), dict(self.original_result, status='SKIPPED'))
        self.assertEqual(before, self.state())

    def test_atom_accepted_only_rss_rejected_only_and_native_empty_feed(self):
        cases = [
            ('atom', b'<feed xmlns="http://www.w3.org/2005/Atom"><entry><id>one</id><title>One</title></entry></feed>',
             dict(accepted=1, rejected=0)),
            ('rejected-rss', b'<rss version="2.0"><channel><item><guid>one</guid></item></channel></rss>',
             dict(accepted=0, rejected=1)),
            # Native posexplode_outer intentionally emits NO_SUPPORTED_ENTRIES,
            # so an empty feed is 0/1, never a fabricated both-empty 0/0 result.
            ('empty-rss', b'<rss version="2.0"><channel/></rss>', dict(accepted=0, rejected=1)),
        ]
        for index, (name, payload, counts) in enumerate(cases, 2):
            with self.subTest(feed=name):
                source = 'narrow-' + name
                ingestion = f'50000000-0000-4000-8000-{index:012d}'
                key = f'bronze/rss/source_id={self.native.encode(source)}/ingestion_date=2026-11-04/ingestion_id={ingestion}/metadata.json'
                metadata = dict(schemaVersion=1, ingestionId=ingestion, sourceId=source,
                    sourceUri='https://example.invalid/' + name, ingestedAt='2026-11-04T13:00:00Z',
                    httpStatus=200, contentType='application/xml', declaredContentLength=None,
                    payloadByteLength=len(payload), payloadSha256=sha256(payload).hexdigest(),
                    payloadKey=key.removesuffix('metadata.json') + 'payload.xml')
                raw = json.dumps(metadata).encode()
                store = Store({('local', key): raw, ('local', metadata['payloadKey']): payload})
                artifact = deepcopy(self.artifact)
                artifact['relativeAddress'] = ('v1/' + sha256(b'local').hexdigest() + '/' +
                    key[len('bronze/rss/'):].removesuffix('metadata.json') + 'complete.json')
                artifact['identity'].update(sourceId=source, ingestionId=ingestion,
                    bronzeMetadataKey=key, bronzePayloadKey=metadata['payloadKey'])
                native_results = []
                def observe(frame, event, result):
                    if frame.f_code is q.qualify.__code__ and event == 'return':
                        native_results.append(result)
                result = self.trace(observe, lambda: guard.process_one_v1(self.spark, store,
                    'local', key, self.root, artifact=artifact, binding=self.binding))
                self.assertEqual(result, dict(status='PROCESSED', **counts))
                self.assertEqual(len(native_results), 1)
                self.assertEqual(native_results[0]['verifier'], 'silver_inputs.load_inputs')
                self.assertEqual(native_results[0]['counts'], counts)
                marker_path = self.root / artifact['relativeAddress']
                expected_marker = dict(identity=dict(metadata_sha256=sha256(raw).hexdigest(),
                    payload_sha256=metadata['payloadSha256'], contract_version=1, processor_revision=1,
                    bucket='local', metadata_key=key), counts=counts)
                self.assertEqual(marker_path.read_bytes(), json.dumps(expected_marker, sort_keys=True).encode())
                local = q.capture(artifact, self.binding)
                for dataset in ('accepted', 'rejected'):
                    self.assertGreaterEqual(local['analyticalFileCounts'][dataset], 1)
                    frame = self.spark.read.parquet(str(marker_path.parent / dataset))
                    self.assertEqual(frame.count(), counts[dataset])
                if name == 'empty-rss':
                    reasons = self.spark.read.parquet(str(marker_path.parent / 'rejected')).first().rejection_reasons
                    self.assertIn('NO_SUPPORTED_ENTRIES', reasons)

    def test_guard_rejection_precedes_tmp_and_incomplete_retry_stays_blocked(self):
        injected = []
        def observe(frame, event, result):
            if frame.f_code is self.native.verify_dataset.__code__ and event == 'return' and not injected:
                for index in range(24):
                    (self.namespace / f'.extra-{index}').write_bytes(b'')
                injected.append(True)
        self.assert_failure('CONTENT_PROFILE_INVALID', lambda: self.trace(observe, self.publish))
        self.assertFalse((self.namespace / 'complete.tmp').exists())
        self.assertFalse((self.namespace / 'complete.json').exists())
        before = self.state()
        self.assert_failure('INCOMPLETE_OUTPUT', self.publish)
        self.assertEqual(before, self.state())

    def test_data_mutation_after_guard_cannot_publish(self):
        def observe(frame, event, result):
            if frame.f_code is guard.precompletion_guard_v1.__wrapped__.__code__ and event == 'return':
                next((self.namespace / 'accepted').glob('*.parquet')).write_bytes(b'changed')
        self.assert_failure('EVIDENCE_CHANGED', lambda: self.trace(observe, self.publish))
        self.assertFalse((self.namespace / 'complete.tmp').exists())
        self.assertFalse((self.namespace / 'complete.json').exists())

    def test_bronze_mutation_after_guard_cannot_publish(self):
        def observe(frame, event, result):
            if frame.f_code is guard.precompletion_guard_v1.__wrapped__.__code__ and event == 'return':
                self.store.objects[('local', self.artifact['identity']['bronzePayloadKey'])] += b' '
        self.assert_failure('BRONZE_CHANGED', lambda: self.trace(observe, self.publish))
        self.assertFalse((self.namespace / 'complete.tmp').exists())
        self.assertFalse((self.namespace / 'complete.json').exists())

    def test_change_during_final_bronze_read_is_detected_before_rename(self):
        changed = []
        def before_read(bucket, key):
            if (self.namespace / 'complete.tmp').exists() and not changed:
                next((self.namespace / 'accepted').glob('*.parquet')).write_bytes(b'changed')
                changed.append(True)
        self.store.before_read = before_read
        self.assert_failure('EVIDENCE_CHANGED', self.publish)
        self.assertEqual(changed, [True])
        self.assertTrue((self.namespace / 'complete.tmp').is_file())
        self.assertFalse((self.namespace / 'complete.json').exists())
        before = self.state()
        self.assert_failure('INCOMPLETE_OUTPUT', self.publish)
        self.assertEqual(before, self.state())

    def test_rename_io_failure_retains_exact_tmp_without_completion(self):
        def observe(frame, event, result):
            if frame.f_code is Path.rename.__code__ and event == 'call' and frame.f_locals['self'].name == 'complete.tmp':
                raise PermissionError('private-path fault before rename')
        self.assert_failure('EVIDENCE_UNAVAILABLE', lambda: self.trace(observe, self.publish))
        self.assertTrue((self.namespace / 'complete.tmp').is_file())
        self.assertFalse((self.namespace / 'complete.json').exists())
        self.assertEqual(json.loads((self.namespace / 'complete.tmp').read_bytes())['counts'], dict(accepted=1, rejected=1))

    def test_namespace_replacement_after_first_write_blocks_second_write(self):
        from pyspark.sql.readwriter import DataFrameWriter
        moved = self.namespace.with_name(self.namespace.name + '-saved')
        changed = []
        def observe(frame, event, result):
            if (frame.f_code is DataFrameWriter.parquet.__code__ and event == 'return' and
                    Path(frame.f_locals['path']) == self.namespace / 'accepted' and not changed):
                self.namespace.rename(moved)
                self.namespace.mkdir()
                changed.append(True)
        self.assert_failure('EVIDENCE_CHANGED', lambda: self.trace(observe, self.publish))
        self.assertEqual(changed, [True])
        self.assertTrue((moved / 'accepted').is_dir())
        self.assertFalse((self.namespace / 'rejected').exists())
        self.assertFalse((self.namespace / 'complete.tmp').exists())
        self.assertFalse((self.namespace / 'complete.json').exists())

    def test_completed_standalone_too_wide_is_rejected_unchanged(self):
        import silver_inputs as native_inputs
        shutil.copytree(self.original, self.root, dirs_exist_ok=True)
        for index in range(5):
            # Real schema-correct empty Parquet increases physical file count
            # without changing any native rows, provenance or marker counts.
            shutil.copyfile(self.empty_accepted_part, self.namespace / 'accepted' / f'extra-{index}.parquet')
        before = self.state()
        selected = native_inputs.load_inputs(self.spark, self.root, markers=[self.artifact['relativeAddress']])
        self.assertEqual(selected.counts['accepted'], self.original_result['accepted'])
        self.assertEqual(selected.counts['rejected'], self.original_result['rejected'])
        self.assertGreater(len(selected.manifest['inputs'][0]['files']['accepted']), 4)
        selected.verify_unchanged()
        self.assertEqual(before, self.state())
        self.assert_failure('CONTENT_PROFILE_INVALID', self.publish)
        self.assertEqual(before, self.state())

    def test_after_rename_ack_loss_reconciles_actual_evidence_readonly(self):
        from silver_import_fixtures import serve
        renamed = []
        def observe(frame, event, result):
            if frame.f_code is Path.rename.__code__ and event == 'return' and frame.f_locals['self'].name == 'complete.tmp':
                renamed.append(True)
                self.assertTrue((self.namespace / 'complete.json').is_file())
                self.assertFalse((self.namespace / 'complete.tmp').exists())
                raise OSError('acknowledgement lost after successful rename')
        self.assert_failure('EVIDENCE_UNAVAILABLE', lambda: self.trace(observe, self.publish))
        self.assertEqual(renamed, [True])
        # Do not retry the publisher. Actual native verification and RECHECK
        # inspect the retained completed publication read-only.
        scratch = self.base / 'scratch'
        scratch.mkdir(mode=0o700)
        value = scratch.stat()
        before = self.state()
        with serve(self.store) as configuration:
            request = dict(protocolVersion=2, operationId='50000000-0000-4000-8000-000000000003',
                selectionSha256=sha256(canonical_bytes(self.artifact)).hexdigest(), mode='VERIFY',
                artifact=self.artifact, binding=self.binding, expectedCapture=None, bronze=configuration,
                runtime=dict(javaHome=str(Path(os.environ['JAVA_HOME']).resolve()), scratchRoot=str(scratch),
                             scratchDevice=str(value.st_dev), scratchInode=str(value.st_ino)))
            p.validate(request)
            p.fields(request['runtime'], ('javaHome', 'scratchRoot', 'scratchDevice', 'scratchInode'))
            code = ('import sys,json;sys.path.insert(0,sys.argv[1]);'
                    'import narrow_silver_precompletion as g;'
                    'from publication_metadata import canonical_bytes;'
                    'r=json.load(sys.stdin);s,e=g.reconcile_v1(r);'
                    'r.update(mode="RECHECK",expectedCapture=e["capture"]);'
                    's2,e2=g.reconcile_v1(r);'
                    'sys.stdout.buffer.write(canonical_bytes(dict(status=s,evidence=e,recheck=s2,recheckEvidence=e2)))')
            result = subprocess.run([sys.executable, '-I', '-B', '-c', code, str(Path(guard.__file__).parent)],
                input=canonical_bytes(request), capture_output=True, timeout=180,
                env=dict(PATH='/usr/bin:/bin', LANG='C.UTF-8', TZ='UTC'))
            self.assertEqual(result.returncode, 0, result.stdout.decode() + result.stderr.decode()[-3000:])
            value = json.loads(result.stdout)
            self.assertEqual(value['status'], 'VERIFIED')
            self.assertEqual(value['evidence']['native']['sparkShutdown'], 'CONFIRMED')
            self.assertEqual(value['recheck'], 'UNCHANGED')
            self.assertIsNone(value['recheckEvidence'])
            n.validate_silver_qualification(dict(qualificationVersion=1, selection=self.artifact,
                                                verification=value['evidence']))
            self.assertEqual(self.store.writes, [])
        self.assertEqual(before, self.state())


if __name__ == '__main__':
    unittest.main()
