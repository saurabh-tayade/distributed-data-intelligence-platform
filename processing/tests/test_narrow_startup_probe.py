"""Bounded startup gate tests; disposable fixtures, no native dispatch."""
from dataclasses import replace
from io import StringIO
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import unittest
from unittest.mock import patch
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import narrow_evidence_sink as sink
import narrow_startup_probe as probe
from narrow_transport_json import canonical_envelope
import test_narrow_evidence_sink as fixtures
from test_narrow_evidence_sink import snapshot


class NarrowStartupProbeTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.NarrowEvidenceSinkTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        expected = self.fixture.expected
        writer = replace(expected.writer, stage='CHECK_INPUTS', ordinal=0,
                         checkpoint_id=sink.checkpoint_id(expected.run_id,
                                                        expected.original_attempt_id, 'CHECK_INPUTS', 0))
        self.expected = replace(expected, writer=writer)
        directory = self.fixture.root / sink._address(self.expected, 'CHECK_INPUTS', 0)
        directory.mkdir(parents=True, mode=0o700)
        directory.parent.chmod(0o700)
        self.fixture.file(directory / 'command.json', b'{}')
        e, w, f = self.expected, writer, self.expected.frozen
        self.value = dict(bootstrapVersion=1, mode='READ_ONLY_PROBE', operationId=str(uuid4()),
                          installationId=e.installation_id, policy=e.policy, planKind=e.plan_kind,
                          runId=e.run_id, attemptId=e.original_attempt_id, sourceId=e.source_id,
                          controlRoot=str(e.control_root), ledgerSha256=e.ledger_sha256,
                          selectionSha256=f.selection_sha256, profileSha256=f.profile_sha256,
                          runtimeSha256=f.runtime_sha256, workerInstanceId=w.worker_instance_id,
                          ownerEpoch=str(w.epoch), stage=w.stage, ordinal=w.ordinal,
                          checkpointId=w.checkpoint_id, commandSha256=w.command_sha256,
                          childPid=str(os.getpid()))
        for prefix, identity in [('control', e.control_identity), ('root', e.root_identity),
                                 ('lock', e.lock_identity)]:
            self.value[prefix + 'Device'] = str(identity.device)
            self.value[prefix + 'Inode'] = str(identity.inode)

    def encoded(self, value=None):
        return canonical_envelope(self.value if value is None else value, probe.BOOTSTRAP_LIMIT)

    def pipe(self, raw, *, close=True):
        read, write = os.pipe()
        self.addCleanup(os.close, read)
        os.write(write, raw)
        if close:
            os.close(write)
        else:
            self.addCleanup(os.close, write)
        return read

    def test_valid_bootstrap_has_separate_parent_correspondence_objects(self):
        expected, current = probe.parse_bootstrap(self.encoded())
        self.assertEqual(self.expected, expected)
        self.assertEqual(expected.writer, current.writer)
        self.assertIsNot(expected.writer, current.writer)
        self.assertIsNot(expected.frozen, current.frozen)

    def test_success_reads_existing_command_without_mutation(self):
        before = snapshot(self.fixture.control)
        probe.run_probe(self.pipe(self.encoded()))
        self.assertEqual(before, snapshot(self.fixture.control))

    def test_valid_prefix_withheld_eof_times_out_without_sink_or_path_reads(self):
        fd = self.pipe(self.encoded(), close=False)
        with patch.object(probe, 'BOOTSTRAP_TIMEOUT_SECONDS', 0.08), \
                patch.object(probe, 'StrictSink') as strict, patch.object(Path, 'lstat') as lstat:
            start = time.monotonic()
            with self.assertRaises(probe.StartupError):
                probe.run_probe(fd)
            self.assertLess(time.monotonic() - start, 1)
            strict.assert_not_called()
            lstat.assert_not_called()

    def test_parent_pipe_close_with_empty_or_incomplete_message_does_no_work(self):
        for raw in (b'', self.encoded()[:-1]):
            with self.subTest(raw_length=len(raw)), patch.object(probe, 'StrictSink') as strict:
                with self.assertRaises(probe.StartupError):
                    probe.run_probe(self.pipe(raw))
                strict.assert_not_called()

    def test_malformed_frames_and_schema_never_construct_sink(self):
        good = self.encoded()
        invalid = [b' ' + good, good + b'\n', good + good, good[:-1] + b',"mode":"READ_ONLY_PROBE"}',
                   b'\xef\xbb\xbf' + good, b'\xff', b'[]', b'{', b'null']
        for key in self.value:
            value = dict(self.value)
            del value[key]
            invalid.append(self.encoded(value))
        changes = {'extra': 'forbidden', 'bootstrapVersion': True, 'ordinal': False,
                   'mode': 'PUBLISH', 'stage': 'PUBLISH_CONTENT', 'planKind': 'RETRY',
                   'operationId': 'not-a-uuid', 'childPid': str(os.getpid() + 1),
                   'ownerEpoch': str(2**63), 'policy': 'legacy', 'controlRoot': '/tmp/../other',
                   'checkpointId': str(uuid4()), 'sourceId': '../source', 'installationId': 'bad',
                   'commandSha256': 'A' * 64, 'rootInode': '0'}
        for key, replacement in changes.items():
            invalid.append(self.encoded(dict(self.value, **{key: replacement})))
        for key in ('childPid', 'ownerEpoch', 'controlDevice', 'controlInode', 'rootDevice',
                    'rootInode', 'lockDevice', 'lockInode'):
            for replacement in (1, '01', '-1', '+1', '1.0', '1' * 21):
                invalid.append(self.encoded(dict(self.value, **{key: replacement})))
        for root in ('relative', '/tmp/', '/tmp//x', '/tmp/./x', '//tmp', '/tmp\x00x'):
            invalid.append(self.encoded(dict(self.value, controlRoot=root)))
        for raw in invalid:
            with self.subTest(raw=raw[:50]), patch.object(probe, 'StrictSink') as strict:
                with self.assertRaises(probe.StartupError):
                    probe.run_probe(self.pipe(raw))
                strict.assert_not_called()

    def test_total_read_never_exceeds_64k_even_with_oversize_stream(self):
        for length in (probe.BOOTSTRAP_LIMIT, probe.BOOTSTRAP_LIMIT + 100):
            read, write = os.pipe()
            def feed():
                try:
                    offset = 0
                    raw = b'x' * length
                    while offset < len(raw):
                        offset += os.write(write, raw[offset:])
                except BrokenPipeError:
                    pass
                finally:
                    os.close(write)
            thread = threading.Thread(target=feed)
            thread.start()
            actual_read = os.read
            total = 0
            def counted(fd, count):
                nonlocal total
                chunk = actual_read(fd, count)
                total += len(chunk)
                return chunk
            try:
                with patch.object(probe.os, 'read', side_effect=counted), \
                        patch.object(probe, 'StrictSink') as strict:
                    with self.assertRaises(probe.StartupError):
                        probe.run_probe(read)
                    self.assertEqual(probe.BOOTSTRAP_LIMIT, total)
                    strict.assert_not_called()
            finally:
                os.close(read)
                thread.join(2)
                self.assertFalse(thread.is_alive())

    def test_regular_file_is_not_a_bootstrap_pipe(self):
        with open(__file__, 'rb') as stream:
            with self.assertRaises(probe.StartupError):
                probe.run_probe(stream.fileno())

    def child(self, incomplete=False):
        child = subprocess.Popen([sys.executable, str(Path(probe.__file__))], stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        raw = self.encoded(dict(self.value, childPid=str(child.pid)))
        try:
            return child, child.communicate(raw[:-1] if incomplete else raw, timeout=10)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()

    def test_fresh_child_success_is_silent_and_incomplete_parent_close_is_safe(self):
        before = snapshot(self.fixture.control)
        child, output = self.child()
        self.assertEqual(0, child.returncode)
        self.assertEqual((b'', b''), output)
        child, output = self.child(incomplete=True)
        self.assertEqual(1, child.returncode)
        self.assertEqual((b'', (probe.ERROR + '\n').encode()), output)
        self.assertEqual(before, snapshot(self.fixture.control))

    def test_fresh_child_with_valid_prefix_but_no_eof_hits_fixed_timeout(self):
        child = subprocess.Popen([sys.executable, str(Path(probe.__file__))], stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            raw = self.encoded(dict(self.value, childPid=str(child.pid)))
            before = snapshot(self.fixture.control)
            start = time.monotonic()
            child.stdin.write(raw)
            child.stdin.flush()
            # Keep the parent writer open: even a complete valid JSON value is
            # not authorization to inspect the attempt until EOF is observed.
            child.wait(timeout=8)
            self.assertLess(time.monotonic() - start, 7)
            self.assertEqual(1, child.returncode)
            self.assertEqual(b'', child.stdout.read())
            self.assertEqual((probe.ERROR + '\n').encode(), child.stderr.read())
            self.assertEqual(before, snapshot(self.fixture.control))
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()
            child.stdin.close()
            child.stdout.close()
            child.stderr.close()

    def test_failure_diagnostics_are_fixed_and_cli_has_no_caller_arguments(self):
        with patch.object(sys, 'argv', ['probe', '/secret-path']), \
                patch.object(sys, 'stderr', StringIO()) as error, \
                patch.object(probe, 'run_probe') as run:
            self.assertEqual(1, probe.main())
            self.assertEqual(probe.ERROR + '\n', error.getvalue())
            run.assert_not_called()


if __name__ == '__main__':
    unittest.main()
